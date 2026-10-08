/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test.tcm;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.ClusterUtils.SerializablePredicate;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareMove;

import static org.apache.cassandra.distributed.impl.TestEndpointCache.toCassandraInetAddressAndPort;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseCommits;
import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;

/**
 * TCM deep review 2026-07, Track D, finding TEST-2.
 *
 * Kills a node's process (via {@link ClusterUtils#stopAbrupt}, i.e. kill -9 semantics: all messages to/from the
 * instance are blocked and then the instance is shut down) while it is the *acting* node of an in-flight,
 * multi-step TCM sequence (BootstrapAndJoin / Move / UnbootstrapAndLeave), at each of that sequence's first three
 * commit points (Prepare*, Start*, Mid*). The Finish* step is deliberately excluded: it is already covered
 * (using a live-JVM pause, not a real kill) by the ConsistentBootstrapTest/ConsistentLeaveTest/ConsistentMoveTest
 * family, and is the one step after which the sequence is actually complete rather than abandoned.
 *
 * For each of the resulting 9 cells, a surviving peer (never the CMS node, never the killed node) is used to:
 *   1. Force-convict the dead node in the survivor's local failure detector (mirrors what an operator/monitoring
 *      system would observe once the process is actually gone; avoids waiting out phi-accrual in the test).
 *   2. Invoke the one operator recovery path that trunk actually exposes for each sequence kind:
 *        - JOIN:  nodetool abortbootstrap --ip <address>
 *        - MOVE:  nodetool abortmove      --node <nodeId>
 *        - LEAVE: nodetool abortdecommission --node <nodeId>
 *   3. Assert that the dead node's entry in ClusterMetadata.current().inProgressSequences is gone AND that
 *      ClusterMetadata.current().lockedRanges is empty again, both observed from the surviving peer.
 *
 * A cell where recovery command fails, throws, times out, or leaves an un-releasable lock / dangling
 * in-progress-sequence entry is expected to happen for some cells - that is a FINDING (see D1-findings/), not a
 * harness bug. See tcm-review/phase2/trackD/D1-notes.md for the full 9-cell matrix and results, and
 * tcm-review/verify/TEST-2.md for the coverage-gap analysis this test closes.
 */
public class KillMidSequenceTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(KillMidSequenceTest.class);
    private static final int TEST_TIMEOUT_MS = 6 * 60 * 1000;
    private static final String KEYSPACE = "killmidseq";

    private enum Point { PREPARE, START, MID }

    // ============================================================ JOIN (BootstrapAndJoin) ==

    @Test(timeout = TEST_TIMEOUT_MS)
    public void joinKillAtPrepareJoin() throws Throwable { runJoinCell(Point.PREPARE); }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void joinKillAtStartJoin() throws Throwable { runJoinCell(Point.START); }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void joinKillAtMidJoin() throws Throwable { runJoinCell(Point.MID); }

    // ============================================================ MOVE ======================

    @Test(timeout = TEST_TIMEOUT_MS)
    public void moveKillAtPrepareMove() throws Throwable { runMoveCell(Point.PREPARE); }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void moveKillAtStartMove() throws Throwable { runMoveCell(Point.START); }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void moveKillAtMidMove() throws Throwable { runMoveCell(Point.MID); }

    // ============================================================ LEAVE (UnbootstrapAndLeave) =

    @Test(timeout = TEST_TIMEOUT_MS)
    public void leaveKillAtPrepareLeave() throws Throwable { runLeaveCell(Point.PREPARE); }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void leaveKillAtStartLeave() throws Throwable { runLeaveCell(Point.START); }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void leaveKillAtMidLeave() throws Throwable { runLeaveCell(Point.MID); }

    // ============================================================ JOIN implementation ========

    private void runJoinCell(Point point) throws Throwable
    {
        String cellId = "JOIN/" + point;
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                        .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                                    .set("write_request_timeout", "10s"))
                                        .start())
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            IInvokableInstance survivor = cluster.get(2);
            waitForCMSToQuiesce(cluster, cmsInstance);

            cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};", KEYSPACE));
            cluster.schemaChange(String.format("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int)", KEYSPACE));
            waitForCMSToQuiesce(cluster, cmsInstance);

            IInstanceConfig joiningConfig = cluster.newInstanceConfig()
                                                   .set("auto_bootstrap", true)
                                                   .set(Constants.KEY_DTEST_FULL_STARTUP, true)
                                                   // We deliberately interrupt this node's startup() call mid-flight
                                                   // (see interruptThenKill). Without this, AbstractCluster$Wrapper's
                                                   // default failed-startup handling marks the Wrapper "isShutdown"
                                                   // purely as bookkeeping *without actually tearing down the
                                                   // delegate's live threads*, which makes the subsequent real
                                                   // stopAbrupt/shutdown() a no-op (it throws "Instance is not
                                                   // running") and leaks every thread the partially-started instance
                                                   // had already spun up. Setting this to false keeps the Wrapper
                                                   // reporting "running" after the interrupt-induced failure so our
                                                   // stopAbrupt call actually reaches delegate.shutdown() and tears
                                                   // the node down for real - the documented pairing for
                                                   // KEY_DTEST_STARTUP_TIMEOUT in Constants.java recommends exactly
                                                   // this for "startup still needs to cleanup" scenarios.
                                                   .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false);
            IInvokableInstance joining = cluster.bootstrap(joiningConfig);
            InetSocketAddress joiningAddr = joining.config().broadcastAddress();
            String joiningIp = joiningAddr.getAddress().getHostAddress();

            logger.info("KILLMIDSEQ [{}] pausing CMS commit, then killing joining node {}", cellId, joiningIp);
            Callable<Epoch> pending = pauseBeforeCommit(cmsInstance, joinPredicate(point));
            Thread driver = asyncRun(joining::startup);
            pending.call();

            // Capture the NodeId while the joining node is still REGISTERED in ClusterMetadata. abortbootstrap's
            // recovery path (StorageService.abortBootstrap) commits an Unregister at the end, which erases the
            // node's directory entry entirely - a getNodeId() lookup done *after* that returns null and NPEs.
            int nodeIntId = ClusterUtils.getNodeId(joining, survivor);

            interruptThenKill(cluster, joining, driver);
            logger.info("KILLMIDSEQ [{}] joining node killed while CMS paused before {}", cellId, point);

            unpauseCommits(cmsInstance);

            boolean quiesced = tryQuiesce(cluster, cmsInstance, joining.config().num());
            logger.info("KILLMIDSEQ [{}] post-kill quiesce success={}", cellId, quiesced);

            forceConvict(survivor, joiningAddr);
            forceConvict(cmsInstance, joiningAddr);

            NodeToolResult result = survivor.nodetoolResult("abortbootstrap", "--ip", joiningIp);
            logger.info("KILLMIDSEQ [{}] abortbootstrap rc={} stdout=[{}] stderr=[{}] error=[{}]",
                        cellId, result.getRc(), result.getStdout(), result.getStderr(), result.getError());

            waitAndAssertRecovered(cellId, survivor, nodeIntId, result);
        }
    }

    private static SerializablePredicate<Transformation> joinPredicate(Point point)
    {
        switch (point)
        {
            case PREPARE: return (e) -> e instanceof PrepareJoin;
            case START:   return (e) -> e instanceof PrepareJoin.StartJoin;
            case MID:     return (e) -> e instanceof PrepareJoin.MidJoin;
            default: throw new IllegalArgumentException(point.toString());
        }
    }

    // ============================================================ MOVE implementation ========

    private void runMoveCell(Point point) throws Throwable
    {
        String cellId = "MOVE/" + point;
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                        .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                                    .set("write_request_timeout", "10s"))
                                        .start())
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            IInvokableInstance movingInstance = cluster.get(2);
            IInvokableInstance survivor = cluster.get(3);
            waitForCMSToQuiesce(cluster, cmsInstance);

            cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};", KEYSPACE));
            cluster.schemaChange(String.format("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int)", KEYSPACE));
            waitForCMSToQuiesce(cluster, cmsInstance);

            InetSocketAddress movingAddr = movingInstance.config().broadcastAddress();

            // Pick a target token that doesn't collide with an existing owner: current token - 1 (single-token,
            // Murmur3Partitioner setup via evenlyDistributedTokens, so this is always safely off any other owner).
            long currentToken = Long.parseLong(ClusterUtils.getLocalTokens(movingInstance).iterator().next());
            long newToken = currentToken - 1;

            logger.info("KILLMIDSEQ [{}] pausing CMS commit, then killing moving node {} (target token {})", cellId, movingAddr, newToken);
            Callable<Epoch> pending = pauseBeforeCommit(cmsInstance, movePredicate(point));
            Thread driver = asyncRun(() -> movingInstance.acceptsOnInstance((Long token) -> StorageService.instance.move(Long.toString(token)))
                                                          .accept(newToken));
            pending.call();

            int nodeIntId = ClusterUtils.getNodeId(movingInstance, survivor);

            interruptThenKill(cluster, movingInstance, driver);
            logger.info("KILLMIDSEQ [{}] moving node killed while CMS paused before {}", cellId, point);

            unpauseCommits(cmsInstance);

            boolean quiesced = tryQuiesce(cluster, cmsInstance, movingInstance.config().num());
            logger.info("KILLMIDSEQ [{}] post-kill quiesce success={}", cellId, quiesced);

            forceConvict(survivor, movingAddr);
            forceConvict(cmsInstance, movingAddr);

            String uuid = uuidOf(nodeIntId, survivor);

            NodeToolResult result = survivor.nodetoolResult("abortmove", "--node", uuid);
            logger.info("KILLMIDSEQ [{}] abortmove rc={} stdout=[{}] stderr=[{}] error=[{}]",
                        cellId, result.getRc(), result.getStdout(), result.getStderr(), result.getError());

            waitAndAssertRecovered(cellId, survivor, nodeIntId, result);
        }
    }

    private static SerializablePredicate<Transformation> movePredicate(Point point)
    {
        switch (point)
        {
            case PREPARE: return (e) -> e instanceof PrepareMove;
            case START:   return (e) -> e instanceof PrepareMove.StartMove;
            case MID:     return (e) -> e instanceof PrepareMove.MidMove;
            default: throw new IllegalArgumentException(point.toString());
        }
    }

    // ============================================================ LEAVE implementation =======

    private void runLeaveCell(Point point) throws Throwable
    {
        String cellId = "LEAVE/" + point;
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                        .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                                    .set("write_request_timeout", "10s"))
                                        .start())
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            IInvokableInstance leavingInstance = cluster.get(2);
            IInvokableInstance survivor = cluster.get(3);
            waitForCMSToQuiesce(cluster, cmsInstance);

            cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};", KEYSPACE));
            cluster.schemaChange(String.format("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int)", KEYSPACE));
            waitForCMSToQuiesce(cluster, cmsInstance);

            InetSocketAddress leavingAddr = leavingInstance.config().broadcastAddress();

            logger.info("KILLMIDSEQ [{}] pausing CMS commit, then killing leaving node {}", cellId, leavingAddr);
            Callable<Epoch> pending = pauseBeforeCommit(cmsInstance, leavePredicate(point));
            Thread driver = asyncRun(() -> leavingInstance.runOnInstance(() -> StorageService.instance.decommission(true)));
            pending.call();

            int nodeIntId = ClusterUtils.getNodeId(leavingInstance, survivor);

            interruptThenKill(cluster, leavingInstance, driver);
            logger.info("KILLMIDSEQ [{}] leaving node killed while CMS paused before {}", cellId, point);

            unpauseCommits(cmsInstance);

            boolean quiesced = tryQuiesce(cluster, cmsInstance, leavingInstance.config().num());
            logger.info("KILLMIDSEQ [{}] post-kill quiesce success={}", cellId, quiesced);

            forceConvict(survivor, leavingAddr);
            forceConvict(cmsInstance, leavingAddr);

            String uuid = uuidOf(nodeIntId, survivor);

            NodeToolResult result = survivor.nodetoolResult("abortdecommission", "--node", uuid);
            logger.info("KILLMIDSEQ [{}] abortdecommission rc={} stdout=[{}] stderr=[{}] error=[{}]",
                        cellId, result.getRc(), result.getStdout(), result.getStderr(), result.getError());

            waitAndAssertRecovered(cellId, survivor, nodeIntId, result);
        }
    }

    private static SerializablePredicate<Transformation> leavePredicate(Point point)
    {
        switch (point)
        {
            case PREPARE: return (e) -> e instanceof PrepareLeave;
            case START:   return (e) -> e instanceof PrepareLeave.StartLeave;
            case MID:     return (e) -> e instanceof PrepareLeave.MidLeave;
            default: throw new IllegalArgumentException(point.toString());
        }
    }

    // ============================================================ shared helpers =============

    private static Thread asyncRun(Runnable r)
    {
        Thread t = new Thread(() -> {
            try
            {
                r.run();
            }
            catch (Throwable t2)
            {
                // Expected: once the acting node is killed mid-flight, its own driver thread/call will
                // typically throw or simply never return cleanly. We only need it to have gotten as far as
                // blocking on the CMS commit that pauseBeforeCommit is waiting on.
                logger.info("KILLMIDSEQ background driver thread ended (often expected once the node is killed): {}", String.valueOf(t2));
            }
        });
        t.setDaemon(true);
        t.setName("killmidseq-driver");
        t.start();
        return t;
    }

    /**
     * {@code AbstractCluster$Wrapper.startup()}/{@code .startup(ICluster)} and {@code .shutdown(boolean)} are both
     * {@code synchronized} on the same {@code Wrapper} instance (see {@code AbstractCluster.java:427,433,492}).
     * The driver thread started by {@link #asyncRun} calls {@code actingInstance.startup()} (or the equivalent
     * {@code move()}/{@code decommission()} entrypoint), which - while {@code pauseBeforeCommit} holds the CMS
     * paused - sits blocked inside {@code RemoteProcessor}'s retrying {@code commit()} call (retry budget up to
     * 1h, see {@code ClusterMetadataService.java:691}, "Committing ... with Retry{{remainingMs=3599999}}"),
     * *holding that monitor for the whole time*. If {@link ClusterUtils#stopAbrupt} is then called directly from
     * the test thread, {@code Wrapper.shutdown()} blocks forever trying to enter the same synchronized method -
     * a harness-level self-deadlock, not a TCM finding (confirmed empirically: cell joinKillAtPrepareJoin hit
     * exactly this, hanging inside {@code AbstractCluster$Wrapper.shutdown(AbstractCluster.java:488)} until the
     * JUnit timeout fired - see D1-notes.md "Harness bugs found & fixed").
     * <p>
     * Fix: interrupt the driver thread first. The blocking wait inside {@code RemoteProcessor}'s commit path is
     * on a plain {@code Future.get()} (interruptible - see {@code RemoteProcessor.java} catch clauses for
     * {@code InterruptedException}), so this reliably unblocks {@code startup()}/{@code move()}/
     * {@code decommission()} (it throws), releasing the monitor, before we ask {@code stopAbrupt} to tear the
     * instance down. This does not change what is being tested: the CMS is still paused and still holds the
     * commit request that was already in flight when the node is interrupted+killed; only the harness's own
     * bookkeeping is unstuck, not the scenario under test.
     */
    private static void interruptThenKill(Cluster cluster, IInvokableInstance actingInstance, Thread driverThread) throws InterruptedException
    {
        driverThread.interrupt();
        driverThread.join(10_000);
        if (driverThread.isAlive())
            logger.warn("KILLMIDSEQ driver thread did not exit within 10s of being interrupted; stopAbrupt may still block");
        ClusterUtils.stopAbrupt(cluster, actingInstance);
    }

    private static boolean tryQuiesce(Cluster cluster, IInvokableInstance cmsInstance, int... ignored)
    {
        try
        {
            waitForCMSToQuiesce(cluster, cmsInstance, ignored);
            return true;
        }
        catch (Throwable t)
        {
            logger.warn("KILLMIDSEQ post-kill quiesce did not converge within timeout: {}", String.valueOf(t));
            return false;
        }
    }

    private static void forceConvict(IInvokableInstance executor, InetSocketAddress deadAddr)
    {
        if (executor.isShutdown())
            return;
        executor.runOnInstance(() -> {
            InetAddressAndPort ep = toCassandraInetAddressAndPort(deadAddr);
            FailureDetector.instance.forceConviction(ep);
        });
    }

    private static String uuidOf(int nodeIntId, IInvokableInstance executor)
    {
        return executor.callOnInstance(() -> new NodeId(nodeIntId).toUUID().toString());
    }

    private static boolean sequenceCleared(IInvokableInstance executor, int nodeIntId)
    {
        return executor.callOnInstance(() -> !ClusterMetadata.current().inProgressSequences.contains(new NodeId(nodeIntId)));
    }

    private static boolean lockedRangesEmpty(IInvokableInstance executor)
    {
        return executor.callOnInstance(() -> ClusterMetadata.current().lockedRanges.locked.isEmpty());
    }

    /**
     * Polls the survivor for up to 30s for the in-progress-sequence entry to be cleared and locked ranges to be
     * released, then asserts both hard. This is the pass/fail line for every cell: a wedge, an un-releasable
     * lock, or a recovery command that itself failed all show up here as an assertion failure (i.e. a FINDING,
     * not a harness bug - see D1-findings/).
     */
    private static void waitAndAssertRecovered(String cellId, IInvokableInstance survivor, int nodeIntId, NodeToolResult recoveryResult) throws InterruptedException
    {
        boolean cleared = false;
        boolean locksFree = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline)
        {
            cleared = sequenceCleared(survivor, nodeIntId);
            locksFree = lockedRangesEmpty(survivor);
            if (cleared && locksFree)
                break;
            Thread.sleep(500);
        }

        logger.info("KILLMIDSEQ_RESULT cell=[{}] recoveryCommandError=[{}] sequenceCleared={} lockedRangesEmpty={}",
                    cellId, recoveryResult.getError(), cleared, locksFree);

        Assert.assertNull(String.format("[%s] recovery command itself threw: %s (stderr=%s)",
                                        cellId, recoveryResult.getError(), recoveryResult.getStderr()),
                          recoveryResult.getError());
        Assert.assertTrue(String.format("[%s] in-progress sequence for the dead node was never cleared from a surviving peer's ClusterMetadata after recovery command", cellId),
                          cleared);
        Assert.assertTrue(String.format("[%s] lockedRanges was not released after recovery command", cellId),
                          locksFree);
    }
}
