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

package org.apache.cassandra.distributed.upgrade;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * TEST-6 / UPG-1 coverage-gap closer.
 *
 * Every existing {@code ClusterMetadataUpgrade*} failure-injection test keeps the CMS-initialization
 * initiator JVM alive throughout the whole election (pause-and-resume, throw-and-catch, or dropped
 * messages between two still-live peers). None of them actually kill the initiator's process between
 * committing {@code PreInitialize} and committing {@code Initialize} - the exact window
 * {@code Election.nominateSelf()} (src/java/org/apache/cassandra/tcm/migration/Election.java) and
 * {@code Startup.initializeAsFirstCMSNode()} (src/java/org/apache/cassandra/tcm/Startup.java:152-172)
 * leave open, and the exact window {@code SystemKeyspaceStorage.hasAnyEpoch()}'s orphan-cleanup
 * (src/java/org/apache/cassandra/tcm/log/SystemKeyspaceStorage.java:89-110) exists to heal.
 *
 * This test:
 *  1. Freezes node1 (the initiator) inside {@code ClusterMetadata.forceInitializedState} via the same
 *     ByteBuddy latch idiom as {@code ClusterMetadataUpgradeDelayedInitializeTest} - i.e. after
 *     {@code PreInitialize} has been durably appended to local storage, before {@code Initialize} is
 *     committed.
 *  2. Kills node1 with {@code ClusterUtils.stopAbrupt} (kill -9 semantics: messages to/from node1 are
 *     hard-dropped first, so peers observe node1 disappearing mid-election, not a graceful leave) while
 *     it is still parked in the latch - i.e. the crash lands inside the PreInitialize/Initialize window
 *     and is never released.
 *  3. Observes what nodes 2 and 3 do with no further input: do they recover, or wedge on the in-memory
 *     {@code Election.initiator} AtomicReference forever?
 *  4. Probes the *wrong* recovery order first (restart node1, retry {@code cms initialize} immediately,
 *     without clearing the peers) to characterize the failure signal an operator would actually see.
 *  5. Probes the documented recovery order: {@code nodetool cms abortinitialization --initiator <node1>}
 *     on a live peer, THEN restart node1 and retry - and confirms this converges the cluster.
 */
public class ClusterMetadataUpgradeInitiatorCrashTest extends UpgradeTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadataUpgradeInitiatorCrashTest.class);

    @Test
    public void initiatorCrashesBetweenPreInitializeAndInitializeTest() throws Throwable
    {
        final int nodeCount = 3;
        Versions dtestVersions = Versions.find();
        Versions.Version earliestVersion = dtestVersions.getLatest(v50);
        Consumer<IInstanceConfig> configUpdater = config -> config.with(Feature.NETWORK, Feature.GOSSIP);
        Consumer<UpgradeableCluster.Builder> builderUpdater =
            builder -> builder.withInstanceInitializer(ClusterMetadataUpgradeInitiatorCrashTest.BBInstaller::installUpgradeVersionBB);

        try (UpgradeableCluster cluster = UpgradeableCluster.create(nodeCount, earliestVersion, configUpdater, builderUpdater))
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':3}"));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k int PRIMARY KEY)");

            Versions.Version upgradeVersion = dtestVersions.getLatest(CURRENT);
            for (IUpgradeableInstance i : cluster)
                upgradeInstance(i, upgradeVersion);
            logger.info(String.format("D2: cluster fully upgraded to %s, still in gossip/UPGRADE mode (cms not yet initialized)", upgradeVersion));

            IUpgradeableInstance node1 = cluster.get(1);
            IUpgradeableInstance node2 = cluster.get(2);
            IUpgradeableInstance node3 = cluster.get(3);
            String node1Address = node1.config().getString("broadcast_address");

            // ============================================================
            // PHASE 1: start cms initialize on node1, let it pause after committing
            // PreInitialize (Epoch: 1) but before committing Initialize.
            // ============================================================
            assertEquals(1, BBState.latch.getCount());
            ExecutorService initExecutor = Executors.newSingleThreadExecutor();
            Future<NodeToolResult> initResult = initExecutor.submit(() -> node1.nodetoolResult("cms", "initialize"));
            logger.info("D2 PHASE 1: waiting for node1 to pause after committing PreInitialize");
            Awaitility.waitAtMost(30, TimeUnit.SECONDS).until(() -> {
                NodeToolResult res = node1.nodetoolResult("cms");
                return res.getStdout().contains("Epoch: 1");
            });
            logger.info("D2 PHASE 1: node1 paused with PreInitialize committed (Epoch: 1), Initialize not yet attempted");

            // ============================================================
            // PHASE 2: kill node1 (kill -9 semantics) while still parked in the latch.
            // The Initialize commit is never attempted; the BBInterceptor thread is
            // abandoned inside the now-dead instance's classloader.
            // ============================================================
            logger.info("D2 PHASE 2: killing node1 with ClusterUtils.stopAbrupt while frozen in the PreInitialize/Initialize window");
            ClusterUtils.stopAbrupt(cluster, node1);
            logger.info("D2 PHASE 2: node1 is down. BBState.latch was never counted down for the killed attempt (Initialize commit never attempted).");
            // IMPORTANT: do NOT count down BBState.latch here. A real `kill -9` gives the process no
            // further execution at all - counting down the latch would let the abandoned thread
            // parked inside node1's now-shut-down (but not yet garbage collected) instance actually
            // *resume* and run zuper.call() (the real forceInitializedState body) followed by the
            // real Initialize commit attempt and, on that failing, Election.nominateSelf's catch-all
            // cleanup - none of which a genuinely killed process could ever do. (Confirmed by an
            // earlier run of this test that DID release the latch here: the abandoned thread's
            // partial commit-then-revert dance left node1's on-disk system_schema permanently
            // diverged from the peers' schema digest, persisting across the PHASE 4 restart and
            // blocking every retry in PHASE 5 with "Got mismatching cluster metadatas" - that
            // divergence was a harness artifact of resuming a thread a real crash would have
            // simply erased, not a reproduction of a real recovery defect.)
            //
            // Instead: disarm the BB interceptor via BBState.armed so it is *not* re-installed on
            // node1's future restart in PHASE 4 (installUpgradeVersionBB below checks this flag).
            // The original abandoned thread stays parked in its 300s await() for the rest of this
            // test's lifetime (which finishes in well under that), so it never resumes at all -
            // faithfully modelling "the process is simply gone, no further code ever runs".
            BBState.armed = false;

            // ============================================================
            // PHASE 3: observe peer state with no further operator input.
            // ============================================================
            logger.info("D2 PHASE 3: capturing peer state with no operator intervention");
            NodeToolResult n2Cms = node2.nodetoolResult("cms");
            NodeToolResult n3Cms = node3.nodetoolResult("cms");
            logger.info("D2 PHASE 3: node2 `nodetool cms describe`:\n{}", n2Cms.getStdout());
            logger.info("D2 PHASE 3: node3 `nodetool cms describe`:\n{}", n3Cms.getStdout());
            // Peers never received PreInitialize or Initialize (only node1 committed those locally) -
            // they should still be reporting the pre-migration sentinel epoch.
            assertTrue("node2 should still be at the pre-migration sentinel epoch",
                       n2Cms.getStdout().contains("Epoch: -9223372036854775807"));
            assertTrue("node3 should still be at the pre-migration sentinel epoch",
                       n3Cms.getStdout().contains("Epoch: -9223372036854775807"));

            // A second, independent `cms initialize` attempt from a live peer must be rejected -
            // this is the observable signature of the peer-side wedge (in-memory Election.initiator
            // AtomicReference still points at node1, set when node1's initiate() fanned out
            // TCM_INIT_MIG_REQ in PHASE 1, and nothing clears it automatically: no FailureDetector
            // check, no timeout, anywhere in Election.java).
            NodeToolResult n2RetryWedged = node2.nodetoolResult("cms", "initialize");
            logger.info("D2 PHASE 3: node2 `cms initialize` while wedged -> rc={}, stdout={}, stderr={}",
                        n2RetryWedged.getRc(), n2RetryWedged.getStdout(), n2RetryWedged.getStderr());
            n2RetryWedged.asserts().failure();

            // ============================================================
            // PHASE 4 (negative-path probe): restart node1 fresh and retry `cms initialize`
            // *without* first clearing the peers, to characterize the failure signal an
            // operator gets if they attempt recovery in the "obvious" order (restart the
            // dead node, try again) instead of the documented order.
            // ============================================================
            logger.info("D2 PHASE 4: restarting node1 WITHOUT clearing peers first, to observe the wrong-order failure signal");
            node1.startup();
            logger.info("D2 PHASE 4: node1 restarted. Checking logs for orphaned-PreInitialize cleanup.");
            // SystemKeyspaceStorage.hasAnyEpoch(): count==1 and it's PRE_INITIALIZE_CMS -> cleans up,
            // returns false -> StartupMode.get() returns UPGRADE (hasBootedBefore=true). Confirmed via
            // the exact log line SystemKeyspaceStorage.java:103 emits.
            boolean cleanupLogged = node1.logs().grep("Cleaning up orphaned PreInitialize").getResult().size() > 0;
            logger.info("D2 PHASE 4: orphan-PreInitialize cleanup log observed on node1 restart = {}", cleanupLogged);
            assertTrue("Expected SystemKeyspaceStorage.hasAnyEpoch() to log orphan PreInitialize cleanup on node1 restart",
                       cleanupLogged);

            NodeToolResult n1RetryBeforeAbort = node1.nodetoolResult("cms", "initialize");
            logger.info("D2 PHASE 4: node1 `cms initialize` retry BEFORE any peer abortinitialization -> rc={}, stdout={}, stderr={}",
                        n1RetryBeforeAbort.getRc(), n1RetryBeforeAbort.getStdout(), n1RetryBeforeAbort.getStderr());
            // Expected: fails, because node2/node3 still have their stale in-memory Election.initiator
            // pointing at node1 from PHASE 1, so PrepareHandler.doVerb's updateInitiator(null, ...) CAS
            // fails on the peers when node1's fresh initiate() fans out a *new* TCM_INIT_MIG_REQ; the
            // peers throw inside doVerb and never respond, so node1's fanoutAndWait times out.
            n1RetryBeforeAbort.asserts().failure();

            // ============================================================
            // PHASE 5: documented recovery order. Clear the peer-side wedge with
            // `nodetool cms abortinitialization --initiator <node1>` on a live peer, THEN retry.
            // ============================================================
            logger.info("D2 PHASE 5: running `nodetool cms abortinitialization --initiator {}` on node2", node1Address);
            NodeToolResult abortResult = node2.nodetoolResult("cms", "abortinitialization", "--initiator", node1Address);
            logger.info("D2 PHASE 5: abortinitialization on node2 -> rc={}, stdout={}, stderr={}",
                        abortResult.getRc(), abortResult.getStdout(), abortResult.getStderr());
            abortResult.asserts().success();

            // Election.abortInitialization fans out TCM_ABORT_MIG to every non-LEFT peer in the
            // directory (not just node2's own local state) - confirm node3 was reached transitively
            // by this single call on node2 via its log (Election.AbortHandler.doVerb), without
            // invoking a side-effecting `cms initialize` directly on node3.
            boolean node3SawAbort = node3.logs().grep("Received election abort message").getResult().size() > 0;
            logger.info("D2 PHASE 5: node3 received TCM_ABORT_MIG transitively from node2's abortinitialization call = {}", node3SawAbort);
            assertTrue("Expected node3 to receive TCM_ABORT_MIG fanned out by node2's abortinitialization call",
                       node3SawAbort);

            // First immediate retry, logged unconditionally regardless of outcome: node1 just
            // restarted a few hundred ms ago (PHASE 4), so its own gossip-derived directory/token
            // proposal may not have finished re-converging with what node2/node3 currently believe
            // about it yet. This characterizes whether that is a real, persistent failure or a
            // transient race that clears on a subsequent retry.
            logger.info("D2 PHASE 5: retrying `cms initialize` on node1 after peer-side abort (attempt 1, immediate)");
            NodeToolResult n1RetryAfterAbort1 = node1.nodetoolResult("cms", "initialize");
            logger.info("D2 PHASE 5: node1 `cms initialize` retry AFTER peer abortinitialization, attempt 1 -> rc={}, stdout={}, stderr={}",
                        n1RetryAfterAbort1.getRc(), n1RetryAfterAbort1.getStdout(), n1RetryAfterAbort1.getStderr());

            NodeToolResult[] lastResult = new NodeToolResult[] { n1RetryAfterAbort1 };
            boolean[] succeeded = new boolean[] { n1RetryAfterAbort1.getRc() == 0 };
            int[] attempts = new int[] { 1 };
            if (!succeeded[0])
            {
                logger.info("D2 PHASE 5: attempt 1 failed ({}) - retrying with backoff for up to 60s to check if this self-clears " +
                            "(gossip/directory reconvergence lag) or is a persistent failure", lastResult[0].getStdout());
                try
                {
                    Awaitility.waitAtMost(60, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until(() -> {
                        attempts[0]++;
                        NodeToolResult r = node1.nodetoolResult("cms", "initialize");
                        logger.info("D2 PHASE 5: node1 `cms initialize` retry AFTER peer abortinitialization, attempt {} -> rc={}, stdout={}, stderr={}",
                                    attempts[0], r.getRc(), r.getStdout(), r.getStderr());
                        lastResult[0] = r;
                        succeeded[0] = r.getRc() == 0;
                        return succeeded[0];
                    });
                }
                catch (org.awaitility.core.ConditionTimeoutException timeout)
                {
                    logger.info("D2 PHASE 5: `cms initialize` never succeeded after {} attempts over 60s post-abort - PERSISTENT failure, not transient",
                                attempts[0]);
                }
            }
            logger.info("D2 PHASE 5: final outcome - succeeded={}, attempts={}, lastResult stdout={}",
                        succeeded[0], attempts[0], lastResult[0].getStdout());
            NodeToolResult n1RetryAfterAbort = lastResult[0];
            n1RetryAfterAbort.asserts().success();

            // ============================================================
            // PHASE 6: confirm full end-to-end convergence.
            // ============================================================
            Awaitility.waitAtMost(60, TimeUnit.SECONDS).until(() -> {
                String e1 = extractEpoch(node1.nodetoolResult("cms").getStdout());
                String e2 = extractEpoch(node2.nodetoolResult("cms").getStdout());
                String e3 = extractEpoch(node3.nodetoolResult("cms").getStdout());
                boolean pastSentinel = !e1.contains("-9223372036854775807") && !e1.isEmpty();
                return pastSentinel && e1.equals(e2) && e2.equals(e3);
            });
            logger.info("D2 PHASE 6: cluster converged after crash + wrong-order retry + peer abort + correct-order retry");
        }
    }

    private static String extractEpoch(String cmsOutput)
    {
        for (String line : cmsOutput.split("\n"))
            if (line.trim().startsWith("Epoch:"))
                return line.trim();
        return "";
    }

    private void upgradeInstance(IUpgradeableInstance instance, Versions.Version upgradeTo) throws Exception
    {
        int instanceId = instance.config().num();
        logger.info("Shutting down instance {} to upgrade to {}", instanceId, upgradeTo.version);
        instance.shutdown(true).get(60, TimeUnit.SECONDS);
        logger.info("Starting instanceId {} on version {}", instanceId, upgradeTo.version);
        instance.setVersion(upgradeTo);
        instance.startup();
        logger.info("Started instanceId {}", instanceId);
    }

    @Shared
    public static class BBState
    {
        public static CountDownLatch latch = new CountDownLatch(1);
        // Once the kill has landed (PHASE 2), set false so the interceptor is not re-installed on
        // node1's future restart(s) - see the comment at the PHASE 2 kill site for why.
        public static volatile boolean armed = true;
    }

    public static class BBInstaller
    {
        public static void installUpgradeVersionBB(ClassLoader classLoader, Integer num)
        {
            if (!BBState.armed)
            {
                logger.info("D2: BBState disarmed, not installing forceInitializedState interceptor for instance {}", num);
                return;
            }
            try
            {
                new ByteBuddy().rebase(ClusterMetadata.class)
                               .method(named("forceInitializedState"))
                               .intercept(MethodDelegation.to(ClusterMetadataUpgradeInitiatorCrashTest.BBInterceptor.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
            catch (NoClassDefFoundError noClassDefFoundError)
            {
                logger.info("... but no class def", noClassDefFoundError);
            }
            catch (Throwable tr)
            {
                logger.info("Unable to intercept forceInitializedState method", tr);
                throw tr;
            }
        }
    }

    public static class BBInterceptor
    {
        @SuppressWarnings("unused")
        public static ClusterMetadata forceInitializedState(@SuperCall Callable<ClusterMetadata> zuper)
        {
            try
            {
                logger.info("D2: forceInitializedState waiting (PreInitialize already committed, Initialize not yet attempted)...");
                // Intentionally never counted down in this test - node1 gets killed while parked here.
                BBState.latch.await(300, TimeUnit.SECONDS);
                logger.info("D2: forceInitializedState continuing...");
                return zuper.call();
            }
            catch (Throwable e)
            {
                logger.info("error:", e);
                throw new RuntimeException(e);
            }
        }
    }
}
