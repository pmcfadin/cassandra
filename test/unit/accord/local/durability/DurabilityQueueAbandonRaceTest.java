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

package accord.local.durability;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.api.Scheduler;
import accord.coordinate.ExecuteSyncPoint;
import accord.coordinate.ExecuteSyncPoint.SyncPointErased;
import accord.impl.PrefixedIntHashKey;
import accord.impl.TopologyFactory;
import accord.local.Node;
import accord.local.durability.DurabilityService.SyncLocal;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.primitives.Deps;
import accord.primitives.FullRangeRoute;
import accord.primitives.PartialSyncPoint;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.SyncPoint;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.SortedArrays.SortedArrayList;

import static accord.burn.BurnTestBase.HASH_RANGE_END;
import static accord.burn.BurnTestBase.HASH_RANGE_START;
import static accord.burn.BurnTestBase.generateIds;

/**
 * Reproducer for accord-review critical finding #4 (accord-review/ACCORD-ASSESSMENT.md 3.1,
 * accord-review/dive-notes/core-topology-durability.md finding 2; tracked as
 * accord-review/phase2/trackB row B4):
 * {@link DurabilityQueue}'s {@code onDone()} completion callback
 * (accord-core/src/main/java/accord/local/durability/DurabilityQueue.java, currently around lines
 * 513-632) calls {@code abandon(pending, false, false)} at line 555, from the
 * require.including/removed-node branch, WITHOUT holding {@code synchronized (DurabilityQueue.this)}.
 * Every other mutator of {@code pendingByRange}/{@code pendingCount}/{@code queued}/{@code Pending.status}
 * in the class -- including the sibling {@code SyncPointErased}/{@code TopologyRetiredException}
 * branch of the very same callback, just a few lines below at 583-587 -- holds that monitor.
 * <p>
 * This test lives in the {@code accord.local.durability} package (under Cassandra's ant-driven
 * {@code test/unit} tree rather than accord-core's own gradle-driven test tree) specifically so it
 * can (a) call the package-private {@code DurabilityQueue.submit(...)} entry point and (b) run via
 * {@code ant testsome} without invoking gradle in {@code modules/accord}. It depends only on classes
 * already present on Cassandra's ant test classpath via the prebuilt {@code cassandra-accord} main
 * and test-support jars (accord-core.build.libs); no production source file is modified.
 * <p>
 * Approach: assert-based demonstration (option (a) from accord-review/DEEPER-STUDY-GUIDE.md, row B4).
 * Rather than adding a {@code Thread.holdsLock(this)} assertion to production code, this test uses
 * {@link DurabilityQueue.Adapter#abandon} -- a real, already-existing extension point of the class
 * under test -- to observe {@code Thread.holdsLock(queue)} at the exact moment production code
 * invokes it. Two independent {@code DurabilityQueue} instances each drive a single {@code Pending}
 * through {@code onDone()} to completion from a background thread (modelling a distinct per-shard
 * completion executor, as the finding describes):
 * <ul>
 *     <li>{@code syncedQueue}: a request-less {@code Pending} fails with {@link SyncPointErased},
 *     driving the abandon() call at DurabilityQueue.java:585 -- the monitor IS held there
 *     (sanity check on the harness itself; must pass).</li>
 *     <li>{@code racyQueue}: a {@code Pending} with a {@code DurabilityRequest} whose
 *     {@code require.including} targets a node the topology reports as removed, driving the
 *     abandon() call at DurabilityQueue.java:555 -- the monitor is NOT held there (the bug; the
 *     final assertion is expected to fail).</li>
 * </ul>
 */
public class DurabilityQueueAbandonRaceTest
{
    @Test
    public void abandonOutsideLockRaceTest() throws Exception
    {
        List<Node.Id> nodes = generateIds(false, 3);
        TopologyFactory factory = new TopologyFactory(3, PrefixedIntHashKey.ranges(0, HASH_RANGE_START, HASH_RANGE_END, 1));
        Topology topology = factory.toTopology(nodes);

        // --- Reference case: abandon() invoked from the SyncPointErased branch of onDone()
        // (DurabilityQueue.java ~583-587), which correctly holds the monitor. This is a sanity
        // check on the harness itself, not on the bug.
        LockObservingAdapter syncedAdapter = new LockObservingAdapter(topology);
        DurabilityQueue syncedQueue = new DurabilityQueue(syncedAdapter);
        syncedAdapter.attach(syncedQueue);

        Range rangeB = PrefixedIntHashKey.range(0, HASH_RANGE_START, HASH_RANGE_END);
        RoutingKey homeKeyB = rangeB.someIntersectingRoutingKey(null);
        FullRangeRoute routeB = new FullRangeRoute(homeKeyB, new Range[]{ rangeB });
        TxnId txnIdB = new TxnId(1, 100, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, nodes.get(0));
        SyncPoint syncPointB = new SyncPoint(txnIdB, txnIdB, routeB, Deps.NONE);

        syncedQueue.submit(syncPointB, null); // request == null

        AtomicReference<Throwable> uncaughtB = new AtomicReference<>();
        Thread completerB = new Thread(() -> syncedAdapter.results.accept(null, new SyncPointErased()));
        completerB.setUncaughtExceptionHandler((t, e) -> uncaughtB.set(e));
        completerB.start();
        completerB.join(10_000);
        Assert.assertFalse("onDone (SyncPointErased path) did not complete in time", completerB.isAlive());
        if (uncaughtB.get() != null)
            throw new AssertionError("Unexpected exception in SyncPointErased completion thread", uncaughtB.get());
        Assert.assertTrue("sanity: abandon() should have been invoked on the SyncPointErased path", syncedAdapter.abandonInvoked.get());
        Assert.assertTrue("sanity check failed: abandon() invoked from the SyncPointErased branch of onDone() " +
                           "(DurabilityQueue.java ~585) is expected to hold the DurabilityQueue monitor -- if this " +
                           "fails, the test harness itself is broken, not the bug under test.",
                           syncedAdapter.heldLockDuringAbandon.get());

        // --- Bug case: abandon() invoked from the require.including/removed-node branch of onDone()
        // (DurabilityQueue.java:555), which does NOT hold the monitor.
        Node.Id removedNode = nodes.get(1);
        Topology topologyWithRemoved = new Topology(topology.epoch(),
                                                      new SortedArrayList<>(new Node.Id[]{ removedNode }),
                                                      new SortedArrayList<>(new Node.Id[0]),
                                                      new SortedArrayList<>(new Node.Id[0]),
                                                      topology.shards().toArray(new Shard[0]));

        LockObservingAdapter racyAdapter = new LockObservingAdapter(topologyWithRemoved);
        DurabilityQueue racyQueue = new DurabilityQueue(racyAdapter);
        racyAdapter.attach(racyQueue);

        Range rangeA = PrefixedIntHashKey.range(0, HASH_RANGE_START, HASH_RANGE_END);
        RoutingKey homeKeyA = rangeA.someIntersectingRoutingKey(null);
        FullRangeRoute routeA = new FullRangeRoute(homeKeyA, new Range[]{ rangeA });
        TxnId txnIdA = new TxnId(1, 100, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, nodes.get(0));
        SyncPoint syncPointA = new SyncPoint(txnIdA, txnIdA, routeA, Deps.NONE);

        SortedArrayList<Node.Id> including = new SortedArrayList<>(new Node.Id[]{ removedNode });
        DurabilityLevel require = new DurabilityLevel(SyncLocal.NoLocal, SyncRemote.NoRemote, including, null);
        DurabilityRequest requestA = new DurabilityRequest("B4-repro", null, txnIdA, Ranges.of(rangeA), require, 0, Long.MAX_VALUE);

        racyQueue.submit(syncPointA, requestA);

        AtomicReference<Throwable> uncaughtA = new AtomicReference<>();
        Thread completerA = new Thread(() -> racyAdapter.results.accept(null, null));
        completerA.setUncaughtExceptionHandler((t, e) -> uncaughtA.set(e));
        completerA.start();
        completerA.join(10_000);
        Assert.assertFalse("onDone (removed-node path) did not complete in time", completerA.isAlive());
        if (uncaughtA.get() != null)
            throw new AssertionError("Unexpected exception in removed-node completion thread", uncaughtA.get());
        Assert.assertTrue("abandon() should have been invoked on the removed-node path", racyAdapter.abandonInvoked.get());

        // THE BUG: this abandon() call site (DurabilityQueue.java:555) does not hold the
        // DurabilityQueue monitor, unlike every sibling abandon()/restart() call site in the same
        // onDone() callback (contrast with the SyncPointErased case above, which correctly holds it).
        Assert.assertTrue("BUG B4: abandon() invoked from the require.including/removed-node branch of onDone() " +
                           "(DurabilityQueue.java:555) does not hold the DurabilityQueue monitor that every other " +
                           "mutator of pendingByRange/pendingCount/queued/Pending.status in this class holds " +
                           "(contrast with the SyncPointErased branch a few lines below, which correctly wraps its " +
                           "abandon() call in synchronized(DurabilityQueue.this)). Concurrent completions on " +
                           "different executors can therefore race on the shared IntervalBTree/heap state.",
                           racyAdapter.heldLockDuringAbandon.get());
    }

    /**
     * Minimal {@link DurabilityQueue.Adapter} that stubs out everything except {@code execute()}
     * (captures the {@link ExecuteSyncPoint.DurabilityResults} so the test can complete it later, from
     * a background thread) and {@code abandon()} (records whether the calling thread held the
     * {@code DurabilityQueue} monitor at the moment production code invoked this callback). This is a
     * legitimate, already-existing extension point of the class under test -- no production source is
     * modified to obtain this signal.
     */
    private static final class LockObservingAdapter implements DurabilityQueue.Adapter
    {
        private final Topology topology;
        private volatile DurabilityQueue queue;

        final AtomicBoolean abandonInvoked = new AtomicBoolean(false);
        final AtomicBoolean heldLockDuringAbandon = new AtomicBoolean(false);
        volatile ExecuteSyncPoint.DurabilityResults results;

        LockObservingAdapter(Topology topology)
        {
            this.topology = topology;
        }

        void attach(DurabilityQueue queue)
        {
            this.queue = queue;
        }

        @Override public Topology currentTopology() { return topology; }
        @Override public long retryDelay(int attempts, TimeUnit units) { return 1_000_000L; }
        @Override public long elapsed(TimeUnit units) { return 1_000_000L; }
        @Override public Scheduler scheduler() { return Scheduler.NEVER_RUN_SCHEDULED; }
        @Override public void unregister(DurabilityRequest request) { }
        @Override public void done(DurabilityRequest request, PartialSyncPoint syncPoint) { }
        @Override public void retry(DurabilityRequest request, PartialSyncPoint syncPoint) { }

        @Override
        public void abandon(DurabilityRequest request, PartialSyncPoint syncPoint, boolean pruned)
        {
            abandonInvoked.set(true);
            heldLockDuringAbandon.set(Thread.holdsLock(queue));
        }

        @Override
        public ExecuteSyncPoint.DurabilityResults execute(PartialSyncPoint syncPoint, int attempt)
        {
            results = new ExecuteSyncPoint.DurabilityResults();
            return results;
        }
    }
}
