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

package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntFunction;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.local.CheckedCommands;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.primitives.FullRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.SortedArrays.SortedArrayList;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCacheEntry.Status;
import org.apache.cassandra.service.accord.AccordCommandStore.ExclusiveCaches;
import org.apache.cassandra.service.accord.AccordExecutor.ExclusiveGlobalCaches;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.journal.AccordJournal;
import org.apache.cassandra.utils.concurrent.Semaphore;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.local.LoadKeys.SYNC;
import static accord.local.LoadKeysFor.READ_WRITE;
import static accord.local.PreLoadContext.contextFor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITH_LOCK;
import static org.apache.cassandra.service.accord.AccordTestUtils.createAccordCommandStore;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.key;
import static org.apache.cassandra.service.accord.AccordTestUtils.keys;
import static org.apache.cassandra.service.accord.AccordTestUtils.txnId;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Burn test for the {@link AccordExecutor} family (task-state machine, submit/wakeup paths, cache
 * load/save/eviction reference counting, quiescence accounting and shutdown), covering each of the
 * selectable queue submission models: SYNC ({@link AccordExecutorSyncSubmit}), SEMI_SYNC
 * ({@link AccordExecutorSemiSyncSubmit}), ASYNC ({@link AccordExecutorAsyncSubmit}), SIGNAL
 * ({@link AccordExecutorSignalLoop}) and EXEC_ST ({@link AccordExecutorSimple}), in both
 * single-thread (THREAD_PER_SHARD / THREAD_PER_SHARD_SYNC_QUEUE) and thread-pool
 * (THREAD_POOL_PER_SHARD) shapes.
 *
 * Multiple generator threads submit randomized mixes of real {@link AccordTask}s (command touches,
 * preaccepts, reads with and without key contexts, priority submissions), plain runnables, cache
 * capacity churn (forcing eviction sweeps through {@code tryShrinkOrEvict}) and NoEvict-queue
 * sweeps against several command stores sharing one executor.
 *
 * Invariants asserted (each failure message names the invariant and the offending state):
 * - completion-liveness: every submitted task's completion callback fires and the executor drains
 *   (detects lost wakeups / stuck signal loops);
 * - callback-multiplicity: each task's completion callback fires exactly once;
 * - terminal-state: every submitted task ends in a terminal {@link AccordTask.State};
 * - premature-quiescence: {@link AccordExecutor#afterSubmittedAndConsequences} must not fire while
 *   any task queued before registration is still unexecuted;
 * - quiescence-liveness: {@link AccordExecutor#waitForQuiescence} returns after drain;
 * - evict-while-referenced / negative-refcount: via {@link AccordCache.Listener} on every store's
 *   cache instances;
 * - post-drain cache health: no dangling references, no entries stuck LOADING/WAITING_TO_LOAD,
 *   no FAILED_TO_LOAD/FAILED_TO_SAVE residue, save pipeline settles;
 * - clean shutdown: {@code shutdown()} followed by {@code awaitTermination} terminates.
 *
 * Runtime is bounded (default well under 5 minutes). System properties:
 * - {@code -Dcassandra.test.accord.executor_burn.seed=<long>} reproduce a specific run;
 * - {@code -Dcassandra.test.accord.executor_burn.tasks=<int>} tasks submitted per executor config
 *   (default 2500);
 * - {@code -Dcassandra.test.accord.executor_burn.seconds=<long>} global time budget for the
 *   submission phase (default 240) — raise it (and tasks) for longer soak runs.
 *
 * Run with: {@code ant burn-testsome -Dtest.name=org.apache.cassandra.service.accord.AccordExecutorBurnTest}
 */
public class AccordExecutorBurnTest
{
    private static final Logger logger = LoggerFactory.getLogger(AccordExecutorBurnTest.class);

    private static final long SEED = Long.getLong("cassandra.test.accord.executor_burn.seed", ThreadLocalRandom.current().nextLong());
    private static final int TASKS_PER_CONFIG = Integer.getInteger("cassandra.test.accord.executor_burn.tasks", 2500);
    private static final long BUDGET_SECONDS = Long.getLong("cassandra.test.accord.executor_burn.seconds", 240);

    private static final int COMMAND_STORES = 4;
    private static final int GENERATORS = 4;
    private static final int CONCURRENCY = 128;
    private static final int POOL_THREADS = 4;
    private static final int KEY_POOL = 8;
    private static final int TXN_POOL = 64;
    private static final long[] CAPACITIES = { 0, 4096, 1 << 16, 1 << 20 };
    private static final long DRAIN_TIMEOUT_NANOS = SECONDS.toNanos(120);
    private static final long SETTLE_TIMEOUT_NANOS = SECONDS.toNanos(30);

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        StorageService.instance.initServer();
    }

    /**
     * One entry per selectable queue submission model / shard model combination from
     * {@link AccordCommandStores#factory()}.
     */
    enum Config
    {
        SYNC_SINGLE_WITH_LOCK,  // SYNC + THREAD_PER_SHARD_SYNC_QUEUE (what most unit tests run)
        SYNC_POOL,              // SYNC + THREAD_POOL_PER_SHARD (production default shape)
        SEMI_SYNC_POOL,         // SEMI_SYNC + THREAD_POOL_PER_SHARD
        ASYNC_POOL,             // ASYNC + THREAD_POOL_PER_SHARD
        SIGNAL_SINGLE,          // SIGNAL + THREAD_PER_SHARD
        SIGNAL_POOL,            // SIGNAL + THREAD_POOL_PER_SHARD
        EXEC_ST;                // EXEC_ST (AccordExecutorSimple, maxThreads == 1)

        AccordExecutor build(int executorId, AccordAgent agent)
        {
            IntFunction<String> names = n -> "AccordExecutorBurn[" + name() + ',' + executorId + ',' + n + ']';
            switch (this)
            {
                case SYNC_SINGLE_WITH_LOCK: return new AccordExecutorSyncSubmit(executorId, RUN_WITH_LOCK, 1, names, agent);
                case SYNC_POOL:             return new AccordExecutorSyncSubmit(executorId, RUN_WITHOUT_LOCK, POOL_THREADS, names, agent);
                case SEMI_SYNC_POOL:        return new AccordExecutorSemiSyncSubmit(executorId, RUN_WITHOUT_LOCK, POOL_THREADS, names, agent);
                case ASYNC_POOL:            return new AccordExecutorAsyncSubmit(executorId, RUN_WITHOUT_LOCK, POOL_THREADS, names, agent);
                case SIGNAL_SINGLE:         return new AccordExecutorSignalLoop(executorId, RUN_WITHOUT_LOCK, 1, 0, 0, TimeUnit.NANOSECONDS, names, agent);
                case SIGNAL_POOL:           return new AccordExecutorSignalLoop(executorId, RUN_WITHOUT_LOCK, POOL_THREADS, 0, 0, TimeUnit.NANOSECONDS, names, agent);
                case EXEC_ST:               return new AccordExecutorSimple(executorId, RUN_WITHOUT_LOCK, 1, names, agent);
                default: throw new AssertionError("Unhandled config: " + this);
            }
        }
    }

    /**
     * Collects invariant violations; every message is tagged with the seed and current config so
     * failures are attributable and reproducible.
     */
    static final class Failures
    {
        private static final int MAX_RECORDED = 100;
        private final ConcurrentLinkedQueue<String> messages = new ConcurrentLinkedQueue<>();
        private final AtomicInteger count = new AtomicInteger();
        volatile String context = "setup";

        void fail(String fmt, Object... args)
        {
            fail(null, fmt, args);
        }

        void fail(Throwable cause, String fmt, Object... args)
        {
            String msg;
            try { msg = String.format(fmt, args); }
            catch (Throwable t) { msg = fmt + " (message format failed: " + t + ')'; }
            msg = msg + " [seed=" + SEED + ", config=" + context + ']';
            logger.error("BURN FAILURE: {}", msg, cause);
            if (count.incrementAndGet() <= MAX_RECORDED)
                messages.add(cause == null ? msg : msg + "; caused by: " + cause);
        }

        boolean isEmpty()
        {
            return count.get() == 0;
        }

        String render()
        {
            StringBuilder sb = new StringBuilder(count.get() + " burn invariant failure(s) [seed=" + SEED + "]:\n");
            for (String msg : messages)
                sb.append("  - ").append(msg).append('\n');
            int dropped = count.get() - MAX_RECORDED;
            if (dropped > 0)
                sb.append("  ... and ").append(dropped).append(" more (see log)\n");
            return sb.toString();
        }
    }

    /**
     * Any exception the executor loops or a task hand to the agent is an invariant failure: this
     * burn submits no failing work, so nothing should ever reach {@code onException}.
     */
    static final class RecordingAgent extends AccordAgent
    {
        private final Failures failures;

        RecordingAgent(Failures failures)
        {
            this.failures = failures;
            setup(Node.Id.NONE);
        }

        @Override
        public void onException(Throwable t)
        {
            failures.fail(t, "INVARIANT no-unexpected-exceptions: agent.onException invoked");
        }

        @Override
        public void onException(Throwable t, String context)
        {
            failures.fail(t, "INVARIANT no-unexpected-exceptions: agent.onException invoked (context: %s)", context);
        }
    }

    /**
     * Asserts eviction reference-counting invariants at the moment the cache mutates an entry.
     */
    static final class EvictionInvariantListener<K, V> implements AccordCache.Listener<K, V>
    {
        private final Failures failures;
        private final String cache;

        EvictionInvariantListener(Failures failures, String cache)
        {
            this.failures = failures;
            this.cache = cache;
        }

        @Override
        public void onAdd(AccordCacheEntry<K, V> entry)
        {
            checkNonNegative(entry, "onAdd");
        }

        @Override
        public void onUpdate(AccordCacheEntry<K, V> entry)
        {
            checkNonNegative(entry, "onUpdate");
        }

        @Override
        public void onEvict(AccordCacheEntry<K, V> entry)
        {
            int refs = entry.references();
            if (refs != 0)
                failures.fail("INVARIANT evict-while-referenced: %s entry %s evicted with %d references (status %s)",
                              cache, entry.key(), refs, safeStatus(entry));
        }

        private void checkNonNegative(AccordCacheEntry<K, V> entry, String event)
        {
            int refs = entry.references();
            if (refs < 0)
                failures.fail("INVARIANT negative-refcount: %s entry %s has %d references at %s (status %s)",
                              cache, entry.key(), refs, event, safeStatus(entry));
        }
    }

    private static Object safeStatus(AccordCacheEntry<?, ?> entry)
    {
        try { return entry.status(); }
        catch (Throwable t) { return "<status() threw " + t + '>'; }
    }

    private static String safeDescribe(AccordTask<?> task)
    {
        try { return task.toDescription(); }
        catch (Throwable t) { return "<toDescription() threw " + t + '>'; }
    }

    /** One tracked chain-submitted task; doubles as its own completion callback. */
    static final class Op implements BiConsumer<Void, Throwable>
    {
        final int slot;
        final String kind;
        final AccordTask<Void> task;
        final State state;
        final AtomicInteger callbacks = new AtomicInteger();
        volatile Throwable failure;

        Op(int slot, String kind, AccordTask<Void> task, State state)
        {
            this.slot = slot;
            this.kind = kind;
            this.task = task;
            this.state = state;
        }

        @Override
        public void accept(Void success, Throwable fail)
        {
            int n = callbacks.incrementAndGet();
            if (n != 1)
            {
                state.failures.fail("INVARIANT callback-multiplicity: op#%d (%s) completion callback fired %d times; task %s",
                                    slot, kind, n, safeDescribe(task));
                return;
            }
            failure = fail;
            state.completed.incrementAndGet();
            state.permits.release(1);
        }

        @Override
        public String toString()
        {
            return "op#" + slot + '(' + kind + ')';
        }
    }

    static class State
    {
        final Config config;
        final Failures failures;
        final AccordExecutor executor;
        final AccordCommandStore[] commandStores;
        final ExecutorPlus loadGen;
        final int generators;
        final int targetCount;
        final long configDeadlineNanos;
        final Semaphore permits;
        final AtomicInteger submitted = new AtomicInteger();
        final AtomicInteger completed = new AtomicInteger();
        final AtomicInteger plainPending = new AtomicInteger();
        final CountDownLatch generatorsDone;
        final ConcurrentLinkedQueue<Op> ops = new ConcurrentLinkedQueue<>();
        final AtomicBoolean checkpointFired = new AtomicBoolean();

        final TxnId[] txnPool;
        final AtomicLong clock;
        final PartialTxn[] partialTxnPool;
        final FullRoute<?>[] routePool;
        final RoutingKey[] routingKeyPool;
        final Participants<RoutingKey>[] participantsPool;

        @SuppressWarnings("unchecked")
        State(Config config, Failures failures, AccordExecutor executor, IntFunction<AccordCommandStore> storeFactory,
              int taskCount, int concurrency, int generators, int commandStores, AtomicLong clock, long configDeadlineNanos)
        {
            this.config = config;
            this.failures = failures;
            this.executor = executor;
            this.targetCount = taskCount;
            this.configDeadlineNanos = configDeadlineNanos;
            this.permits = Semaphore.newSemaphore(concurrency);
            this.generators = generators;
            this.generatorsDone = new CountDownLatch(generators);
            this.loadGen = executorFactory().pooled("AccordExecutorBurnGen[" + config + ']', generators);
            this.commandStores = new AccordCommandStore[commandStores];
            for (int i = 0 ; i < commandStores ; ++i)
                this.commandStores[i] = storeFactory.apply(i);

            // register eviction/refcount invariant listeners on every store's cache instances
            for (AccordCommandStore store : this.commandStores)
            {
                try (ExclusiveCaches caches = store.lockCaches())
                {
                    caches.commands().register(new EvictionInvariantListener<>(failures, config + "/commands[" + store.id() + ']'));
                    caches.commandsForKeys().register(new EvictionInvariantListener<>(failures, config + "/commandsForKey[" + store.id() + ']'));
                }
            }
            try (ExclusiveGlobalCaches caches = executor.lockCaches())
            {
                caches.global.setCapacity(1 << 20);
            }

            TableMetadata metadata = Schema.instance.getTableMetadata("ks", "tbl");
            // share the store's own clock (rather than an independent counter) so hlc values generated here stay
            // reachable by the stores' uniqueNow() — the test NodeCommandStoreService's uniqueNow(atLeast) ignores
            // atLeast and just returns now.getAsLong(), so a disjoint/higher-starting counter would make preaccept's
            // computed executeAt fall permanently behind fresh txnIds, violating Command.validate's executeAt>=txnId
            // invariant (see AccordTaskTest for the same clock-sharing convention).
            this.clock = clock;
            this.txnPool = new TxnId[TXN_POOL];
            for (int i = 0 ; i < TXN_POOL ; ++i)
                txnPool[i] = txnId(1, clock.incrementAndGet(), 1);
            this.partialTxnPool = new PartialTxn[KEY_POOL];
            this.routePool = new FullRoute<?>[KEY_POOL];
            this.routingKeyPool = new RoutingKey[KEY_POOL];
            this.participantsPool = new Participants[KEY_POOL];
            for (int i = 0 ; i < KEY_POOL ; ++i)
            {
                partialTxnPool[i] = createPartialTxn(i);
                routingKeyPool[i] = key(metadata, i).toUnseekable();
                routePool[i] = partialTxnPool[i].keys().toRoute(routingKeyPool[i]);
                participantsPool[i] = keys(metadata, i).toParticipants();
            }
        }

        void start()
        {
            for (int i = 0 ; i < generators ; ++i)
            {
                int generatorIndex = i;
                loadGen.execute(() -> run(generatorIndex));
            }
        }

        void run(int generatorIndex)
        {
            Random random = new Random(SEED ^ (0x9E3779B97F4A7C15L * (((long) config.ordinal() << 8) | (generatorIndex + 1))));
            try
            {
                while (true)
                {
                    int slot = submitted.get();
                    if (slot >= targetCount)
                        return;
                    if (nanoTime() - configDeadlineNanos > 0)
                    {
                        logger.info("{} generator[{}] stopping at deadline after {} submissions", config, generatorIndex, slot);
                        return;
                    }
                    if (!submitted.compareAndSet(slot, slot + 1))
                        continue;

                    try
                    {
                        if (!permits.tryAcquire(1, 30, SECONDS))
                        {
                            failures.fail("LIVENESS generator-starved: generator[%d] waited 30s for a permit (submitted %d, completed %d, executor.hasTasks %s)",
                                          generatorIndex, submitted.get(), completed.get(), executor.hasTasks());
                            completed.incrementAndGet(); // balance the reserved slot
                            return;
                        }
                    }
                    catch (InterruptedException e)
                    {
                        throw new UncheckedInterruptedException(e);
                    }
                    submitSomething(random, slot);
                }
            }
            catch (UncheckedInterruptedException e)
            {
                throw e;
            }
            catch (Throwable t)
            {
                failures.fail(t, "load generator[%d] died", generatorIndex);
            }
            finally
            {
                generatorsDone.countDown();
            }
        }

        private void submitSomething(Random random, int slot)
        {
            AccordCommandStore store = commandStores[random.nextInt(commandStores.length)];
            int p = random.nextInt(100);
            try
            {
                if (p < 30)      submitTouch(random, slot, store, false);
                else if (p < 50) submitPreaccept(random, slot, store);
                else if (p < 70) submitRead(random, slot, store);
                else if (p < 85) submitPlain(store);
                else if (p < 90) submitTouch(random, slot, store, true);
                else if (p < 95) churnCapacity(random);
                else             sweepNoEvictQueue();
            }
            catch (Throwable t)
            {
                failures.fail(t, "synchronous failure submitting op#%d (branch %d)", slot, p);
                completed.incrementAndGet();
                permits.release(1);
            }
        }

        /** Loads a pooled command and initialises it if absent — exercises load + modify + save. */
        private void submitTouch(Random random, int slot, AccordCommandStore store, boolean priority)
        {
            TxnId id = txnPool[random.nextInt(txnPool.length)];
            PreLoadContext ctx = contextFor(id, "burn-touch");
            Consumer<SafeCommandStore> consumer = safe -> safe.get(id, StoreParticipants.empty(id));
            beginTracked(slot, priority ? "touch-priority" : "touch", AccordTask.create(store, ctx, consumer), priority);
        }

        /** Preaccepts a fresh txn on a pooled key — deterministic journal save traffic + CFK contention. */
        private void submitPreaccept(Random random, int slot, AccordCommandStore store)
        {
            int k = random.nextInt(KEY_POOL);
            TxnId id = txnId(1, clock.incrementAndGet(), 1);
            PartialTxn partialTxn = partialTxnPool[k];
            FullRoute<?> route = routePool[k];
            PreLoadContext ctx = contextFor(id, route, SYNC, READ_WRITE, "burn-preaccept");
            Consumer<SafeCommandStore> consumer = safe -> CheckedCommands.preaccept(safe, id, partialTxn, route);
            beginTracked(slot, "preaccept", AccordTask.create(store, ctx, consumer), false);
        }

        /** Read-only task, with or without a key context (CommandsForKey load). */
        private void submitRead(Random random, int slot, AccordCommandStore store)
        {
            TxnId id = txnPool[random.nextInt(txnPool.length)];
            if (random.nextBoolean())
            {
                PreLoadContext ctx = contextFor(id, "burn-read");
                Consumer<SafeCommandStore> consumer = safe -> {
                    safe.ifLoadedAndInitialised(id);
                };
                beginTracked(slot, "read", AccordTask.create(store, ctx, consumer), false);
            }
            else
            {
                int k = random.nextInt(KEY_POOL);
                RoutingKey routingKey = routingKeyPool[k];
                PreLoadContext ctx = contextFor(id, null, participantsPool[k], SYNC, READ_WRITE, "burn-read-keys");
                Consumer<SafeCommandStore> consumer = safe -> {
                    safe.ifInitialised(id);
                    safe.ifLoadedAndInitialised(routingKey);
                };
                beginTracked(slot, "read-keys", AccordTask.create(store, ctx, consumer), false);
            }
        }

        /** Plain runnable through the executor's PlainRunnable path. */
        private void submitPlain(AccordCommandStore store)
        {
            plainPending.incrementAndGet();
            store.executor().execute(() -> {
                plainPending.decrementAndGet();
                completed.incrementAndGet();
                permits.release(1);
            });
        }

        /** Randomly grows/shrinks the shared cache; close() runs tryShrinkOrEvict, forcing eviction sweeps. */
        private void churnCapacity(Random random)
        {
            long capacity = CAPACITIES[random.nextInt(CAPACITIES.length)];
            try (ExclusiveGlobalCaches caches = executor.lockCaches())
            {
                caches.global.setCapacity(capacity);
            }
            completed.incrementAndGet();
            permits.release(1);
        }

        /** Runs the scheduled-maintenance NoEvict sweep, as AccordCommandStores does every second. */
        private void sweepNoEvictQueue()
        {
            executor.executeDirectlyWithLock(() -> executor.cacheExclusive().processNoEvictQueue());
            completed.incrementAndGet();
            permits.release(1);
        }

        private void beginTracked(int slot, String kind, AccordTask<Void> task, boolean priority)
        {
            Op op = new Op(slot, kind, task, this);
            ops.add(op);
            (priority ? task.priorityChain() : task.chain()).begin(op);
        }

        void awaitCompletion(ExecutorPlus infra) throws InterruptedException
        {
            // 1. wait for the generators to finish submitting
            long submitWait = Math.max(SECONDS.toNanos(60), configDeadlineNanos - nanoTime() + SECONDS.toNanos(60));
            if (!generatorsDone.await(submitWait, TimeUnit.NANOSECONDS))
                failures.fail("LIVENESS generators failed to finish submitting (submitted %d / target %d, completed %d)",
                              submitted.get(), targetCount, completed.get());
            loadGen.shutdown();

            // 2. register the premature-quiescence checkpoint while the executor is still busy:
            //    all tracked tasks are queued (their submit() calls returned), so once each has left
            //    INITIALIZED it holds a queue position < the registration position, and the executor
            //    contract says the callback must only fire after every one of them has completed.
            registerCheckpoint();

            // 3. every completion callback must fire
            long drainDeadline = nanoTime() + DRAIN_TIMEOUT_NANOS;
            while (completed.get() < submitted.get())
            {
                if (nanoTime() - drainDeadline > 0)
                {
                    failures.fail("LIVENESS drain-timeout: %d of %d submissions incomplete after %ds (plain pending %d, executor.hasTasks %s); lost wakeup or stuck task suspected",
                                  submitted.get() - completed.get(), submitted.get(), SECONDS.convert(DRAIN_TIMEOUT_NANOS, TimeUnit.NANOSECONDS),
                                  plainPending.get(), executor.hasTasks());
                    dumpIncompleteOps();
                    break;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }

            // 4. the executor's own accounting must drain
            long hasTasksDeadline = nanoTime() + SECONDS.toNanos(60);
            while (executor.hasTasks())
            {
                if (nanoTime() - hasTasksDeadline > 0)
                {
                    failures.fail("LIVENESS executor-drain: executor.hasTasks() still true 60s after all callbacks fired");
                    break;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }

            // 5. the checkpoint callback must have fired by now (lost-notification detector)
            long checkpointDeadline = nanoTime() + SECONDS.toNanos(30);
            while (!checkpointFired.get())
            {
                if (nanoTime() - checkpointDeadline > 0)
                {
                    failures.fail("LIVENESS lost-notification: afterSubmittedAndConsequences callback never fired despite full drain");
                    break;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }

            // 6. waitForQuiescence must return (guards the runningThreads quiescence accounting)
            Future<?> quiescence = infra.submit((Runnable) executor::waitForQuiescence);
            try
            {
                quiescence.get(60, SECONDS);
            }
            catch (TimeoutException e)
            {
                failures.fail("LIVENESS waitForQuiescence hung 60s after full drain (quiescence accounting corrupted?)");
                quiescence.cancel(true);
            }
            catch (ExecutionException e)
            {
                failures.fail(e.getCause(), "waitForQuiescence threw");
            }

            verifyTerminalStates();
            verifyCachesSettled();
        }

        private void registerCheckpoint()
        {
            // wait (bounded) for every incomplete tracked task to have been assigned a queue position
            long beginDeadline = nanoTime() + SECONDS.toNanos(60);
            for (Op op : ops)
            {
                while (op.callbacks.get() == 0 && op.task.state() == AccordTask.State.INITIALIZED)
                {
                    if (nanoTime() - beginDeadline > 0)
                    {
                        failures.fail("LIVENESS op %s still INITIALIZED 60s after submission: %s", op, safeDescribe(op.task));
                        break;
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
            }

            List<Op> snapshot = new ArrayList<>();
            for (Op op : ops)
            {
                if (op.callbacks.get() == 0)
                    snapshot.add(op);
            }
            executor.afterSubmittedAndConsequences(() -> {
                checkpointFired.set(true);
                for (Op op : snapshot)
                {
                    AccordTask.State state = op.task.state();
                    if (!state.isExecuted())
                        failures.fail("INVARIANT premature-quiescence: afterSubmittedAndConsequences fired while %s was still %s: %s",
                                      op, state, safeDescribe(op.task));
                }
            });
        }

        private void dumpIncompleteOps()
        {
            int dumped = 0;
            for (Op op : ops)
            {
                if (op.callbacks.get() != 0)
                    continue;
                failures.fail("LIVENESS incomplete-op: %s never completed; state %s; %s", op, op.task.state(), safeDescribe(op.task));
                if (++dumped >= 20)
                {
                    logger.error("... further incomplete ops suppressed");
                    break;
                }
            }
        }

        private void verifyTerminalStates()
        {
            int reported = 0;
            for (Op op : ops)
            {
                if (op.callbacks.get() == 0)
                    continue; // already reported by dumpIncompleteOps
                AccordTask.State state = op.task.state();
                if (!state.isComplete() && reported++ < 20)
                    failures.fail("INVARIANT non-terminal-state: %s completion callback fired but task state is %s: %s",
                                  op, state, safeDescribe(op.task));
                if (op.failure != null && reported++ < 20)
                    failures.fail(op.failure, "INVARIANT unexpected-task-failure: %s completed exceptionally", op);
            }
        }

        private void verifyCachesSettled()
        {
            // let in-flight loads/saves settle (journal flush period is 1s)
            long deadline = nanoTime() + SETTLE_TIMEOUT_NANOS;
            while (true)
            {
                boolean incomplete = false;
                for (AccordCommandStore store : commandStores)
                {
                    try (ExclusiveCaches caches = store.lockCaches())
                    {
                        incomplete |= anyInFlight(caches.commands()) || anyInFlight(caches.commandsForKeys());
                    }
                }
                if (!incomplete)
                    break;
                if (nanoTime() - deadline > 0)
                {
                    failures.fail("LIVENESS save-pipeline: cache entries still LOADING/SAVING/WAITING_TO_SAVE %ds after full drain",
                                  SECONDS.convert(SETTLE_TIMEOUT_NANOS, TimeUnit.NANOSECONDS));
                    break;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            }

            for (AccordCommandStore store : commandStores)
            {
                try (ExclusiveCaches caches = store.lockCaches())
                {
                    sweepSettled(config + "/commands[" + store.id() + ']', caches.commands());
                    sweepSettled(config + "/commandsForKey[" + store.id() + ']', caches.commandsForKeys());
                }
            }
        }

        private <K, V> boolean anyInFlight(AccordCache.Type<K, V, ?>.Instance instance)
        {
            for (AccordCacheEntry<K, V> entry : instance)
            {
                if (!entry.isComplete() || entry.isSavingOrWaiting())
                    return true;
            }
            return false;
        }

        private <K, V> void sweepSettled(String cache, AccordCache.Type<K, V, ?>.Instance instance)
        {
            for (AccordCacheEntry<K, V> entry : instance)
            {
                int refs = entry.references();
                if (refs != 0)
                    failures.fail("INVARIANT dangling-reference: %s entry %s holds %d references after full drain (status %s)",
                                  cache, entry.key(), refs, safeStatus(entry));
                if (entry.is(Status.LOADING) || entry.is(Status.WAITING_TO_LOAD))
                    failures.fail("INVARIANT stuck-load: %s entry %s is %s with no tasks in flight",
                                  cache, entry.key(), safeStatus(entry));
                if (entry.is(Status.FAILED_TO_LOAD))
                    failures.fail("INVARIANT failed-load-residue: %s entry %s is FAILED_TO_LOAD though no load was made to fail",
                                  cache, entry.key());
                if (entry.is(Status.FAILED_TO_SAVE))
                    failures.fail("INVARIANT failed-save-residue: %s entry %s is FAILED_TO_SAVE though no save was made to fail",
                                  cache, entry.key());
            }
        }

        void shutdownAndVerify() throws InterruptedException
        {
            executor.shutdown();
            if (!executor.awaitTermination(60, SECONDS))
                failures.fail("INVARIANT shutdown-hang: executor failed to terminate within 60s of shutdown()");
        }
    }

    @Test
    public void test() throws Throwable
    {
        logger.info("AccordExecutorBurnTest SEED: {} (reproduce with -Dcassandra.test.accord.executor_burn.seed={})", SEED, SEED);
        System.out.println("AccordExecutorBurnTest SEED: " + SEED);

        Failures failures = new Failures();
        ExecutorPlus infra = executorFactory().pooled("AccordExecutorBurnInfra", 2);

        if (new File(DatabaseDescriptor.getAccordJournalDirectory()).exists())
            ServerTestUtils.cleanupDirectory(DatabaseDescriptor.getAccordJournalDirectory());

        TableMetadata metadata = Schema.instance.getTableMetadata("ks", "tbl");
        TokenRange range = TokenRange.fullRange(metadata.id, metadata.partitioner);
        Node.Id node = new Node.Id(1);
        Topology topology = new Topology(1, Shard.create(range, new SortedArrayList<>(new Node.Id[]{ node }), Sets.newHashSet(node)));
        AtomicLong clock = new AtomicLong(0);

        Config[] configs = Config.values();
        long globalDeadline = nanoTime() + SECONDS.toNanos(BUDGET_SECONDS);
        try
        {
            for (int i = 0 ; i < configs.length ; ++i)
            {
                Config config = configs[i];
                failures.context = config.name();
                long remaining = globalDeadline - nanoTime();
                if (remaining <= 0)
                {
                    logger.warn("Global budget exhausted; skipping configs {}..{}", config, configs[configs.length - 1]);
                    break;
                }
                long configDeadline = nanoTime() + remaining / (configs.length - i);
                logger.info("=== {} starting: target {} tasks, {} stores, {} generators, slice {}s ===",
                            config, TASKS_PER_CONFIG, COMMAND_STORES, GENERATORS,
                            SECONDS.convert(configDeadline - nanoTime(), TimeUnit.NANOSECONDS));

                RecordingAgent agent = new RecordingAgent(failures);
                File journalDir = new File(DatabaseDescriptor.getAccordJournalDirectory(), "burn-" + config.name().toLowerCase());
                journalDir.tryCreateDirectories();
                AccordConfig.JournalConfig journalConfig = new AccordConfig.JournalConfig();
                journalConfig.flushPeriod = new DurationSpec.IntSecondsBound(1);
                AccordJournal journal = new AccordJournal(journalConfig, journalDir,
                                                          Keyspace.open(AccordKeyspace.metadata().name).getColumnFamilyStore(AccordKeyspace.JOURNAL));
                journal.start(null);

                AccordExecutor executor = config.build(i, agent);
                long start = nanoTime();
                State state = new State(config, failures, executor,
                                        id -> createAccordCommandStore(node, clock::incrementAndGet, topology, executor, journal, id, agent),
                                        TASKS_PER_CONFIG, CONCURRENCY, GENERATORS, COMMAND_STORES, clock, configDeadline);
                state.start();
                state.awaitCompletion(infra);
                state.shutdownAndVerify();
                teardownJournal(infra, config, journal);
                logger.info("=== {} finished: {} submitted, {} completed in {}ms ===",
                            config, state.submitted.get(), state.completed.get(),
                            TimeUnit.NANOSECONDS.toMillis(nanoTime() - start));

                if (!failures.isEmpty())
                {
                    logger.error("{} recorded failures; skipping remaining configs", config);
                    break;
                }
            }
        }
        finally
        {
            infra.shutdownNow();
        }

        if (!failures.isEmpty())
            Assert.fail(failures.render());
    }

    /**
     * Journal teardown is best-effort: the journal stop/close path has its own known defects
     * (tracked separately from this executor burn) and is not what this test measures, so a
     * teardown failure is logged loudly but does not fail the run.
     */
    private static void teardownJournal(ExecutorPlus infra, Config config, AccordJournal journal)
    {
        Future<?> stop = infra.submit(() -> {
            journal.stop();
            journal.close();
        });
        try
        {
            stop.get(30, SECONDS);
        }
        catch (Throwable t)
        {
            logger.warn("{}: journal teardown failed or timed out (out of scope for this burn test)", config, t);
            stop.cancel(true);
        }
    }
}
