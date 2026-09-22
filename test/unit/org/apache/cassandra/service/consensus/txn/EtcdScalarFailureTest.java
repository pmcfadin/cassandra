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

package org.apache.cassandra.service.consensus.txn;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.membership.NodeId;

/** Failure and durability checks against the externally provisioned etcd cluster. */
public class EtcdScalarFailureTest
{
    private static final String ENDPOINTS = "ETCD_ENDPOINTS";
    private static final String MANIFEST = "ETCD_MANIFEST";
    private static final String SCRIPT = "ETCD_CLUSTER_SCRIPT";
    private static final long SCHEMA_EPOCH = 1;
    private static final int TIMEOUT_MILLIS = 3000;

    private EtcdScalarStore store;
    private Domain domain;

    @Before
    public void before()
    {
        Assume.assumeTrue("set RUN_ETCD_TESTS=true to run the external etcd tests",
                          "true".equalsIgnoreCase(EtcdTestEnvironment.get("RUN_ETCD_TESTS")));
        if (!present(ENDPOINTS) || !present(MANIFEST) || !present(SCRIPT))
            throw new IllegalStateException("ETCD_ENDPOINTS, ETCD_MANIFEST and ETCD_CLUSTER_SCRIPT are required when RUN_ETCD_TESTS=true");
        store = new EtcdScalarStore(endpoints(), TIMEOUT_MILLIS);
        domain = newDomain(store);
    }

    @After
    public void after()
    {
        if (store == null)
            return;
        try
        {
            harness("start");
            waitForHarness();
        }
        catch (Exception ignored)
        {
            // Preserve the original failure; the harness is best-effort cleanup.
        }
    }

    @Test
    public void leaderLossRetainsOriginalReceiptAfterMajorityRecovery() throws Exception
    {
        EtcdScalarRequest request = writeRequest(UUID.randomUUID(), 1, 11, 1);
        EtcdScalarResult original = store.submit(request);

        harness("kill");
        waitForHarness();
        waitForReceipt(request);

        EtcdScalarResult replay = store.lookup(request);
        assertSameReceipt(original, replay);
        assertSameReceipt(original, store.submit(request));
        EtcdScalarResult read = store.submit(readRequest(UUID.randomUUID(), 1, 2));
        Assertions.assertThat(read.reads.get(2).value).isEqualTo(11);
    }

    @Test
    public void majorityLossCannotCreateAReceiptAndSameIdentityResolvesAfterRestore() throws Exception
    {
        EtcdScalarRequest request = writeRequest(UUID.randomUUID(), 2, 22, 2);
        harness("kill", "m1");
        harness("kill", "m2");
        try
        {
            try
            {
                Assertions.assertThat(store.submit(request).known).isFalse();
            }
            catch (EtcdScalarStore.UnknownOutcomeException expected)
            {
                // The transport may report the write as explicitly unknown.
            }
        }
        finally
        {
            harness("start");
            waitForHarness();
        }

        waitForHarness();
        EtcdScalarResult resolved = store.submit(request);
        Assertions.assertThat(resolved.known).isTrue();
        assertSameReceipt(resolved, store.lookup(request));
    }

    @Test
    public void fullGroupRestartPreservesStateAndOriginalOutcome() throws Exception
    {
        EtcdScalarRequest request = writeRequest(UUID.randomUUID(), 3, 33, 3);
        EtcdScalarResult original = store.submit(request);

        harness("kill", "m1");
        harness("kill", "m2");
        harness("kill", "m3");
        harness("start");
        waitForHarness();
        waitForReceipt(request);

        assertSameReceipt(original, store.lookup(request));
        assertSameReceipt(original, store.submit(request));
        EtcdScalarResult read = store.submit(readRequest(UUID.randomUUID(), 3, 4));
        Assertions.assertThat(read.reads.get(4).value).isEqualTo(33);
    }

    @Test
    public void compactionPreservesRetainedReceipts() throws Exception
    {
        EtcdScalarRequest request = writeRequest(UUID.randomUUID(), 4, 44, 4);
        EtcdScalarResult original = store.submit(request);

        harness("compact");
        assertSameReceipt(original, store.lookup(request));
    }

    @Test
    public void receiptCapacityRejectsThe129thRequestAndRetainsKnownDuplicates() throws Exception
    {
        List<EtcdScalarRequest> requests = new ArrayList<>();
        List<EtcdScalarResult> receipts = new ArrayList<>();
        for (int i = 0; i < 128; i++)
        {
            EtcdScalarRequest request = writeRequest(UUID.randomUUID(), i, i, i);
            requests.add(request);
            receipts.add(store.submit(request));
        }
        for (int i = 0; i < requests.size(); i++)
            assertSameReceipt(receipts.get(i), store.submit(requests.get(i)));

        EtcdScalarRequest overflow = writeRequest(UUID.randomUUID(), 127, 1000, 1000);
        Assertions.assertThatThrownBy(() -> store.submit(overflow))
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessageContaining("receipt capacity");
    }

    @Test
    public void concurrentSameIdentityExecutesOnceAndRetainsFalseReadOnlyOutcome() throws Exception
    {
        EtcdScalarRequest request = writeRequest(UUID.randomUUID(), 5, 55, 5);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try
        {
            List<Future<EtcdScalarResult>> futures = new ArrayList<>();
            for (int i = 0; i < 16; i++)
                futures.add(executor.submit(() -> store.submit(request)));
            List<EtcdScalarResult> results = new ArrayList<>();
            for (Future<EtcdScalarResult> future : futures)
                results.add(future.get(10, TimeUnit.SECONDS));
            for (EtcdScalarResult result : results)
            {
                Assertions.assertThat(result.known).isTrue();
                Assertions.assertThat(result.conditionMet).isTrue();
                Assertions.assertThat(result.decisionPosition).isEqualTo(1);
                Assertions.assertThat(result.decisionPosition).isEqualTo(results.get(0).decisionPosition);
            }
            assertSameReceipt(results.get(0), store.lookup(request));
            EtcdScalarResult later = store.submit(writeRequest(UUID.randomUUID(), 5, 66, 7));
            Assertions.assertThat(later.decisionPosition).isEqualTo(2);
            assertSameReceipt(results.get(0), store.submit(request));
            Assertions.assertThat(store.submit(readRequest(UUID.randomUUID(), 5, 6)).reads.get(6).value).isEqualTo(66);
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    @Test
    public void staleGenerationIsRejectedAfterBackendFence()
    {
        store.fence(domain.id, domain.generation, domain.generation + 1);
        EtcdScalarRequest stale = writeRequest(UUID.randomUUID(), 6, 66, 6);
        Assertions.assertThatThrownBy(() -> store.submit(stale))
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessageContaining("active etcd owner");
    }

    @Test
    public void falseConditionAndReadOnlyReceiptsRetainOriginalOutcomeAfterOverwrite()
    {
        store.submit(writeRequest(UUID.randomUUID(), 40, 7, 8));
        EtcdScalarRequest falseRequest = conditionalRequest(UUID.randomUUID(), 40, 8, 9, true);
        EtcdScalarResult falseResult = store.submit(falseRequest);
        Assertions.assertThat(falseResult.conditionMet).isFalse();
        store.submit(writeRequest(UUID.randomUUID(), 40, 9, 10));
        assertSameReceipt(falseResult, store.submit(falseRequest));
        Assertions.assertThat(store.submit(readRequest(UUID.randomUUID(), 41, 13)).reads.get(13).present).isFalse();

        EtcdScalarRequest readOnly = readRequest(UUID.randomUUID(), 40, 11);
        EtcdScalarResult readResult = store.submit(readOnly);
        Assertions.assertThat(readResult.reads.get(11).value).isEqualTo(9);
        store.submit(writeRequest(UUID.randomUUID(), 40, 10, 12));
        assertSameReceipt(readResult, store.submit(readOnly));
    }

    @Test
    public void concurrentDifferentIdentitiesSerializeConditionalUpdates() throws Exception
    {
        store.submit(writeRequest(UUID.randomUUID(), 50, 7, 20));
        EtcdScalarRequest first = conditionalSameKey(UUID.randomUUID(), 50, 7, 8, 21);
        EtcdScalarRequest second = conditionalSameKey(UUID.randomUUID(), 50, 7, 9, 22);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try
        {
            Future<EtcdScalarResult> firstResult = executor.submit(() -> store.submit(first));
            Future<EtcdScalarResult> secondResult = executor.submit(() -> store.submit(second));
            EtcdScalarResult firstReceipt = firstResult.get(10, TimeUnit.SECONDS);
            EtcdScalarResult secondReceipt = secondResult.get(10, TimeUnit.SECONDS);
            Assertions.assertThat(firstReceipt.known).isTrue();
            Assertions.assertThat(secondReceipt.known).isTrue();
            Assertions.assertThat(firstReceipt.decisionPosition).isNotEqualTo(secondReceipt.decisionPosition);
            Assertions.assertThat(firstReceipt.conditionMet ^ secondReceipt.conditionMet).isTrue();
            EtcdScalarResult winnerReceipt = firstReceipt.conditionMet ? firstReceipt : secondReceipt;
            EtcdScalarResult losingReceipt = firstReceipt.conditionMet ? secondReceipt : firstReceipt;
            Assertions.assertThat(winnerReceipt.reads.values().iterator().next().value).isEqualTo(7);
            Assertions.assertThat(losingReceipt.reads.values().iterator().next().value).isIn(8, 9);

            EtcdScalarRequest losingRequest = firstReceipt.conditionMet ? second : first;
            Assertions.assertThat(losingReceipt.conditionMet).isFalse();
            Assertions.assertThat(store.submit(readRequest(UUID.randomUUID(), 50, 23)).reads.get(23).value)
                      .isIn(8, 9);
            store.submit(writeRequest(UUID.randomUUID(), 50, 99, 24));
            assertSameReceipt(losingReceipt, store.submit(losingRequest));
            Assertions.assertThat(store.submit(readRequest(UUID.randomUUID(), 50, 25)).reads.get(25).value)
                      .isEqualTo(99);
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    @Test
    public void absentReferenceRejectsBeforePublishingEarlierMultiwrite()
    {
        EtcdScalarRequest request = new EtcdScalarRequest(domain.id,
                                                           domain.tableId,
                                                           UUID.randomUUID(),
                                                           domain.generation,
                                                           SCHEMA_EPOCH,
                                                           domain.binding,
                                                           Collections.singletonList(new EtcdScalarRequest.Read(30, 60)),
                                                           Collections.emptyList(),
                                                           List.of(EtcdScalarRequest.Write.constant(61, 1),
                                                                   EtcdScalarRequest.Write.reference(62, 30)));
        Assertions.assertThatThrownBy(() -> store.submit(request))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("absent row");
        Assertions.assertThat(store.lookup(request).known).isFalse();
        Assertions.assertThat(store.submit(readRequest(UUID.randomUUID(), 61, 31)).reads.get(31).present).isFalse();
        Assertions.assertThat(store.submit(readRequest(UUID.randomUUID(), 62, 32)).reads.get(32).present).isFalse();
    }

    private EtcdScalarRequest writeRequest(UUID requestId, int key, int value, int slot)
    {
        return new EtcdScalarRequest(domain.id,
                                     domain.tableId,
                                     requestId,
                                     domain.generation,
                                     SCHEMA_EPOCH,
                                     domain.binding,
                                     Collections.singletonList(new EtcdScalarRequest.Read(slot, key)),
                                     Collections.emptyList(),
                                     Collections.singletonList(EtcdScalarRequest.Write.constant(key, value)));
    }

    private EtcdScalarRequest readRequest(UUID requestId, int key, int slot)
    {
        return new EtcdScalarRequest(domain.id, domain.tableId, requestId, domain.generation, SCHEMA_EPOCH,
                                     domain.binding,
                                     Collections.singletonList(new EtcdScalarRequest.Read(slot, key)),
                                     Collections.emptyList(), Collections.emptyList());
    }

    private EtcdScalarRequest conditionalRequest(UUID requestId, int key, int value, int slot, boolean write)
    {
        return new EtcdScalarRequest(domain.id, domain.tableId, requestId, domain.generation, SCHEMA_EPOCH,
                                     domain.binding,
                                     Collections.singletonList(new EtcdScalarRequest.Read(slot, key)),
                                     Collections.singletonList(new EtcdScalarRequest.Condition(slot, false,
                                                                                              EtcdScalarRequest.Operator.EQ, value)),
                                     write ? Collections.singletonList(EtcdScalarRequest.Write.constant(key + 1, value))
                                           : Collections.emptyList());
    }

    private EtcdScalarRequest conditionalSameKey(UUID requestId, int key, int expected, int value, int slot)
    {
        return new EtcdScalarRequest(domain.id, domain.tableId, requestId, domain.generation, SCHEMA_EPOCH,
                                     domain.binding,
                                     Collections.singletonList(new EtcdScalarRequest.Read(slot, key)),
                                     Collections.singletonList(new EtcdScalarRequest.Condition(slot, false,
                                                                                              EtcdScalarRequest.Operator.EQ, expected)),
                                     Collections.singletonList(EtcdScalarRequest.Write.constant(key, value)));
    }

    private static Domain newDomain(EtcdScalarStore store)
    {
        UUID id = UUID.randomUUID();
        TableId tableId = TableId.fromUUID(UUID.randomUUID());
        ExternalTransactionDomainBinding binding = new ExternalTransactionDomainBinding(id.toString(), store.clusterId());
        TransactionDomainDescriptor prepared = new TransactionDomainDescriptor(id,
                                                                                tableId,
                                                                                EtcdScalarRequest.PROVIDER,
                                                                                EtcdScalarRequest.PROFILE,
                                                                                1,
                                                                                1,
                                                                                1,
                                                                                1,
                                                                                Set.of(new NodeId(1), new NodeId(2), new NodeId(3)));
        store.prepare(prepared, binding, SCHEMA_EPOCH);
        TransactionDomainDescriptor active = prepared.activate(binding);
        store.activate(active, SCHEMA_EPOCH);
        return new Domain(id, tableId, binding, 1);
    }

    private void assertSameReceipt(EtcdScalarResult expected, EtcdScalarResult actual)
    {
        Assertions.assertThat(expected.known).isTrue();
        Assertions.assertThat(actual.known).isTrue();
        Assertions.assertThat(actual.conditionMet).isEqualTo(expected.conditionMet);
        Assertions.assertThat(actual.atMicros).isEqualTo(expected.atMicros);
        Assertions.assertThat(actual.decisionPosition).isEqualTo(expected.decisionPosition);
        Assertions.assertThat(actual.reads).isEqualTo(expected.reads);
    }

    private void waitForHarness() throws Exception
    {
        for (int i = 0; i < 40; i++)
        {
            try
            {
                harness("status");
                store.clusterId();
                return;
            }
            catch (RuntimeException | IOException e)
            {
                Thread.sleep(250);
            }
        }
        throw new IOException("etcd harness did not recover");
    }

    private void waitForReceipt(EtcdScalarRequest request) throws Exception
    {
        for (int i = 0; i < 40; i++)
        {
            if (store.lookup(request).known)
                return;
            Thread.sleep(250);
        }
        throw new IOException("etcd receipt did not become readable");
    }

    private static void harness(String... arguments) throws Exception
    {
        List<String> command = new ArrayList<>();
        command.add("python3");
        command.add(EtcdTestEnvironment.get(SCRIPT));
        command.add("--manifest");
        command.add(EtcdTestEnvironment.get(MANIFEST));
        Collections.addAll(command, arguments);
        Path output = Files.createTempFile("etcd-harness-", ".log");
        try
        {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
            if (!process.waitFor(30, TimeUnit.SECONDS))
            {
                process.destroyForcibly();
                throw new IOException("etcd harness command timed out");
            }
            if (process.exitValue() != 0)
                throw new IOException("etcd harness command failed: " + Files.readString(output, StandardCharsets.UTF_8));
        }
        finally
        {
            Files.deleteIfExists(output);
        }
    }

    private static List<String> endpoints()
    {
        String value = EtcdTestEnvironment.get(ENDPOINTS);
        List<String> endpoints = new ArrayList<>();
        for (String endpoint : value.split(","))
            if (!endpoint.trim().isEmpty())
                endpoints.add(endpoint.trim());
        return endpoints;
    }

    private static boolean present(String name)
    {
        return EtcdTestEnvironment.get(name) != null && !EtcdTestEnvironment.get(name).trim().isEmpty();
    }

    private static final class Domain
    {
        private final UUID id;
        private final TableId tableId;
        private final ExternalTransactionDomainBinding binding;
        private final long generation;

        private Domain(UUID id, TableId tableId, ExternalTransactionDomainBinding binding, long generation)
        {
            this.id = id;
            this.tableId = tableId;
            this.binding = binding;
            this.generation = generation;
        }
    }
}
