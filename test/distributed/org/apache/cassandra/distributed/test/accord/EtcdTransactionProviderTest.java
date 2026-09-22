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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.consensus.txn.AccordTransactionProvider;
import org.apache.cassandra.service.consensus.txn.EtcdScalarRequest;
import org.apache.cassandra.service.consensus.txn.EtcdScalarResult;
import org.apache.cassandra.service.consensus.txn.EtcdScalarStore;
import org.apache.cassandra.service.consensus.txn.EtcdTestEnvironment;
import org.apache.cassandra.service.consensus.txn.EtcdTransactionProvider;
import org.apache.cassandra.service.consensus.txn.ExternalTransactionDomainBinding;
import org.apache.cassandra.service.consensus.txn.TransactionDomain;
import org.apache.cassandra.service.consensus.txn.TransactionDomainDescriptor;
import org.apache.cassandra.service.consensus.txn.TransactionExecutionContext;
import org.apache.cassandra.service.consensus.txn.TransactionPlan;
import org.apache.cassandra.service.consensus.txn.TransactionProvider;
import org.apache.cassandra.service.consensus.txn.TransactionProviderRegistry;
import org.apache.cassandra.service.consensus.txn.TransactionProviders;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.transformations.ActivateTransactionDomain;
import org.apache.cassandra.tcm.transformations.PrepareTransactionDomain;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Isolated;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Opt-in verification against a live three-member etcd cluster. */
public class EtcdTransactionProviderTest
{
    private static final String KEYSPACE = "etcd_scalar_test";
    private static final String RUN = "RUN_ETCD_DTESTS";
    private static final String ENDPOINTS = "ETCD_ENDPOINTS";

    @Test
    public void cqlUsesEtcdProviderAtBothCoordinators() throws Exception
    {
        requireOptIn();
        try (Cluster cluster = newCluster())
        {
            Installed installed = install(cluster);
            try
            {
                String table = KEYSPACE + '.' + installed.tableName;
                Object[][] first = cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                                                   "  UPDATE " + table + " SET v = 7 WHERE k = 1;\n" +
                                                                   "COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                assertEquals(null, first);
                Object[][] second = cluster.coordinator(2).execute("BEGIN TRANSACTION\n" +
                                                                    "  SELECT v FROM " + table + " WHERE k = 1;\n" +
                                                                    "COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                assertArrayEquals(new Object[]{ 7 }, second[0]);

                Object[][] copied = cluster.coordinator(2).execute("BEGIN TRANSACTION\n" +
                        "LET source = (SELECT v FROM " + table + " WHERE k = 1);\n" +
                        "SELECT source.v;\n" +
                        "IF source.v = 7 THEN UPDATE " + table + " SET v = source.v WHERE k = 2; END IF\n" +
                        "COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                assertArrayEquals(new Object[]{ 7 }, copied[0]);
                Object[][] copiedRow = cluster.coordinator(1).execute("BEGIN TRANSACTION SELECT v FROM " + table +
                        " WHERE k = 2; COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                assertArrayEquals(new Object[]{ 7 }, copiedRow[0]);
                cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                        "LET source = (SELECT v FROM " + table + " WHERE k = 1);\n" +
                        "IF source.v = 99 THEN UPDATE " + table + " SET v = 8 WHERE k = 1; END IF\n" +
                        "COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                Object[][] unchanged = cluster.coordinator(2).execute("BEGIN TRANSACTION SELECT v FROM " + table +
                        " WHERE k = 1; COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                assertArrayEquals(new Object[]{ 7 }, unchanged[0]);

                assertRejected(() -> cluster.coordinator(1).execute("SELECT * FROM " + table + " WHERE k = 1", ConsistencyLevel.ONE));
                assertRejected(() -> cluster.coordinator(2).execute("UPDATE " + table + " SET v = 8 WHERE k = 1", ConsistencyLevel.ONE));
                assertRejected(() -> cluster.coordinator(1).execute("INSERT INTO " + table + " (k, v) VALUES (2, 2) IF NOT EXISTS", ConsistencyLevel.ONE));
            }
            finally
            {
                restore(cluster);
            }
        }
    }

    @Test
    public void receiptSurvivesCoordinatorShutdownAndRejectsMismatch() throws Exception
    {
        requireOptIn();
        try (Cluster cluster = newCluster())
        {
            Installed installed = install(cluster);
            try
            {
                String query = "BEGIN TRANSACTION UPDATE " + KEYSPACE + '.' + installed.tableName +
                               " SET v = 33 WHERE k = 3; COMMIT TRANSACTION;";
                UUID requestId = UUID.randomUUID();
                byte[] encoded = cluster.get(1).callOnInstance(() -> AdapterState.compile(query, requestId));
                EtcdScalarRequest request = EtcdScalarRequest.decode(encoded);
                // The coordinator executes successfully, but the caller discards the response.
                cluster.get(1).runOnInstance(() -> AdapterState.submit(encoded));
                cluster.get(1).shutdown().get();
                EtcdScalarResult original = lookup(cluster.get(2), encoded);
                assertTrue(original.known);
                assertTrue(original.conditionMet);
                long position = original.decisionPosition;

                EtcdScalarResult replay = lookup(cluster.get(2), encoded);
                assertTrue(replay.known);
                assertEquals(position, replay.decisionPosition);
                assertEquals(original.atMicros, replay.atMicros);

                EtcdScalarRequest later = request(installed, UUID.randomUUID(),
                                                  Collections.emptyList(), Collections.emptyList(),
                                                  Collections.singletonList(EtcdScalarRequest.Write.constant(3, 66)));
                byte[] laterEncoded = later.encode();
                submit(cluster.get(2), laterEncoded);
                EtcdScalarResult replayAfterLaterWrite = lookup(cluster.get(2), encoded);
                assertEquals(position, replayAfterLaterWrite.decisionPosition);
                assertArrayEquals(original.encode(), submit(cluster.get(2), encoded).encode());
                Object[][] current = cluster.coordinator(2).execute("BEGIN TRANSACTION SELECT v FROM " + KEYSPACE + '.' +
                        installed.tableName + " WHERE k = 3; COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                assertArrayEquals(new Object[]{ 66 }, current[0]);

                EtcdScalarRequest mismatch = request(installed, request.requestId,
                                                      Collections.emptyList(), Collections.emptyList(),
                                                      Collections.singletonList(EtcdScalarRequest.Write.constant(3, 34)));
                mismatch = new EtcdScalarRequest(mismatch.domainId, mismatch.tableId, mismatch.requestId,
                                                 mismatch.generation, mismatch.schemaEpoch, mismatch.binding,
                                                 mismatch.reads, mismatch.conditions, mismatch.writes, request.returnShape);
                byte[] mismatchEncoded = mismatch.encode();
                assertThrows(() -> submit(cluster.get(2), mismatchEncoded), "request id was reused with different content");
            }
            finally
            {
                restore(cluster);
            }
        }
    }

    @Test
    public void falseConditionAndReadOnlyReceiptsAreRetained() throws Exception
    {
        requireOptIn();
        try (Cluster cluster = newCluster())
        {
            Installed installed = install(cluster);
            try
            {
                EtcdScalarRequest seed = request(installed, UUID.randomUUID(), Collections.emptyList(), Collections.emptyList(),
                                                 Collections.singletonList(EtcdScalarRequest.Write.constant(4, 44)));
                byte[] seedEncoded = seed.encode();
                submit(cluster.get(1), seedEncoded);

                EtcdScalarRequest read = request(installed, UUID.randomUUID(),
                                                 Collections.singletonList(new EtcdScalarRequest.Read(1, 4)),
                                                 Collections.emptyList(), Collections.emptyList());
                byte[] readEncoded = read.encode();
                EtcdScalarResult readResult = submit(cluster.get(2), readEncoded);
                assertTrue(readResult.known);
                assertTrue(readResult.conditionMet);
                assertEquals(Integer.valueOf(44), readResult.reads.get(1).value);
                assertEquals(readResult.decisionPosition,
                             lookup(cluster.get(1), readEncoded).decisionPosition);

                EtcdScalarRequest falseRequest = request(installed, UUID.randomUUID(),
                                                          Collections.singletonList(new EtcdScalarRequest.Read(2, 4)),
                                                          Collections.singletonList(new EtcdScalarRequest.Condition(2,
                                                                                                                       false,
                                                                                                                       EtcdScalarRequest.Operator.EQ,
                                                                                                                       99)),
                                                          Collections.singletonList(EtcdScalarRequest.Write.constant(5, 55)));
                byte[] falseEncoded = falseRequest.encode();
                EtcdScalarResult falseResult = submit(cluster.get(1), falseEncoded);
                assertTrue(falseResult.known);
                assertFalse(falseResult.conditionMet);
                assertEquals(Integer.valueOf(44), falseResult.reads.get(2).value);
                assertEquals(falseResult.decisionPosition, lookup(cluster.get(2), falseEncoded).decisionPosition);
            }
            finally
            {
                restore(cluster);
            }
        }
    }

    @Test
    public void staleGenerationIsRejectedByBackend() throws Exception
    {
        requireOptIn();
        try (Cluster cluster = newCluster())
        {
            Installed installed = install(cluster);
            try
            {
                String domainId = installed.domainId.toString();
                cluster.get(1).runOnInstance(() -> AdapterState.fence(domainId, 1, 2));
                EtcdScalarRequest stale = request(installed, UUID.randomUUID(), Collections.emptyList(), Collections.emptyList(),
                                                  Collections.singletonList(EtcdScalarRequest.Write.constant(8, 88)));
                byte[] staleEncoded = stale.encode();
                assertThrows(() -> submit(cluster.get(2), staleEncoded), "request does not match active etcd owner");
            }
            finally
            {
                restore(cluster);
            }
        }
    }

    private static Cluster newCluster() throws IOException
    {
        Cluster cluster = Cluster.build(2).withoutVNodes().withConfig(c -> c.set("accord.enabled", false)).start();
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}");
        return cluster;
    }

    private static Installed install(Cluster cluster)
    {
        String tableName = "scalar_" + UUID.randomUUID().toString().replace('-', '_');
        UUID domainId = UUID.randomUUID();
        UUID groupId = UUID.randomUUID();
        cluster.get(1).runOnInstance(() -> {
            ClusterMetadata current = ClusterMetadata.current();
            Set<NodeId> participants = current.directory.states.entrySet().stream()
                                                           .filter(entry -> entry.getValue() == NodeState.JOINED)
                                                           .map(Map.Entry::getKey)
                                                           .collect(Collectors.toCollection(HashSet::new));
            TableId tableId = TableId.generate();
            TableMetadata table = TableMetadata.builder(KEYSPACE, tableName, tableId)
                                               .addPartitionKeyColumn("k", Int32Type.instance)
                                               .addRegularColumn("v", Int32Type.instance)
                                               .build();
            TransactionDomainDescriptor descriptor = new TransactionDomainDescriptor(domainId, tableId, "etcd-scalar",
                                                                                       PrepareTransactionDomain.PROFILE_ID, 1, 1, 1, 1, participants);
            ClusterMetadataService.instance().commit(new PrepareTransactionDomain(table, descriptor));
        });
        waitForMetadata(cluster, tableName, domainId);
        ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
        String[] endpoints = configuredEndpoints();
        String endpointCsv = String.join(",", endpoints);
        String clusterId = cluster.get(1).callOnInstance(() -> {
            AdapterState.open(endpointCsv);
            return AdapterState.store.clusterId();
        });
        ExternalTransactionDomainBinding binding = new ExternalTransactionDomainBinding(groupId.toString(), clusterId);
        long schemaEpoch = tableEpoch(cluster, tableName);
        String bindingGroupId = binding.groupId();
        String bindingClusterId = binding.clusterId();
        cluster.get(1).runOnInstance(() -> AdapterState.prepare(tableName, bindingGroupId, bindingClusterId, schemaEpoch));
        cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().commit(new ActivateTransactionDomain(
                Schema.instance.getTableMetadata(KEYSPACE, tableName).id,
                domainId,
                1,
                new ExternalTransactionDomainBinding(bindingGroupId, bindingClusterId))));
        waitForState(cluster, tableName, TransactionDomainDescriptor.State.ACTIVE);
        ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
        for (IInvokableInstance instance : cluster)
        {
            instance.runOnInstance(() -> AdapterState.open(endpointCsv));
            instance.runOnInstance(() -> AdapterState.activate(tableName, bindingGroupId, bindingClusterId));
        }
        return new Installed(tableName, domainId, tableId(cluster, tableName), binding, schemaEpoch);
    }

    private static void waitForMetadata(Cluster cluster, String table, UUID domain)
    {
        cluster.get(1).callOnInstance(() -> {
            assertNotNull(ClusterMetadata.current().consistencyDomains.forTable(Schema.instance.getTableMetadata(KEYSPACE, table).id));
            return null;
        });
    }

    private static void waitForState(Cluster cluster, String table, TransactionDomainDescriptor.State state)
    {
        cluster.get(1).callOnInstance(() -> {
            assertEquals(state, ClusterMetadata.current().consistencyDomains.forTable(Schema.instance.getTableMetadata(KEYSPACE, table).id).state());
            return null;
        });
    }

    private static TableId tableId(Cluster cluster, String table)
    {
        return TableId.fromString(cluster.get(1).callOnInstance(() -> Schema.instance.getTableMetadata(KEYSPACE, table).id.toString()));
    }

    private static long tableEpoch(Cluster cluster, String table)
    {
        return cluster.get(1).callOnInstance(() -> Schema.instance.getTableMetadata(KEYSPACE, table).epoch.getEpoch());
    }

    private static void restore(Cluster cluster)
    {
        for (IInvokableInstance instance : cluster)
        {
            if (!instance.isShutdown())
                instance.runOnInstance(AdapterState::restore);
        }
    }

    private static EtcdScalarRequest request(Installed i, UUID id, java.util.List<EtcdScalarRequest.Read> reads,
                                             java.util.List<EtcdScalarRequest.Condition> conditions,
                                             java.util.List<EtcdScalarRequest.Write> writes)
    {
        return new EtcdScalarRequest(i.domainId, i.tableId, id, 1, i.schemaEpoch, i.binding, reads, conditions, writes);
    }

    private static String[] configuredEndpoints()
    {
        String value = EtcdTestEnvironment.get(ENDPOINTS);
        if (value == null || value.trim().isEmpty())
            throw new AssertionError(ENDPOINTS + " must contain comma-separated etcd endpoints when " + RUN + "=true");
        return Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
    }

    private static void requireOptIn()
    {
        if (!"true".equalsIgnoreCase(EtcdTestEnvironment.get(RUN)))
            throw new org.junit.internal.AssumptionViolatedException("Set " + RUN + "=true to run the live etcd distributed tests");
    }

    private static void assertRejected(Runnable operation)
    {
        assertThrows(operation, "ACTIVE (closed)");
    }

    private static void assertThrows(Runnable operation, String expected)
    {
        Throwable failure = null;
        try
        {
            operation.run();
        }
        catch (Throwable t)
        {
            failure = t;
        }
        if (failure == null)
            fail("Expected operation to fail with " + expected);
        String message = String.valueOf(failure);
        for (Throwable cause = failure.getCause(); cause != null; cause = cause.getCause())
            message += " " + cause;
        assertTrue(message.toLowerCase().contains(expected.toLowerCase()));
    }

    private static EtcdScalarResult submit(IInvokableInstance node, byte[] encoded)
    {
        return EtcdScalarResult.decode(node.callOnInstance(() -> AdapterState.submit(encoded)));
    }

    private static EtcdScalarResult lookup(IInvokableInstance node, byte[] encoded)
    {
        return EtcdScalarResult.decode(node.callOnInstance(() -> AdapterState.lookup(encoded)));
    }

    private static final class Installed
    {
        final String tableName; final UUID domainId; final TableId tableId; final ExternalTransactionDomainBinding binding; final long schemaEpoch;
        Installed(String tableName,
                  UUID domainId,
                  TableId tableId,
                  ExternalTransactionDomainBinding binding,
                  long schemaEpoch)
        {
            this.tableName = tableName;
            this.domainId = domainId;
            this.tableId = tableId;
            this.binding = binding;
            this.schemaEpoch = schemaEpoch;
        }
    }

    @Isolated
    public static final class AdapterState
    {
        private static EtcdScalarStore store;
        private static EtcdTransactionProvider provider;
        private static TransactionProviderRegistry previous;

        private AdapterState() { }
        static void open(String endpointCsv)
        {
            store = new EtcdScalarStore(Arrays.asList(endpointCsv.split(",")), 5000);
        }
        static void prepare(String table, String groupId, String clusterId, long schemaEpoch)
        {
            TransactionDomainDescriptor descriptor = ClusterMetadata.current().consistencyDomains
                                                             .forTable(Schema.instance.getTableMetadata(KEYSPACE, table).id);
            ExternalTransactionDomainBinding binding = new ExternalTransactionDomainBinding(groupId, clusterId);
            store.prepare(descriptor, binding, schemaEpoch);
        }
        static void activate(String table, String groupId, String clusterId)
        {
            TransactionDomainDescriptor descriptor = ClusterMetadata.current().consistencyDomains
                                                             .forTable(Schema.instance.getTableMetadata(KEYSPACE, table).id);
            store.activate(descriptor, Schema.instance.getTableMetadata(KEYSPACE, table).epoch.getEpoch());
            provider = new EtcdTransactionProvider(store);
            Map<String, TransactionProvider> providers = new HashMap<>();
            providers.put(AccordTransactionProvider.ID, AccordTransactionProvider.INSTANCE);
            providers.put(provider.id(), provider);
            TransactionProviderRegistry registry = new TransactionProviderRegistry(providers, Collections.emptyMap());
            previous = TransactionProviders.unsafeSetRegistryForTesting(registry);
        }
        static byte[] compile(String query, UUID requestId)
        {
            TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
            TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
            QueryOptions options = QueryProcessor.makeInternalOptions(statement, new Object[0]);
            TransactionPlan plan = statement.toPlan(ClientState.forInternalCalls(), options);
            TransactionDomainDescriptor descriptor = ClusterMetadata.current().consistencyDomains
                                                                     .forTable(plan.tables().keySet().iterator().next());
            TransactionExecutionContext context = new TransactionExecutionContext(
                    new TransactionDomain(descriptor.id().toString(), provider.id()),
                    org.apache.cassandra.db.ConsistencyLevel.ONE, org.apache.cassandra.db.ConsistencyLevel.SERIAL,
                    ProtocolVersion.CURRENT, Dispatcher.RequestTime.forImmediateExecution());
            return provider.compile(plan, context, requestId).encode();
        }

        static byte[] submit(byte[] encoded) { return provider.submit(EtcdScalarRequest.decode(encoded)).encode(); }
        static byte[] lookup(byte[] encoded) { return provider.lookup(EtcdScalarRequest.decode(encoded)).encode(); }
        static void fence(String domain, long oldGeneration, long newGeneration) { store.fence(UUID.fromString(domain), oldGeneration, newGeneration); }
        static void restore()
        {
            if (previous != null)
                TransactionProviders.unsafeSetRegistryForTesting(previous);
            previous = null;
            provider = null;
            store = null;
        }
    }
}
