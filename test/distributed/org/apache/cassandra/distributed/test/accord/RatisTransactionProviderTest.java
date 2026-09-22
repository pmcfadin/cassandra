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
import org.apache.cassandra.service.consensus.txn.ExternalTransactionDomainBinding;
import org.apache.cassandra.service.consensus.txn.RatisScalarRequest;
import org.apache.cassandra.service.consensus.txn.RatisScalarResult;
import org.apache.cassandra.service.consensus.txn.RatisScalarStore;
import org.apache.cassandra.service.consensus.txn.RatisTestEnvironment;
import org.apache.cassandra.service.consensus.txn.RatisTransactionProvider;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Opt-in CQL verification against one live three-member Ratis group. */
public class RatisTransactionProviderTest
{
    private static final String KEYSPACE = "ratis_scalar_test";
    private static final String RUN = "RUN_RATIS_DTESTS";
    private static final String ENDPOINTS = "RATIS_ENDPOINTS";
    private static final String GROUP = "RATIS_GROUP";

    @Test
    public void cqlLifecycleAndRetryAcrossCoordinators() throws Exception
    {
        requireOptIn();
        try (Cluster cluster = newCluster())
        {
            Installed installed = prepare(cluster);
            try
            {
                // A reservation is closed until the committed Ratis attestation is activated.
                assertRejected(() -> cluster.coordinator(1).execute("SELECT * FROM " + installed.table + " WHERE k = 1", ConsistencyLevel.ONE),
                              "reserved", "closed", "transaction");
                assertRejected(() -> cluster.coordinator(2).execute("BEGIN TRANSACTION UPDATE " + installed.table +
                                                                     " SET v = 1 WHERE k = 1; COMMIT TRANSACTION;", ConsistencyLevel.ONE),
                              "reserved", "closed", "transaction");

                activate(cluster, installed);

                Object[][] write = cluster.coordinator(1).execute("BEGIN TRANSACTION UPDATE " + installed.table +
                                                                   " SET v = 7 WHERE k = 1; COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                assertEquals(null, write);
                Object[][] read = cluster.coordinator(2).execute("BEGIN TRANSACTION SELECT v FROM " + installed.table +
                                                                  " WHERE k = 1; COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                assertArrayEquals(new Object[]{ 7 }, read[0]);
                Object[][] reference = cluster.coordinator(2).execute("BEGIN TRANSACTION\n" +
                        "LET source = (SELECT v FROM " + installed.table + " WHERE k = 1);\n" +
                        "SELECT source.v;\n" +
                        "IF source.v = 7 THEN UPDATE " + installed.table + " SET v = source.v WHERE k = 2; END IF\n" +
                        "COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                assertArrayEquals(new Object[]{ 7 }, reference[0]);
                Object[][] projection = cluster.coordinator(1).execute("BEGIN TRANSACTION SELECT k, v FROM " + installed.table +
                                                                       " WHERE k = 2; COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                assertArrayEquals(new Object[]{ 2, 7 }, projection[0]);
                Object[][] falseCondition = cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                        "LET source = (SELECT v FROM " + installed.table + " WHERE k = 1);\n" +
                        "IF source.v = 99 THEN UPDATE " + installed.table + " SET v = 8 WHERE k = 1; END IF\n" +
                        "COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                assertEquals(null, falseCondition);
                Object[][] unchanged = cluster.coordinator(2).execute("BEGIN TRANSACTION SELECT v FROM " + installed.table +
                                                                       " WHERE k = 1; COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                assertArrayEquals(new Object[]{ 7 }, unchanged[0]);
                assertRejected(() -> cluster.coordinator(1).execute("SELECT * FROM " + installed.table + " WHERE k = 1", ConsistencyLevel.ONE),
                              "reserved", "transaction");
                assertRejected(() -> cluster.coordinator(2).execute("UPDATE " + installed.table + " SET v = 8 WHERE k = 1", ConsistencyLevel.ONE),
                              "reserved", "transaction");
                assertRejected(() -> cluster.coordinator(1).execute("INSERT INTO " + installed.table +
                                                                     " (k, v) VALUES (9, 9) IF NOT EXISTS", ConsistencyLevel.ONE),
                              "reserved", "transaction");

                // Binding identity is checked before a request reaches the group.
                String tableName = installed.tableName;
                byte[] stable = cluster.get(1).callOnInstance(() -> AdapterState.compile(tableName, UUID.randomUUID()));
                RatisScalarRequest stableRequest = RatisScalarRequest.decode(stable);
                RatisScalarRequest wrongGroup = new RatisScalarRequest(stableRequest.domainId, stableRequest.tableId,
                        stableRequest.requestId, stableRequest.generation, stableRequest.schemaEpoch, UUID.randomUUID().toString(),
                        stableRequest.atMicros, stableRequest.reads, stableRequest.conditions, stableRequest.writes,
                        stableRequest.returnShape);
                assertRejected(() -> submit(cluster.get(2), wrongGroup.encode()), "wrong group", "binding");

                // Compile and submit on A, deliberately lose its reply, then resolve on B after a new write.
                cluster.get(1).runOnInstance(() -> AdapterState.submit(stable));
                cluster.get(1).shutdown().get();
                RatisScalarResult original = lookup(cluster.get(2), stable);
                assertTrue(original.known);
                assertTrue(original.conditionMet);
                assertTrue(original.reads.containsValue(7));
                long position = original.decisionPosition;
                byte[] overwrite = cluster.get(2).callOnInstance(() -> AdapterState.compileOverwrite(tableName, UUID.randomUUID()));
                submit(cluster.get(2), overwrite);
                RatisScalarResult replay = lookup(cluster.get(2), stable);
                assertEquals(position, replay.decisionPosition);
                assertEquals(Integer.valueOf(7), replay.reads.values().stream().filter(value -> value != null).findFirst().orElse(null));
                assertArrayEquals(original.encode(), submit(cluster.get(2), stable).encode());
                Object[][] current = cluster.coordinator(2).execute("BEGIN TRANSACTION\n" +
                        "LET first = (SELECT v FROM " + installed.table + " WHERE k = 1);\n" +
                        "LET third = (SELECT v FROM " + installed.table + " WHERE k = 3);\n" +
                        "SELECT first.v, third.v;\n" +
                        "COMMIT TRANSACTION;", ConsistencyLevel.ONE);
                assertArrayEquals(new Object[]{ 88, 66 }, current[0]);

                // Fence is intentionally last: the stable request must become stale only after retry checks.
                cluster.get(2).runOnInstance(() -> AdapterState.fence(stable));
                RatisScalarResult retained = RatisScalarResult.decode(cluster.get(2).callOnInstance(() -> AdapterState.rawLookup(stable)));
                assertArrayEquals(original.encode(), retained.encode());
                assertRejected(() -> submit(cluster.get(2), stable), "not_active");
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

    private static Installed prepare(Cluster cluster)
    {
        String tableName = "scalar_" + UUID.randomUUID().toString().replace('-', '_');
        UUID domainId = UUID.randomUUID();
        String groupId = configuredGroup();
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
            TransactionDomainDescriptor descriptor = new TransactionDomainDescriptor(domainId, tableId,
                                                                                       RatisTransactionProvider.ID,
                                                                                       "scalar-int", 1, 1, 1, 1, participants);
            ClusterMetadataService.instance().commit(new PrepareTransactionDomain(table, descriptor));
        });
        waitForMetadata(cluster, tableName);
        ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
        long schemaEpoch = tableEpoch(cluster, tableName);
        return new Installed(tableName, KEYSPACE + '.' + tableName, domainId, tableId(cluster, tableName), groupId, schemaEpoch);
    }

    private static void activate(Cluster cluster, Installed installed)
    {
        String tableName = installed.tableName;
        String groupId = installed.groupId;
        String domainId = installed.domainId.toString();
        String tableId = installed.tableId.toString();
        long schemaEpoch = installed.schemaEpoch;
        cluster.get(1).runOnInstance(AdapterState::open);
        String wrongGroup = UUID.randomUUID().toString();
        assertRejected(() -> cluster.get(1).runOnInstance(() -> AdapterState.attest(tableName, schemaEpoch, wrongGroup)),
                       "attestation", "owner", "rejected", "group");
        String[] attestation = cluster.get(1).callOnInstance(() -> AdapterState.prepareAndAttest(tableName, schemaEpoch, groupId));
        String attestedGroup = attestation[0];
        long attestedEpoch = Long.parseLong(attestation[1]);
        long readinessIndex = Long.parseLong(attestation[2]);
        cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().commit(new ActivateTransactionDomain(
                Schema.instance.getTableMetadata(KEYSPACE, tableName).id, UUID.fromString(domainId), 1,
                ExternalTransactionDomainBinding.ratis(attestedGroup, attestedEpoch, readinessIndex))));
        waitForState(cluster, tableName, TransactionDomainDescriptor.State.ACTIVE);
        ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
        for (IInvokableInstance instance : cluster)
            instance.runOnInstance(() -> AdapterState.activate(tableName, groupId));
        assertTrue(cluster.get(1).callOnInstance(() -> ClusterMetadata.current().consistencyDomains.forTable(TableId.fromString(tableId)) != null));
    }

    private static void waitForMetadata(Cluster cluster, String table)
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

    private static String[] configuredEndpoints()
    {
        String value = RatisTestEnvironment.get(ENDPOINTS);
        if (value == null || value.trim().isEmpty())
            throw new AssertionError(ENDPOINTS + " must contain comma-separated Ratis endpoints when " + RUN + "=true");
        String[] endpoints = Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
        if (endpoints.length != 3)
            throw new AssertionError(ENDPOINTS + " must contain the three Ratis member endpoints");
        return endpoints;
    }

    private static String configuredGroup()
    {
        String value = RatisTestEnvironment.get(GROUP);
        if (value == null || value.trim().isEmpty())
            throw new AssertionError(GROUP + " must contain the canonical Ratis group UUID when " + RUN + "=true");
        UUID.fromString(value.trim());
        return value.trim();
    }

    private static void requireOptIn()
    {
        if (!"true".equalsIgnoreCase(RatisTestEnvironment.get(RUN)))
            throw new org.junit.internal.AssumptionViolatedException("Set " + RUN + "=true to run the live Ratis distributed test");
        configuredEndpoints();
        configuredGroup();
    }

    private static void assertRejected(Runnable operation, String... expectedFragments)
    {
        try
        {
            operation.run();
        }
        catch (RuntimeException expected)
        {
            String message = String.valueOf(expected);
            for (Throwable cause = expected.getCause(); cause != null; cause = cause.getCause())
                message += " " + cause;
            String lower = message.toLowerCase();
            for (String fragment : expectedFragments)
                if (lower.contains(fragment.toLowerCase()))
                    return;
            fail("Expected rejection containing one of " + Arrays.toString(expectedFragments) + " but got: " + message);
        }
        fail("Expected operation to be rejected");
    }

    private static RatisScalarResult submit(IInvokableInstance node, byte[] encoded)
    {
        return RatisScalarResult.decode(node.callOnInstance(() -> AdapterState.submit(encoded)));
    }

    private static RatisScalarResult lookup(IInvokableInstance node, byte[] encoded)
    {
        return RatisScalarResult.decode(node.callOnInstance(() -> AdapterState.lookup(encoded)));
    }

    private static void restore(Cluster cluster)
    {
        for (IInvokableInstance instance : cluster)
        {
            if (!instance.isShutdown())
                instance.runOnInstance(AdapterState::restore);
        }
    }

    private static final class Installed
    {
        final String tableName;
        final String table;
        final UUID domainId;
        final TableId tableId;
        final String groupId;
        final long schemaEpoch;

        private Installed(String tableName, String table, UUID domainId, TableId tableId,
                          String groupId, long schemaEpoch)
        {
            this.tableName = tableName;
            this.table = table;
            this.domainId = domainId;
            this.tableId = tableId;
            this.groupId = groupId;
            this.schemaEpoch = schemaEpoch;
        }
    }

    @Isolated
    public static final class AdapterState
    {
        private static RatisScalarStore store;
        private static RatisTransactionProvider provider;
        private static TransactionProviderRegistry previous;

        private AdapterState() { }

        static void open()
        {
            store = new RatisScalarStore(Arrays.asList(configuredEndpoints()), 5000);
        }

        static void attest(String table, long schemaEpoch, String groupId)
        {
            if (store == null)
                open();
            TransactionDomainDescriptor descriptor = ClusterMetadata.current().consistencyDomains
                                                             .forTable(Schema.instance.getTableMetadata(KEYSPACE, table).id);
            store.attest(descriptor, schemaEpoch, groupId);
        }

        static String[] prepareAndAttest(String table, long schemaEpoch, String groupId)
        {
            if (store == null)
                open();
            TransactionDomainDescriptor descriptor = ClusterMetadata.current().consistencyDomains
                                                             .forTable(Schema.instance.getTableMetadata(KEYSPACE, table).id);
            ExternalTransactionDomainBinding binding = store.prepareAndAttest(descriptor, schemaEpoch, groupId);
            return new String[]{ binding.groupId(), Long.toString(binding.schemaEpoch()), Long.toString(binding.readinessIndex()) };
        }

        static void activate(String table, String groupId)
        {
            if (store == null)
                store = new RatisScalarStore(Arrays.asList(configuredEndpoints()), 5000);
            provider = new RatisTransactionProvider(store);
            Map<String, TransactionProvider> providers = new HashMap<>();
            providers.put(AccordTransactionProvider.ID, AccordTransactionProvider.INSTANCE);
            providers.put(provider.id(), provider);
            previous = TransactionProviders.unsafeSetRegistryForTesting(new TransactionProviderRegistry(providers, Collections.emptyMap()));
        }

        static byte[] compile(String table, UUID requestId)
        {
            String query = "BEGIN TRANSACTION\n" +
                           "LET source = (SELECT v FROM " + KEYSPACE + '.' + table + " WHERE k = 1);\n" +
                           "SELECT source.v;\n" +
                           "UPDATE " + KEYSPACE + '.' + table + " SET v = 33 WHERE k = 3;\n" +
                           "COMMIT TRANSACTION;";
            return compileQuery(query, requestId);
        }

        static byte[] compile(String table, UUID requestId, int value, int key)
        {
            String query = "BEGIN TRANSACTION UPDATE " + KEYSPACE + '.' + table + " SET v = " + value + " WHERE k = " + key + "; COMMIT TRANSACTION;";
            return compileQuery(query, requestId);
        }

        static byte[] compileOverwrite(String table, UUID requestId)
        {
            String query = "BEGIN TRANSACTION\n" +
                           "UPDATE " + KEYSPACE + '.' + table + " SET v = 88 WHERE k = 1;\n" +
                           "UPDATE " + KEYSPACE + '.' + table + " SET v = 66 WHERE k = 3;\n" +
                           "COMMIT TRANSACTION;";
            return compileQuery(query, requestId);
        }

        private static byte[] compileQuery(String query, UUID requestId)
        {
            TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
            TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
            QueryOptions options = QueryProcessor.makeInternalOptions(statement, new Object[0]);
            TransactionPlan plan = statement.toPlan(ClientState.forInternalCalls(), options);
            TransactionDomainDescriptor descriptor = ClusterMetadata.current().consistencyDomains.forTable(plan.tables().keySet().iterator().next());
            TransactionExecutionContext context = new TransactionExecutionContext(new TransactionDomain(descriptor.id().toString(), provider.id()),
                                                                                   org.apache.cassandra.db.ConsistencyLevel.ONE,
                                                                                   org.apache.cassandra.db.ConsistencyLevel.SERIAL,
                                                                                   ProtocolVersion.CURRENT, Dispatcher.RequestTime.forImmediateExecution());
            return provider.compile(plan, context, requestId).encode();
        }

        static byte[] submit(byte[] encoded)
        {
            return provider.submit(RatisScalarRequest.decode(encoded)).encode();
        }

        static byte[] lookup(byte[] encoded)
        {
            return provider.lookup(RatisScalarRequest.decode(encoded)).encode();
        }

        static byte[] rawLookup(byte[] encoded)
        {
            return store.lookup(RatisScalarRequest.decode(encoded)).encode();
        }

        static void fence(byte[] encoded)
        {
            store.fence(RatisScalarRequest.decode(encoded), 2);
        }

        static void restore()
        {
            if (previous != null)
                TransactionProviders.unsafeSetRegistryForTesting(previous);
            previous = null;
            provider = null;
            if (store != null)
                store.close();
            store = null;
        }
    }
}
