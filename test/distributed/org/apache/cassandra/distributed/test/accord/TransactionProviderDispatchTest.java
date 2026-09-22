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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.consensus.txn.AccordTransactionProvider;
import org.apache.cassandra.service.consensus.txn.ScalarTransactionProvider;
import org.apache.cassandra.service.consensus.txn.TransactionDomain;
import org.apache.cassandra.service.consensus.txn.TransactionExecutionContext;
import org.apache.cassandra.service.consensus.txn.TransactionOutcome;
import org.apache.cassandra.service.consensus.txn.TransactionPlan;
import org.apache.cassandra.service.consensus.txn.TransactionProvider;
import org.apache.cassandra.service.consensus.txn.TransactionProviderRegistry;
import org.apache.cassandra.service.consensus.txn.TransactionProviders;
import org.apache.cassandra.utils.Isolated;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransactionProviderDispatchTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(TransactionProviderDispatchTest.class);

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(Function.identity(), 2);
    }

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @Test
    public void testDefaultAccordBehavior() throws Exception
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (pk int PRIMARY KEY, v int) WITH transactional_mode='full'", cluster -> {
            String conditional = "BEGIN TRANSACTION\n" +
                                 "  LET row1 = (SELECT v FROM " + qualifiedAccordTableName + " WHERE pk = 1);\n" +
                                 "  SELECT row1.v;\n" +
                                 "  IF row1 IS NULL THEN\n" +
                                 "    INSERT INTO " + qualifiedAccordTableName + " (pk, v) VALUES (1, 7);\n" +
                                 "  END IF\n" +
                                 "COMMIT TRANSACTION;";

            Object[][] first = cluster.coordinator(1).execute(conditional, ConsistencyLevel.ALL);
            assertEquals(1, first.length);
            assertArrayEquals(new Object[]{ null }, first[0]);

            String conditionalFalse = "BEGIN TRANSACTION\n" +
                                     "  LET row1 = (SELECT v FROM " + qualifiedAccordTableName + " WHERE pk = 1);\n" +
                                     "  SELECT row1.v;\n" +
                                     "  IF row1 IS NULL THEN\n" +
                                     "    UPDATE " + qualifiedAccordTableName + " SET v = 8 WHERE pk = 1;\n" +
                                     "  END IF\n" +
                                     "COMMIT TRANSACTION;";
            Object[][] second = cluster.coordinator(1).execute(conditionalFalse, ConsistencyLevel.ALL);
            assertEquals(1, second.length);
            assertArrayEquals(new Object[]{ 7 }, second[0]);
            assertValue(cluster, qualifiedAccordTableName, 1, 7);

            cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                           "  UPDATE " + qualifiedAccordTableName + " SET v = 9 WHERE pk = 1;\n" +
                                           "COMMIT TRANSACTION;", ConsistencyLevel.ALL);
            assertValue(cluster, qualifiedAccordTableName, 1, 9);
        });
    }

    @Test
    public void testInstrumentedProviderDelegatesToAccord() throws Exception
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (pk int PRIMARY KEY, v int) WITH transactional_mode='full'", cluster -> {
            cluster.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (pk, v) VALUES (1, 11);", ConsistencyLevel.ALL);
            install(cluster.get(1), accordTableName, "instrumented", null, null, "instrumented");
            try
            {
                Object[][] result = cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                                                   "  SELECT v FROM " + qualifiedAccordTableName + " WHERE pk = 1;\n" +
                                                                   "COMMIT TRANSACTION;", ConsistencyLevel.ALL);
                assertEquals(1, result.length);
                assertArrayEquals(new Object[]{ 11 }, result[0]);
                assertEquals(1, invocations(cluster.get(1)));
            }
            finally
            {
                restore(cluster.get(1));
            }
        });
    }

    @Test
    public void testInsufficientCapabilitiesRejectBeforeExecution() throws Exception
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (pk int PRIMARY KEY, v int) WITH transactional_mode='full'", cluster -> {
            cluster.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (pk, v) VALUES (1, 1);", ConsistencyLevel.ALL);
            install(cluster.get(1), accordTableName, "weak", null, null, "weak");
            try
            {
                assertInvalid("does not support required capability",
                              () -> cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                                                    "  UPDATE " + qualifiedAccordTableName + " SET v = 2 WHERE pk = 1;\n" +
                                                                    "COMMIT TRANSACTION;", ConsistencyLevel.ALL));
                assertEquals(0, invocations(cluster.get(1)));
                assertValue(cluster, qualifiedAccordTableName, 1, 1);
            }
            finally
            {
                restore(cluster.get(1));
            }
        });
    }

    @Test
    public void testAbsentProviderRejectsBeforeExecution() throws Exception
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (pk int PRIMARY KEY, v int) WITH transactional_mode='full'", cluster -> {
            cluster.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (pk, v) VALUES (1, 1);", ConsistencyLevel.ALL);
            install(cluster.get(1), accordTableName, "missing", null, null, "absent");
            try
            {
                assertInvalid("not registered",
                              () -> cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                                                    "  UPDATE " + qualifiedAccordTableName + " SET v = 2 WHERE pk = 1;\n" +
                                                                    "COMMIT TRANSACTION;", ConsistencyLevel.ALL));
                assertValue(cluster, qualifiedAccordTableName, 1, 1);
            }
            finally
            {
                restore(cluster.get(1));
            }
        });
    }

    @Test
    public void testMixedOwnersRejectBeforeExecution() throws Exception
    {
        String first = qualifiedAccordTableName;
        String second = qualifiedRegularTableName;
        test(Arrays.asList("CREATE TABLE " + first + " (pk int PRIMARY KEY, v int) WITH transactional_mode='full'",
                           "CREATE TABLE " + second + " (pk int PRIMARY KEY, v int) WITH transactional_mode='full'"), cluster -> {
            cluster.coordinator(1).execute("INSERT INTO " + first + " (pk, v) VALUES (1, 1);", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO " + second + " (pk, v) VALUES (1, 1);", ConsistencyLevel.ALL);
            install(cluster.get(1), accordTableName, "accord", regularTableName, "instrumented", "mixed");
            try
            {
                assertInvalid("different providers",
                              () -> cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                                                    "  UPDATE " + first + " SET v = 2 WHERE pk = 1;\n" +
                                                                    "  UPDATE " + second + " SET v = 2 WHERE pk = 1;\n" +
                                                                    "COMMIT TRANSACTION;", ConsistencyLevel.ALL));
                assertEquals(0, invocations(cluster.get(1)));
                assertValue(cluster, first, 1, 1);
                assertValue(cluster, second, 1, 1);
            }
            finally
            {
                restore(cluster.get(1));
            }
        });
    }

    @Test
    public void testScalarProviderExecutesAParsedPlanThroughCqlTransport() throws Exception
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (pk int PRIMARY KEY, v int) WITH transactional_mode='full'", cluster -> {
            install(cluster.get(1), accordTableName, ScalarTransactionProvider.ID, null, null, "scalar");
            try
            {
                Object[][] inserted = cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                                                       "  UPDATE " + qualifiedAccordTableName + " SET v = 7 WHERE pk = 1;\n" +
                                                                       "COMMIT TRANSACTION;", ConsistencyLevel.ALL);
                assertNull(inserted);

                Object[][] updated = cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                                                      "  LET source = (SELECT v FROM " + qualifiedAccordTableName + " WHERE pk = 1);\n" +
                                                                      "  SELECT source.v;\n" +
                                                                      "  IF source.v = 7 THEN\n" +
                                                                      "    UPDATE " + qualifiedAccordTableName + " SET v = source.v WHERE pk = 2;\n" +
                                                                      "  END IF\n" +
                                                                      "COMMIT TRANSACTION;", ConsistencyLevel.ALL);
                assertEquals(1, updated.length);
                assertArrayEquals(new Object[]{ 7 }, updated[0]);
                assertEquals(2, invocations(cluster.get(1)));

                Object[][] read = cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                                                  "  SELECT v FROM " + qualifiedAccordTableName + " WHERE pk = 2;\n" +
                                                                  "COMMIT TRANSACTION;", ConsistencyLevel.ALL);
                assertEquals(1, read.length);
                assertArrayEquals(new Object[]{ 7 }, read[0]);
            }
            finally
            {
                restore(cluster.get(1));
            }
        });
    }

    @Test
    public void testSameProviderDifferentDomainsRejectBeforeExecution() throws Exception
    {
        String first = qualifiedAccordTableName;
        String second = qualifiedRegularTableName;
        test(Arrays.asList("CREATE TABLE " + first + " (pk int PRIMARY KEY, v int) WITH transactional_mode='full'",
                           "CREATE TABLE " + second + " (pk int PRIMARY KEY, v int) WITH transactional_mode='full'"), cluster -> {
            install(cluster.get(1), accordTableName, ScalarTransactionProvider.ID, regularTableName, ScalarTransactionProvider.ID, "same-provider-different-domain");
            try
            {
                assertInvalid("different domains",
                              () -> cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                                                    "  UPDATE " + first + " SET v = 2 WHERE pk = 1;\n" +
                                                                    "  UPDATE " + second + " SET v = 2 WHERE pk = 1;\n" +
                                                                    "COMMIT TRANSACTION;", ConsistencyLevel.ALL));
                assertEquals(0, invocations(cluster.get(1)));
            }
            finally
            {
                restore(cluster.get(1));
            }
        });
    }

    private static void install(IInvokableInstance instance,
                                String firstTable,
                                String firstOwner,
                                String secondTable,
                                String secondOwner,
                                String profile)
    {
        instance.runOnInstance(() -> NodeState.install(KEYSPACE, firstTable, firstOwner, secondTable, secondOwner, profile));
    }

    private static void restore(IInvokableInstance instance)
    {
        instance.runOnInstance(NodeState::restore);
    }

    private static int invocations(IInvokableInstance instance)
    {
        return instance.callOnInstance(NodeState::invocations);
    }

    private static void assertValue(Cluster cluster, String table, int key, int value)
    {
        Object[][] rows = cluster.coordinator(1).execute("SELECT pk, v FROM " + table + " WHERE pk = ?", ConsistencyLevel.ALL, key);
        assertEquals(1, rows.length);
        assertArrayEquals(new Object[]{ key, value }, rows[0]);
    }

    private static void assertInvalid(String expectedMessage, Runnable operation)
    {
        try
        {
            operation.run();
            fail("Expected InvalidRequestException");
        }
        catch (Throwable t)
        {
            assertEquals(InvalidRequestException.class.getName(), t.getClass().getName());
            assertTrue(t.getMessage().contains(expectedMessage));
        }
    }

    @Isolated
    public static final class NodeState
    {
        private static TransactionProviderRegistry previous;
        private static int providerInvocations;
        private static ScalarTransactionProvider scalarProvider;

        private NodeState()
        {
        }

        private static void install(String keyspace,
                                    String firstTable,
                                    String firstOwner,
                                    String secondTable,
                                    String secondOwner,
                                    String profile)
        {
            Map<String, TransactionProvider> providers = new HashMap<>();
            if ("instrumented".equals(profile) || "mixed".equals(profile))
                providers.put("instrumented", new ProbeProvider("instrumented", EnumSet.allOf(TransactionProvider.Capability.class)));
            else if ("weak".equals(profile))
                providers.put("weak", new ProbeProvider("weak", EnumSet.of(TransactionProvider.Capability.READ)));
            else if ("scalar".equals(profile) || "same-provider-different-domain".equals(profile))
            {
                scalarProvider = new ScalarTransactionProvider();
                providers.put(ScalarTransactionProvider.ID, scalarProvider);
            }
            providers.put(AccordTransactionProvider.ID, AccordTransactionProvider.INSTANCE);

            Map<TableId, String> assignments = new HashMap<>();
            assignments.put(Schema.instance.getTableMetadata(keyspace, firstTable).id, firstOwner);
            if (secondTable != null)
                assignments.put(Schema.instance.getTableMetadata(keyspace, secondTable).id, secondOwner);

            providerInvocations = 0;
            if ("same-provider-different-domain".equals(profile))
            {
                Map<TableId, TransactionDomain> domains = new HashMap<>();
                domains.put(Schema.instance.getTableMetadata(keyspace, firstTable).id,
                            new TransactionDomain("scalar-first", ScalarTransactionProvider.ID));
                domains.put(Schema.instance.getTableMetadata(keyspace, secondTable).id,
                            new TransactionDomain("scalar-second", ScalarTransactionProvider.ID));
                previous = TransactionProviders.unsafeSetRegistryForTesting(TransactionProviderRegistry.withDomains(providers, domains));
            }
            else
            {
                previous = TransactionProviders.unsafeSetRegistryForTesting(new TransactionProviderRegistry(providers, assignments));
            }
        }

        private static void restore()
        {
            TransactionProviderRegistry prior = previous;
            previous = null;
            scalarProvider = null;
            if (prior != null)
                TransactionProviders.unsafeSetRegistryForTesting(prior);
        }

        private static int invocations()
        {
            return scalarProvider == null ? providerInvocations : scalarProvider.invocationCount();
        }

        private static final class ProbeProvider implements TransactionProvider
        {
            private final String id;
            private final Set<Capability> capabilities;

            private ProbeProvider(String id, Set<Capability> capabilities)
            {
                this.id = id;
                this.capabilities = Collections.unmodifiableSet(EnumSet.copyOf(capabilities));
            }

            @Override
            public String id()
            {
                return id;
            }

            @Override
            public Set<Capability> capabilities()
            {
                return capabilities;
            }

            @Override
            public TransactionOutcome execute(TransactionPlan plan, TransactionExecutionContext context)
            {
                providerInvocations++;
                return AccordTransactionProvider.INSTANCE.execute(plan, context);
            }
        }
    }
}
