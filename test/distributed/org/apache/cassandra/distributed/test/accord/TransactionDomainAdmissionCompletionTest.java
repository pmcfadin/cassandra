/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.txn.TransactionDomainDescriptor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.transformations.PrepareTransactionDomain;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.test.TestBaseImpl.KEYSPACE;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Focused coverage for closed-domain admission at durable maintenance boundaries. */
public class TransactionDomainAdmissionCompletionTest extends org.apache.cassandra.distributed.test.TestBaseImpl
{
    private static final String PROVIDER_ID = "accord";
    private static final String PROFILE_ID = PrepareTransactionDomain.PROFILE_ID;

    @Test
    public void directAllowReadIsRejectedBeforeLocalExecution() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            String reservedTable = reserve(cluster).table;
            assertReserved(() -> cluster.get(1).runOnInstance(() -> {
                TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, reservedTable);
                DecoratedKey key = table.partitioner.decorateKey(Int32Type.instance.decompose(1));
                SinglePartitionReadCommand command = SinglePartitionReadCommand.create(table,
                                                                                       FBUtilities.nowInSeconds(),
                                                                                       ColumnFilter.all(table),
                                                                                       RowFilter.none(),
                                                                                       DataLimits.NONE,
                                                                                       key,
                                                                                       new ClusteringIndexSliceFilter(Slices.ALL, false),
                                                                                       PotentialTxnConflicts.ALLOW);
                try (org.apache.cassandra.db.ReadExecutionController controller = command.executionController();
                     UnfilteredPartitionIterator ignored = command.executeLocally(controller))
                {
                    fail("ALLOW must not bypass transaction-domain ownership admission");
                }
            }));
        }
    }

    @Test
    public void nonemptySstableImportIsRejectedBeforePublication() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            String sentinel = "sstable_sentinel";
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + sentinel + " (k int PRIMARY KEY, v int)");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + sentinel + " (k, v) VALUES (1, 10)", ConsistencyLevel.ONE);
            cluster.get(1).nodetool("flush", KEYSPACE, sentinel);
            String source = cluster.get(1).callOnInstance(() ->
                Keyspace.open(KEYSPACE).getColumnFamilyStore(sentinel).getLiveSSTables().iterator().next().descriptor.directory.toString());
            String reservedTable = reserve(cluster).table;

            assertReserved(() -> cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(reservedTable);
                cfs.importNewSSTables(Collections.singleton(source), false, false, true, true, true, true, true);
            }));
            assertReserved(() -> cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore sourceCfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(sentinel);
                ColumnFamilyStore reservedCfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(reservedTable);
                reservedCfs.addSSTables(sourceCfs.getLiveSSTables());
            }));
            assertEquals("ordinary source SSTable remains visible after rejected import", 1,
                         (int) cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                                         .getColumnFamilyStore(sentinel)
                                                                         .getLiveSSTables().size()));
            assertRows(cluster.coordinator(1).execute("SELECT k, v FROM " + KEYSPACE + '.' + sentinel + " WHERE k = 1", ConsistencyLevel.ONE),
                       org.apache.cassandra.distributed.shared.AssertUtils.row(1, 10));
        }
    }

    @Test
    public void encodedBatchlogIsRejectedBeforeRecordPersistence() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            String reservedTable = reserve(cluster).table;
            int before = cluster.get(1).callOnInstance(() -> BatchlogManager.instance.countAllBatches());
            assertReserved(() -> cluster.get(1).runOnInstance(() -> {
                Mutation mutation = mutation(reservedTable, "ordinary");
                try (org.apache.cassandra.io.util.DataOutputBuffer buffer = new org.apache.cassandra.io.util.DataOutputBuffer())
                {
                    Mutation.serializer.serialize(mutation, buffer, org.apache.cassandra.net.MessagingService.current_version);
                    ByteBuffer encoded = buffer.buffer();
                    BatchlogManager.store(Batch.createRemote(nextTimeUUID(), FBUtilities.timestampMicros(), Collections.singleton(encoded)));
                }
                catch (java.io.IOException e)
                {
                    throw new AssertionError(e);
                }
            }));
            assertEquals("encoded rejection must not persist a batchlog row", before,
                         (int) cluster.get(1).callOnInstance(() -> BatchlogManager.instance.countAllBatches()));
            assertRows(cluster.coordinator(1).execute("SELECT k, v FROM " + KEYSPACE + ".ordinary WHERE k = 1", ConsistencyLevel.ONE),
                       org.apache.cassandra.distributed.shared.AssertUtils.row(1, 7));
        }
    }

    @Test
    public void hintReplayIsRejectedBeforeMutationApply() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            String reservedTable = reserve(cluster).table;
            assertReserved(() -> cluster.get(1).runOnInstance(() -> {
                Hint hint = Hint.create(mutation(reservedTable, "ordinary"), currentTimeMillis());
                try
                {
                    Method applyFuture = Hint.class.getDeclaredMethod("applyFuture");
                    applyFuture.setAccessible(true);
                    ((java.util.concurrent.Future<?>) applyFuture.invoke(hint)).get();
                }
                catch (InvocationTargetException e)
                {
                    throw (RuntimeException) e.getCause();
                }
                catch (ReflectiveOperationException | InterruptedException e)
                {
                    throw new AssertionError(e);
                }
                catch (java.util.concurrent.ExecutionException e)
                {
                    if (e.getCause() instanceof RuntimeException)
                        throw (RuntimeException) e.getCause();
                    throw new AssertionError(e.getCause());
                }
            }));
            assertRows(cluster.coordinator(1).execute("SELECT k, v FROM " + KEYSPACE + ".ordinary WHERE k = 1", ConsistencyLevel.ONE),
                       org.apache.cassandra.distributed.shared.AssertUtils.row(1, 7));
        }
    }

    @Test
    public void repairApiRejectsReservedTable() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            cluster.schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}");
            String reservedTable = reserve(cluster).table;
            cluster.get(1).nodetoolResult("repair", KEYSPACE, reservedTable).asserts().failure()
                   .errorContains("PREPARED (closed)");
        }
    }

    private static Cluster newCluster() throws Exception
    {
        Cluster cluster = Cluster.build(2).withoutVNodes().start();
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}");
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".ordinary (k int PRIMARY KEY, v int)");
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".ordinary (k, v) VALUES (1, 7)", ConsistencyLevel.ONE);
        return cluster;
    }

    private static Reservation reserve(Cluster cluster)
    {
        String tableName = "reserved_" + UUID.randomUUID().toString().replace('-', '_');
        UUID domainId = UUID.randomUUID();
        cluster.get(1).runOnInstance(() -> {
            ClusterMetadata current = ClusterMetadata.current();
            Set<NodeId> participants = current.directory.states.entrySet().stream()
                                                           .filter(entry -> entry.getValue() == NodeState.JOINED)
                                                           .map(java.util.Map.Entry::getKey)
                                                           .collect(Collectors.toCollection(HashSet::new));
            TableId tableId = TableId.generate();
            TableMetadata table = TableMetadata.builder(KEYSPACE, tableName, tableId)
                                               .addPartitionKeyColumn("k", Int32Type.instance)
                                               .addRegularColumn("v", Int32Type.instance)
                                               .build();
            TransactionDomainDescriptor descriptor = new TransactionDomainDescriptor(domainId, tableId, PROVIDER_ID,
                                                                                       PROFILE_ID, 1, 1, 1, 1, participants);
            ClusterMetadataService.instance().commit(new PrepareTransactionDomain(table, descriptor));
        });
        return new Reservation(tableName);
    }

    private static Mutation mutation(String reservedTable, String ordinaryTable)
    {
        DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(Int32Type.instance.decompose(1));
        org.apache.cassandra.db.SimpleBuilders.MutationBuilder builder =
            new org.apache.cassandra.db.SimpleBuilders.MutationBuilder(KEYSPACE, key);
        builder.timestamp(FBUtilities.timestampMicros());
        builder.update(Schema.instance.getTableMetadata(KEYSPACE, reservedTable)).row().add("v", 11);
        builder.update(Schema.instance.getTableMetadata(KEYSPACE, ordinaryTable)).row().add("v", 12);
        return builder.build();
    }

    private static void assertReserved(Runnable operation)
    {
        try
        {
            operation.run();
            fail("Expected a reserved transaction-domain operation to be rejected");
        }
        catch (RuntimeException failure)
        {
            Throwable current = failure;
            while (current != null && !InvalidRequestException.class.getName().equals(current.getClass().getName()))
                current = current.getCause();
            assertNotNull("expected InvalidRequestException, got " + failure, current);
            assertTrue("expected closed reservation message: " + current.getMessage(),
                       current.getMessage() != null && current.getMessage().contains("PREPARED (closed)"));
        }
    }

    private static final class Reservation
    {
        private final String table;

        private Reservation(String table)
        {
            this.table = table;
        }
    }
}
