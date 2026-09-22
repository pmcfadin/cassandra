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
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogReplayer;
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
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingService;
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
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.test.TestBaseImpl.KEYSPACE;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Focused behavior checks for durable replay and work queued before reservation. */
public class TransactionDomainReplayCompletionTest extends org.apache.cassandra.distributed.test.TestBaseImpl
{
    private static final String PROVIDER_ID = "accord";
    private static final String PROFILE_ID = PrepareTransactionDomain.PROFILE_ID;

    @Test
    public void encodedBatchlogReplayRejectsBeforeFilteringMixedMutation() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            String reservedTable = reserve(cluster).table;
            cluster.get(1).runOnInstance(() -> {
                try
                {
                    ByteBuffer fixture = serialize(mutation(reservedTable, "ordinary"));
                    File fixtureFile = FileUtils.createTempFile("phase3-batchlog-replay", ".fixture");
                    Files.write(fixtureFile.toPath(), bytes(fixture));
                    byte[] before = Files.readAllBytes(fixtureFile.toPath());
                    ByteBuffer persistedFixture = ByteBuffer.wrap(before);
                    Class<?> replayingBatch = Class.forName("org.apache.cassandra.batchlog.BatchlogManager$ReplayingBatch");
                    java.lang.reflect.Constructor<?> constructor = replayingBatch.getDeclaredConstructor(TimeUUID.class,
                                                                                                           int.class,
                                                                                                           List.class,
                                                                                                           ClusterMetadata.class);
                    constructor.setAccessible(true);
                    expectClosed(() -> {
                        try
                        {
                            constructor.newInstance(nextTimeUUID(), MessagingService.current_version,
                                                    Collections.singletonList(persistedFixture.duplicate()), ClusterMetadata.current());
                        }
                        catch (InvocationTargetException e)
                        {
                            throw unchecked(e.getCause());
                        }
                        catch (ReflectiveOperationException e)
                        {
                            throw new AssertionError(e);
                        }
                    });
                    assertTrue("decoded batchlog fixture bytes must remain intact",
                               Arrays.equals(before, Files.readAllBytes(fixtureFile.toPath())));
                    Files.deleteIfExists(fixtureFile.toPath());
                }
                catch (Exception e)
                {
                    throw new AssertionError(e);
                }
            });
            assertRows(cluster.coordinator(1).execute("SELECT k, v FROM " + KEYSPACE + ".ordinary WHERE k = 1", ConsistencyLevel.ONE),
                       org.apache.cassandra.distributed.shared.AssertUtils.row(1, 7));
        }
    }

    @Test
    public void commitlogReplayRejectsBeforeReplayFiltering() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            String reservedTable = reserve(cluster).table;
            String expectedRejection = cluster.get(1).callOnInstance(() ->
                "Transaction domain for table " + Schema.instance.getTableMetadata(KEYSPACE, reservedTable).id +
                " is PREPARED (closed); commit log replay is not admitted");
            // The real replay executor reports its expected task failure to the uncaught handler too.
            cluster.setUncaughtExceptionsFilter(error -> InvalidRequestException.class.getName().equals(error.getClass().getName())
                                                         && expectedRejection.equals(error.getMessage()));
            cluster.get(1).runOnInstance(() -> {
                try
                {
                    Mutation mutation = mutation(reservedTable, "ordinary");
                    ByteBuffer fixture = serialize(mutation);
                    File fixtureFile = FileUtils.createTempFile("phase3-commitlog-replay", ".fixture");
                    Files.write(fixtureFile.toPath(), bytes(fixture));
                    byte[] before = Files.readAllBytes(fixtureFile.toPath());
                    ByteBuffer persistedFixture = ByteBuffer.wrap(before);
                    CommitLogReplayer replayer = CommitLogReplayer.construct(CommitLog.instance, SystemKeyspace.getLocalHostId());
                    expectClosed(() -> {
                        try
                        {
                            Method initiate = CommitLogReplayer.MutationInitiator.class.getDeclaredMethod("initiateMutation",
                                                                                                            Mutation.class,
                                                                                                            long.class,
                                                                                                            int.class,
                                                                                                            int.class,
                                                                                                            CommitLogReplayer.class);
                            initiate.setAccessible(true);
                            Object future = initiate.invoke(CommitLogReplayer.mutationInitiator, mutation, 1L,
                                                            persistedFixture.remaining(), 1, replayer);
                            ((java.util.concurrent.Future<?>) future).get();
                        }
                        catch (InvocationTargetException e)
                        {
                            throw unchecked(e.getCause());
                        }
                        catch (ReflectiveOperationException | InterruptedException e)
                        {
                            throw new AssertionError(e);
                        }
                        catch (ExecutionException e)
                        {
                            throw unchecked(e.getCause());
                        }
                    });
                    assertTrue("commitlog fixture bytes must remain intact",
                               Arrays.equals(before, Files.readAllBytes(fixtureFile.toPath())));
                    Files.deleteIfExists(fixtureFile.toPath());
                }
                catch (Exception e)
                {
                    throw new AssertionError(e);
                }
            });
            assertRows(cluster.coordinator(1).execute("SELECT k, v FROM " + KEYSPACE + ".ordinary WHERE k = 1", ConsistencyLevel.ONE),
                       org.apache.cassandra.distributed.shared.AssertUtils.row(1, 7));
        }
    }

    @Test
    public void persistedHintDecodeRejectsBeforeApplyAndRetainsBytes() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            String reservedTable = reserve(cluster).table;
            cluster.get(1).runOnInstance(() -> {
                try
                {
                    Hint original = Hint.create(mutation(reservedTable, "ordinary"), currentTimeMillis());
                    ByteBuffer fixture;
                    try (DataOutputBuffer out = new DataOutputBuffer())
                    {
                        ((org.apache.cassandra.io.IVersionedSerializer<Hint>) Hint.serializer).serialize(original, out, MessagingService.current_version);
                        fixture = out.buffer();
                    }
                    File fixtureFile = FileUtils.createTempFile("phase3-hint-replay", ".fixture");
                    Files.write(fixtureFile.toPath(), bytes(fixture));
                    byte[] before = Files.readAllBytes(fixtureFile.toPath());
                    ByteBuffer persistedFixture = ByteBuffer.wrap(before);
                    Hint decoded;
                    try (DataInputBuffer in = new DataInputBuffer(persistedFixture.duplicate(), true))
                    {
                        decoded = ((org.apache.cassandra.io.IVersionedSerializer<Hint>) Hint.serializer).deserialize(in, MessagingService.current_version);
                    }
                    Hint persisted = decoded;
                    expectClosed(() -> {
                        try
                        {
                            Method applyFuture = Hint.class.getDeclaredMethod("applyFuture");
                            applyFuture.setAccessible(true);
                            ((java.util.concurrent.Future<?>) applyFuture.invoke(persisted)).get();
                        }
                        catch (InvocationTargetException e)
                        {
                            throw unchecked(e.getCause());
                        }
                        catch (ReflectiveOperationException | InterruptedException e)
                        {
                            throw new AssertionError(e);
                        }
                        catch (ExecutionException e)
                        {
                            throw unchecked(e.getCause());
                        }
                    });
                    assertTrue("persisted hint fixture bytes must remain intact",
                               Arrays.equals(before, Files.readAllBytes(fixtureFile.toPath())));
                    Files.deleteIfExists(fixtureFile.toPath());
                }
                catch (Exception e)
                {
                    throw new AssertionError(e);
                }
            });
            assertRows(cluster.coordinator(1).execute("SELECT k, v FROM " + KEYSPACE + ".ordinary WHERE k = 1", ConsistencyLevel.ONE),
                       org.apache.cassandra.distributed.shared.AssertUtils.row(1, 7));
        }
    }

    @Test
    public void readBuiltWithUnmarkedMetadataBeforePrepareIsRejectedAfterCommit() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            String tableName = "reserved_" + UUID.randomUUID().toString().replace('-', '_');
            cluster.get(1).runOnInstance(() -> {
                TableId tableId = TableId.generate();
                TableMetadata unmarked = TableMetadata.builder(KEYSPACE, tableName, tableId)
                                                      .addPartitionKeyColumn("k", Int32Type.instance)
                                                      .addRegularColumn("v", Int32Type.instance)
                                                      .build();
                org.apache.cassandra.db.DecoratedKey decorated = unmarked.partitioner.decorateKey(Int32Type.instance.decompose(1));
                SinglePartitionReadCommand queued = SinglePartitionReadCommand.create(unmarked, FBUtilities.nowInSeconds(),
                                                                                        ColumnFilter.all(unmarked), RowFilter.none(),
                                                                                        DataLimits.NONE, decorated,
                                                                                        new ClusteringIndexSliceFilter(Slices.ALL, false),
                                                                                        PotentialTxnConflicts.ALLOW);
                ClusterMetadata current = ClusterMetadata.current();
                Set<NodeId> participants = current.directory.states.entrySet().stream()
                                                               .filter(entry -> entry.getValue() == NodeState.JOINED)
                                                               .map(java.util.Map.Entry::getKey)
                                                               .collect(java.util.stream.Collectors.toCollection(HashSet::new));
                ClusterMetadataService.instance().commit(new PrepareTransactionDomain(unmarked,
                    new TransactionDomainDescriptor(UUID.randomUUID(), tableId, PROVIDER_ID, PROFILE_ID, 1, 1, 1, 1, participants)));
                expectClosed(() -> {
                    try (org.apache.cassandra.db.ReadExecutionController controller = queued.executionController();
                         UnfilteredPartitionIterator ignored = queued.executeLocally(controller))
                    {
                        fail("a queued read must not bypass the committed ownership fence");
                    }
                });
            });
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
                                                           .collect(java.util.stream.Collectors.toCollection(HashSet::new));
            TableId tableId = TableId.generate();
            TableMetadata table = TableMetadata.builder(KEYSPACE, tableName, tableId)
                                               .addPartitionKeyColumn("k", Int32Type.instance)
                                               .addRegularColumn("v", Int32Type.instance)
                                               .build();
            ClusterMetadataService.instance().commit(new PrepareTransactionDomain(table,
                new TransactionDomainDescriptor(domainId, tableId, PROVIDER_ID, PROFILE_ID, 1, 1, 1, 1, participants)));
        });
        return new Reservation(tableName);
    }

    private static Mutation mutation(String reservedTable, String ordinaryTable)
    {
        org.apache.cassandra.db.DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(Int32Type.instance.decompose(1));
        org.apache.cassandra.db.SimpleBuilders.MutationBuilder builder = new org.apache.cassandra.db.SimpleBuilders.MutationBuilder(KEYSPACE, key);
        builder.timestamp(FBUtilities.timestampMicros());
        builder.update(Schema.instance.getTableMetadata(KEYSPACE, reservedTable)).row().add("v", 11);
        builder.update(Schema.instance.getTableMetadata(KEYSPACE, ordinaryTable)).row().add("v", 12);
        return builder.build();
    }

    private static ByteBuffer serialize(Mutation mutation) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Mutation.serializer.serialize(mutation, out, MessagingService.current_version);
            return out.buffer();
        }
    }

    private static byte[] bytes(ByteBuffer value)
    {
        ByteBuffer duplicate = value.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    private static RuntimeException unchecked(Throwable failure)
    {
        if (failure instanceof RuntimeException)
            return (RuntimeException) failure;
        return new RuntimeException(failure);
    }

    private static void expectClosed(Runnable operation)
    {
        try
        {
            operation.run();
            fail("expected PREPARED (closed) replay/read rejection");
        }
        catch (Throwable failure)
        {
            Throwable current = failure;
            while (current != null && !(current instanceof InvalidRequestException))
                current = current.getCause();
            assertNotNull("expected InvalidRequestException, got " + failure, current);
            assertTrue("expected PREPARED (closed): " + current.getMessage(),
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
