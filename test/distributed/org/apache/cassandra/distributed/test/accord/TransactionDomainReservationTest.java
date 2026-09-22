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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.txn.TransactionDomainDescriptor;
import org.apache.cassandra.service.consensus.txn.TransactionDomainGuard;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.transformations.PrepareTransactionDomain;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Distributed coverage for increment 3.1's atomic, closed transaction-domain reservation. */
public class TransactionDomainReservationTest extends org.apache.cassandra.distributed.test.TestBaseImpl
{
    private static final String PROVIDER_ID = "accord";
    private static final String PROFILE_ID = PrepareTransactionDomain.PROFILE_ID;

    @Test
    public void prepareInstallsClosedReservationAtOneEpoch() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            Reservation reservation = reserve(cluster);
            waitForCMSToQuiesce(cluster, cluster.get(1));

            for (IInvokableInstance instance : cluster)
            {
                String snapshot = snapshot(instance, reservation.table, reservation.domain);
                String[] values = snapshot.split("\\|");
                assertEquals("reservation must use one metadata epoch for schema and ownership", values[0], values[1]);
                assertEquals("one domain should be installed", "1", values[2]);
                assertEquals("reservation marker must be present", "true", values[3]);
                assertEquals("descriptor is closed until a later increment", "PREPARED", values[4]);
                assertEquals(reservation.domain.toString(), values[5]);
            }
        }
    }

    @Test
    public void reservedTableRejectsCqlAndBatchBeforeAnySentinelMutation() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            String sentinel = "sentinel";
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + sentinel + " (k int PRIMARY KEY, v int)");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + sentinel + " (k, v) VALUES (1, 10)", ConsistencyLevel.ONE);

            Reservation reservation = reserve(cluster);
            String reserved = KEYSPACE + '.' + reservation.table;
            String unreserved = KEYSPACE + '.' + sentinel;

            assertReserved(() -> cluster.coordinator(1).execute("SELECT * FROM " + reserved + " WHERE k = 1", ConsistencyLevel.ONE));
            assertReserved(() -> cluster.coordinator(1).execute("INSERT INTO " + reserved + " (k, v) VALUES (1, 11)", ConsistencyLevel.ONE));
            assertReserved(() -> cluster.coordinator(1).execute("UPDATE " + reserved + " SET v = 11 WHERE k = 1", ConsistencyLevel.ONE));
            assertReserved(() -> cluster.coordinator(1).execute("SELECT * FROM " + reserved + " WHERE k = 1 ALLOW FILTERING", ConsistencyLevel.ONE));
            assertReserved(() -> cluster.get(1).executeInternal("SELECT * FROM " + reserved + " WHERE k = 1"));
            assertReserved(() -> cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                                                  "  SELECT v FROM " + reserved + " WHERE k = 1;\n" +
                                                                  "COMMIT TRANSACTION", ConsistencyLevel.ONE));
            assertReserved(() -> cluster.coordinator(1).execute("INSERT INTO " + reserved + " (k, v) VALUES (1, 11) IF NOT EXISTS", ConsistencyLevel.ONE));
            assertReserved(() -> cluster.coordinator(1).execute("TRUNCATE " + reserved, ConsistencyLevel.ONE));

            assertReserved(() -> cluster.coordinator(1).execute("BEGIN BATCH\n" +
                                                                  "  UPDATE " + unreserved + " SET v = 99 WHERE k = 1;\n" +
                                                                  "  INSERT INTO " + reserved + " (k, v) VALUES (1, 11);\n" +
                                                                  "APPLY BATCH", ConsistencyLevel.ONE));

            assertRows(cluster.coordinator(1).execute("SELECT k, v FROM " + unreserved + " WHERE k = 1", ConsistencyLevel.ONE),
                       org.apache.cassandra.distributed.shared.AssertUtils.row(1, 10));
        }
    }

    @Test
    public void allowMutationAndDirectCommitLogAppendRejectBeforeWALMovement() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            String sentinel = "wal_sentinel";
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + sentinel + " (k int PRIMARY KEY, v int)");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + sentinel + " (k, v) VALUES (7, 10)", ConsistencyLevel.ONE);
            Reservation reservation = reserve(cluster);
            String reservedTable = reservation.table;

            // This test invokes local storage directly, so seed and inspect that same replica.
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + '.' + sentinel + " (k, v) VALUES (7, 10)");

            String result = cluster.get(1).callOnInstance(() -> {
                TableMetadata reservedMetadata = Schema.instance.getTableMetadata(KEYSPACE, reservedTable);
                TableMetadata sentinelMetadata = Schema.instance.getTableMetadata(KEYSPACE, sentinel);
                org.apache.cassandra.dht.IPartitioner partitioner = reservedMetadata.partitioner;
                org.apache.cassandra.db.DecoratedKey key = partitioner.decorateKey(Int32Type.instance.decompose(7));

                PartitionUpdate.SimpleBuilder reservedBuilder = PartitionUpdate.simpleBuilder(reservedMetadata, key);
                reservedBuilder.row().add("v", 11);
                PartitionUpdate.SimpleBuilder sentinelBuilder = PartitionUpdate.simpleBuilder(sentinelMetadata, key);
                sentinelBuilder.row().add("v", 99);

                Mutation.PartitionUpdateCollector collector = new Mutation.PartitionUpdateCollector(KEYSPACE,
                                                                                                     key,
                                                                                                     PotentialTxnConflicts.ALLOW);
                Mutation mutation = collector.add(sentinelBuilder.build()).add(reservedBuilder.build()).build();
                String before = CommitLog.instance.getCurrentPosition().toString();
                String applyException;
                try
                {
                    mutation.applyUnsafe();
                    applyException = "accepted";
                }
                catch (Throwable t)
                {
                    applyException = t.getClass().getName() + ":" + t.getMessage();
                }
                String afterApply = CommitLog.instance.getCurrentPosition().toString();
                String commitException;
                try
                {
                    CommitLog.instance.add(mutation);
                    commitException = "accepted";
                }
                catch (Throwable t)
                {
                    commitException = t.getClass().getName() + ":" + t.getMessage();
                }
                String afterCommit = CommitLog.instance.getCurrentPosition().toString();
                return before + "|" + applyException + "|" + afterApply + "|" + commitException + "|" + afterCommit;
            });

            String[] values = result.split("\\|", 5);
            assertEquals("rejected mutation must not move the active WAL position", values[0], values[2]);
            assertEquals("rejected append must not move the active WAL position", values[2], values[4]);
            assertTrue(values[1].startsWith(InvalidRequestException.class.getName()));
            assertTrue(values[1].contains("PREPARED (closed)"));
            assertTrue(values[3].startsWith(InvalidRequestException.class.getName()));
            assertTrue(values[3].contains("PREPARED (closed)"));
            assertRows(cluster.get(1).executeInternal("SELECT k, v FROM " + KEYSPACE + '.' + sentinel + " WHERE k = 7"),
                       org.apache.cassandra.distributed.shared.AssertUtils.row(7, 10));
            assertRows(cluster.coordinator(1).execute("SELECT k, v FROM " + KEYSPACE + '.' + sentinel + " WHERE k = 7", ConsistencyLevel.ONE),
                       org.apache.cassandra.distributed.shared.AssertUtils.row(7, 10));
        }
    }

    @Test
    public void reservedTableRejectsSchemaMutationAndLeavesOrdinaryTableUsable() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            String sentinel = "ordinary";
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + sentinel + " (k int PRIMARY KEY, v int)");
            Reservation reservation = reserve(cluster);
            String reserved = KEYSPACE + '.' + reservation.table;

            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + sentinel + " (k, v) VALUES (1, 7)", ConsistencyLevel.ONE);
            assertReserved(() -> cluster.schemaChange("ALTER TABLE " + reserved + " ADD extra int"));
            assertReserved(() -> cluster.schemaChange("ALTER TABLE " + reserved + " WITH comment = 'blocked'"));
            assertReserved(() -> cluster.schemaChange("DROP TABLE " + reserved));

            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + '.' + sentinel + " SET v = 8 WHERE k = 1", ConsistencyLevel.ONE);
            assertRows(cluster.coordinator(1).execute("SELECT k, v FROM " + KEYSPACE + '.' + sentinel + " WHERE k = 1", ConsistencyLevel.ONE),
                       org.apache.cassandra.distributed.shared.AssertUtils.row(1, 8));
        }
    }

    @Test
    public void restartRetainsReservationAndClosedMarker() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            Reservation reservation = reserve(cluster);
            waitForCMSToQuiesce(cluster, cluster.get(1));
            String before = snapshot(cluster.get(1), reservation.table, reservation.domain);

            FBUtilities.waitOnFuture(cluster.get(1).shutdown());
            FBUtilities.waitOnFuture(cluster.get(2).shutdown());
            // Both peers start without Accord tables; bound topology retry delays in this metadata-recovery test.
            for (IInvokableInstance instance : cluster)
                instance.config().set("accord.retry_fetch_topology", "10ms,retries=100");
            // Stopped nodes have no isolated executor; start their fresh instances from the harness.
            ExecutorService restarts = Executors.newFixedThreadPool(2);
            try
            {
                Future<?> first = restarts.submit(() -> cluster.get(1).startup());
                Future<?> second = restarts.submit(() -> cluster.get(2).startup());
                first.get(1, TimeUnit.MINUTES);
                second.get(1, TimeUnit.MINUTES);
            }
            finally
            {
                restarts.shutdownNow();
            }
            waitForCMSToQuiesce(cluster, cluster.get(2));

            assertEquals(before, snapshot(cluster.get(1), reservation.table, reservation.domain));
            assertEquals(before, snapshot(cluster.get(2), reservation.table, reservation.domain));
            assertReserved(() -> cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + '.' + reservation.table + " WHERE k = 1", ConsistencyLevel.ONE));
            assertReserved(() -> cluster.coordinator(2).execute("UPDATE " + KEYSPACE + '.' + reservation.table + " SET v = 1 WHERE k = 1", ConsistencyLevel.ONE));
        }
    }

    private static Cluster newCluster() throws Exception
    {
        Cluster cluster = Cluster.build(2).withoutVNodes().start();
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}");
        return cluster;
    }

    private static Reservation reserve(Cluster cluster)
    {
        String tableName = "reserved_" + UUID.randomUUID().toString().replace('-', '_');
        String domain = UUID.randomUUID().toString();
        cluster.get(1).runOnInstance(() -> {
            ClusterMetadata current = ClusterMetadata.current();
            Set<NodeId> participants = current.directory.states.entrySet().stream()
                                                           .filter(entry -> entry.getValue() == NodeState.JOINED)
                                                           .map(java.util.Map.Entry::getKey)
                                                           .collect(Collectors.toCollection(HashSet::new));
            TableId tableId = TableId.generate();
            TableMetadata table = TableMetadata.builder(KEYSPACE, tableName, tableId)
                                               .addPartitionKeyColumn("k", org.apache.cassandra.db.marshal.Int32Type.instance)
                                               .addRegularColumn("v", org.apache.cassandra.db.marshal.Int32Type.instance)
                                               .build();
            TransactionDomainDescriptor descriptor = new TransactionDomainDescriptor(UUID.fromString(domain),
                                                                                       tableId,
                                                                                       PROVIDER_ID,
                                                                                       PROFILE_ID,
                                                                                       1,
                                                                                       1,
                                                                                       1,
                                                                                       1,
                                                                                       participants);
            ClusterMetadataService.instance().commit(new PrepareTransactionDomain(table, descriptor));
        });
        return new Reservation(tableName, UUID.fromString(domain));
    }

    private static String snapshot(IInvokableInstance instance, String tableName, UUID domainId)
    {
        return instance.callOnInstance(() -> {
            TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, tableName);
            assertNotNull(table);
            ClusterMetadata metadata = ClusterMetadata.current();
            TransactionDomainDescriptor descriptor = metadata.consistencyDomains.forTable(table.id);
            assertNotNull(descriptor);
            return table.epoch.getEpoch() + "|" + metadata.consistencyDomains.lastModified().getEpoch() + "|" + metadata.consistencyDomains.size() + "|" +
                   TransactionDomainGuard.isReserved(table) + "|" + descriptor.state() + "|" + descriptor.id();
        });
    }

    private static void assertReserved(Runnable operation)
    {
        try
        {
            operation.run();
            fail("Expected a reserved transaction-domain operation to be rejected");
        }
        catch (Throwable failure)
        {
            Throwable current = failure;
            while (current != null && !InvalidRequestException.class.getName().equals(current.getClass().getName()))
                current = current.getCause();
            assertNotNull("expected InvalidRequestException, got " + failure, current);
            assertTrue("reservation rejection must identify the closed PREPARED state",
                       current.getMessage() != null && current.getMessage().contains("PREPARED (closed)"));
        }
    }

    private static final class Reservation
    {
        private final String table;
        private final UUID domain;

        private Reservation(String table, UUID domain)
        {
            this.table = table;
            this.domain = domain;
        }
    }
}
