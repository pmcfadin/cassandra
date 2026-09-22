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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SimpleBuilders.MutationBuilder;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.txn.TransactionDomainDescriptor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.apache.cassandra.tcm.transformations.PrepareTransactionDomain;
import org.apache.cassandra.tcm.transformations.Unregister;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.test.TestBaseImpl.KEYSPACE;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Distributed coverage for maintenance and topology admission of closed transaction domains. */
public class TransactionDomainMaintenanceTest extends org.apache.cassandra.distributed.test.TestBaseImpl
{
    private static final String PROVIDER_ID = "accord";
    private static final String PROFILE_ID = PrepareTransactionDomain.PROFILE_ID;

    @Test
    public void closedReservationRejectsHintsBatchlogInternalReadsAndTopologyChanges() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            String ordinaryTable = "ordinary_" + UUID.randomUUID().toString().replace('-', '_');
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + ordinaryTable + " (k int PRIMARY KEY, v int)");
            Reservation reservation = reserve(cluster);

            String reservedTable = reservation.table;
            long hintsBefore = cluster.get(1).callOnInstance(() -> StorageMetrics.totalHints.getCount());
            assertReserved(() -> cluster.get(1).runOnInstance(() -> {
                Mutation mutation = mutation(reservedTable, ordinaryTable);
                HintsService.instance.write(UUID.randomUUID(), Hint.create(mutation, currentTimeMillis()));
            }));
            assertEquals("rejected hint must not increment hint accounting", hintsBefore,
                         (long) cluster.get(1).callOnInstance(() -> StorageMetrics.totalHints.getCount()));

            int batchesBefore = cluster.get(1).callOnInstance(() -> BatchlogManager.instance.countAllBatches());
            assertReserved(() -> cluster.get(1).runOnInstance(() -> {
                Mutation mutation = mutation(reservedTable, ordinaryTable);
                BatchlogManager.store(Batch.createLocal(nextTimeUUID(), FBUtilities.timestampMicros(), Collections.singletonList(mutation)));
            }));
            assertEquals("rejected mixed batch must not persist a batchlog row", batchesBefore,
                         (int) cluster.get(1).callOnInstance(() -> BatchlogManager.instance.countAllBatches()));

            assertReserved(() -> cluster.get(1).runOnInstance(() ->
                QueryProcessor.executeInternal("SELECT * FROM " + KEYSPACE + '.' + reservedTable + " WHERE k = 1")));

            String moveReason = cluster.get(1).callOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                Transformation.Result result = new PrepareMove(metadata.myNodeId(), Collections.emptySet(),
                                                                ClusterMetadataService.instance().placementProvider(), false)
                                                 .execute(metadata);
                assertTrue(result.isRejected());
                return result.rejected().reason;
            });
            assertTrue(moveReason, moveReason.contains("transaction domains are reserved"));

            String unregisterReason = cluster.get(1).callOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                Transformation.Result result = new Unregister(metadata.myNodeId(),
                                                              java.util.EnumSet.allOf(NodeState.class),
                                                              ClusterMetadataService.instance().placementProvider())
                                                 .execute(metadata);
                assertTrue(result.isRejected());
                return result.rejected().reason;
            });
            assertTrue(unregisterReason, unregisterReason.contains("transaction domains are reserved"));
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
            TransactionDomainDescriptor descriptor = new TransactionDomainDescriptor(domainId,
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
        return new Reservation(tableName);
    }

    private static Mutation mutation(String reservedTable, String ordinaryTable)
    {
        DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(Int32Type.instance.decompose(1));
        MutationBuilder builder = new MutationBuilder(KEYSPACE, key);
        builder.timestamp(FBUtilities.timestampMicros());
        builder.update(Schema.instance.getTableMetadata(KEYSPACE, reservedTable)).row().add("v", 1);
        builder.update(Schema.instance.getTableMetadata(KEYSPACE, ordinaryTable)).row().add("v", 2);
        return builder.build();
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
            assertTrue("expected closed reservation message: " + current.getMessage(),
                       current.getMessage().contains("PREPARED (closed)"));
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
