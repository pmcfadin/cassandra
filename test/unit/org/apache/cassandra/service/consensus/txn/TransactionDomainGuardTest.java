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

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TransactionDomainGuardTest
{
    private static final String KEYSPACE = "transaction_domain_guard_ks";
    private static final TableId RESERVED_ID = TableId.fromString("00000000-0000-0000-0000-000000000501");
    private static final TableId ORDINARY_ID = TableId.fromString("00000000-0000-0000-0000-000000000502");
    private static final UUID DOMAIN_ID = UUID.fromString("00000000-0000-0000-0000-000000000601");
    private ClusterMetadataService previousService;

    @BeforeClass
    public static void initialize()
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Before
    public void installAuthoritativeMetadata()
    {
        previousService = ClusterMetadataService.instance();
        if (previousService != null)
            ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting(metadata(reservedTable("reserved")).forceEpoch(Epoch.EMPTY)));
    }

    @After
    public void restoreMetadataService()
    {
        ClusterMetadataService.unsetInstance();
        if (previousService != null)
            ClusterMetadataService.setInstance(previousService);
    }

    @Test
    public void suppliedMarkedTableIsRejectedBeforeMutationEffects()
    {
        TableMetadata reserved = reservedTable("reserved");
        TableMetadata ordinary = ordinaryTable("ordinary", ORDINARY_ID);
        Mutation mutation = mutation(reserved, ordinary);

        assertThatThrownBy(() -> TransactionDomainGuard.check(reserved, "write"))
        .isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> TransactionDomainGuard.check(mutation, "write"))
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void unknownProviderReservationRemainsClosed()
    {
        TableMetadata marked = TransactionDomainGuard.markReserved(ordinaryTable("future", RESERVED_ID), DOMAIN_ID);
        assertTrue(TransactionDomainGuard.isReserved(marked));
        assertThatThrownBy(() -> TransactionDomainGuard.check(marked, "read"))
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void ordinaryUnmarkedTableRemainsAllowed()
    {
        TableMetadata ordinary = ordinaryTable("ordinary", ORDINARY_ID);
        assertFalse(TransactionDomainGuard.isReserved(ordinary));
        assertThatCode(() -> TransactionDomainGuard.check(ordinary, "write")).doesNotThrowAnyException();
        assertThatCode(() -> TransactionDomainGuard.checkTables(Collections.singleton(ordinary.id), "read"))
        .doesNotThrowAnyException();
    }

    @Test
    public void unmarkedSuppliedMetadataWithReservedIdIsRejectedByAuthoritativeLookup()
    {
        TableMetadata staleRequest = ordinaryTable("stale", RESERVED_ID);

        assertThatThrownBy(() -> TransactionDomainGuard.check(staleRequest, "write"))
        .isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> TransactionDomainGuard.checkTables(Arrays.asList(ORDINARY_ID, RESERVED_ID), "write"))
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void descriptorAloneClosesUnmarkedSchema()
    {
        TableMetadata unmarked = ordinaryTable("unmarked", RESERVED_ID);
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting(metadata(unmarked).forceEpoch(Epoch.EMPTY)));
        assertFalse(TransactionDomainGuard.isReserved(unmarked));
        assertTrue(TransactionDomainGuard.hasReservations());
        assertThatThrownBy(() -> TransactionDomainGuard.check(unmarked, "write"))
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void markerAloneRequiresDecodingMaintenancePayloads()
    {
        ClusterMetadata marked = metadata(reservedTable("orphan")).transformer().with(ConsistencyDomains.EMPTY).build().metadata;
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting(marked.forceEpoch(Epoch.EMPTY)));
        assertTrue(TransactionDomainGuard.hasReservations());
        assertThatThrownBy(() -> TransactionDomainGuard.check(RESERVED_ID, "read"))
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void authoritativeMarkedMetadataRejectsUnmarkedSameIdSchemaReplacement()
    {
        TableMetadata reserved = reservedTable("reserved");
        TableMetadata staleRequest = ordinaryTable("stale", RESERVED_ID);
        ClusterMetadata before = metadata(reserved);
        KeyspaceMetadata keyspace = before.schema.getKeyspaces().get(KEYSPACE).get();
        Keyspaces after = before.schema.getKeyspaces().withAddedOrUpdated(keyspace.withSwapped(Tables.of(staleRequest)));

        assertThatThrownBy(() -> TransactionDomainGuard.checkSchemaChange(before, after))
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void schemaMarkerAndReservedTableCannotBeAlteredOrRemoved()
    {
        TableMetadata reserved = reservedTable("reserved");
        ClusterMetadata before = metadata(reserved);
        KeyspaceMetadata keyspace = before.schema.getKeyspaces().get(KEYSPACE).get();

        TableMetadata altered = reserved.unbuild().comment("changed").build();
        Keyspaces alteredSchema = before.schema.getKeyspaces().withAddedOrUpdated(keyspace.withSwapped(Tables.of(altered)));
        assertThatThrownBy(() -> TransactionDomainGuard.checkSchemaChange(before, alteredSchema))
        .isInstanceOf(InvalidRequestException.class);

        Keyspaces removedSchema = before.schema.getKeyspaces().withAddedOrUpdated(keyspace.withSwapped(Tables.none()));
        assertThatThrownBy(() -> TransactionDomainGuard.checkSchemaChange(before, removedSchema))
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void orphanMarkerFailsClosed()
    {
        TableMetadata orphan = TransactionDomainGuard.markReserved(ordinaryTable("orphan", ORDINARY_ID), DOMAIN_ID);
        assertThatThrownBy(() -> TransactionDomainGuard.check(orphan, "write"))
                  .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void activeExternalDomainIsAdmittedOnlyThroughTransactionFence()
    {
        TableMetadata active = reservedTable("active");
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting(metadata(active,
                                                                                           new ExternalTransactionDomainBinding(DOMAIN_ID.toString(), "7"),
                                                                                           DOMAIN_ID).forceEpoch(Epoch.EMPTY)));

        assertThatThrownBy(() -> TransactionDomainGuard.check(active, "write"))
                .isInstanceOf(InvalidRequestException.class);
        assertThatCode(() -> TransactionDomainGuard.checkTransaction(active, "TRANSACTION"))
                .doesNotThrowAnyException();
    }

    @Test
    public void activeExternalDomainRejectsMarkerMismatch()
    {
        TableMetadata active = TransactionDomainGuard.markReserved(ordinaryTable("active", RESERVED_ID),
                                                                    UUID.fromString("00000000-0000-0000-0000-000000000602"));
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting(metadata(active,
                                                                                           new ExternalTransactionDomainBinding(DOMAIN_ID.toString(), "7"),
                                                                                           DOMAIN_ID).forceEpoch(Epoch.EMPTY)));

        assertThatThrownBy(() -> TransactionDomainGuard.checkTransaction(active, "TRANSACTION"))
                .isInstanceOf(InvalidRequestException.class);
    }

    private static TableMetadata reservedTable(String name)
    {
        return TransactionDomainGuard.markReserved(ordinaryTable(name, RESERVED_ID), DOMAIN_ID);
    }

    private static TableMetadata ordinaryTable(String name, TableId id)
    {
        return TableMetadata.builder(KEYSPACE, name)
                            .id(id)
                            .partitioner(Murmur3Partitioner.instance)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addRegularColumn("value", Int32Type.instance)
                            .build();
    }

    private static ClusterMetadata metadata(TableMetadata reserved)
    {
        return metadata(reserved, null, DOMAIN_ID);
    }

    private static ClusterMetadata metadata(TableMetadata reserved,
                                            ExternalTransactionDomainBinding binding,
                                            UUID domainId)
    {
        KeyspaceMetadata keyspace = KeyspaceMetadata.create(KEYSPACE,
                                                             KeyspaceParams.simple(1),
                                                             Tables.of(reserved));
        TransactionDomainDescriptor descriptor = new TransactionDomainDescriptor(domainId,
                                                                                   RESERVED_ID,
                                                                                   binding == null ? "unknown-provider" : "etcd-scalar",
                                                                                   "scalar-int",
                                                                                   1, 1, 1, 1,
                                                                                   Collections.singleton(new NodeId(1)));
        if (binding != null)
            descriptor = descriptor.activate(binding);
        return new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY,
                                    new DistributedSchema(Keyspaces.of(keyspace)))
               .transformer().with(new ConsistencyDomains(Epoch.FIRST,
                                                          Collections.singletonMap(RESERVED_ID, descriptor))).build().metadata;
    }

    private static Mutation mutation(TableMetadata reserved, TableMetadata ordinary)
    {
        org.apache.cassandra.db.DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(1));
        return new Mutation.PartitionUpdateCollector(KEYSPACE, key)
               .add(PartitionUpdate.emptyUpdate(reserved, key))
               .add(PartitionUpdate.emptyUpdate(ordinary, key))
               .build();
    }
}
