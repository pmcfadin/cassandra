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

package org.apache.cassandra.tcm.transformations;

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;
import org.apache.cassandra.service.consensus.txn.TransactionDomainDescriptor;
import org.apache.cassandra.service.consensus.txn.TransactionDomainGuard;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

/** Atomically creates a fresh scalar table and its closed ownership reservation. */
public final class PrepareTransactionDomain implements Transformation
{
    public static final String PROFILE_ID = "scalar-int";
    public static final Serializer serializer = new Serializer();

    private final TableMetadata table;
    private final TransactionDomainDescriptor descriptor;

    public PrepareTransactionDomain(TableMetadata table, TransactionDomainDescriptor descriptor)
    {
        this.table = Objects.requireNonNull(table, "table");
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
    }

    @Override
    public Kind kind()
    {
        return Kind.PREPARE_TRANSACTION_DOMAIN;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        try
        {
            validate(prev);
            Keyspaces keyspaces = prev.schema.getKeyspaces();
            KeyspaceMetadata keyspace = keyspaces.getNullable(table.keyspace);
            TableMetadata reserved = TransactionDomainGuard.markReserved(table, descriptor.id())
                                                          .unbuild().epoch(prev.nextEpoch()).build();
            DistributedSchema schema = new DistributedSchema(keyspaces.withAddedOrUpdated(keyspace.withSwapped(keyspace.tables.with(reserved))));
            return Transformation.success(prev.transformer().with(schema).with(prev.consistencyDomains.withDomain(descriptor)),
                                          LockedRanges.AffectedRanges.EMPTY);
        }
        catch (IllegalArgumentException | InvalidRequestException | ConfigurationException e)
        {
            return new Rejected(INVALID, e.getMessage());
        }
    }

    private void validate(ClusterMetadata prev)
    {
        require(prev.directory.commonSerializationVersion.isAtLeast(Version.V11), "All nodes must support transaction domain metadata V11");
        // The directory's common version excludes pre-TCM (OLD) nodes.
        for (NodeId node : prev.directory.peerIds())
            if (prev.directory.peerState(node) != NodeState.LEFT)
                require(prev.directory.versions.get(node).serializationVersion().isAtLeast(Version.V11),
                        "All current nodes must support transaction domain metadata V11: " + node);
        require(prev.lockedRanges.locked.isEmpty() && prev.inProgressSequences.isEmpty(), "Cannot reserve a domain during a topology change");
        Keyspaces keyspaces = prev.schema.getKeyspaces();
        KeyspaceMetadata keyspace = keyspaces.getNullable(table.keyspace);
        require(keyspace != null && !SchemaConstants.isSystemKeyspace(table.keyspace) && !keyspace.params.replication.isLocal(),
                "Transaction domains require an existing non-system replicated keyspace");
        require(keyspaces.getTableOrViewNullable(table.id) == null && keyspace.getTableOrViewNullable(table.name) == null,
                "Transaction domains require a fresh table name and ID");
        require(descriptor.tableId().equals(table.id), "Domain table ID does not match the new table");
        require(PROFILE_ID.equals(descriptor.profileId()) && descriptor.profileVersion() == 1 &&
                descriptor.protocolVersion() == 1 && descriptor.storageVersion() == 1 && descriptor.generation() == 1,
                "Only scalar-int profile/version 1 with initial generation 1 is supported");
        require(!descriptor.participants().isEmpty(), "A transaction domain needs participants");
        for (NodeId participant : descriptor.participants())
            require(prev.directory.peerState(participant) == NodeState.JOINED, "Domain participants must be registered and joined: " + participant);
        require(table.partitionKeyColumns().size() == 1 && table.partitionKeyColumns().get(0).type == Int32Type.instance &&
                table.regularColumns().size() == 1 && table.regularColumns().iterator().next().type == Int32Type.instance &&
                table.clusteringColumns().isEmpty() && table.staticColumns().isEmpty(),
                "scalar-int requires one int partition key and one int regular column");
        require(!table.isCounter() && !table.isCompactTable() && !table.isView() && !table.isVirtual() && !table.isIndex() &&
                table.indexes.isEmpty() && table.triggers.isEmpty() && table.droppedColumns.isEmpty() &&
                table.params.defaultTimeToLive == 0 && !table.params.cdc && table.params.extensions.isEmpty() &&
                table.params.transactionalMode == TransactionalMode.off &&
                table.params.transactionalMigrationFrom == TransactionalMigrationFromMode.none && !table.params.pendingDrop,
                "scalar-int does not support custom table features, TTL, CDC, indexes, triggers, or Accord migration");
        require(table.partitioner.equals(prev.partitioner), "Domain table must use the cluster partitioner");
        for (ColumnMetadata column : table.columns())
            require(!column.isMasked() && !column.hasConstraint(), "scalar-int does not support column masking or constraints");
        table.validate();
    }

    private static void require(boolean condition, String message)
    {
        if (!condition)
            throw new InvalidRequestException(message);
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, PrepareTransactionDomain>
    {
        private void requireVersion(Version version)
        {
            if (!version.isAtLeast(Version.V11))
                throw new IllegalArgumentException("Transaction domain preparation requires metadata V11");
        }

        public void serialize(Transformation transformation, DataOutputPlus out, Version version) throws IOException
        {
            requireVersion(version);
            PrepareTransactionDomain prepare = (PrepareTransactionDomain) transformation;
            TableMetadata.serializer.serialize(prepare.table, out, version);
            TransactionDomainDescriptor.serializer.serialize(prepare.descriptor, out, version);
        }

        public PrepareTransactionDomain deserialize(DataInputPlus in, Version version) throws IOException
        {
            requireVersion(version);
            TableMetadata table = TableMetadata.serializer.deserialize(in, Types.none(), UserFunctions.none(), version);
            return new PrepareTransactionDomain(table, TransactionDomainDescriptor.serializer.deserialize(in, version));
        }

        public long serializedSize(Transformation transformation, Version version)
        {
            requireVersion(version);
            PrepareTransactionDomain prepare = (PrepareTransactionDomain) transformation;
            return TableMetadata.serializer.serializedSize(prepare.table, version) +
                   TransactionDomainDescriptor.serializer.serializedSize(prepare.descriptor, version);
        }
    }
}
