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
import java.util.UUID;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.txn.ExternalTransactionDomainBinding;
import org.apache.cassandra.service.consensus.txn.TransactionDomainDescriptor;
import org.apache.cassandra.service.consensus.txn.TransactionDomainGuard;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

/** Records the external binding for an already prepared domain. */
public final class ActivateTransactionDomain implements Transformation
{
    public static final Serializer serializer = new Serializer();

    private final TableId tableId;
    private final UUID domainId;
    private final long generation;
    private final ExternalTransactionDomainBinding binding;

    public ActivateTransactionDomain(TableId tableId, UUID domainId, long generation,
                                     ExternalTransactionDomainBinding binding)
    {
        this.tableId = Objects.requireNonNull(tableId, "tableId");
        this.domainId = Objects.requireNonNull(domainId, "domainId");
        this.binding = Objects.requireNonNull(binding, "binding");
        if (generation <= 0)
            throw new IllegalArgumentException("generation must be positive");
        this.generation = generation;
    }

    @Override
    public Kind kind()
    {
        return Kind.ACTIVATE_TRANSACTION_DOMAIN;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        try
        {
            TableMetadata table = prev.schema.getTableMetadata(tableId);
            TransactionDomainDescriptor prepared = prev.consistencyDomains.forTable(tableId);
            require(table != null, "No table exists for transaction domain " + tableId);
            require(prepared != null, "No transaction domain reserves table " + tableId);
            require(prepared.id().equals(domainId), "Transaction domain identity does not match the reservation");
            require(prepared.generation() == generation, "Transaction domain generation does not match the reservation");
            require(prepared.state() == TransactionDomainDescriptor.State.PREPARED, "Transaction domain is already active");
            require(prepared.providerId().equals("etcd-scalar") || prepared.providerId().equals("ratis-scalar"),
                    "Unsupported transaction domain provider");
            require((prepared.providerId().equals("etcd-scalar") && binding.kind() == ExternalTransactionDomainBinding.Kind.ETCD_CLUSTER) ||
                    (prepared.providerId().equals("ratis-scalar") && binding.kind() == ExternalTransactionDomainBinding.Kind.RATIS_GROUP),
                    "Transaction domain provider does not match binding kind");
            require(prepared.profileId().equals(PrepareTransactionDomain.PROFILE_ID) &&
                    prepared.profileVersion() == 1 && prepared.protocolVersion() == 1 && prepared.storageVersion() == 1,
                    "Only scalar-int profile/version 1 is supported");
            require(markerMatches(table, domainId), "Transaction domain marker does not match the reservation");
            require(prev.lockedRanges.locked.isEmpty() && prev.inProgressSequences.isEmpty(), "Cannot activate a domain during a topology change");
            for (NodeId node : prev.directory.peerIds())
            {
                if (prev.directory.peerState(node) != NodeState.LEFT)
                {
                    Version required = binding.minimumVersion();
                    require(prev.directory.versions.get(node).serializationVersion().isAtLeast(required),
                            "All non-LEFT nodes must support transaction domain metadata " + required + ": " + node);
                }
            }
            if (binding.kind() == ExternalTransactionDomainBinding.Kind.RATIS_GROUP)
                require(binding.schemaEpoch() == table.epoch.getEpoch(),
                        "Ratis readiness schema epoch does not match the reserved table");
            for (NodeId participant : prepared.participants())
                require(prev.directory.peerState(participant) == NodeState.JOINED,
                        "Domain participants must be registered and joined: " + participant);
            TransactionDomainDescriptor active = prepared.activate(binding);
            return Transformation.success(prev.transformer().with(prev.consistencyDomains.replace(active)),
                                          LockedRanges.AffectedRanges.EMPTY);
        }
        catch (IllegalArgumentException | InvalidRequestException e)
        {
            return new Rejected(INVALID, e.getMessage());
        }
    }

    private static boolean markerMatches(TableMetadata table, UUID domainId)
    {
        return ByteBufferUtil.bytes(domainId.toString()).equals(table.params.extensions.get(TransactionDomainGuard.MARKER));
    }

    private static void require(boolean condition, String message)
    {
        if (!condition)
            throw new InvalidRequestException(message);
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, ActivateTransactionDomain>
    {
        @Override
        public void serialize(Transformation transformation, DataOutputPlus out, Version version) throws IOException
        {
            ActivateTransactionDomain activate = (ActivateTransactionDomain) transformation;
            if (version.isBefore(activate.binding.minimumVersion()))
                throw new IllegalArgumentException("Transaction domain activation requires metadata " + activate.binding.minimumVersion());
            activate.binding.checkSerializationVersion(version);
            TableId.metadataSerializer.serialize(activate.tableId, out, version);
            out.writeLong(activate.domainId.getMostSignificantBits());
            out.writeLong(activate.domainId.getLeastSignificantBits());
            out.writeUnsignedVInt(activate.generation);
            ExternalTransactionDomainBinding.serializer.serialize(activate.binding, out, version);
        }

        @Override
        public ActivateTransactionDomain deserialize(DataInputPlus in, Version version) throws IOException
        {
            if (version.isBefore(Version.V12))
                throw new IOException("Transaction domain activation requires metadata V12");
            TableId tableId = TableId.metadataSerializer.deserialize(in, version);
            UUID domainId = new UUID(in.readLong(), in.readLong());
            long generation = in.readUnsignedVInt();
            return new ActivateTransactionDomain(tableId, domainId, generation,
                                                 ExternalTransactionDomainBinding.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(Transformation transformation, Version version)
        {
            ActivateTransactionDomain activate = (ActivateTransactionDomain) transformation;
            if (version.isBefore(activate.binding.minimumVersion()))
                throw new IllegalArgumentException("Transaction domain activation requires metadata " + activate.binding.minimumVersion());
            activate.binding.checkSerializationVersion(version);
            return TableId.metadataSerializer.serializedSize(activate.tableId, version) + 16 +
                   org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt(activate.generation) +
                   ExternalTransactionDomainBinding.serializer.serializedSize(activate.binding, version);
        }
    }
}
