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

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * The immutable, cluster-authoritative identity and format of one transaction domain.
 *
 * <p>PREPARED reservations are closed. ACTIVE reservations carry the immutable
 * binding of the external provider which owns the domain.</p>
 */
public final class TransactionDomainDescriptor
{
    public static final int MAX_IDENTIFIER_LENGTH = 128;
    public static final int MAX_PARTICIPANTS = 1024;
    private static final int PREPARED_STATE_TAG = 1;
    private static final int ACTIVE_STATE_TAG = 2;
    private static final Pattern IDENTIFIER = Pattern.compile("[a-z0-9][a-z0-9._-]{0,127}");

    public static final Serializer serializer = new Serializer();

    public enum State
    {
        PREPARED,
        ACTIVE
    }

    private final UUID id;
    private final TableId tableId;
    private final String providerId;
    private final String profileId;
    private final int profileVersion;
    private final int protocolVersion;
    private final int storageVersion;
    private final long generation;
    private final Set<NodeId> participants;
    private final State state;
    private final ExternalTransactionDomainBinding externalBinding;

    public TransactionDomainDescriptor(UUID id,
                                      TableId tableId,
                                      String providerId,
                                      String profileId,
                                      int profileVersion,
                                      int protocolVersion,
                                      int storageVersion,
                                      long generation,
                                      Set<NodeId> participants)
    {
        this.id = Objects.requireNonNull(id, "id");
        this.tableId = Objects.requireNonNull(tableId, "tableId");
        if (TableId.UNDEFINED.equals(tableId))
            throw new IllegalArgumentException("tableId must identify a concrete table");
        this.providerId = identifier(providerId, "providerId");
        this.profileId = identifier(profileId, "profileId");
        if (profileVersion <= 0)
            throw new IllegalArgumentException("profileVersion must be positive");
        if (protocolVersion <= 0)
            throw new IllegalArgumentException("protocolVersion must be positive");
        if (storageVersion <= 0)
            throw new IllegalArgumentException("storageVersion must be positive");
        if (generation <= 0)
            throw new IllegalArgumentException("generation must be positive");
        this.profileVersion = profileVersion;
        this.protocolVersion = protocolVersion;
        this.storageVersion = storageVersion;
        this.generation = generation;
        Objects.requireNonNull(participants, "participants");
        if (participants.isEmpty() || participants.size() > MAX_PARTICIPANTS)
            throw new IllegalArgumentException("participants must contain between one and " + MAX_PARTICIPANTS + " nodes");

        TreeSet<NodeId> sorted = new TreeSet<>();
        for (NodeId participant : participants)
        {
            Objects.requireNonNull(participant, "participants contains null");
            if (participant.id() < 0)
                throw new IllegalArgumentException("participants must contain registered node ids");
            sorted.add(participant);
        }
        this.participants = Collections.unmodifiableSet(new LinkedHashSet<>(sorted));
        this.state = State.PREPARED;
        this.externalBinding = null;
    }

    private TransactionDomainDescriptor(TransactionDomainDescriptor prepared, ExternalTransactionDomainBinding binding)
    {
        if ((!"etcd-scalar".equals(prepared.providerId) && !"ratis-scalar".equals(prepared.providerId)) ||
            !"scalar-int".equals(prepared.profileId) ||
            prepared.profileVersion != 1 || prepared.protocolVersion != 1 || prepared.storageVersion != 1)
            throw new IllegalArgumentException("Only etcd-scalar or ratis-scalar/scalar-int version 1 domains can be activated");
        this.id = prepared.id;
        this.tableId = prepared.tableId;
        this.providerId = prepared.providerId;
        this.profileId = prepared.profileId;
        this.profileVersion = prepared.profileVersion;
        this.protocolVersion = prepared.protocolVersion;
        this.storageVersion = prepared.storageVersion;
        this.generation = prepared.generation;
        this.participants = prepared.participants;
        this.state = State.ACTIVE;
        this.externalBinding = Objects.requireNonNull(binding, "binding");
        if (("etcd-scalar".equals(prepared.providerId) && binding.kind() != ExternalTransactionDomainBinding.Kind.ETCD_CLUSTER) ||
            ("ratis-scalar".equals(prepared.providerId) && binding.kind() != ExternalTransactionDomainBinding.Kind.RATIS_GROUP))
            throw new IllegalArgumentException("Transaction domain provider does not match binding kind");
    }

    private static String identifier(String value, String name)
    {
        if (value == null || value.length() > MAX_IDENTIFIER_LENGTH || !IDENTIFIER.matcher(value).matches())
            throw new IllegalArgumentException(name + " must be a canonical lower-case nonblank identifier");
        return value;
    }

    public UUID id()
    {
        return id;
    }

    public TableId tableId()
    {
        return tableId;
    }

    public String providerId()
    {
        return providerId;
    }

    public String profileId()
    {
        return profileId;
    }

    public int profileVersion()
    {
        return profileVersion;
    }

    public int protocolVersion()
    {
        return protocolVersion;
    }

    public int storageVersion()
    {
        return storageVersion;
    }

    public long generation()
    {
        return generation;
    }

    public Set<NodeId> participants()
    {
        return participants;
    }

    public State state()
    {
        return state;
    }

    public ExternalTransactionDomainBinding externalBinding()
    {
        return externalBinding;
    }

    public TransactionDomainDescriptor activate(ExternalTransactionDomainBinding binding)
    {
        if (state != State.PREPARED)
            throw new IllegalStateException("Transaction domain is already active");
        if ((!"etcd-scalar".equals(providerId) && !"ratis-scalar".equals(providerId)) ||
            !"scalar-int".equals(profileId) ||
            profileVersion != 1 || protocolVersion != 1 || storageVersion != 1)
            throw new IllegalArgumentException("Only etcd-scalar or ratis-scalar/scalar-int version 1 domains can be activated");
        return new TransactionDomainDescriptor(this, binding);
    }

    public Version minimumVersion()
    {
        return state == State.ACTIVE ? externalBinding.minimumVersion() : Version.V11;
    }

    public void checkSerializationVersion(Version version)
    {
        if (state == State.ACTIVE)
            externalBinding.checkSerializationVersion(version);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
        if (!(other instanceof TransactionDomainDescriptor))
            return false;
        TransactionDomainDescriptor that = (TransactionDomainDescriptor) other;
        return profileVersion == that.profileVersion &&
               protocolVersion == that.protocolVersion &&
               storageVersion == that.storageVersion &&
               generation == that.generation &&
               state == that.state &&
               id.equals(that.id) &&
               tableId.equals(that.tableId) &&
               providerId.equals(that.providerId) &&
               profileId.equals(that.profileId) &&
               participants.equals(that.participants) &&
               Objects.equals(externalBinding, that.externalBinding);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, tableId, providerId, profileId, profileVersion, protocolVersion, storageVersion, generation, participants, state, externalBinding);
    }

    @Override
    public String toString()
    {
        return "TransactionDomainDescriptor{" +
               "id=" + id +
               ", tableId=" + tableId +
               ", providerId='" + providerId + '\'' +
               ", profileId='" + profileId + '\'' +
               ", profileVersion=" + profileVersion +
               ", protocolVersion=" + protocolVersion +
               ", storageVersion=" + storageVersion +
               ", generation=" + generation +
               ", state=" + state +
               ", externalBinding=" + externalBinding +
               ", participants=" + participants +
               '}';
    }

    public static final class Serializer implements MetadataSerializer<TransactionDomainDescriptor>
    {
        @Override
        public void serialize(TransactionDomainDescriptor descriptor, DataOutputPlus out, Version version) throws IOException
        {
            descriptor.checkSerializationVersion(version);
            out.writeUnsignedVInt32(descriptor.state == State.ACTIVE ? ACTIVE_STATE_TAG : PREPARED_STATE_TAG);
            out.writeLong(descriptor.id.getMostSignificantBits());
            out.writeLong(descriptor.id.getLeastSignificantBits());
            TableId.metadataSerializer.serialize(descriptor.tableId, out, version);
            out.writeUTF(descriptor.providerId);
            out.writeUTF(descriptor.profileId);
            out.writeUnsignedVInt32(descriptor.profileVersion);
            out.writeUnsignedVInt32(descriptor.protocolVersion);
            out.writeUnsignedVInt32(descriptor.storageVersion);
            out.writeUnsignedVInt(descriptor.generation);
            out.writeUnsignedVInt32(descriptor.participants.size());
            for (NodeId participant : descriptor.participants)
                NodeId.serializer.serialize(participant, out, version);
            if (descriptor.state == State.ACTIVE)
                ExternalTransactionDomainBinding.serializer.serialize(descriptor.externalBinding, out, version);
        }

        @Override
        public TransactionDomainDescriptor deserialize(DataInputPlus in, Version version) throws IOException
        {
            int state = in.readUnsignedVInt32();
            if (state != PREPARED_STATE_TAG && state != ACTIVE_STATE_TAG)
                throw new IOException("Unsupported transaction domain state: " + state);
            UUID id = new UUID(in.readLong(), in.readLong());
            TableId tableId = TableId.metadataSerializer.deserialize(in, version);
            String providerId = in.readUTF();
            String profileId = in.readUTF();
            int profileVersion = in.readUnsignedVInt32();
            int protocolVersion = in.readUnsignedVInt32();
            int storageVersion = in.readUnsignedVInt32();
            long generation = in.readUnsignedVInt();
            int count = in.readUnsignedVInt32();
            if (count == 0 || count > MAX_PARTICIPANTS)
                throw new IOException("Invalid transaction domain participant count: " + count);
            Set<NodeId> participants = new LinkedHashSet<>(count);
            for (int i = 0; i < count; i++)
            {
                NodeId participant = NodeId.serializer.deserialize(in, version);
                if (!participants.add(participant))
                    throw new IOException("Duplicate transaction domain participant: " + participant);
            }
            TransactionDomainDescriptor descriptor = new TransactionDomainDescriptor(id, tableId, providerId, profileId,
                                                                                       profileVersion, protocolVersion, storageVersion,
                                                                                       generation, participants);
            if (state == ACTIVE_STATE_TAG)
            {
                if (version.isBefore(Version.V12))
                    throw new IOException("ACTIVE transaction domains require metadata V12");
                descriptor = descriptor.activate(ExternalTransactionDomainBinding.serializer.deserialize(in, version));
            }
            return descriptor;
        }

        @Override
        public long serializedSize(TransactionDomainDescriptor descriptor, Version version)
        {
            descriptor.checkSerializationVersion(version);
            return TypeSizes.sizeofUnsignedVInt(descriptor.state == State.ACTIVE ? ACTIVE_STATE_TAG : PREPARED_STATE_TAG) + 16 +
                   TableId.metadataSerializer.serializedSize(descriptor.tableId, version) +
                   TypeSizes.sizeof(descriptor.providerId) + TypeSizes.sizeof(descriptor.profileId) +
                   TypeSizes.sizeofUnsignedVInt(descriptor.profileVersion) +
                   TypeSizes.sizeofUnsignedVInt(descriptor.protocolVersion) +
                   TypeSizes.sizeofUnsignedVInt(descriptor.storageVersion) +
                   TypeSizes.sizeofUnsignedVInt(descriptor.generation) +
                   TypeSizes.sizeofUnsignedVInt(descriptor.participants.size()) +
                   descriptor.participants.stream().mapToLong(p -> NodeId.serializer.serializedSize(p, version)).sum() +
                   (descriptor.state == State.ACTIVE ? ExternalTransactionDomainBinding.serializer.serializedSize(descriptor.externalBinding, version) : 0);
        }
    }
}
