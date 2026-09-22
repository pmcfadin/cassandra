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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.sequences.DropAccordTable.TableReference;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.ActivateTransactionDomain;
import org.apache.cassandra.tcm.transformations.FinishDropAccordTable;
import org.apache.cassandra.tcm.transformations.PrepareDropAccordTable;
import org.apache.cassandra.tcm.transformations.PrepareTransactionDomain;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.Startup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class TransactionDomainMetadataTest
{
    private static final TableId TABLE_ID = TableId.fromString("00000000-0000-0000-0000-000000000301");
    private static final TableId SECOND_TABLE_ID = TableId.fromString("00000000-0000-0000-0000-000000000302");
    private static final UUID DOMAIN_ID = UUID.fromString("00000000-0000-0000-0000-000000000401");

    @BeforeClass
    public static void initialize()
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Test
    public void descriptorIsImmutableAndValueBased()
    {
        Set<NodeId> participants = new LinkedHashSet<>(Arrays.asList(new NodeId(2), new NodeId(1)));
        TransactionDomainDescriptor descriptor = descriptor(TABLE_ID, DOMAIN_ID, participants);

        participants.add(new NodeId(3));
        assertThat(descriptor.participants()).containsExactly(new NodeId(1), new NodeId(2));
        assertThatThrownBy(() -> descriptor.participants().add(new NodeId(4)))
        .isInstanceOf(UnsupportedOperationException.class);

        TransactionDomainDescriptor equivalent = descriptor(TABLE_ID, DOMAIN_ID,
                                                            new LinkedHashSet<>(Arrays.asList(new NodeId(1), new NodeId(2))));
        assertThat(descriptor).isEqualTo(equivalent).hasSameHashCodeAs(equivalent);
        assertThat(descriptor.state()).isEqualTo(TransactionDomainDescriptor.State.PREPARED);
    }

    @Test
    public void descriptorRejectsNonCanonicalIdentifiersAndInvalidNumbers()
    {
        assertThatThrownBy(() -> descriptor(TABLE_ID, DOMAIN_ID, "Provider", "profile-1", 1, 1, 1, 1, participants()))
        .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> descriptor(TABLE_ID, DOMAIN_ID, "provider", " profile", 1, 1, 1, 1, participants()))
        .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> descriptor(TABLE_ID, DOMAIN_ID, "provider", "profile", 0, 1, 1, 1, participants()))
        .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> descriptor(TABLE_ID, DOMAIN_ID, "provider", "profile", 1, 0, 1, 1, participants()))
        .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> descriptor(TABLE_ID, DOMAIN_ID, "provider", "profile", 1, 1, 0, 1, participants()))
        .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> descriptor(TABLE_ID, DOMAIN_ID, "provider", "profile", 1, 1, 1, 0, participants()))
        .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> descriptor(TABLE_ID, DOMAIN_ID, "provider", "profile", 1, 1, 1, 1, Collections.emptySet()))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void descriptorSerializerRoundTripsStableIdentifiers() throws IOException
    {
        TransactionDomainDescriptor descriptor = descriptor(TABLE_ID, DOMAIN_ID, participants());
        DataOutputBuffer output = new DataOutputBuffer();
        TransactionDomainDescriptor.serializer.serialize(descriptor, output, Version.V11);
        assertEquals(TransactionDomainDescriptor.serializer.serializedSize(descriptor, Version.V11), output.getLength());

        DataInputBuffer input = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
        assertThat(TransactionDomainDescriptor.serializer.deserialize(input, Version.V11)).isEqualTo(descriptor);
    }

    @Test
    public void preparedEncodingIsIdenticalAtV11AndV12() throws IOException
    {
        TransactionDomainDescriptor descriptor = descriptor(TABLE_ID, DOMAIN_ID, participants());
        DataOutputBuffer v11 = new DataOutputBuffer();
        DataOutputBuffer v12 = new DataOutputBuffer();
        TransactionDomainDescriptor.serializer.serialize(descriptor, v11, Version.V11);
        TransactionDomainDescriptor.serializer.serialize(descriptor, v12, Version.V12);
        assertThat(v11.toByteArray()).isEqualTo(v12.toByteArray());
        assertThat(TransactionDomainDescriptor.serializer.serializedSize(descriptor, Version.V11)).isEqualTo(v11.getLength());
        assertThat(TransactionDomainDescriptor.serializer.serializedSize(descriptor, Version.V12)).isEqualTo(v12.getLength());
    }

    @Test
    public void domainsRejectDuplicateTableAndDomainIdsAndExposeImmutableValues()
    {
        TransactionDomainDescriptor first = descriptor(TABLE_ID, DOMAIN_ID, participants());
        ConsistencyDomains domains = new ConsistencyDomains(Epoch.FIRST,
                                                            Collections.singletonMap(TABLE_ID, first));
        TransactionDomainDescriptor duplicateTable = descriptor(TABLE_ID, UUID.randomUUID(), participants());
        TransactionDomainDescriptor duplicateDomain = descriptor(SECOND_TABLE_ID, DOMAIN_ID, participants());

        assertThatThrownBy(() -> domains.withDomain(duplicateTable)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> domains.withDomain(duplicateDomain)).isInstanceOf(IllegalArgumentException.class);
        assertThat(domains.domains()).containsExactly(first);
        assertThatThrownBy(() -> domains.domains().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertSame(first, domains.forTable(TABLE_ID));
    }

    @Test
    public void domainsRoundTripInClusterMetadataAtV11() throws IOException
    {
        TransactionDomainDescriptor descriptor = descriptor(TABLE_ID, DOMAIN_ID, participants());
        ConsistencyDomains domains = new ConsistencyDomains(Epoch.FIRST,
                                                            Collections.singletonMap(TABLE_ID, descriptor));
        ClusterMetadata metadata = ClusterMetadataTestFixtures.metadata().transformer().with(domains).build().metadata;

        DataOutputBuffer output = new DataOutputBuffer();
        ClusterMetadata.serializer.serialize(metadata, output, Version.V11);
        DataInputBuffer input = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
        ClusterMetadata restored = ClusterMetadata.serializer.deserialize(input, Version.V11);

        assertThat(restored.consistencyDomains).isEqualTo(metadata.consistencyDomains);
        assertThat(restored.consistencyDomains.forTable(TABLE_ID)).isEqualTo(descriptor);
    }

    @Test
    public void nonEmptyDomainsCannotBeSerializedToOlderMetadataVersion() throws IOException
    {
        ConsistencyDomains domains = new ConsistencyDomains(Epoch.FIRST,
                                                            Collections.singletonMap(TABLE_ID,
                                                                                     descriptor(TABLE_ID, DOMAIN_ID, participants())));
        ClusterMetadata metadata = ClusterMetadataTestFixtures.metadata().transformer().with(domains).build().metadata;

        assertThatThrownBy(() -> ClusterMetadata.serializer.serialize(metadata, new DataOutputBuffer(), Version.V10))
        .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void emptyDomainsRemainReadableBeforeV11() throws IOException
    {
        ClusterMetadata metadata = ClusterMetadataTestFixtures.metadata();
        DataOutputBuffer output = new DataOutputBuffer();
        ClusterMetadata.serializer.serialize(metadata, output, Version.V10);
        DataInputBuffer input = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
        ClusterMetadata restored = ClusterMetadata.serializer.deserialize(input, Version.V10);

        assertThat(restored.consistencyDomains).isEqualTo(ConsistencyDomains.EMPTY);
    }

    @Test
    public void forceEpochAndUnrelatedTransformRetainDomains()
    {
        TransactionDomainDescriptor descriptor = descriptor(TABLE_ID, DOMAIN_ID, participants());
        ConsistencyDomains domains = new ConsistencyDomains(Epoch.FIRST,
                                                            Collections.singletonMap(TABLE_ID, descriptor));
        ClusterMetadata metadata = ClusterMetadataTestFixtures.metadata().transformer().with(domains).build().metadata;

        ClusterMetadata forced = metadata.forceEpoch(metadata.epoch.nextEpoch());
        assertThat(forced.consistencyDomains.domains()).containsExactly(descriptor);

        ClusterMetadata transformed = metadata.transformer().with(DistributedSchema.empty()).build().metadata;
        assertThat(transformed.consistencyDomains.domains()).containsExactly(descriptor);
    }

    @Test
    public void prepareTransactionDomainInstallsMarkerAndDescriptorAtomically()
    {
        ClusterMetadata metadata = prepareMetadata();
        TableMetadata table = scalarTable("reserved", TableId.fromString("00000000-0000-0000-0000-000000000303"));
        TransactionDomainDescriptor descriptor = prepareDescriptor(table.id, "unknown-provider");

        Transformation.Result result = new PrepareTransactionDomain(table, descriptor).execute(metadata);

        assertThat(result.isSuccess()).isTrue();
        ClusterMetadata next = result.success().metadata;
        TableMetadata installed = next.schema.getTableMetadata(table.id);
        assertThat(installed).isNotNull();
        assertThat(TransactionDomainGuard.isReserved(installed)).isTrue();
        assertThat(next.consistencyDomains.forTable(table.id)).isEqualTo(descriptor);
    }

    @Test
    public void prepareTransactionDomainRejectsDuplicateNameAndDomainWithoutChangingState()
    {
        ClusterMetadata metadata = prepareMetadata();
        TableMetadata duplicateName = scalarTable("existing", TableId.fromString("00000000-0000-0000-0000-000000000304"));
        Transformation.Result nameResult = new PrepareTransactionDomain(duplicateName,
                                                                         prepareDescriptor(duplicateName.id, "provider"))
                                            .execute(metadata);
        assertThat(nameResult.isRejected()).isTrue();
        assertThat(metadata.schema.getTableMetadata(duplicateName.id)).isNull();
        assertThat(metadata.consistencyDomains.isEmpty()).isTrue();

        TableMetadata duplicateTableId = scalarTable("fresh-id", TABLE_ID);
        Transformation.Result tableResult = new PrepareTransactionDomain(duplicateTableId,
                                                                          prepareDescriptor(duplicateTableId.id, "provider"))
                                             .execute(metadata);
        assertThat(tableResult.isRejected()).isTrue();
        assertThat(metadata.schema.getTableMetadata(TABLE_ID).name).isEqualTo("existing");
        assertThat(metadata.consistencyDomains.isEmpty()).isTrue();

        TableMetadata fresh = scalarTable("fresh", TableId.fromString("00000000-0000-0000-0000-000000000305"));
        TransactionDomainDescriptor existingDomain = prepareDescriptor(TABLE_ID, "provider");
        ClusterMetadata reservedMetadata = metadata.transformer()
                                                      .with(new ConsistencyDomains(Epoch.FIRST,
                                                                                    Collections.singletonMap(TABLE_ID, existingDomain)))
                                                      .build().metadata;
        TransactionDomainDescriptor duplicateId = new TransactionDomainDescriptor(existingDomain.id(),
                                                                                     fresh.id,
                                                                                     "provider",
                                                                                     PrepareTransactionDomain.PROFILE_ID,
                                                                                     1, 1, 1, 1,
                                                                                     Collections.singleton(new NodeId(1)));
        Transformation.Result idResult = new PrepareTransactionDomain(fresh, duplicateId).execute(reservedMetadata);
        assertThat(idResult.isRejected()).isTrue();
        assertThat(reservedMetadata.schema.getTableMetadata(fresh.id)).isNull();
        assertThat(reservedMetadata.consistencyDomains.forTable(TABLE_ID)).isEqualTo(existingDomain);
    }

    @Test
    public void prepareSerializerRequiresV11AndRoundTripsAtV11() throws IOException
    {
        ClusterMetadata metadata = prepareMetadata();
        TableMetadata table = scalarTable("serialized", TableId.fromString("00000000-0000-0000-0000-000000000306"));
        PrepareTransactionDomain prepare = new PrepareTransactionDomain(table, prepareDescriptor(table.id, "provider"));

        DataOutputBuffer output = new DataOutputBuffer();
        PrepareTransactionDomain.serializer.serialize(prepare, output, Version.V11);
        assertThat(PrepareTransactionDomain.serializer.serializedSize(prepare, Version.V11)).isEqualTo(output.getLength());
        PrepareTransactionDomain decoded = PrepareTransactionDomain.serializer.deserialize(new DataInputBuffer(output.unsafeGetBufferAndFlip(), false), Version.V11);
        assertThat(decoded.execute(metadata).isSuccess()).isTrue();

        assertThatThrownBy(() -> PrepareTransactionDomain.serializer.serialize(prepare, new DataOutputBuffer(), Version.V10))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void reservationRejectsUnsupportedProfileAndOldNodes()
    {
        ClusterMetadata metadata = prepareMetadata();
        TableMetadata table = scalarTable("unsupported", TableId.generate());
        TransactionDomainDescriptor unsupported = new TransactionDomainDescriptor(UUID.randomUUID(), table.id, "provider",
                                                                                   PrepareTransactionDomain.PROFILE_ID, 2, 1, 1, 1,
                                                                                   Collections.singleton(new NodeId(1)));
        assertThat(new PrepareTransactionDomain(table, unsupported).execute(metadata).isRejected()).isTrue();
        NodeVersion old = new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V10);
        ClusterMetadata mixed = metadata.transformer().with(metadata.directory.withNodeVersion(new NodeId(1), old)).build().metadata;
        PrepareTransactionDomain prepare = new PrepareTransactionDomain(table, prepareDescriptor(table.id, "provider"));
        assertThat(prepare.eligibleToCommit(mixed)).isFalse();
        assertThat(prepare.execute(mixed).isRejected()).isTrue();
        assertThat(metadata.schema.getTableMetadata(table.id)).isNull();
        assertThat(metadata.consistencyDomains.isEmpty()).isTrue();
    }

    @Test
    public void reservedMetadataRejectsDowngradeAndAddressChangesButAllowsSameAddressRestart()
    {
        ClusterMetadata metadata = prepareMetadata();
        TableMetadata table = scalarTable("restart", TableId.generate());
        metadata = new PrepareTransactionDomain(table, prepareDescriptor(table.id, "provider")).execute(metadata).success().metadata;
        NodeId participant = new NodeId(1);
        NodeVersion old = new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V10);
        NodeAddresses existing = metadata.directory.addresses.get(participant);
        NodeAddresses changed = new NodeAddresses(InetAddressAndPort.getByNameUnchecked("127.0.0.2"));
        assertThat(new Startup(participant, existing, old).execute(metadata).isRejected()).isTrue();
        assertThat(new Register(changed, new Location("dc1", "rack1"), old).execute(metadata).isRejected()).isTrue();
        assertThat(new Startup(participant, changed, NodeVersion.CURRENT).execute(metadata).isRejected()).isTrue();
        assertThat(new Startup(participant, existing, NodeVersion.CURRENT).execute(metadata).isSuccess()).isTrue();
    }

    @Test
    public void accordDropTransformationsCannotRemoveReservedTable()
    {
        TableMetadata table = scalarTable("closed", TableId.generate());
        ClusterMetadata metadata = new PrepareTransactionDomain(table, prepareDescriptor(table.id, "provider"))
                                   .execute(prepareMetadata()).success().metadata;
        TableReference reference = new TableReference(table.id);
        assertThat(new PrepareDropAccordTable(reference).execute(metadata).isRejected()).isTrue();
        assertThat(new FinishDropAccordTable(reference).execute(metadata).isRejected()).isTrue();
        assertThat(metadata.schema.getTableMetadata(table.id)).isNotNull();
    }

    @Test
    public void descriptorRejectsUnknownWireState() throws IOException
    {
        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            output.writeUnsignedVInt32(99);
            try (DataInputBuffer input = new DataInputBuffer(output.asNewBuffer(), false))
            {
                assertThatThrownBy(() -> TransactionDomainDescriptor.serializer.deserialize(input, Version.V11))
                .isInstanceOf(IOException.class).hasMessageContaining("Unsupported transaction domain state");
            }
        }
    }

    @Test
    public void emptyV11SnapshotRoundTripsWithExactSize() throws IOException
    {
        ClusterMetadata metadata = ClusterMetadataTestFixtures.metadata();
        try (DataOutputBuffer output = new DataOutputBuffer())
        {
            ClusterMetadata.serializer.serialize(metadata, output, Version.V11);
            try (DataInputBuffer input = new DataInputBuffer(output.asNewBuffer(), false))
            {
                ClusterMetadata restored = ClusterMetadata.serializer.deserialize(input, Version.V11);
                assertThat(restored).isEqualTo(metadata);
            }
            assertEquals(ClusterMetadata.serializer.serializedSize(metadata, Version.V11), output.getLength());
        }
    }

    @Test
    public void activeSnapshotUsesV12AndCannotBeDowngraded() throws IOException
    {
        TableMetadata table = scalarTable("active", TableId.generate());
        ClusterMetadata prepared = new PrepareTransactionDomain(table, prepareDescriptor(table.id, "etcd-scalar"))
                                   .execute(prepareMetadata()).success().metadata;
        TransactionDomainDescriptor reservation = prepared.consistencyDomains.forTable(table.id);
        ExternalTransactionDomainBinding binding = new ExternalTransactionDomainBinding("00000000-0000-0000-0000-000000000501", "42");
        ClusterMetadata active = new ActivateTransactionDomain(table.id, reservation.id(), reservation.generation(), binding)
                                .execute(prepared).success().metadata;
        assertThat(active.consistencyDomains.forTable(table.id).state()).isEqualTo(TransactionDomainDescriptor.State.ACTIVE);
        assertThat(active.consistencyDomains.forTable(table.id).externalBinding()).isEqualTo(binding);

        DataOutputBuffer v12 = new DataOutputBuffer();
        ClusterMetadata.serializer.serialize(active, v12, Version.V12);
        ClusterMetadata restored = ClusterMetadata.serializer.deserialize(new DataInputBuffer(v12.unsafeGetBufferAndFlip(), false), Version.V12);
        assertThat(restored.consistencyDomains).isEqualTo(active.consistencyDomains);
        DataOutputBuffer v11 = new DataOutputBuffer();
        assertThatThrownBy(() -> ClusterMetadata.serializer.serialize(active, v11, Version.V11))
        .isInstanceOf(IllegalStateException.class);
        assertThat(v11.getLength()).isZero();
        assertThatThrownBy(() -> ClusterMetadata.serializer.serializedSize(active, Version.V11))
        .isInstanceOf(IllegalStateException.class);

        DataOutputBuffer component = new DataOutputBuffer();
        assertThatThrownBy(() -> ConsistencyDomains.serializer.serialize(active.consistencyDomains, component, Version.V11))
        .isInstanceOf(IllegalStateException.class);
        assertThat(component.getLength()).isZero();
        assertThatThrownBy(() -> ConsistencyDomains.serializer.serializedSize(active.consistencyDomains, Version.V11))
        .isInstanceOf(IllegalStateException.class);

        TransactionDomainDescriptor activeDescriptor = active.consistencyDomains.forTable(table.id);
        DataOutputBuffer activeBytes = new DataOutputBuffer();
        TransactionDomainDescriptor.serializer.serialize(activeDescriptor, activeBytes, Version.V12);
        assertThat(TransactionDomainDescriptor.serializer.serializedSize(activeDescriptor, Version.V12)).isEqualTo(activeBytes.getLength());
        assertThat(TransactionDomainDescriptor.serializer.deserialize(new DataInputBuffer(activeBytes.asNewBuffer(), false), Version.V12))
        .isEqualTo(activeDescriptor);
    }

    @Test
    public void activationSerializerRoundTripsAndRejectsDowngrade() throws IOException
    {
        TableMetadata table = scalarTable("activation_serialized", TableId.generate());
        TransactionDomainDescriptor descriptor = prepareDescriptor(table.id, "etcd-scalar");
        ExternalTransactionDomainBinding binding = new ExternalTransactionDomainBinding("00000000-0000-0000-0000-000000000504", "45");
        ActivateTransactionDomain activation = new ActivateTransactionDomain(table.id, descriptor.id(), descriptor.generation(), binding);
        DataOutputBuffer output = new DataOutputBuffer();
        ActivateTransactionDomain.serializer.serialize(activation, output, Version.V12);
        assertThat(ActivateTransactionDomain.serializer.serializedSize(activation, Version.V12)).isEqualTo(output.getLength());
        ActivateTransactionDomain decoded = ActivateTransactionDomain.serializer.deserialize(new DataInputBuffer(output.asNewBuffer(), false), Version.V12);
        ClusterMetadata prepared = new PrepareTransactionDomain(table, descriptor).execute(prepareMetadata()).success().metadata;
        assertThat(decoded.execute(prepared).isSuccess()).isTrue();

        DataOutputBuffer old = new DataOutputBuffer();
        assertThatThrownBy(() -> ActivateTransactionDomain.serializer.serialize(activation, old, Version.V11))
        .isInstanceOf(IllegalArgumentException.class);
        assertThat(old.getLength()).isZero();
        assertThatThrownBy(() -> ActivateTransactionDomain.serializer.serializedSize(activation, Version.V11))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void activationRejectsMismatchedIdentityAndArbitraryProvider()
    {
        TableMetadata table = scalarTable("invalid_active", TableId.generate());
        ClusterMetadata prepared = new PrepareTransactionDomain(table, prepareDescriptor(table.id, "provider"))
                                   .execute(prepareMetadata()).success().metadata;
        TransactionDomainDescriptor reservation = prepared.consistencyDomains.forTable(table.id);
        ExternalTransactionDomainBinding binding = new ExternalTransactionDomainBinding("00000000-0000-0000-0000-000000000502", "43");
        assertThat(new ActivateTransactionDomain(table.id, UUID.randomUUID(), reservation.generation(), binding)
                   .execute(prepared).isRejected()).isTrue();
        assertThat(new ActivateTransactionDomain(table.id, reservation.id(), reservation.generation(), binding)
                   .execute(prepared).isRejected()).isTrue();
    }

    @Test
    public void descriptorActivationRejectsUnsupportedProviderProfileAndVersions()
    {
        ExternalTransactionDomainBinding binding = new ExternalTransactionDomainBinding("00000000-0000-0000-0000-000000000503", "44");
        assertThatThrownBy(() -> descriptor(TABLE_ID, DOMAIN_ID, "provider", PrepareTransactionDomain.PROFILE_ID,
                                            1, 1, 1, 1, participants()).activate(binding))
        .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> descriptor(TABLE_ID, DOMAIN_ID, "etcd-scalar", "other-profile",
                                            1, 1, 1, 1, participants()).activate(binding))
        .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> descriptor(TABLE_ID, DOMAIN_ID, "etcd-scalar", PrepareTransactionDomain.PROFILE_ID,
                                            2, 1, 1, 1, participants()).activate(binding))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void activationRejectsWrongGenerationReplayOldPeerAndUnjoinedParticipant()
    {
        TableMetadata table = scalarTable("activation_invalid", TableId.generate());
        ExternalTransactionDomainBinding binding = new ExternalTransactionDomainBinding("00000000-0000-0000-0000-000000000505", "46");
        TransactionDomainDescriptor descriptor = prepareDescriptor(table.id, "etcd-scalar");
        ClusterMetadata prepared = new PrepareTransactionDomain(table, descriptor).execute(prepareMetadata()).success().metadata;
        assertThat(new ActivateTransactionDomain(table.id, descriptor.id(), descriptor.generation() + 1, binding)
                   .execute(prepared).isRejected()).isTrue();
        ClusterMetadata active = new ActivateTransactionDomain(table.id, descriptor.id(), descriptor.generation(), binding)
                                .execute(prepared).success().metadata;
        assertThat(new ActivateTransactionDomain(table.id, descriptor.id(), descriptor.generation(),
                                                 new ExternalTransactionDomainBinding("00000000-0000-0000-0000-000000000506", "47"))
                   .execute(active).isRejected()).isTrue();

        NodeId second = new NodeId(2);
        Directory oldDirectory = prepared.directory.unsafeWithNodeForTesting(second,
                                                                              new NodeAddresses(InetAddressAndPort.getByNameUnchecked("127.0.0.2")),
                                                                              new Location("dc1", "rack1"),
                                                                              new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V11))
                                                     .withNodeState(second, NodeState.JOINED);
        ClusterMetadata oldPeer = prepared.transformer().with(oldDirectory).build().metadata;
        TransactionDomainDescriptor secondParticipant = new TransactionDomainDescriptor(descriptor.id(), table.id,
                                                                                           "etcd-scalar", PrepareTransactionDomain.PROFILE_ID,
                                                                                           1, 1, 1, 1, Collections.singleton(second));
        ClusterMetadata oldPrepared = oldPeer.transformer()
                                              .with(new ConsistencyDomains(Epoch.FIRST,
                                                                           Collections.singletonMap(table.id, secondParticipant)))
                                              .build().metadata;
        assertThat(new ActivateTransactionDomain(table.id, secondParticipant.id(), 1, binding).execute(oldPrepared).isRejected()).isTrue();

        Directory registered = prepared.directory.unsafeWithNodeForTesting(second,
                                                                             new NodeAddresses(InetAddressAndPort.getByNameUnchecked("127.0.0.2")),
                                                                             new Location("dc1", "rack1"), NodeVersion.CURRENT);
        ClusterMetadata unjoined = prepared.transformer().with(registered).build().metadata;
        TransactionDomainDescriptor unjoinedDescriptor = new TransactionDomainDescriptor(UUID.randomUUID(), table.id,
                                                                                           "etcd-scalar", PrepareTransactionDomain.PROFILE_ID,
                                                                                           1, 1, 1, 1, Collections.singleton(second));
        ClusterMetadata unjoinedPrepared = unjoined.transformer()
                                                   .with(new ConsistencyDomains(Epoch.FIRST,
                                                                                Collections.singletonMap(table.id, unjoinedDescriptor)))
                                                   .build().metadata;
        assertThat(new ActivateTransactionDomain(table.id, unjoinedDescriptor.id(), 1, binding).execute(unjoinedPrepared).isRejected()).isTrue();
    }

    @Test
    public void bindingValidatesUnsignedClusterId()
    {
        String group = "00000000-0000-0000-0000-000000000507";
        assertThat(new ExternalTransactionDomainBinding(group, "18446744073709551615").clusterId())
        .isEqualTo("18446744073709551615");
        assertThatThrownBy(() -> new ExternalTransactionDomainBinding(group, "18446744073709551616"))
        .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExternalTransactionDomainBinding(group, "01"))
        .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExternalTransactionDomainBinding(group, "-1"))
                   .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void ratisBindingUsesExplicitV13KindAndRejectsDowngradeBeforeBytes() throws IOException
    {
        ExternalTransactionDomainBinding binding = ExternalTransactionDomainBinding.ratis(
                "00000000-0000-0000-0000-000000000508", 9, 17);
        assertThat(binding.kind()).isEqualTo(ExternalTransactionDomainBinding.Kind.RATIS_GROUP);
        assertThatThrownBy(binding::clusterId).isInstanceOf(IllegalStateException.class);

        DataOutputBuffer output = new DataOutputBuffer();
        ExternalTransactionDomainBinding.serializer.serialize(binding, output, Version.V13);
        assertThat(ExternalTransactionDomainBinding.serializer.serializedSize(binding, Version.V13)).isEqualTo(output.getLength());
        assertThat(ExternalTransactionDomainBinding.serializer.deserialize(new DataInputBuffer(output.asNewBuffer(), false), Version.V13))
        .isEqualTo(binding);

        DataOutputBuffer old = new DataOutputBuffer();
        assertThatThrownBy(() -> ExternalTransactionDomainBinding.serializer.serialize(binding, old, Version.V12))
        .isInstanceOf(IllegalStateException.class);
        assertThat(old.getLength()).isZero();
    }

    @Test
    public void activeRatisMetadataRoundTripsAtV13AndRejectsV12BeforeBytes() throws IOException
    {
        TableMetadata table = scalarTable("ratis_roundtrip", TableId.generate());
        ClusterMetadata prepared = new PrepareTransactionDomain(table, prepareDescriptor(table.id, "ratis-scalar"))
                                   .execute(prepareMetadata()).success().metadata;
        TableMetadata reserved = prepared.schema.getTableMetadata(table.id);
        TransactionDomainDescriptor descriptor = prepared.consistencyDomains.forTable(table.id);
        ExternalTransactionDomainBinding binding = ExternalTransactionDomainBinding.ratis(
                "00000000-0000-0000-0000-000000000514", reserved.epoch.getEpoch(), 31);
        ClusterMetadata active = new ActivateTransactionDomain(table.id, descriptor.id(), descriptor.generation(), binding)
                                .execute(prepared).success().metadata;
        TransactionDomainDescriptor activeDescriptor = active.consistencyDomains.forTable(table.id);

        DataOutputBuffer descriptorBytes = new DataOutputBuffer();
        TransactionDomainDescriptor.serializer.serialize(activeDescriptor, descriptorBytes, Version.V13);
        assertThat(TransactionDomainDescriptor.serializer.serializedSize(activeDescriptor, Version.V13))
        .isEqualTo(descriptorBytes.getLength());
        assertThat(TransactionDomainDescriptor.serializer.deserialize(new DataInputBuffer(descriptorBytes.asNewBuffer(), false), Version.V13))
        .isEqualTo(activeDescriptor);

        DataOutputBuffer domainsBytes = new DataOutputBuffer();
        ConsistencyDomains.serializer.serialize(active.consistencyDomains, domainsBytes, Version.V13);
        assertThat(ConsistencyDomains.serializer.serializedSize(active.consistencyDomains, Version.V13))
        .isEqualTo(domainsBytes.getLength());
        assertThat(ConsistencyDomains.serializer.deserialize(new DataInputBuffer(domainsBytes.asNewBuffer(), false), Version.V13))
        .isEqualTo(active.consistencyDomains);

        DataOutputBuffer metadataBytes = new DataOutputBuffer();
        ClusterMetadata.serializer.serialize(active, metadataBytes, Version.V13);
        assertThat(ClusterMetadata.serializer.serializedSize(active, Version.V13)).isEqualTo(metadataBytes.getLength());
        assertThat(ClusterMetadata.serializer.deserialize(new DataInputBuffer(metadataBytes.asNewBuffer(), false), Version.V13))
        .isEqualTo(active);

        DataOutputBuffer oldDescriptor = new DataOutputBuffer();
        assertThatThrownBy(() -> TransactionDomainDescriptor.serializer.serialize(activeDescriptor, oldDescriptor, Version.V12))
        .isInstanceOf(IllegalStateException.class);
        assertThat(oldDescriptor.getLength()).isZero();
        assertThatThrownBy(() -> TransactionDomainDescriptor.serializer.serializedSize(activeDescriptor, Version.V12))
        .isInstanceOf(IllegalStateException.class);
        DataOutputBuffer oldDomains = new DataOutputBuffer();
        assertThatThrownBy(() -> ConsistencyDomains.serializer.serialize(active.consistencyDomains, oldDomains, Version.V12))
        .isInstanceOf(IllegalStateException.class);
        assertThat(oldDomains.getLength()).isZero();
        assertThatThrownBy(() -> ConsistencyDomains.serializer.serializedSize(active.consistencyDomains, Version.V12))
        .isInstanceOf(IllegalStateException.class);
        DataOutputBuffer oldMetadata = new DataOutputBuffer();
        assertThatThrownBy(() -> ClusterMetadata.serializer.serialize(active, oldMetadata, Version.V12))
        .isInstanceOf(IllegalStateException.class);
        assertThat(oldMetadata.getLength()).isZero();
        assertThatThrownBy(() -> ClusterMetadata.serializer.serializedSize(active, Version.V12))
        .isInstanceOf(IllegalStateException.class);

        DataOutputBuffer oldActivation = new DataOutputBuffer();
        ActivateTransactionDomain activation = new ActivateTransactionDomain(table.id, descriptor.id(), descriptor.generation(), binding);
        DataOutputBuffer activationBytes = new DataOutputBuffer();
        ActivateTransactionDomain.serializer.serialize(activation, activationBytes, Version.V13);
        assertThat(ActivateTransactionDomain.serializer.serializedSize(activation, Version.V13)).isEqualTo(activationBytes.getLength());
        ActivateTransactionDomain restoredActivation = ActivateTransactionDomain.serializer.deserialize(new DataInputBuffer(activationBytes.asNewBuffer(), false), Version.V13);
        assertThat(restoredActivation.execute(prepared).success().metadata).isEqualTo(active);
        assertThatThrownBy(() -> ActivateTransactionDomain.serializer.serialize(activation, oldActivation, Version.V12))
        .isInstanceOf(IllegalArgumentException.class);
        assertThat(oldActivation.getLength()).isZero();
        assertThatThrownBy(() -> ActivateTransactionDomain.serializer.serializedSize(activation, Version.V12))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void etcdV12BindingEncodingRemainsTheLegacyTwoUtfFields() throws IOException
    {
        ExternalTransactionDomainBinding binding = new ExternalTransactionDomainBinding(
                "00000000-0000-0000-0000-000000000511", "42");
        DataOutputBuffer expected = new DataOutputBuffer();
        expected.writeUTF(binding.groupId());
        expected.writeUTF(binding.clusterId());
        DataOutputBuffer actual = new DataOutputBuffer();
        ExternalTransactionDomainBinding.serializer.serialize(binding, actual, Version.V12);
        assertThat(actual.toByteArray()).isEqualTo(expected.toByteArray());
    }

    @Test
    public void ratisActivationChecksSchemaEpochAndRequiresV13Peers()
    {
        TableMetadata table = scalarTable("ratis_active", TableId.generate());
        ClusterMetadata prepared = new PrepareTransactionDomain(table, prepareDescriptor(table.id, "ratis-scalar"))
                                   .execute(prepareMetadata()).success().metadata;
        TableMetadata reserved = prepared.schema.getTableMetadata(table.id);
        TransactionDomainDescriptor descriptor = prepared.consistencyDomains.forTable(table.id);
        ExternalTransactionDomainBinding binding = ExternalTransactionDomainBinding.ratis(
                "00000000-0000-0000-0000-000000000509", reserved.epoch.getEpoch(), 23);
        assertThat(new ActivateTransactionDomain(table.id, descriptor.id(), descriptor.generation(), binding)
                   .execute(prepared).isSuccess()).isTrue();

        ExternalTransactionDomainBinding wrongEpoch = ExternalTransactionDomainBinding.ratis(
                "00000000-0000-0000-0000-000000000510", reserved.epoch.getEpoch() + 1, 24);
        assertThat(new ActivateTransactionDomain(table.id, descriptor.id(), descriptor.generation(), wrongEpoch)
                   .execute(prepared).isRejected()).isTrue();

        Directory oldPeer = prepared.directory.unsafeWithNodeForTesting(new NodeId(2),
                                                                           new NodeAddresses(InetAddressAndPort.getByNameUnchecked("127.0.0.2")),
                                                                           new Location("dc1", "rack1"),
                                                                           new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V12))
                                                       .withNodeState(new NodeId(2), NodeState.JOINED);
        ClusterMetadata oldPeerMetadata = prepared.transformer().with(oldPeer).build().metadata;
        assertThat(new ActivateTransactionDomain(table.id, descriptor.id(), descriptor.generation(), binding)
                   .execute(oldPeerMetadata).isRejected()).isTrue();
    }

    @Test
    public void startupAndRegisterRejectV12ForActiveRatis()
    {
        TableMetadata table = scalarTable("ratis_guard", TableId.generate());
        ClusterMetadata prepared = new PrepareTransactionDomain(table, prepareDescriptor(table.id, "ratis-scalar"))
                                   .execute(prepareMetadata()).success().metadata;
        TableMetadata reserved = prepared.schema.getTableMetadata(table.id);
        TransactionDomainDescriptor descriptor = prepared.consistencyDomains.forTable(table.id);
        ExternalTransactionDomainBinding binding = ExternalTransactionDomainBinding.ratis(
                "00000000-0000-0000-0000-000000000515", reserved.epoch.getEpoch(), 32);
        ClusterMetadata active = new ActivateTransactionDomain(table.id, descriptor.id(), descriptor.generation(), binding)
                                .execute(prepared).success().metadata;
        Directory v11Directory = active.directory.unsafeWithNodeForTesting(new NodeId(1),
                                                                              active.directory.addresses.get(new NodeId(1)),
                                                                              active.directory.location(new NodeId(1)),
                                                                              new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V11));
        ClusterMetadata inconsistent = active.transformer().with(v11Directory).build().metadata;
        NodeAddresses address = inconsistent.directory.addresses.get(new NodeId(1));
        assertThat(new Startup(new NodeId(1), address,
                               new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V12))
                   .execute(inconsistent).isRejected()).isTrue();
        assertThat(new Register(new NodeAddresses(InetAddressAndPort.getByNameUnchecked("127.0.0.3")),
                                new Location("dc1", "rack1"),
                                new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V12))
                   .execute(inconsistent).isRejected()).isTrue();
    }

    @Test
    public void domainCollectionMinimumVersionTracksBindingKinds()
    {
        TransactionDomainDescriptor prepared = prepareDescriptor(TABLE_ID, "etcd-scalar");
        assertThat(new ConsistencyDomains(Epoch.FIRST, Collections.singletonMap(TABLE_ID, prepared)).minimumVersion())
        .isEqualTo(Version.V11);

        TransactionDomainDescriptor activeEtcd = prepared.activate(new ExternalTransactionDomainBinding(
                "00000000-0000-0000-0000-000000000512", "42"));
        assertThat(new ConsistencyDomains(Epoch.FIRST, Collections.singletonMap(TABLE_ID, activeEtcd)).minimumVersion())
        .isEqualTo(Version.V12);

        TransactionDomainDescriptor ratis = new TransactionDomainDescriptor(DOMAIN_ID, TABLE_ID, "ratis-scalar",
                                                                              PrepareTransactionDomain.PROFILE_ID, 1, 1, 1, 1,
                                                                              participants());
        TransactionDomainDescriptor activeRatis = ratis.activate(ExternalTransactionDomainBinding.ratis(
                "00000000-0000-0000-0000-000000000513", 0, 1));
        assertThat(new ConsistencyDomains(Epoch.FIRST, Collections.singletonMap(TABLE_ID, activeRatis)).minimumVersion())
        .isEqualTo(Version.V13);

        assertThatThrownBy(() -> prepared.activate(ExternalTransactionDomainBinding.ratis(
                "00000000-0000-0000-0000-000000000516", 0, 1)))
        .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ratis.activate(new ExternalTransactionDomainBinding(
                "00000000-0000-0000-0000-000000000517", "42")))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void preTcmNodeCannotBeHiddenByCommonVersion()
    {
        ClusterMetadata metadata = prepareMetadata();
        NodeVersion old = new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.OLD);
        Directory directory = metadata.directory.unsafeWithNodeForTesting(new NodeId(2),
                                                                          new NodeAddresses(InetAddressAndPort.getByNameUnchecked("127.0.0.2")),
                                                                          new Location("dc1", "rack1"), old)
                                               .withNodeState(new NodeId(2), NodeState.JOINED);
        metadata = metadata.transformer().with(directory).build().metadata;
        assertThat(metadata.directory.commonSerializationVersion).isEqualTo(Version.V13);
        TableMetadata table = scalarTable("old_node", TableId.generate());
        assertThat(new PrepareTransactionDomain(table, prepareDescriptor(table.id, "provider")).execute(metadata).isRejected()).isTrue();
    }

    private static Set<NodeId> participants()
    {
        return new LinkedHashSet<>(Arrays.asList(new NodeId(1), new NodeId(2)));
    }

    private static TransactionDomainDescriptor prepareDescriptor(TableId tableId, String provider)
    {
        return new TransactionDomainDescriptor(UUID.randomUUID(), tableId, provider,
                                                PrepareTransactionDomain.PROFILE_ID, 1, 1, 1, 1,
                                                Collections.singleton(new NodeId(1)));
    }

    private static TableMetadata scalarTable(String name, TableId tableId)
    {
        return TableMetadata.builder("domain_metadata_ks", name)
                            .id(tableId)
                            .partitioner(Murmur3Partitioner.instance)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addRegularColumn("value", Int32Type.instance)
                            .build();
    }

    private static ClusterMetadata prepareMetadata()
    {
        TableMetadata existing = TableMetadata.builder("domain_metadata_ks", "existing")
                                               .id(TABLE_ID)
                                               .addPartitionKeyColumn("pk", Int32Type.instance)
                                               .build();
        KeyspaceMetadata keyspace = KeyspaceMetadata.create("domain_metadata_ks",
                                                             KeyspaceParams.simple(1),
                                                             Tables.of(existing));
        NodeId participant = new NodeId(1);
        Directory directory = new Directory().unsafeWithNodeForTesting(participant,
                                                                         new NodeAddresses(InetAddressAndPort.getByNameUnchecked("127.0.0.1")),
                                                                         new Location("dc1", "rack1"),
                                                                         org.apache.cassandra.tcm.membership.NodeVersion.CURRENT)
                                              .withNodeState(participant, NodeState.JOINED);
        return new ClusterMetadata(Murmur3Partitioner.instance, directory,
                                    new DistributedSchema(Keyspaces.of(keyspace)));
    }

    private static TransactionDomainDescriptor descriptor(TableId tableId, UUID id, Set<NodeId> participants)
    {
        return descriptor(tableId, id, "provider", "profile", 1, 1, 1, 1, participants);
    }

    private static TransactionDomainDescriptor descriptor(TableId tableId,
                                                          UUID id,
                                                          String providerId,
                                                          String profileId,
                                                          int profileVersion,
                                                          int protocolVersion,
                                                          int storageVersion,
                                                          long generation,
                                                          Set<NodeId> participants)
    {
        return new TransactionDomainDescriptor(id, tableId, providerId, profileId,
                                                profileVersion, protocolVersion, storageVersion,
                                                generation, participants);
    }

    private static final class ClusterMetadataTestFixtures
    {
        private static ClusterMetadata metadata()
        {
            TableMetadata table = TableMetadata.builder("domain_metadata_ks", "existing")
                                                .id(TABLE_ID)
                                                .addPartitionKeyColumn("pk", org.apache.cassandra.db.marshal.Int32Type.instance)
                                                .build();
            KeyspaceMetadata keyspace = KeyspaceMetadata.create("domain_metadata_ks",
                                                                 KeyspaceParams.simple(1),
                                                                 Tables.of(table));
            return new ClusterMetadata(Murmur3Partitioner.instance,
                                        org.apache.cassandra.tcm.membership.Directory.EMPTY,
                                        new DistributedSchema(Keyspaces.of(keyspace)));
        }
    }
}
