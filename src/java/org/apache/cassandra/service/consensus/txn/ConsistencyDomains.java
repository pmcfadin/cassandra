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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

/** Immutable TCM ownership reservations for transaction domains. */
public final class ConsistencyDomains implements MetadataValue<ConsistencyDomains>
{
    public static final int MAX_DOMAINS = 1 << 20;
    public static final ConsistencyDomains EMPTY = new ConsistencyDomains(Epoch.EMPTY, Collections.emptyMap());
    public static final Serializer serializer = new Serializer();

    private final Epoch lastModified;
    private final Map<TableId, TransactionDomainDescriptor> domains;

    public ConsistencyDomains(Epoch lastModified, Map<TableId, TransactionDomainDescriptor> domains)
    {
        this.lastModified = Objects.requireNonNull(lastModified, "lastModified");
        Objects.requireNonNull(domains, "domains");
        if (domains.size() > MAX_DOMAINS)
            throw new IllegalArgumentException("Too many transaction domains: " + domains.size());

        Map<TableId, TransactionDomainDescriptor> copy = new HashMap<>(domains.size());
        Map<UUID, TableId> domainIds = new HashMap<>(domains.size());
        for (Map.Entry<TableId, TransactionDomainDescriptor> entry : domains.entrySet())
        {
            TableId tableId = Objects.requireNonNull(entry.getKey(), "domains contains null table id");
            TransactionDomainDescriptor descriptor = Objects.requireNonNull(entry.getValue(), "domains contains null descriptor");
            if (!tableId.equals(descriptor.tableId()))
                throw new IllegalArgumentException("Domain table id does not match its map key");
            TableId existing = domainIds.put(descriptor.id(), tableId);
            if (existing != null)
                throw new IllegalArgumentException("Duplicate transaction domain id: " + descriptor.id());
            copy.put(tableId, descriptor);
        }
        this.domains = Collections.unmodifiableMap(copy);
    }

    public boolean isEmpty()
    {
        return domains.isEmpty();
    }

    public boolean hasActive()
    {
        return domains.values().stream().anyMatch(d -> d.state() == TransactionDomainDescriptor.State.ACTIVE);
    }

    /** Minimum metadata version needed for every reservation in this collection. */
    public Version minimumVersion()
    {
        Version minimum = Version.V0;
        for (TransactionDomainDescriptor descriptor : domains.values())
        {
            Version required = descriptor.minimumVersion();
            if (required.isAfter(minimum))
                minimum = required;
        }
        return minimum;
    }

    public int size()
    {
        return domains.size();
    }

    public TransactionDomainDescriptor forTable(TableId tableId)
    {
        return domains.get(tableId);
    }

    public Collection<TransactionDomainDescriptor> domains()
    {
        return Collections.unmodifiableCollection(domains.values());
    }

    public ConsistencyDomains withDomain(TransactionDomainDescriptor descriptor)
    {
        Objects.requireNonNull(descriptor, "descriptor");
        if (domains.containsKey(descriptor.tableId()))
            throw new IllegalArgumentException("A transaction domain already reserves table " + descriptor.tableId());
        for (TransactionDomainDescriptor existing : domains.values())
        {
            if (existing.id().equals(descriptor.id()))
                throw new IllegalArgumentException("A transaction domain already uses id " + descriptor.id());
        }
        Map<TableId, TransactionDomainDescriptor> updated = new HashMap<>(domains);
        updated.put(descriptor.tableId(), descriptor);
        return new ConsistencyDomains(lastModified, updated);
    }

    public ConsistencyDomains replace(TransactionDomainDescriptor descriptor)
    {
        Objects.requireNonNull(descriptor, "descriptor");
        TransactionDomainDescriptor current = domains.get(descriptor.tableId());
        if (current == null || !current.id().equals(descriptor.id()))
            throw new IllegalArgumentException("No matching transaction domain to replace");
        Map<TableId, TransactionDomainDescriptor> updated = new HashMap<>(domains);
        updated.put(descriptor.tableId(), descriptor);
        return new ConsistencyDomains(lastModified, updated);
    }

    @Override
    public ConsistencyDomains withLastModified(Epoch epoch)
    {
        return new ConsistencyDomains(Objects.requireNonNull(epoch, "epoch"), domains);
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
        if (!(other instanceof ConsistencyDomains))
            return false;
        ConsistencyDomains that = (ConsistencyDomains) other;
        return lastModified.equals(that.lastModified) && domains.equals(that.domains);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lastModified, domains);
    }

    @Override
    public String toString()
    {
        return "ConsistencyDomains{" +
               "lastModified=" + lastModified +
               ", domains=" + domains +
               '}';
    }

    public static final class Serializer implements MetadataSerializer<ConsistencyDomains>
    {
        @Override
        public void serialize(ConsistencyDomains value, DataOutputPlus out, Version version) throws IOException
        {
            if (version.isBefore(Version.V11))
            {
                if (!value.isEmpty())
                    throw new IllegalStateException("Cannot serialize non-empty transaction domains with metadata version " + version);
                Epoch.serializer.serialize(value.lastModified, out, version);
                return;
            }
            if (version.isBefore(Version.V12) && value.hasActive())
                throw new IllegalStateException("Cannot serialize ACTIVE transaction domains with metadata version " + version);
            for (TransactionDomainDescriptor descriptor : value.domains.values())
                descriptor.checkSerializationVersion(version);

            Epoch.serializer.serialize(value.lastModified, out, version);
            out.writeUnsignedVInt32(value.domains.size());
            for (Map.Entry<TableId, TransactionDomainDescriptor> entry : value.domains.entrySet().stream()
                                                                                         .sorted(Map.Entry.comparingByKey())
                                                                                         .collect(java.util.stream.Collectors.toList()))
            {
                TableId.metadataSerializer.serialize(entry.getKey(), out, version);
                TransactionDomainDescriptor.serializer.serialize(entry.getValue(), out, version);
            }
        }

        @Override
        public ConsistencyDomains deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            if (version.isBefore(Version.V11))
                return EMPTY.withLastModified(lastModified);
            int count = in.readUnsignedVInt32();
            if (count > MAX_DOMAINS)
                throw new IOException("Invalid transaction domain count: " + count);
            Map<TableId, TransactionDomainDescriptor> domains = new HashMap<>(count);
            for (int i = 0; i < count; i++)
            {
                TableId tableId = TableId.metadataSerializer.deserialize(in, version);
                TransactionDomainDescriptor descriptor = TransactionDomainDescriptor.serializer.deserialize(in, version);
                if (!tableId.equals(descriptor.tableId()))
                    throw new IOException("Transaction domain table id does not match map key");
                if (domains.put(tableId, descriptor) != null)
                    throw new IOException("Duplicate transaction domain table id: " + tableId);
            }
            return new ConsistencyDomains(lastModified, domains);
        }

        @Override
        public long serializedSize(ConsistencyDomains value, Version version)
        {
            if (version.isBefore(Version.V11) && !value.isEmpty())
                throw new IllegalStateException("Cannot serialize non-empty transaction domains with metadata version " + version);
            if (version.isBefore(Version.V12) && value.hasActive())
                throw new IllegalStateException("Cannot size ACTIVE transaction domains with metadata version " + version);
            for (TransactionDomainDescriptor descriptor : value.domains.values())
                descriptor.checkSerializationVersion(version);
            long size = Epoch.serializer.serializedSize(value.lastModified, version);
            if (version.isBefore(Version.V11))
                return size;
            size += TypeSizes.sizeofUnsignedVInt(value.domains.size());
            for (Map.Entry<TableId, TransactionDomainDescriptor> entry : value.domains.entrySet())
                size += TableId.metadataSerializer.serializedSize(entry.getKey(), version) +
                        TransactionDomainDescriptor.serializer.serializedSize(entry.getValue(), version);
            return size;
        }
    }
}
