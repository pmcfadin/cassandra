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
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

/** Immutable identity of the external provider group which owns a domain. */
public final class ExternalTransactionDomainBinding
{
    public enum Kind
    {
        ETCD_CLUSTER,
        RATIS_GROUP
    }

    private static final int ETCD_KIND_TAG = 1;
    private static final int RATIS_KIND_TAG = 2;
    private static final Pattern CLUSTER_ID = Pattern.compile("[1-9][0-9]*");
    public static final Serializer serializer = new Serializer();

    private final String groupId;
    private final String clusterId;
    private final Kind kind;
    private final long schemaEpoch;
    private final long readinessIndex;

    public ExternalTransactionDomainBinding(String groupId, String clusterId)
    {
        UUID parsed = parseUuid(groupId);
        if (!parsed.toString().equals(groupId))
            throw new IllegalArgumentException("groupId must be canonical UUID text");
        if (clusterId == null || !CLUSTER_ID.matcher(clusterId).matches())
            throw new IllegalArgumentException("clusterId must be a positive unsigned decimal");
        try
        {
            Long.parseUnsignedLong(clusterId);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException("clusterId must fit in an unsigned 64-bit integer", e);
        }
        this.groupId = groupId;
        this.clusterId = clusterId;
        this.kind = Kind.ETCD_CLUSTER;
        this.schemaEpoch = 0;
        this.readinessIndex = 0;
    }

    private ExternalTransactionDomainBinding(String groupId, long schemaEpoch, long readinessIndex)
    {
        UUID parsed = parseUuid(groupId);
        if (!parsed.toString().equals(groupId))
            throw new IllegalArgumentException("groupId must be canonical UUID text");
        if (schemaEpoch < 0)
            throw new IllegalArgumentException("schemaEpoch must be non-negative");
        if (readinessIndex <= 0)
            throw new IllegalArgumentException("readinessIndex must be positive");
        this.groupId = groupId;
        this.clusterId = null;
        this.kind = Kind.RATIS_GROUP;
        this.schemaEpoch = schemaEpoch;
        this.readinessIndex = readinessIndex;
    }

    public static ExternalTransactionDomainBinding ratis(String groupId, long schemaEpoch, long readinessIndex)
    {
        return new ExternalTransactionDomainBinding(groupId, schemaEpoch, readinessIndex);
    }

    private static UUID parseUuid(String value)
    {
        try
        {
            return UUID.fromString(Objects.requireNonNull(value, "groupId"));
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalArgumentException("groupId must be canonical UUID text", e);
        }
    }

    public String groupId()
    {
        return groupId;
    }

    public String clusterId()
    {
        if (kind != Kind.ETCD_CLUSTER)
            throw new IllegalStateException("Ratis bindings do not have an etcd cluster id");
        return clusterId;
    }

    public Kind kind()
    {
        return kind;
    }

    public long schemaEpoch()
    {
        return schemaEpoch;
    }

    public long readinessIndex()
    {
        return readinessIndex;
    }

    public Version minimumVersion()
    {
        return kind == Kind.RATIS_GROUP ? Version.V13 : Version.V12;
    }

    public void checkSerializationVersion(Version version)
    {
        if (version.isBefore(minimumVersion()))
            throw new IllegalStateException("" + kind + " transaction domain binding requires metadata " + minimumVersion());
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
        if (!(other instanceof ExternalTransactionDomainBinding))
            return false;
        ExternalTransactionDomainBinding that = (ExternalTransactionDomainBinding) other;
        return kind == that.kind && groupId.equals(that.groupId) && Objects.equals(clusterId, that.clusterId) &&
               schemaEpoch == that.schemaEpoch && readinessIndex == that.readinessIndex;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(groupId, clusterId, kind, schemaEpoch, readinessIndex);
    }

    @Override
    public String toString()
    {
        return kind == Kind.ETCD_CLUSTER
               ? "ExternalTransactionDomainBinding{" + groupId + "," + clusterId + '}'
               : "ExternalTransactionDomainBinding{" + groupId + "," + schemaEpoch + "," + readinessIndex + '}';
    }

    public static final class Serializer implements MetadataSerializer<ExternalTransactionDomainBinding>
    {
        @Override
        public void serialize(ExternalTransactionDomainBinding value, DataOutputPlus out, Version version) throws IOException
        {
            value.checkSerializationVersion(version);
            if (version.isBefore(Version.V13))
            {
                out.writeUTF(value.groupId);
                out.writeUTF(value.clusterId);
                return;
            }
            out.writeUnsignedVInt32(value.kind == Kind.ETCD_CLUSTER ? ETCD_KIND_TAG : RATIS_KIND_TAG);
            out.writeUTF(value.groupId);
            if (value.kind == Kind.ETCD_CLUSTER)
                out.writeUTF(value.clusterId);
            else
            {
                out.writeUnsignedVInt(value.schemaEpoch);
                out.writeUnsignedVInt(value.readinessIndex);
            }
        }

        @Override
        public ExternalTransactionDomainBinding deserialize(DataInputPlus in, Version version) throws IOException
        {
            if (version.isBefore(Version.V13))
                return new ExternalTransactionDomainBinding(in.readUTF(), in.readUTF());
            int tag = in.readUnsignedVInt32();
            String groupId = in.readUTF();
            switch (tag)
            {
                case ETCD_KIND_TAG:
                    return new ExternalTransactionDomainBinding(groupId, in.readUTF());
                case RATIS_KIND_TAG:
                    return ratis(groupId, in.readUnsignedVInt(), in.readUnsignedVInt());
                default:
                    throw new IOException("Unsupported external transaction binding kind: " + tag);
            }
        }

        @Override
        public long serializedSize(ExternalTransactionDomainBinding value, Version version)
        {
            value.checkSerializationVersion(version);
            if (version.isBefore(Version.V13))
                return org.apache.cassandra.db.TypeSizes.sizeof(value.groupId) +
                       org.apache.cassandra.db.TypeSizes.sizeof(value.clusterId);
            return org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt(value.kind == Kind.ETCD_CLUSTER ? ETCD_KIND_TAG : RATIS_KIND_TAG) +
                   org.apache.cassandra.db.TypeSizes.sizeof(value.groupId) +
                   (value.kind == Kind.ETCD_CLUSTER
                    ? org.apache.cassandra.db.TypeSizes.sizeof(value.clusterId)
                    : org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt(value.schemaEpoch) +
                      org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt(value.readinessIndex));
        }
    }
}
