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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.JsonUtils;

/**
 * Bounded, test-only etcd v3 JSON-gateway store.  A domain is one etcd value,
 * so its owner, rows, and receipts are protected by one MOD-revision compare.
 */
public final class EtcdScalarStore
{
    /** Internal marker for an operation whose commit status could not be learned. */
    public static final class UnknownOutcomeException extends RuntimeException
    {
        public UnknownOutcomeException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }
    private static final String PREFIX = "/cassandra-pluggable/";
    private static final int VERSION = 1;
    private static final int MAX_ROWS = 128;
    private static final int MAX_RECEIPTS = 128;
    private static final int MAX_VALUE = 1024 * 1024;
    private static final int MAX_ATTEMPTS = 8;
    private static final ObjectMapper JSON = JsonUtils.JSON_OBJECT_MAPPER;
    private static final JsonFactory JSON_FACTORY = JSON.getFactory();

    private final List<String> endpoints;
    private final int timeoutMillis;
    private volatile String pinnedClusterId;

    public EtcdScalarStore(List<String> endpoints, int timeoutMillis)
    {
        if (endpoints == null || endpoints.isEmpty() || timeoutMillis <= 0)
            throw new IllegalArgumentException("etcd endpoints and timeout are required");
        List<String> copy = new ArrayList<>(endpoints.size());
        for (String endpoint : endpoints)
            copy.add(Objects.requireNonNull(endpoint, "endpoint").replaceAll("/$", ""));
        this.endpoints = Collections.unmodifiableList(copy);
        this.timeoutMillis = timeoutMillis;
    }

    public String clusterId()
    {
        for (int i = 0; i < endpoints.size(); i++)
        {
            try
            {
                Response response = request("/v3/kv/range", rangeJson("/cassandra-pluggable/cluster-id"));
                String cluster = response.clusterId;
                if (cluster == null || cluster.equals("0"))
                    throw new IllegalStateException("etcd did not return a cluster id");
                pinCluster(cluster);
                return cluster;
            }
            catch (IOException e)
            {
                if (i + 1 == endpoints.size())
                    throw new IllegalStateException("cannot reach etcd", e);
            }
        }
        throw new IllegalStateException("cannot reach etcd");
    }

    public void prepare(TransactionDomainDescriptor descriptor,
                         ExternalTransactionDomainBinding binding,
                         long schemaEpoch)
    {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(binding, "binding");
        if (descriptor.state().name().equals("ACTIVE"))
            throw new IllegalArgumentException("prepare requires PREPARED descriptor");
        if (!"etcd-scalar".equals(descriptor.providerId())
            || !"scalar-int".equals(descriptor.profileId())
            || descriptor.profileVersion() != 1
            || descriptor.protocolVersion() != 1
            || descriptor.storageVersion() != 1
            || schemaEpoch < 0)
            throw new IllegalArgumentException("descriptor is not the etcd scalar v1 profile");
        pinCluster(binding.clusterId());
        State state = new State(descriptor.id(), descriptor.tableId(), descriptor.generation(), schemaEpoch,
                                binding.groupId(), binding.clusterId(), false);
        createIfAbsent(state);
    }

    public void activate(TransactionDomainDescriptor descriptor, long schemaEpoch)
    {
        Objects.requireNonNull(descriptor, "descriptor");
        if (!descriptor.state().name().equals("ACTIVE") || descriptor.externalBinding() == null)
            throw new IllegalArgumentException("activation requires an ACTIVE external descriptor");
        pinCluster(descriptor.externalBinding().clusterId());
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++)
        {
            ReadValue read;
            try
            {
                read = readValue(descriptor.id());
            }
            catch (IOException e)
            {
                throw new UnknownOutcomeException("cannot determine activation outcome", e);
            }
            State current = read.state;
            if (current == null || current.generation != descriptor.generation()
                || !current.domainId.equals(descriptor.id())
                || !current.tableId.equals(descriptor.tableId())
                || current.schemaEpoch != schemaEpoch
                || !current.groupId.equals(descriptor.externalBinding().groupId())
                || !current.clusterId.equals(descriptor.externalBinding().clusterId()))
                throw new IllegalStateException("prepared etcd state does not match active descriptor");
            if (current.fenced)
                throw new IllegalStateException("fenced etcd state cannot be reactivated");
            if (current.active)
                return;
            current.active = true;
            try
            {
                if (cas(current, read.modRevision))
                    return;
            }
            catch (IOException e)
            {
                throw new UnknownOutcomeException("cannot determine activation outcome", e);
            }
        }
        throw new IllegalStateException("activation compare conflicted");
    }

    public EtcdScalarResult submit(EtcdScalarRequest request)
    {
        Objects.requireNonNull(request, "request");
        byte[] content = request.encode();
        if (content.length > 64 * 1024)
            throw new IllegalArgumentException("request exceeds 64KiB");
        pinCluster(request.binding.clusterId());
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++)
        {
            ReadValue value;
            try
            {
                value = readValue(request.domainId);
            }
            catch (IOException e)
            {
                throw new UnknownOutcomeException("cannot determine etcd read outcome", e);
            }
            State state = value.state;
            if (state == null)
                return unknown();
            validateIdentity(request, state);
            if (request.generation != state.generation || !state.active || state.fenced)
                throw new IllegalStateException("request does not match active etcd owner");
            Receipt old = state.receipts.get(request.requestId);
            byte[] hash = request.contentHash();
            if (old != null)
            {
                if (!MessageDigest.isEqual(old.hash, hash))
                    throw new IllegalArgumentException("request id was reused with different content");
                return decodeResult(request, old.result);
            }
            if (state.receipts.size() >= MAX_RECEIPTS)
                throw new IllegalStateException("receipt capacity exhausted");
            EtcdScalarResult result = evaluate(request, state);
            state.decisionPosition++;
            state.receipts.put(request.requestId, new Receipt(hash, result.encode()));
            try
            {
                if (cas(state, value.modRevision))
                    return result;
            }
            catch (IOException e)
            {
                // The transaction may have committed.  Do not apply a second receipt write.
                throw new UnknownOutcomeException("cannot determine etcd transaction outcome", e);
            }
        }
        return unknown();
    }

    public EtcdScalarResult lookup(EtcdScalarRequest request)
    {
        Objects.requireNonNull(request, "request");
        pinCluster(request.binding.clusterId());
        try
        {
            ReadValue value = readValue(request.domainId);
            if (value.state == null)
                return unknown();
            validateIdentity(request, value.state);
            Receipt receipt = value.state.receipts.get(request.requestId);
            if (receipt == null)
                return unknown();
            if (!MessageDigest.isEqual(receipt.hash, request.contentHash()))
                throw new IllegalArgumentException("request id was reused with different content");
            return decodeResult(request, receipt.result);
        }
        catch (IOException e)
        {
            return unknown();
        }
    }

    public void fence(UUID domainId, long oldGeneration, long newGeneration)
    {
        if (newGeneration <= oldGeneration)
            throw new IllegalArgumentException("new generation must advance");
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++)
        {
            try
            {
                ReadValue value = readValue(domainId);
                if (value.state == null)
                    throw new IllegalStateException("cannot fence an absent domain");
                if (value.state.generation != oldGeneration)
                    throw new IllegalStateException("old generation does not own domain");
                value.state.active = false;
                value.state.fenced = true;
                value.state.generation = newGeneration;
                if (cas(value.state, value.modRevision))
                    return;
            }
            catch (IOException e)
            {
                throw new UnknownOutcomeException("unknown fence outcome", e);
            }
        }
        throw new IllegalStateException("fence conflicted");
    }

    private EtcdScalarResult evaluate(EtcdScalarRequest request, State state)
    {
        Map<Integer, EtcdScalarResult.Row> reads = new LinkedHashMap<>();
        Map<Integer, Integer> slots = new HashMap<>();
        for (EtcdScalarRequest.Read read : request.reads)
        {
            Integer value = state.rows.get(read.key);
            reads.put(read.slot, new EtcdScalarResult.Row(value != null, value));
            slots.put(read.slot, value);
        }
        boolean conditions = true;
        for (EtcdScalarRequest.Condition condition : request.conditions)
        {
            Integer value = slots.get(condition.slot);
            boolean present = value != null;
            switch (condition.operator)
            {
                case IS_NULL: conditions &= !present; break;
                case IS_NOT_NULL: conditions &= present; break;
                case EQ: conditions &= present && value.equals(condition.value); break;
                case NEQ: conditions &= !present || !value.equals(condition.value); break;
                case LT: conditions &= present && value < condition.value; break;
                case LTE: conditions &= present && value <= condition.value; break;
                case GT: conditions &= present && value > condition.value; break;
                case GTE: conditions &= present && value >= condition.value; break;
                default: throw new IllegalArgumentException("unknown condition operator");
            }
        }
        if (conditions)
        {
            int newRows = 0;
            for (EtcdScalarRequest.Write write : request.writes)
                if (!state.rows.containsKey(write.key))
                    newRows++;
            if (state.rows.size() + newRows > MAX_ROWS)
                throw new IllegalStateException("row capacity exhausted");
            for (EtcdScalarRequest.Write write : request.writes)
            {
                Integer value = write.constant != null ? write.constant : slots.get(write.sourceSlot);
                if (value == null)
                    throw new IllegalArgumentException("reference write selected an absent row");
                state.rows.put(write.key, value);
            }
        }
        long now = System.currentTimeMillis() * 1000L;
        return new EtcdScalarResult(true, conditions, now, state.decisionPosition + 1, reads);
    }

    private void validateIdentity(EtcdScalarRequest request, State state)
    {
        if (!request.domainId.equals(state.domainId) || !request.tableId.equals(state.tableId)
            || request.schemaEpoch != state.schemaEpoch
            || !request.binding.groupId().equals(state.groupId) || !request.binding.clusterId().equals(state.clusterId)
            || request.binding.clusterId().equals("0"))
            throw new IllegalStateException("request does not match active etcd owner");
    }

    private void createIfAbsent(State state)
    {
        try
        {
            String key = key(state.domainId);
            ObjectNode body = JSON.createObjectNode();
            body.putArray("compare").addObject().put("target", "VERSION").put("key", b64(key)).put("version", 0);
            body.putArray("success").addObject().putObject("request_put").put("key", b64(key)).put("value", b64(state.encode()));
            body.putArray("failure").addObject().putObject("request_range").put("key", b64(key));
            Response response = request("/v3/kv/txn", body);
            if (response.node.path("succeeded").asBoolean(false))
                return;
            State existing = read(state.domainId);
            if (existing == null)
                throw new IllegalStateException("etcd prepare did not create state");
            if (!existing.sameIdentity(state))
                throw new IllegalStateException("existing etcd state does not match prepare");
        }
        catch (IOException e)
        {
            throw new UnknownOutcomeException("cannot determine prepare outcome", e);
        }
    }

    private State read(UUID domainId)
    {
        try
        {
            return readValue(domainId).state;
        }
        catch (IOException e)
        {
            throw new IllegalStateException(e);
        }
    }

    private ReadValue readValue(UUID domainId) throws IOException
    {
        Response response = request("/v3/kv/range", rangeJson(key(domainId)));
        JsonNode kvs = response.node.path("kvs");
        if (!kvs.isArray() || kvs.size() == 0)
            return new ReadValue(null, 0);
        if (kvs.size() != 1)
            throw new IllegalStateException("duplicate etcd domain state");
        JsonNode kv = kvs.get(0);
        return new ReadValue(State.decode(unb64(kv.path("value").asText())), kv.path("mod_revision").asLong());
    }

    private boolean cas(State state, long revision) throws IOException
    {
        ObjectNode body = JSON.createObjectNode();
        body.putArray("compare").addObject().put("target", "MOD").put("key", b64(key(state.domainId))).put("mod_revision", revision);
        body.putArray("success").addObject().putObject("request_put").put("key", b64(key(state.domainId))).put("value", b64(state.encode()));
        body.putArray("failure").addObject().putObject("request_range").put("key", b64(key(state.domainId)));
        Response response = request("/v3/kv/txn", body);
        return response.node.path("succeeded").asBoolean(false);
    }

    private Response request(String path, ObjectNode body) throws IOException
    {
        IOException last = null;
        int start = ThreadLocalRandom.current().nextInt(endpoints.size());
        for (int i = 0; i < endpoints.size(); i++)
        {
            HttpURLConnection connection = null;
            try
            {
                connection = (HttpURLConnection) new URL(endpoints.get((start + i) % endpoints.size()) + path).openConnection();
                connection.setConnectTimeout(timeoutMillis);
                connection.setReadTimeout(timeoutMillis);
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                connection.setInstanceFollowRedirects(false);
                connection.setRequestProperty("Content-Type", "application/json");
                byte[] request = JSON.writeValueAsBytes(body);
                connection.getOutputStream().write(request);
                int status = connection.getResponseCode();
                if (status < 200 || status >= 300)
                    throw new IOException("etcd HTTP status " + status);
                byte[] bytes = readBounded(connection, MAX_VALUE * 2);
                JsonNode node = parse(bytes);
                String cluster = node.path("header").path("cluster_id").asText(null);
                pinCluster(cluster);
                return new Response(node, cluster);
            }
            catch (IOException e)
            {
                last = e;
            }
            finally
            {
                if (connection != null)
                    connection.disconnect();
            }
        }
        throw last == null ? new IOException("no etcd endpoint") : last;
    }

    private void pinCluster(String cluster)
    {
        if (cluster == null || cluster.isEmpty())
            throw new IllegalStateException("etcd response has no cluster id");
        String existing = pinnedClusterId;
        if (existing == null)
            synchronized (this)
            {
                if (pinnedClusterId == null)
                    pinnedClusterId = cluster;
                else
                    existing = pinnedClusterId;
            }
        if (existing != null && !existing.equals(cluster))
            throw new IllegalStateException("etcd cluster id changed");
    }

    private static JsonNode parse(byte[] bytes) throws IOException
    {
        try (JsonParser parser = JSON_FACTORY.createParser(bytes))
        {
            parser.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
            JsonNode node = JSON.readTree(parser);
            if (node == null || parser.nextToken() != null)
                throw new IOException("trailing JSON data");
            return node;
        }
    }

    private static byte[] readBounded(HttpURLConnection connection, int max) throws IOException
    {
        try (java.io.InputStream input = connection.getInputStream(); ByteArrayOutputStream output = new ByteArrayOutputStream())
        {
            byte[] buffer = new byte[8192];
            int total = 0;
            int count;
            while ((count = input.read(buffer)) != -1)
            {
                total += count;
                if (total > max)
                    throw new IOException("oversized etcd response");
                output.write(buffer, 0, count);
            }
            return output.toByteArray();
        }
    }

    private static ObjectNode rangeJson(String key)
    {
        ObjectNode body = JSON.createObjectNode();
        body.put("key", b64(key));
        return body;
    }

    private static String key(UUID domainId)
    {
        return PREFIX + domainId + "/state";
    }

    private static String b64(String value)
    {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String b64(byte[] value)
    {
        return Base64.getEncoder().encodeToString(value);
    }

    private static byte[] unb64(String value)
    {
        return Base64.getDecoder().decode(value);
    }

    private static EtcdScalarResult decodeResult(EtcdScalarRequest request, byte[] bytes)
    {
        EtcdScalarResult result = EtcdScalarResult.decode(bytes);
        if (!result.known || result.reads.size() != request.reads.size())
            throw new IllegalStateException("retained receipt does not match requested reads");
        for (EtcdScalarRequest.Read read : request.reads)
            if (!result.reads.containsKey(read.slot))
                throw new IllegalStateException("retained receipt is missing requested read slot " + read.slot);
        return result;
    }

    private static EtcdScalarResult unknown()
    {
        return EtcdScalarResult.unknown();
    }

    private static final class Response
    {
        final JsonNode node;
        final String clusterId;

        Response(JsonNode node, String clusterId)
        {
            this.node = node;
            this.clusterId = clusterId;
        }
    }

    private static final class ReadValue
    {
        final State state;
        final long modRevision;

        ReadValue(State state, long modRevision)
        {
            this.state = state;
            this.modRevision = modRevision;
        }
    }

    private static final class Receipt
    {
        final byte[] hash;
        final byte[] result;

        Receipt(byte[] hash, byte[] result)
        {
            this.hash = hash;
            this.result = result;
        }
    }

    private static final class State
    {
        final UUID domainId;
        final TableId tableId;
        long generation;
        final long schemaEpoch;
        final String groupId;
        final String clusterId;
        boolean active;
        boolean fenced;
        long decisionPosition;
        final Map<Integer, Integer> rows = new LinkedHashMap<>();
        final Map<UUID, Receipt> receipts = new LinkedHashMap<>();

        State(UUID domainId, TableId tableId, long generation, long schemaEpoch,
              String groupId, String clusterId, boolean active)
        {
            this.domainId = domainId;
            this.tableId = tableId;
            this.generation = generation;
            this.schemaEpoch = schemaEpoch;
            this.groupId = groupId;
            this.clusterId = clusterId;
            this.active = active;
        }

        boolean sameIdentity(State other)
        {
            return domainId.equals(other.domainId)
                   && tableId.equals(other.tableId)
                   && generation == other.generation
                   && schemaEpoch == other.schemaEpoch
                   && groupId.equals(other.groupId)
                   && clusterId.equals(other.clusterId);
        }

        byte[] encode() throws IOException
        {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(0x45545331);
            out.writeInt(VERSION);
            out.writeLong(domainId.getMostSignificantBits());
            out.writeLong(domainId.getLeastSignificantBits());
            out.writeUTF(tableId.toString());
            out.writeLong(generation);
            out.writeLong(schemaEpoch);
            out.writeUTF(groupId);
            out.writeUTF(clusterId);
            out.writeBoolean(active);
            out.writeBoolean(fenced);
            out.writeLong(decisionPosition);
            out.writeInt(rows.size());
            for (Map.Entry<Integer, Integer> row : rows.entrySet())
            {
                out.writeInt(row.getKey());
                out.writeInt(row.getValue());
            }
            out.writeInt(receipts.size());
            for (Map.Entry<UUID, Receipt> receipt : receipts.entrySet())
            {
                out.writeLong(receipt.getKey().getMostSignificantBits());
                out.writeLong(receipt.getKey().getLeastSignificantBits());
                out.writeInt(receipt.getValue().hash.length);
                out.write(receipt.getValue().hash);
                out.writeInt(receipt.getValue().result.length);
                out.write(receipt.getValue().result);
            }
            out.flush();
            if (bytes.size() > MAX_VALUE)
                throw new IllegalStateException("etcd state exceeds 1MiB");
            return bytes.toByteArray();
        }

        static State decode(byte[] bytes) throws IOException
        {
            if (bytes.length > MAX_VALUE)
                throw new IOException("oversized etcd state");
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            if (in.readInt() != 0x45545331 || in.readInt() != VERSION)
                throw new IOException("unknown etcd state version");
            UUID domainId = new UUID(in.readLong(), in.readLong());
            String tableText = in.readUTF();
            TableId tableId = TableId.fromString(tableText);
            if (!tableId.toString().equals(tableText))
                throw new IOException("noncanonical table id");
            State state = new State(domainId,
                                    tableId,
                                    in.readLong(),
                                    in.readLong(),
                                    in.readUTF(),
                                    in.readUTF(),
                                    readBoolean(in));
            state.fenced = readBoolean(in);
            state.decisionPosition = in.readLong();
            if (state.generation <= 0 || state.schemaEpoch < 0 || state.decisionPosition < 0 || (state.active && state.fenced))
                throw new IOException("invalid owner state");
            try
            {
                if (!UUID.fromString(state.groupId).toString().equals(state.groupId))
                    throw new IllegalArgumentException();
            }
            catch (IllegalArgumentException e)
            {
                throw new IOException("invalid group id", e);
            }
            try
            {
                if (!state.clusterId.matches("[1-9][0-9]*")
                    || Long.compareUnsigned(Long.parseUnsignedLong(state.clusterId), 0L) <= 0)
                    throw new NumberFormatException();
            }
            catch (NumberFormatException e)
            {
                throw new IOException("invalid cluster id", e);
            }
            int rows = in.readInt();
            if (rows < 0 || rows > MAX_ROWS)
                throw new IOException("invalid row count");
            for (int i = 0; i < rows; i++)
            {
                int key = in.readInt();
                if (state.rows.put(key, in.readInt()) != null)
                    throw new IOException("duplicate row key");
            }
            int receipts = in.readInt();
            if (receipts < 0 || receipts > MAX_RECEIPTS)
                throw new IOException("invalid receipt count");
            if (state.decisionPosition != receipts)
                throw new IOException("decision counter does not match retained receipt count");
            for (int i = 0; i < receipts; i++)
            {
                UUID id = new UUID(in.readLong(), in.readLong());
                if (state.receipts.containsKey(id))
                    throw new IOException("duplicate receipt id");
                byte[] hash = readBytes(in, 32);
                if (hash.length != 32)
                    throw new IOException("invalid receipt hash");
                byte[] result = readBytes(in, 1024 * 64);
                EtcdScalarResult decoded = EtcdScalarResult.decode(result);
                if (!decoded.known || decoded.decisionPosition != i + 1)
                    throw new IOException("invalid receipt decision position");
                state.receipts.put(id, new Receipt(hash, result));
            }
            if (in.available() != 0)
                throw new IOException("trailing state bytes");
            return state;
        }

        private static boolean readBoolean(DataInputStream in) throws IOException
        {
            int value = in.readUnsignedByte();
            if (value > 1)
                throw new IOException("noncanonical boolean");
            return value == 1;
        }

        private static byte[] readBytes(DataInputStream in, int max) throws IOException
        {
            int length = in.readInt();
            if (length < 0 || length > max)
                throw new IOException("invalid state bytes");
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return bytes;
        }
    }
}
