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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.JsonUtils;

/** JDK HTTP loopback client for the test-only Ratis participant. */
public final class RatisScalarStore implements AutoCloseable
{
    public static final int MAX_RESPONSE_BYTES = 64 * 1024;
    private static final ObjectMapper JSON = JsonUtils.JSON_OBJECT_MAPPER.copy()
                                               .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION.mappedFeature());
    private final List<String> endpoints;
    private final int timeoutMillis;

    public static final class UnknownOutcomeException extends RuntimeException
    {
        public UnknownOutcomeException(String message, Throwable cause)
        {
            super(message, cause);
        }

        public UnknownOutcomeException(String message)
        {
            super(message);
        }
    }

    public RatisScalarStore(List<String> endpoints, int timeoutMillis)
    {
        if (endpoints == null || endpoints.isEmpty() || timeoutMillis <= 0)
            throw new IllegalArgumentException("Ratis endpoints and timeout are required");
        List<String> copy = new ArrayList<>(endpoints.size());
        for (String endpoint : endpoints)
        {
            if (endpoint == null || endpoint.isEmpty())
                throw new IllegalArgumentException("endpoint is missing");
            try
            {
                URL url = new URL(endpoint);
                if (!"http".equalsIgnoreCase(url.getProtocol()) || url.getHost().isEmpty()
                    || url.getUserInfo() != null || url.getQuery() != null || url.getRef() != null
                    || !url.getPath().isEmpty() && !url.getPath().equals("/"))
                    throw new IllegalArgumentException("endpoint must be an HTTP origin");
            }
            catch (IOException e)
            {
                throw new IllegalArgumentException("invalid endpoint", e);
            }
            copy.add(endpoint.replaceAll("/$", ""));
        }
        this.endpoints = Collections.unmodifiableList(copy);
        this.timeoutMillis = timeoutMillis;
    }

    public RatisScalarResult submit(RatisScalarRequest request)
    {
        return transaction(request, "txn");
    }

    public RatisScalarResult lookup(RatisScalarRequest request)
    {
        return transaction(request, "lookup");
    }

    public ExternalTransactionDomainBinding prepareAndAttest(TransactionDomainDescriptor prepared, long schemaEpoch, String groupId)
    {
        validateReservation(prepared, schemaEpoch, groupId);
        requireAdministrativeAck(command(identity("init", prepared.id(), prepared.tableId().asUUID(), groupId, prepared.generation(), schemaEpoch)));
        return attest(prepared, schemaEpoch, groupId);
    }

    private static void validateReservation(TransactionDomainDescriptor prepared, long schemaEpoch, String groupId)
    {
        Objects.requireNonNull(prepared, "prepared");
        if (prepared.state() != TransactionDomainDescriptor.State.PREPARED || schemaEpoch < 0 || !canonical(groupId)
            || !"ratis-scalar".equals(prepared.providerId()) || !"scalar-int".equals(prepared.profileId())
            || prepared.profileVersion() != 1 || prepared.protocolVersion() != 1 || prepared.storageVersion() != 1)
            throw new IllegalArgumentException("attestation requires a PREPARED Ratis scalar-int reservation and canonical identity");
    }

    public ExternalTransactionDomainBinding attest(TransactionDomainDescriptor prepared, long schemaEpoch, String groupId)
    {
        validateReservation(prepared, schemaEpoch, groupId);
        JsonNode response = command(identity("attest", prepared.id(), prepared.tableId().asUUID(), groupId, prepared.generation(), schemaEpoch));
        if (!validAttestationShape(response))
            throw new UnknownOutcomeException("participant attestation is malformed");
        if (!response.path("known").booleanValue() || !response.path("active").booleanValue()
            || !prepared.id().toString().equals(response.path("domain").asText())
            || !prepared.tableId().asUUID().toString().equals(response.path("table").asText())
            || !groupId.equals(response.path("group").asText())
            || response.path("generation").asLong(-1) != prepared.generation()
            || response.path("schemaEpoch").asLong(-1) != schemaEpoch
            || !"scalar-int".equals(response.path("profile").asText())
            || response.path("profileVersion").asInt(-1) != 1
            || response.path("protocolVersion").asInt(-1) != 1
            || response.path("storageVersion").asInt(-1) != 1
            || response.path("appliedIndex").asLong(0) <= 0)
            throw new InvalidRequestException("participant attestation does not match the reserved owner");
        return ExternalTransactionDomainBinding.ratis(groupId, schemaEpoch, response.path("appliedIndex").asLong());
    }

    private static boolean validAttestationShape(JsonNode response)
    {
        return response.isObject() && response.size() == 13
               && response.path("domain").isTextual()
               && response.path("table").isTextual()
               && response.path("group").isTextual()
               && response.path("profile").isTextual()
               && response.path("known").isBoolean()
               && response.path("active").isBoolean()
               && response.path("generation").isIntegralNumber()
               && response.path("schemaEpoch").isIntegralNumber()
               && response.path("profileVersion").isIntegralNumber()
               && response.path("protocolVersion").isIntegralNumber()
               && response.path("storageVersion").isIntegralNumber()
               && response.path("appliedIndex").isIntegralNumber()
               && response.path("generation").canConvertToLong()
               && response.path("schemaEpoch").canConvertToLong()
               && response.path("appliedIndex").canConvertToLong()
               && response.path("profileVersion").canConvertToInt()
               && response.path("protocolVersion").canConvertToInt()
               && response.path("storageVersion").canConvertToInt();
    }

    public void fence(RatisScalarRequest identity, long nextGeneration)
    {
        Objects.requireNonNull(identity, "identity");
        if (nextGeneration <= identity.generation)
            throw new IllegalArgumentException("next generation must advance");
        ObjectNode command = identity("fence", identity.domainId, identity.tableId.asUUID(), identity.groupId, identity.generation, identity.schemaEpoch);
        command.put("nextGeneration", nextGeneration);
        requireAdministrativeAck(this.command(command));
    }

    private static void requireAdministrativeAck(JsonNode response)
    {
        if (response.size() != 2 || !response.path("known").isBoolean() || !response.path("known").booleanValue())
            throw new UnknownOutcomeException("participant administrative response is malformed");
    }

    @Override
    public void close()
    {
        // HttpURLConnection has no persistent client resources owned by this store.
    }

    private RatisScalarResult transaction(RatisScalarRequest request, String op)
    {
        Objects.requireNonNull(request, "request");
        byte[] encoded = "lookup".equals(op) ? request.lookupBytes() : request.encode();
        JsonNode response = command(encoded);
        try
        {
            if (response.size() == 2 && response.path("known").isBoolean() && !response.path("known").booleanValue())
            {
                if ("lookup".equals(op))
                    return RatisScalarResult.unknown();
                throw new IOException("transaction has no known outcome");
            }
            ObjectNode result = (ObjectNode) response.deepCopy();
            result.remove("status");
            RatisScalarResult parsed = RatisScalarResult.decode(JSON.writeValueAsBytes(result));
            if (!parsed.known || parsed.atMicros != request.atMicros || !parsed.reads.keySet().equals(readSlots(request)))
                throw new IOException("receipt does not match request");
            return parsed;
        }
        catch (IOException | RuntimeException e)
        {
            if (e instanceof InvalidRequestException)
                throw (InvalidRequestException) e;
            throw new UnknownOutcomeException("participant response is malformed", e);
        }
    }

    private static Set<Integer> readSlots(RatisScalarRequest request)
    {
        Set<Integer> slots = new HashSet<>();
        for (ScalarTransactionPlan.Read read : request.reads)
            slots.add(read.slot);
        return slots;
    }

    private JsonNode command(byte[] body)
    {
        IOException last = null;
        int start = ThreadLocalRandom.current().nextInt(endpoints.size());
        for (int i = 0; i < endpoints.size(); i++)
        {
            HttpURLConnection connection = null;
            try
            {
                connection = (HttpURLConnection) new URL(endpoints.get((start + i) % endpoints.size()) + "/command").openConnection();
                connection.setConnectTimeout(timeoutMillis);
                connection.setReadTimeout(timeoutMillis);
                connection.setInstanceFollowRedirects(false);
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                try (java.io.OutputStream output = connection.getOutputStream())
                {
                    output.write(body);
                }
                int status = connection.getResponseCode();
                if (status >= 500)
                    throw new IOException("participant quorum unavailable");
                byte[] bytes = readBounded(status >= 400 ? connection.getErrorStream() : connection.getInputStream());
                JsonNode response;
                try
                {
                    response = parse(bytes);
                }
                catch (InvalidRequestException e)
                {
                    throw new IOException("malformed participant response", e);
                }
                if ("rejected".equals(response.path("status").asText()) && response.path("reason").isTextual())
                    throw new InvalidRequestException(response.path("reason").textValue());
                if (status < 200 || status >= 300 || !"ok".equals(response.path("status").asText()))
                    throw new IOException("participant response has no successful status");
                return response;
            }
            catch (InvalidRequestException e)
            {
                throw e;
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
        throw new UnknownOutcomeException("transport outcome is unknown", last);
    }

    private JsonNode command(ObjectNode command)
    {
        try
        {
            return command(JSON.writeValueAsBytes(command));
        }
        catch (IOException e)
        {
            throw new IllegalStateException(e);
        }
    }

    private static ObjectNode identity(String op, UUID domain, UUID table, String group, long generation, long schemaEpoch)
    {
        return JSON.createObjectNode().put("v", 1).put("op", op).put("domain", domain.toString()).put("table", table.toString())
                          .put("group", group).put("generation", generation).put("schemaEpoch", schemaEpoch);
    }

    private static JsonNode parse(byte[] bytes)
    {
        if (bytes == null || bytes.length == 0 || bytes.length > MAX_RESPONSE_BYTES)
            throw new InvalidRequestException("invalid participant response");
        try (JsonParser parser = JSON.getFactory().createParser(bytes))
        {
            JsonNode n = JSON.readTree(parser);
            if (n == null || !n.isObject() || parser.nextToken() != null)
                throw new InvalidRequestException("invalid participant response");
            return n;
        }
        catch (IOException e)
        {
            throw new InvalidRequestException("malformed participant response");
        }
    }

    private static byte[] readBounded(InputStream input) throws IOException
    {
        if (input == null)
            throw new IOException("empty participant response");
        try (InputStream stream = input)
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = stream.read(buffer)) != -1)
            {
                if (out.size() + read > MAX_RESPONSE_BYTES)
                    throw new IOException("participant response exceeds bound");
                out.write(buffer, 0, read);
            }
            return out.toByteArray();
        }
    }

    private static boolean canonical(String value)
    {
        try
        {
            return UUID.fromString(value).toString().equals(value);
        }
        catch (RuntimeException e)
        {
            return false;
        }
    }
}
