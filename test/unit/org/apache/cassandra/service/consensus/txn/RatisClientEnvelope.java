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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.cassandra.utils.JsonUtils;

/** A bounded, portable client envelope for a prepared Ratis transaction. */
public final class RatisClientEnvelope
{
    public static final int VERSION = 1;
    public static final int MAX_CQL_BYTES = 16 * 1024;
    public static final int MAX_BYTES = 128 * 1024;
    private static final ObjectMapper JSON = JsonUtils.JSON_OBJECT_MAPPER.copy()
                                                       .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION.mappedFeature())
                                                       .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);

    public final String cql;
    public final RatisScalarRequest request;
    public final String digest;

    private RatisClientEnvelope(String cql, RatisScalarRequest request, String digest)
    {
        this.cql = cql;
        this.request = request;
        this.digest = digest;
    }

    public static RatisClientEnvelope create(String cql, RatisScalarRequest request)
    {
        if (cql == null || request == null)
            throw new NullPointerException(cql == null ? "cql" : "request");
        validateCql(cql);
        byte[] requestBytes = request.encode();
        return new RatisClientEnvelope(cql, request, digest(cql, requestBytes));
    }

    public byte[] encode()
    {
        ObjectNode root = JSON.createObjectNode();
        root.put("v", VERSION).put("cql", cql)
            .put("request", Base64.getEncoder().encodeToString(request.encode()))
            .put("digest", digest);
        try
        {
            byte[] encoded = JSON.writeValueAsBytes(root);
            if (encoded.length > MAX_BYTES)
                throw new IllegalArgumentException("envelope exceeds 128 KiB");
            return encoded;
        }
        catch (java.io.IOException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public static RatisClientEnvelope decode(byte[] encoded)
    {
        if (encoded == null || encoded.length == 0 || encoded.length > MAX_BYTES)
            throw new IllegalArgumentException("invalid envelope length");
        JsonNode root = parse(encoded);
        requireFields(root, "v", "cql", "request", "digest");
        JsonNode version = root.get("v");
        if (!version.isIntegralNumber() || !version.canConvertToInt() || version.intValue() != VERSION)
            throw new IllegalArgumentException("unsupported envelope version");
        String cql = text(root, "cql");
        validateCql(cql);
        String requestText = text(root, "request");
        byte[] requestBytes;
        try
        {
            requestBytes = Base64.getDecoder().decode(requestText);
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalArgumentException("invalid request base64", e);
        }
        if (!Base64.getEncoder().encodeToString(requestBytes).equals(requestText))
            throw new IllegalArgumentException("noncanonical request base64");
        RatisScalarRequest request = RatisScalarRequest.decode(requestBytes);
        if (!Arrays.equals(requestBytes, request.encode()))
            throw new IllegalArgumentException("noncanonical request bytes");
        String digest = text(root, "digest");
        if (!digest.matches("[0-9a-f]{64}") || !MessageDigest.isEqual(digest.getBytes(StandardCharsets.US_ASCII),
                                                                  digest(cql, requestBytes).getBytes(StandardCharsets.US_ASCII)))
            throw new IllegalArgumentException("envelope digest mismatch");
        return new RatisClientEnvelope(cql, request, digest);
    }

    private static String digest(String cql, byte[] request)
    {
        try
        {
            byte[] cqlBytes = cql.getBytes(StandardCharsets.UTF_8);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(ByteBuffer.allocate(4).putInt(cqlBytes.length).array());
            digest.update(cqlBytes);
            digest.update(ByteBuffer.allocate(4).putInt(request.length).array());
            digest.update(request);
            StringBuilder result = new StringBuilder(64);
            for (byte value : digest.digest())
            {
                int unsigned = value & 0xff;
                result.append(Character.forDigit(unsigned >>> 4, 16));
                result.append(Character.forDigit(unsigned & 0xf, 16));
            }
            return result.toString();
        }
        catch (java.security.NoSuchAlgorithmException e)
        {
            throw new AssertionError(e);
        }
    }

    private static void validateCql(String cql)
    {
        if (cql.getBytes(StandardCharsets.UTF_8).length > MAX_CQL_BYTES)
            throw new IllegalArgumentException("CQL exceeds 16 KiB");
    }

    private static JsonNode parse(byte[] bytes)
    {
        try
        {
            com.fasterxml.jackson.core.JsonParser parser = JSON.getFactory().createParser(bytes);
            JsonNode node = JSON.readTree(parser);
            if (node == null || !node.isObject() || parser.nextToken() != null)
                throw new IllegalArgumentException("envelope must be one object");
            return node;
        }
        catch (java.io.IOException | RuntimeException e)
        {
            if (e instanceof IllegalArgumentException)
                throw (IllegalArgumentException) e;
            throw new IllegalArgumentException("malformed envelope", e);
        }
    }

    private static void requireFields(JsonNode node, String... fields)
    {
        Set<String> expected = new HashSet<>(Arrays.asList(fields));
        java.util.Iterator<String> names = node.fieldNames();
        while (names.hasNext())
            if (!expected.contains(names.next()))
                throw new IllegalArgumentException("unknown envelope field");
        for (String field : fields)
            if (!node.has(field))
                throw new IllegalArgumentException("missing envelope field " + field);
    }

    private static String text(JsonNode node, String field)
    {
        if (!node.get(field).isTextual())
            throw new IllegalArgumentException("invalid envelope " + field);
        return node.get(field).textValue();
    }
}
