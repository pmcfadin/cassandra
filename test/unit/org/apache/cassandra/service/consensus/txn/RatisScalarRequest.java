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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.JsonUtils;

/** The bounded, canonical JSON command sent to the standalone scalar participant. */
public final class RatisScalarRequest
{
    public static final String PROVIDER = "ratis-scalar";
    public static final String PROFILE = "scalar-int";
    public static final int VERSION = 1;
    public static final int MAX_OPERATIONS = 16;
    public static final int MAX_BYTES = 64 * 1024;
    private static final ObjectMapper JSON = JsonUtils.JSON_OBJECT_MAPPER.copy()
                                               .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION.mappedFeature())
                                               .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);

    public final UUID domainId;
    public final TableId tableId;
    public final UUID requestId;
    public final long generation;
    public final long schemaEpoch;
    public final String groupId;
    public final long atMicros;
    public final List<ScalarTransactionPlan.Read> reads;
    public final List<ScalarTransactionPlan.Condition> conditions;
    public final List<ScalarTransactionPlan.Write> writes;
    public final String returnShape;

    public RatisScalarRequest(UUID domainId, TableId tableId, UUID requestId, long generation, long schemaEpoch,
                              String groupId, long atMicros, List<ScalarTransactionPlan.Read> reads,
                              List<ScalarTransactionPlan.Condition> conditions, List<ScalarTransactionPlan.Write> writes,
                              String returnShape)
    {
        this.domainId = require(domainId, "domainId");
        this.tableId = require(tableId, "tableId");
        this.requestId = require(requestId, "requestId");
        if (generation <= 0 || schemaEpoch < 0 || atMicros < 0)
            throw new IllegalArgumentException("generation must be positive; schemaEpoch and atMicros nonnegative");
        this.generation = generation;
        this.schemaEpoch = schemaEpoch;
        this.groupId = canonicalUuid(groupId, "groupId");
        this.atMicros = atMicros;
        this.reads = immutable(reads, "reads");
        this.conditions = immutable(conditions, "conditions");
        this.writes = immutable(writes, "writes");
        if (returnShape == null || returnShape.length() > 256)
            throw new IllegalArgumentException("returnShape is missing or too long");
        this.returnShape = returnShape;
        validateShape();
    }

    public byte[] encode()
    {
        ObjectNode root = JSON.createObjectNode();
        root.put("v", VERSION).put("op", "txn").put("domain", domainId.toString()).put("table", tableId.asUUID().toString())
            .put("group", groupId).put("generation", generation).put("schemaEpoch", schemaEpoch)
            .put("requestId", requestId.toString()).put("atMicros", atMicros).put("returnShape", returnShape);
        putArrays(root);
        return bytes(root);
    }

    public byte[] lookupBytes()
    {
        ObjectNode root = (ObjectNode) parse(encode());
        root.put("op", "lookup");
        return bytes(root);
    }

    public static RatisScalarRequest decode(byte[] encoded)
    {
        if (encoded == null || encoded.length == 0 || encoded.length > MAX_BYTES)
            throw new IllegalArgumentException("invalid request length");
        JsonNode root = parse(encoded);
        requireFields(root, "v", "op", "domain", "table", "group", "generation", "schemaEpoch", "requestId", "atMicros", "returnShape", "reads", "conditions", "writes");
        if (root.get("v").asInt() != VERSION || !(root.get("op").asText().equals("txn") || root.get("op").asText().equals("lookup")))
            throw new IllegalArgumentException("unsupported request version or operation");
        List<ScalarTransactionPlan.Read> reads = new ArrayList<>();
        for (JsonNode n : array(root, "reads"))
            reads.add(new ScalarTransactionPlan.Read(integer(n, "slot"), integer(n, "key")));
        List<ScalarTransactionPlan.Condition> conditions = new ArrayList<>();
        for (JsonNode n : array(root, "conditions"))
        {
            String kind = text(n, "operator");
            if (n.has("value"))
                requireNested(n, "slot", "operator", "value");
            else
                requireNested(n, "slot", "operator");
            if (n.has("value") && !n.get("value").isNull() && !n.get("value").isIntegralNumber())
                throw new IllegalArgumentException("invalid condition value");
            conditions.add(new ScalarTransactionPlan.Condition(integer(n, "slot"), false,
                                                               kind(kind), n.has("value") && !n.get("value").isNull() ? integer(n, "value") : null));
        }
        List<ScalarTransactionPlan.Write> writes = new ArrayList<>();
        for (JsonNode n : array(root, "writes"))
        {
            if ((n.has("value") && n.has("sourceSlot")) || (!n.has("value") && !n.has("sourceSlot")))
                throw new IllegalArgumentException("write must contain exactly one value source");
            requireNested(n, "key", n.has("value") ? "value" : "sourceSlot");
            writes.add(n.has("value") ? ScalarTransactionPlan.Write.constant(integer(n, "key"), integer(n, "value"))
                                           : ScalarTransactionPlan.Write.reference(integer(n, "key"), integer(n, "sourceSlot")));
        }
        RatisScalarRequest result = new RatisScalarRequest(UUID.fromString(text(root, "domain")), TableId.fromUUID(UUID.fromString(text(root, "table"))),
                                                            UUID.fromString(text(root, "requestId")), longValue(root, "generation"), longValue(root, "schemaEpoch"),
                                                            text(root, "group"), longValue(root, "atMicros"), reads, conditions, writes, text(root, "returnShape"));
        if (!java.util.Arrays.equals(encoded, result.encode()) && !java.util.Arrays.equals(encoded, result.lookupBytes()))
            throw new IllegalArgumentException("noncanonical request encoding");
        return result;
    }

    private void putArrays(ObjectNode root)
    {
        ArrayNode readArray = root.putArray("reads");
        for (ScalarTransactionPlan.Read read : reads)
            readArray.addObject().put("slot", read.slot).put("key", read.key);
        ArrayNode conditionArray = root.putArray("conditions");
        for (ScalarTransactionPlan.Condition condition : conditions)
        {
            ObjectNode n = conditionArray.addObject().put("slot", condition.slot).put("operator", wireKind(condition.kind));
            if (condition.value != null)
                n.put("value", condition.value);
        }
        ArrayNode writeArray = root.putArray("writes");
        for (ScalarTransactionPlan.Write write : writes)
        {
            ObjectNode n = writeArray.addObject().put("key", write.key);
            if (write.constant != null)
                n.put("value", write.constant);
            else
                n.put("sourceSlot", write.sourceSlot);
        }
    }

    private void validateShape()
    {
        if (reads.size() > MAX_OPERATIONS || conditions.size() > MAX_OPERATIONS || writes.size() > MAX_OPERATIONS)
            throw new IllegalArgumentException("too many scalar operations");
        Set<Integer> slots = new HashSet<>();
        for (ScalarTransactionPlan.Read read : reads)
            if (!slots.add(read.slot)) throw new IllegalArgumentException("duplicate read slot");
        for (ScalarTransactionPlan.Condition condition : conditions)
            if (!slots.contains(condition.slot)) throw new IllegalArgumentException("dangling condition slot");
        Set<Integer> keys = new HashSet<>();
        for (ScalarTransactionPlan.Write write : writes)
            if (!keys.add(write.key) || (write.constant == null && !slots.contains(write.sourceSlot)))
                throw new IllegalArgumentException("invalid write");
    }

    private static String wireKind(TransactionCondition.Kind kind)
    {
        switch (kind)
        {
            case EQUAL: return "EQ";
            case NOT_EQUAL: return "NEQ";
            case GREATER_THAN: return "GT";
            case GREATER_THAN_OR_EQUAL: return "GTE";
            case LESS_THAN: return "LT";
            case LESS_THAN_OR_EQUAL: return "LTE";
            case IS_NULL: case IS_NOT_NULL: return kind.name();
            default: throw new IllegalArgumentException("unknown condition kind");
        }
    }

    private static TransactionCondition.Kind kind(String value)
    {
        switch (value)
        {
            case "EQ": return TransactionCondition.Kind.EQUAL;
            case "NEQ": return TransactionCondition.Kind.NOT_EQUAL;
            case "GT": return TransactionCondition.Kind.GREATER_THAN;
            case "GTE": return TransactionCondition.Kind.GREATER_THAN_OR_EQUAL;
            case "LT": return TransactionCondition.Kind.LESS_THAN;
            case "LTE": return TransactionCondition.Kind.LESS_THAN_OR_EQUAL;
            default: return TransactionCondition.Kind.valueOf(value);
        }
    }

    private static byte[] bytes(JsonNode node)
    {
        try
        {
            byte[] result = JSON.writeValueAsBytes(node);
            if (result.length > MAX_BYTES)
                throw new IllegalArgumentException("request exceeds 64 KiB");
            return result;
        }
        catch (java.io.IOException e)
        {
            throw new IllegalStateException(e);
        }
    }
    private static JsonNode parse(byte[] bytes)
    {
        try
        {
            com.fasterxml.jackson.core.JsonParser parser = JSON.getFactory().createParser(bytes);
            JsonNode n = JSON.readTree(parser);
            if (n == null || !n.isObject() || parser.nextToken() != null)
                throw new IllegalArgumentException("request must be one object");
            return n;
        }
        catch (java.io.IOException | RuntimeException e)
        {
            if (e instanceof IllegalArgumentException)
                throw (IllegalArgumentException) e;
            throw new IllegalArgumentException("malformed request", e);
        }
    }
    private static void requireFields(JsonNode n, String... fields)
    {
        Set<String> expected = new HashSet<>(java.util.Arrays.asList(fields));
        java.util.Iterator<String> it = n.fieldNames();
        while (it.hasNext())
            if (!expected.contains(it.next()))
                throw new IllegalArgumentException("unknown request field");
        for (String field : fields)
            if (!n.has(field))
                throw new IllegalArgumentException("missing request field " + field);
    }
    private static ArrayNode array(JsonNode n, String field)
    {
        if (!n.get(field).isArray() || n.get(field).size() > MAX_OPERATIONS)
            throw new IllegalArgumentException("invalid " + field);
        return (ArrayNode) n.get(field);
    }
    private static void requireNested(JsonNode n, String... fields)
    {
        Set<String> expected = new HashSet<>(java.util.Arrays.asList(fields));
        java.util.Iterator<String> it = n.fieldNames();
        while (it.hasNext())
            if (!expected.contains(it.next()))
                throw new IllegalArgumentException("unknown transaction field");
        for (String field : fields)
            if (!n.has(field))
                throw new IllegalArgumentException("missing transaction field");
    }
    private static int integer(JsonNode n, String field)
    {
        if (!n.has(field) || !n.get(field).isIntegralNumber() || !n.get(field).canConvertToInt())
            throw new IllegalArgumentException("invalid " + field);
        return n.get(field).intValue();
    }
    private static long longValue(JsonNode n, String field)
    {
        if (!n.has(field) || !n.get(field).isIntegralNumber() || !n.get(field).canConvertToLong())
            throw new IllegalArgumentException("invalid " + field);
        return n.get(field).longValue();
    }
    private static String text(JsonNode n, String field)
    {
        if (!n.has(field) || !n.get(field).isTextual())
            throw new IllegalArgumentException("invalid " + field);
        return n.get(field).textValue();
    }
    private static String canonicalUuid(String value, String name)
    {
        try
        {
            UUID uuid = UUID.fromString(value);
            if (!uuid.toString().equals(value))
                throw new IllegalArgumentException(name + " must be canonical UUID");
            return value;
        }
        catch (RuntimeException e)
        {
            throw new IllegalArgumentException(name + " must be canonical UUID", e);
        }
    }
    private static <T> T require(T value, String name)
    {
        if (value == null)
            throw new NullPointerException(name);
        return value;
    }
    private static <T> List<T> immutable(List<T> values, String name)
    {
        if (values == null)
            throw new NullPointerException(name);
        if (values.contains(null))
            throw new IllegalArgumentException(name + " contains null");
        return Collections.unmodifiableList(new ArrayList<>(values));
    }
}
