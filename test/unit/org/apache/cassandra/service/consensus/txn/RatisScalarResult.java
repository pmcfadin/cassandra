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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.cassandra.utils.JsonUtils;

/** Original scalar outcome returned by a committed Ratis command. */
public final class RatisScalarResult
{
    public static final int MAX_BYTES = RatisScalarRequest.MAX_BYTES;
    private static final ObjectMapper JSON = JsonUtils.JSON_OBJECT_MAPPER.copy()
                                               .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION.mappedFeature())
                                               .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);
    public final boolean known;
    public final boolean conditionMet;
    public final long atMicros;
    public final long decisionPosition;
    public final Map<Integer, Integer> reads;

    public RatisScalarResult(boolean known, boolean conditionMet, long atMicros, long decisionPosition, Map<Integer, Integer> reads)
    {
        if (atMicros < 0 || decisionPosition < 0 || reads == null || reads.size() > RatisScalarRequest.MAX_OPERATIONS)
            throw new IllegalArgumentException("invalid result");
        if (!known && (conditionMet || atMicros != 0 || decisionPosition != 0 || !reads.isEmpty()))
            throw new IllegalArgumentException("unknown result must be empty");
        if (known && decisionPosition == 0)
            throw new IllegalArgumentException("known result must have a decision position");
        Map<Integer, Integer> copy = new LinkedHashMap<>();
        for (Map.Entry<Integer, Integer> e : reads.entrySet())
        {
            if (e.getKey() == null || e.getKey() < 0 || copy.put(e.getKey(), e.getValue()) != null)
                throw new IllegalArgumentException("invalid read slot");
        }
        this.known = known;
        this.conditionMet = conditionMet;
        this.atMicros = atMicros;
        this.decisionPosition = decisionPosition;
        this.reads = Collections.unmodifiableMap(copy);
    }

    public static RatisScalarResult unknown()
    {
        return new RatisScalarResult(false, false, 0, 0, Collections.emptyMap());
    }

    public byte[] encode()
    {
        ObjectNode n = JSON.createObjectNode().put("known", known).put("conditionMet", conditionMet)
                              .put("atMicros", atMicros).put("decisionPosition", decisionPosition);
        ObjectNode readNode = n.putObject("reads");
        for (Map.Entry<Integer, Integer> e : reads.entrySet())
        {
            if (e.getValue() == null) readNode.putNull(String.valueOf(e.getKey()));
            else readNode.put(String.valueOf(e.getKey()), e.getValue());
        }
        try
        {
            byte[] result = JSON.writeValueAsBytes(n);
            if (result.length > MAX_BYTES)
                throw new IllegalArgumentException("result exceeds 64 KiB");
            return result;
        }
        catch (java.io.IOException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public static RatisScalarResult decode(byte[] encoded)
    {
        if (encoded == null || encoded.length == 0 || encoded.length > MAX_BYTES)
            throw new IllegalArgumentException("invalid result length");
        try
        {
            com.fasterxml.jackson.core.JsonParser parser = JSON.getFactory().createParser(encoded);
            JsonNode n = JSON.readTree(parser);
            if (parser.nextToken() != null)
                throw new IllegalArgumentException("trailing result bytes");
            if (n == null || !n.isObject() || n.size() != 5 || !n.has("known") || !n.has("conditionMet") || !n.has("atMicros") || !n.has("decisionPosition") || !n.has("reads"))
                throw new IllegalArgumentException("invalid result shape");
            if (!n.get("known").isBoolean() || !n.get("conditionMet").isBoolean() || !n.get("atMicros").isIntegralNumber() || !n.get("atMicros").canConvertToLong() || !n.get("decisionPosition").isIntegralNumber() || !n.get("decisionPosition").canConvertToLong() || !n.get("reads").isObject())
                throw new IllegalArgumentException("invalid result fields");
            Map<Integer, Integer> reads = new LinkedHashMap<>();
            java.util.Iterator<Map.Entry<String, JsonNode>> it = n.get("reads").fields();
            while (it.hasNext())
            {
                Map.Entry<String, JsonNode> e = it.next();
                int slot;
                try
                {
                    slot = Integer.parseInt(e.getKey());
                }
                catch (NumberFormatException ex)
                {
                    throw new IllegalArgumentException("invalid read slot", ex);
                }
                if (slot < 0 || !Integer.toString(slot).equals(e.getKey()) || !e.getValue().isNull() && (!e.getValue().isIntegralNumber() || !e.getValue().canConvertToInt()) || reads.containsKey(slot))
                    throw new IllegalArgumentException("invalid read");
                reads.put(slot, e.getValue().isNull() ? null : e.getValue().intValue());
            }
            RatisScalarResult result = new RatisScalarResult(n.get("known").booleanValue(), n.get("conditionMet").booleanValue(), n.get("atMicros").longValue(), n.get("decisionPosition").longValue(), reads);
            return result;
        }
        catch (java.io.IOException | RuntimeException e)
        {
            if (e instanceof IllegalArgumentException) throw (IllegalArgumentException)e;
            throw new IllegalArgumentException("malformed result", e);
        }
    }
}
