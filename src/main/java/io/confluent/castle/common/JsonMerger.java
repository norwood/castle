/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.castle.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.Map;

public class JsonMerger {
    /**
     * Apply a delta to a JsonNode.
     *
     * @param input         The input JsonNode.  Will not be modified.
     * @param input         The delta to apply to the input JsonNode.  Will not be modified.
     * @return              A deep copy of the merged node.
     */
    public static JsonNode merge(JsonNode input, JsonNode delta) {
        if (input == null) {
            if (delta == null) {
                return null;
            } else {
                return delta.deepCopy();
            }
        } else if (delta == null) {
            return input.deepCopy();
        }
        if ((input.getNodeType() != JsonNodeType.OBJECT) ||
                (delta.getNodeType() != JsonNodeType.OBJECT)) {
            // In the case where either node is not an object, just take the whole delta.
            // TODO: merge arrays?
            return delta.deepCopy();
        }
        ObjectNode merged = new ObjectNode(JsonNodeFactory.instance);
        for (Iterator<Map.Entry<String, JsonNode>> iter = input.fields(); iter.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = iter.next();
            JsonNode patchNode = delta.get(entry.getKey());
            merged.set(entry.getKey(), merge(entry.getValue(), patchNode));
        }
        for (Iterator<Map.Entry<String, JsonNode>> iter = delta.fields(); iter.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = iter.next();
            if (!merged.has(entry.getKey())) {
                merged.set(entry.getKey(), entry.getValue().deepCopy());
            }
        }
        return merged;
    }

    /**
     * Generate a delta which can transform JsonNode A into JsonNode B.
     *
     * @param a     The first object.  Will not be modified.
     * @param b     The second object.  Will not be modified.
     * @return      null if there is no delta;
     *              NullNode.instance if the entry in A should be erased by the delta,
     *              The delta node to use otherwise.
     */
    public static JsonNode delta(JsonNode a, JsonNode b) {
        if (b == null) {
            if (a == null) {
                return null;
            } else {
                return NullNode.instance;
            }
        } else if (a == null) {
            return b.deepCopy();
        }
        if ((a.getNodeType() != JsonNodeType.OBJECT) ||
                (b.getNodeType() != JsonNodeType.OBJECT)) {
            if (a.equals(b)) {
                return null;
            } else {
                return b.deepCopy();
            }
        }
        ObjectNode delta = new ObjectNode(JsonNodeFactory.instance);
        for (Iterator<Map.Entry<String, JsonNode>> iter = a.fields(); iter.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = iter.next();
            JsonNode aNode = entry.getValue();
            JsonNode bNode = b.get(entry.getKey());
            JsonNode dNode = delta(aNode, bNode);
            if (dNode != null) {
                delta.set(entry.getKey(), dNode);
            }
        }
        for (Iterator<Map.Entry<String, JsonNode>> iter = b.fields(); iter.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = iter.next();
            JsonNode aNode = a.get(entry.getKey());
            if (aNode == null) {
                delta.set(entry.getKey(), entry.getValue().deepCopy());
            }
        }
        return delta;
    }
};
