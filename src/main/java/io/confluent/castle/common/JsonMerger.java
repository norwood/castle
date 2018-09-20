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
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.Map;

public class JsonMerger {
    /**
     * Apply a patch to a JsonNode.
     *
     * @param input         The input JsonNode.  Will not be modified.
     * @param input         The patch to apply to the input JsonNode.  Will not be modified.
     * @return              A deep copy of the merged node.
     */
    public static JsonNode merge(JsonNode input, JsonNode patch) {
        if (input == null) {
            if (patch == null) {
                return null;
            } else {
                return patch.deepCopy();
            }
        } else if (patch == null) {
            return input.deepCopy();
        }
        if ((input.getNodeType() != JsonNodeType.OBJECT) ||
                (patch.getNodeType() != JsonNodeType.OBJECT)) {
            // In the case where either node is not an object, just take the patch.
            // TODO: merge arrays?
            return patch.deepCopy();
        }
        ObjectNode merged = new ObjectNode(JsonNodeFactory.instance);
        for (Iterator<Map.Entry<String, JsonNode>> iter = input.fields(); iter.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = iter.next();
            JsonNode patchNode = patch.get(entry.getKey());
            merged.set(entry.getKey(), merge(entry.getValue(), patchNode));
        }
        for (Iterator<Map.Entry<String, JsonNode>> iter = patch.fields(); iter.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = iter.next();
            if (!merged.has(entry.getKey())) {
                merged.set(entry.getKey(), entry.getValue().deepCopy());
            }
        }
        return merged;
    }
};
