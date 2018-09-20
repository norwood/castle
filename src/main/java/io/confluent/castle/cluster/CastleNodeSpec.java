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

package io.confluent.castle.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CastleNodeSpec {
    private final List<String> roleNames;
    private final Map<String, JsonNode> rolePatches;

    @JsonCreator
    public CastleNodeSpec(@JsonProperty("roleNames") List<String> roleNames,
                          @JsonProperty("rolePatches") Map<String, JsonNode> rolePatches) {
        this.roleNames = Collections.unmodifiableList(
            (roleNames == null) ? new ArrayList<>() : new ArrayList<>(roleNames));
        this.rolePatches = Collections.unmodifiableMap(
            (rolePatches == null) ? Collections.emptyMap() :
                new TreeMap<>(rolePatches));
    }

    @JsonProperty
    public List<String> roleNames() {
        return roleNames;
    }

    @JsonProperty
    public Map<String, JsonNode> rolePatches() {
        return rolePatches;
    }
}
