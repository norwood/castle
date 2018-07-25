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

package io.confluent.castle.cloud;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.castle.tool.CastleTool;

/**
 * Information about an EC2 node instance.
 */
public final class Ec2InstanceInfo {
    private final String instanceId;
    private final String privateDns;
    private final String publicDns;
    private final String state;

    @JsonCreator
    public Ec2InstanceInfo(@JsonProperty("instanceId") String instanceId,
                           @JsonProperty("privateDns") String privateDns,
                           @JsonProperty("publicDns") String publicDns,
                           @JsonProperty("state") String state) {
        this.instanceId = instanceId == null ? "" : instanceId;
        this.privateDns = privateDns == null ? "" : privateDns;
        this.publicDns = publicDns == null ? "" : publicDns;
        this.state = state == null ? "" : state;
    }

    @JsonProperty
    public String instanceId() {
        return instanceId;
    }

    @JsonProperty
    public String privateDns() {
        return privateDns;
    }

    @JsonProperty
    public String publicDns() {
        return publicDns;
    }

    @JsonProperty
    public String state() {
        return state;
    }

    @Override
    public String toString() {
        try {
            return CastleTool.JSON_SERDE.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
