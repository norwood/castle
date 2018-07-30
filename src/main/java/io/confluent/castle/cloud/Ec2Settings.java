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
 * The settings to use when creating an EC2 client.
 */
public final class Ec2Settings {
    private final String keyPair;
    private final String securityGroup;
    private final String region;

    @JsonCreator
    public Ec2Settings(@JsonProperty("keyPair") String keyPair,
                       @JsonProperty("securityGroup") String securityGroup,
                       @JsonProperty("region") String region) {
        this.keyPair = keyPair == null ? "" : keyPair;
        this.securityGroup = securityGroup == null ? "" : securityGroup;
        this.region = region == null ? "" : region;
    }

    @JsonProperty
    public String keyPair() {
        return keyPair;
    }

    @JsonProperty
    public  String securityGroup() {
        return securityGroup;
    }

    @JsonProperty
    public  String region() {
        return region;
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
