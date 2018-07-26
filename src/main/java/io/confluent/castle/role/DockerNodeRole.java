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

package io.confluent.castle.role;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.castle.action.Action;
import io.confluent.castle.action.DockerDestroyAction;
import io.confluent.castle.action.DockerInitAction;
import io.confluent.castle.action.UplinkCheckAction;
import io.confluent.castle.cloud.DockerCloud;
import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.uplink.DockerUplink;
import io.confluent.castle.uplink.Uplink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

public class DockerNodeRole implements Role, UplinkRole {
    /**
     * The Docker image ID to use.
     */
    private final String imageId;

    /**
     * The docker user.
     * If this is not set, the default will be used.
     */
    private final String dockerUser;

    /**
     * The ssh port to use.
     */
    private int sshPort;

    /**
     * The docker container name.
     * If this is not set, an empty string will be used.
     */
    private String containerName;

    /**
     * The ssh identity file path.
     * If this is not set, an empty string will be used.
     */
    private String sshIdentityPath;

    @JsonCreator
    public DockerNodeRole(@JsonProperty("imageId") String imageId,
                          @JsonProperty("dockerUser") String dockerUser,
                          @JsonProperty("sshPort") int sshPort,
                          @JsonProperty("containerName") String containerName,
                          @JsonProperty("sshIdentityPath") String sshIdentityPath) {
        this.imageId = imageId == null ? "" : imageId;
        this.dockerUser = dockerUser == null ? "" : dockerUser;
        this.sshPort = sshPort < 0 ? 0 : sshPort;
        this.containerName = containerName == null ? "" : containerName;
        this.sshIdentityPath = sshIdentityPath == null ? "" : sshIdentityPath;
    }

    @JsonProperty
    public String imageId() {
        return imageId;
    }

    @JsonProperty
    public String dockerUser() {
        return dockerUser;
    }

    @JsonProperty
    public synchronized int sshPort() {
        return sshPort;
    }

    public synchronized void setSshPort(int sshPort) {
        this.sshPort = sshPort;
    }

    @JsonProperty
    public synchronized String containerName() {
        return containerName;
    }

    public synchronized void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    @JsonProperty
    public synchronized String sshIdentityPath() {
        return sshIdentityPath;
    }

    public synchronized void setSshIdentityPath(String sshIdentityPath) {
        this.sshIdentityPath = sshIdentityPath;
    }

    @Override
    public Collection<Action> createActions(String nodeName) {
        ArrayList<Action> actions = new ArrayList<>();
        actions.add(new DockerDestroyAction(nodeName, this));
        actions.add(new DockerInitAction(nodeName, this));
        actions.add(new UplinkCheckAction(nodeName));
        return actions;
    }

    @Override
    public Uplink createUplink(CastleCluster cluster, CastleNode node) {
        DockerCloud cloud = cluster.cloudCache().getOrCreate("DockerCloud{}",
            new Function<Void, DockerCloud>() {
                @Override
                public DockerCloud apply(Void aVoid) {
                    return new DockerCloud();
                }
            });
        return new DockerUplink(this, cluster, node, cloud);
    }
};
