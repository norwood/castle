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

package io.confluent.castle.uplink;

import io.confluent.castle.cloud.DockerCloud;
import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.command.Command;
import io.confluent.castle.command.PortAccessor;
import io.confluent.castle.command.SshCommand;
import io.confluent.castle.common.CastleLog;
import io.confluent.castle.common.CastleUtil;
import io.confluent.castle.role.DockerNodeRole;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Represents an uplink to a Docker node.
 */
public class DockerUplink implements Uplink {
    private final DockerNodeRole role;

    private final CastleCluster cluster;

    private final CastleNode node;

    private final DockerCloud cloud;

    public DockerUplink(DockerNodeRole role, CastleCluster cluster, CastleNode node,
                        DockerCloud cloud) {
        this.role = role;
        this.cluster = cluster;
        this.node = node;
        this.cloud = cloud;
    }

    /**
     * Create a new command that will run on the given node.
     */
    @Override
    public Command command() {
        return new SshCommand(node, "localhost", role.dockerUser(),
            role.sshPort(), role.sshIdentityPath());
    }

    @Override
    public String internalDns() {
        return role.containerName();
    }

    @Override
    public boolean started() {
        return !role.containerName().isEmpty();
    }

    @Override
    public boolean canLogin() {
        return role.sshPort() > 0;
    }

    @Override
    public PortAccessor openPort(int port) throws Exception {
        SshCommand command = new SshCommand(node, "localhost",
            role.dockerUser(), role.sshPort(), role.sshIdentityPath());
        return command.new Tunnel(node, role.sshPort());
    }

    @Override
    public void startup() throws Exception {
        if (!role.containerName().isEmpty()) {
            throw new RuntimeException("Can't start node " + node.nodeName() +
                " because there is already a container name set.");
        }
        if (role.sshPort() > 0) {
            throw new RuntimeException("Can't start node " + node.nodeName() +
                " because there is already an ssh port set.");
        }
        if (!role.sshIdentityPath().isEmpty()) {
            throw new RuntimeException("Can't start node " + node.nodeName() +
                " because there is already an ssh identity path set.");
        }
        String containerName = String.format("ducker%02d", node.nodeIndex());
        node.log().printf("*** Creating new docker container %s with image ID %s%n",
            containerName, role.imageId());
        String containerId = cloud.startup(cluster, node, role, containerName);
        node.log().printf("*** Created a new docker container %s%n", containerId);
        role.setContainerName(containerName);
        role.setSshPort(cloud.getDockerPort(cluster, node, containerName));
        role.setSshIdentityPath(cloud.saveSshKeyFile(cluster, node, containerName, role.dockerUser()));
    }

    @Override
    public void check() throws Exception {
        Set<String> containerNames = cloud.listContainers(node);
        node.log().printf("*** Found container name(s): %s%n", String.join(", ", containerNames));
        if (role.containerName().isEmpty()) {
            CastleLog.printToAll(String.format("*** %s: No docker container name.%n", node.nodeName()),
                node.log(), cluster.clusterLog());
        } else if (containerNames.contains(role.containerName())) {
            CastleLog.printToAll(String.format("*** %s: Found container name %s.%n",
                node.nodeName(), role.containerName()),
                node.log(), cluster.clusterLog());
        } else {
            CastleLog.printToAll(String.format("*** %s: Failed to find container name %s.%n",
                node.nodeName(), role.containerName()),
                node.log(), cluster.clusterLog());
        }
    }

    @Override
    public CompletableFuture<Void> shutdown() throws Exception {
        if (!role.containerName().isEmpty()) {
            cloud.shutdown(node, role.containerName());
            role.setContainerName("");
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        CastleUtil.completeNull(future);
        return future;
    }

    @Override
    public void shutdownAll() throws Exception {
        cloud.shutdownAll(cluster, node);
    }

    @Override
    public void close() throws Exception {
        // nothing to do
    }
}
