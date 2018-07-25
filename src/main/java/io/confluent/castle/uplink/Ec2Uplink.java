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

import io.confluent.castle.cloud.Ec2Cloud;
import io.confluent.castle.cloud.Ec2InstanceInfo;
import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.command.Command;
import io.confluent.castle.command.PortAccessor;
import io.confluent.castle.command.SshCommand;
import io.confluent.castle.common.CastleLog;
import io.confluent.castle.role.AwsNodeRole;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Represents an uplink to a Docker node.
 */
public class Ec2Uplink implements Uplink {
    private final static int DNS_POLL_DELAY_MS = 200;

    private final static int SSH_POLL_DELAY_MS = 200;

    private final AwsNodeRole role;

    private final CastleCluster cluster;

    private final CastleNode node;

    private final Ec2Cloud cloud;

    public Ec2Uplink(AwsNodeRole role, CastleCluster cluster, CastleNode node, Ec2Cloud cloud) {
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
        return sshCommand();
    }

    private SshCommand sshCommand() {
        return new SshCommand(node,
            role.dns(),
            role.sshUser(),
            role.sshPort(),
            role.sshIdentityFile());
    }

    @Override
    public String internalDns() {
        return role.privateDns();
    }

    @Override
    public PortAccessor openPort(int port) throws Exception {
        return sshCommand().new Tunnel(node, role.sshPort());
    }

    @Override
    public void startup() throws Exception {
        node.log().printf("*** Creating new instance with instance type %s, imageId %s%n",
            role.instanceType(), role.imageId());
        String instanceId = cloud.createInstance(role.instanceType(), role.imageId()).get();
        role.setInstanceId(instanceId);

        // Wait for the DNS to be set up.
        do {
            if (DNS_POLL_DELAY_MS > 0) {
                Thread.sleep(DNS_POLL_DELAY_MS);
            }
        } while (!checkStartingInstanceDns());

        // Wait for the SSH to work
        do {
            if (SSH_POLL_DELAY_MS > 0) {
                Thread.sleep(SSH_POLL_DELAY_MS);
            }
        } while (!checkStartingInstanceSsh());
    }

    private boolean checkStartingInstanceDns() throws Exception {
        Ec2InstanceInfo info = cloud.describeInstance(role.instanceId()).get();
        if (info.privateDns().isEmpty()) {
            node.log().printf("*** Waiting for private DNS name for %s...%n", role.instanceId());
            return false;
        }
        if (info.publicDns().isEmpty()) {
            node.log().printf("*** Waiting for public DNS name for %s...%n", role.instanceId());
            return false;
        }
        node.log().printf("*** Got privateDnsName = %s, publicDnsName = %s%n",
            info.privateDns(), info.publicDns());
        role.setPrivateDns(info.privateDns());
        role.setPublicDns(info.publicDns());
        return true;
    }

    private boolean checkStartingInstanceSsh() throws Exception {
        try {
            command().args("-n", "--", "echo").mustRun();
        } catch (Exception e) {
            node.log().printf("*** Unable to ssh to %s: %s%n",
                node.nodeName(), e.getMessage());
            return false;
        }
        CastleLog.printToAll(String.format("*** Successfully created an AWS node for %s%n",
            node.nodeName()), node.log(), cluster.clusterLog());
        return true;
    }

    @Override
    public void check() throws Exception {
        Collection<Ec2InstanceInfo> infos = cloud.describeAllInstances().get();
        boolean found = false;
        for (Ec2InstanceInfo info : infos) {
            node.log().printf("** Found %s.%n", info.toString());
            if (info.instanceId().equals(role.instanceId())) {
                found = true;
            }
        }
        if (role.instanceId().isEmpty()) {
            CastleLog.printToAll(String.format("*** %s: No AWS instanceID configured.%n", node.nodeName()),
                node.log(), cluster.clusterLog());
        } else if (found) {
            CastleLog.printToAll(String.format("*** %s: Found instanceID %s.%n",
                node.nodeName(), role.instanceId()),
                node.log(), cluster.clusterLog());
        } else {
            CastleLog.printToAll(String.format("*** %s: Failed to find instanceID %s.%n",
                node.nodeName(), role.instanceId()),
                node.log(), cluster.clusterLog());
        }
    }

    @Override
    public CompletableFuture<Void> shutdown() throws Exception {
        return cloud.terminateInstance(role.instanceId());
    }

    @Override
    public void close() throws Exception {
        // nothing to do
    }
}
