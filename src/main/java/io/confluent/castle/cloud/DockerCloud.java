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

import io.confluent.castle.action.ActionPaths;
import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleClusterConf;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.command.NodeShellRunner;
import io.confluent.castle.common.CastleUtil;
import io.confluent.castle.role.DockerNodeRole;
import io.confluent.castle.tool.CastleEnvironment;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class DockerCloud implements AutoCloseable {
    private final ExecutorService executorService;

    private Future<Void> networkCheckFuture;

    public DockerCloud() {
        this.executorService = Executors.newSingleThreadScheduledExecutor(
            CastleUtil.createThreadFactory("DockerCloudThread", false));
        this.networkCheckFuture = null;
    }

    @Override
    public void close() throws Exception {
        // nothing to do
    }

    @Override
    public String toString() {
        return "DockerCloud{}";
    }

    public String startup(CastleCluster cluster, CastleNode node,
                          DockerNodeRole role, String containerName) throws Exception {
        getNetworkCheckFuture(node).get();
        List<String> run = new ArrayList<>();
        run.addAll(Arrays.asList(new String[] {"docker", "run", "-d",
            "--privileged", "--memory=3G", "--memory-swappiness=1",
            "--name", containerName, "-h", containerName,
            "--network=ducknet"}));
        if (role.sshPort() > 0) {
            run.add("-p");
            run.add(String.format("%d:22", role.sshPort()));
        }
        if (!cluster.conf().castlePath().isEmpty()) {
            run.add("-v");
            run.add(String.format("%s:%s", cluster.conf().castlePath(),
                ActionPaths.CASTLE_SRC));
        }
        if (!cluster.conf().kafkaPath().isEmpty()) {
            run.add("-v");
            run.add(String.format("%s:%s", cluster.conf().kafkaPath(),
                ActionPaths.KAFKA_SRC));
        }
        if (!cluster.env().clusterOutputPath().isEmpty()) {
            String logDir = Paths.get(cluster.env().workingDirectory(),
                "logs",
                node.nodeName()).toAbsolutePath().
                toString();
            run.add("-v");
            run.add(String.format("%s:%s", logDir, ActionPaths.LOGS_ROOT));
        }
        run.add("--");
        run.add(role.imageId());
        StringBuilder stringBuilder = new StringBuilder();
        new NodeShellRunner(node, run, stringBuilder).
            setRedirectErrorStream(false).
            mustRun();
        return stringBuilder.toString().trim();
    }

    public synchronized Future<Void> getNetworkCheckFuture(CastleNode node) throws Exception {
        if (networkCheckFuture != null) {
            return networkCheckFuture;
        }
        networkCheckFuture = executorService.submit(new NetworkCheck(node));
        return networkCheckFuture;
    }

    public String saveSshKeyFile(CastleCluster cluster, CastleNode node,
                                 String containerName, String dockerUser) throws Exception {
        getNetworkCheckFuture(node).get();
        List<String> run = new ArrayList<>();
        run.add("docker");
        run.add("exec");
        if (!dockerUser.isEmpty()) {
            run.add("--user");
            run.add(dockerUser);
        }
        run.addAll(Arrays.asList(new String[] {
            containerName, "bash", "-c", "cat ~/.ssh/id_rsa"
        }));
        StringBuilder stringBuilder = new StringBuilder();
        if (new NodeShellRunner(node, run, stringBuilder).
                setRedirectErrorStream(false).
                run() != 0) {
            throw new RuntimeException("Failed to get the ssh key file for " + containerName);
        }
        Path sshKeyPath = Paths.get(cluster.env().workingDirectory(),
            containerName + ".id_rsa").toAbsolutePath();
        try (BufferedWriter writer = Files.newBufferedWriter(sshKeyPath)) {
            writer.write(stringBuilder.toString());
        }
        List<String> chmod = Arrays.asList(new String[] {
            "chmod", "0600", sshKeyPath.toString()
        });
        new NodeShellRunner(node, chmod, stringBuilder).mustRun();
        return sshKeyPath.toString();
    }

    public String[] listContainers(CastleNode node) throws Exception {
        getNetworkCheckFuture(node).get();
        StringBuilder stringBuilder = new StringBuilder();
        new NodeShellRunner(node,
            Arrays.asList(new String[] { "docker", "ps",
                "-f=network=ducknet", "-q", "--format", "'{{.Name}}'"}),
            stringBuilder).
            setRedirectErrorStream(false).
            mustRun();
        stringBuilder.toString();
        String[] lines = stringBuilder.toString().split(System.lineSeparator());
        Arrays.sort(lines);
        for (int i = 0; i < lines.length; i++) {
            lines[i] = lines[i].trim();
        }
        return lines;
    }

    private static class NetworkCheck implements Callable<Void> {
        private final CastleNode node;

        NetworkCheck(CastleNode node) {
            this.node = node;
        }

        @Override
        public Void call() throws Exception {
            List<String> inspect = Arrays.asList(new String[] {"docker", "network", "inspect", "ducknet"});
            if (new NodeShellRunner(node, inspect, null).run() == 0) {
                node.log().printf("** ducknet is running.%n");
                return null;
            }
            node.log().printf("** starting ducknet.%n");
            List<String> create = Arrays.asList(new String[] {"docker", "network", "create", "ducknet"});
            if (new NodeShellRunner(node, create, null).run() == 0) {
                node.log().printf("** successfully created ducknet.%n");
                return null;
            }
            throw new RuntimeException("Failed to create ducknet.");
        }
    }

    public void shutdown(CastleNode node, String containerName) throws Exception {
        List<String> kill = Arrays.asList(new String[] {"docker", "kill", containerName});
        new NodeShellRunner(node, kill, null).run();
        List<String> rm = Arrays.asList(new String[] {"docker", "rm", containerName});
        new NodeShellRunner(node, rm, null).run();
    }
}
