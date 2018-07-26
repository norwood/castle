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
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.command.NodeShellRunner;
import io.confluent.castle.common.CastleUtil;
import io.confluent.castle.role.DockerNodeRole;

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
    private final static String NETWORK = "ducknet";

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

    public synchronized Future<Void> getNetworkCheckFuture(CastleNode node) throws Exception {
        if (networkCheckFuture != null) {
            return networkCheckFuture;
        }
        networkCheckFuture = executorService.submit(new NetworkCheck(node));
        return networkCheckFuture;
    }

    public String startup(CastleCluster cluster, CastleNode node,
                          DockerNodeRole role, String containerName) throws Exception {
        getNetworkCheckFuture(node).get();
        List<String> run = new ArrayList<>();
        run.addAll(Arrays.asList(new String[] {"docker", "run", "-d",
            "--privileged", "--memory=3G", "--memory-swappiness=1",
            "--name", containerName, "-h", containerName,
            "--network=" + NETWORK}));
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
        new NodeShellRunner(node, run).
            setCaptureOutput(stringBuilder).
            setCaptureStderr(false).
            mustRun();
        return stringBuilder.toString().trim();
    }

    /**
     * Get the port which is used by the docker container.
     */
    public int getDockerPort(CastleCluster cluster, CastleNode node,
                             String containerName) throws Exception {
        getNetworkCheckFuture(node).get();
        List<String> docker = new ArrayList<>();
        docker.addAll(Arrays.asList(new String[]{"docker", "port"}));
        docker.add(containerName);
        StringBuilder stringBuilder = new StringBuilder();
        new NodeShellRunner(node, docker).
            setCaptureOutput(stringBuilder).
            setCaptureStderr(false).
            mustRun();
        String text = stringBuilder.toString().trim();
        // The format of the "docker port" output is something like:
        // 22/tcp -> 0.0.0.0:32768
        // We assume that there is only one port.
        int index = text.lastIndexOf(":");
        if (index < 0) {
            throw new RuntimeException("Expected to find a colon in the " +
                "'docker port' output for " + node.nodeName());
        }
        String portString = text.substring(index);
        int port = Integer.parseInt(portString);
        return port;
    }

    /**
     * Save the ssh private key from the docker container.
     * This will let us ssh into the container.
     */
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
        // Run this command through the shell, so that "~" will be expanded to
        // the current home directory.
        run.addAll(Arrays.asList(new String[] {
            containerName, "bash", "-c", "cat ~/.ssh/id_rsa"
        }));
        StringBuilder stringBuilder = new StringBuilder();
        if (new NodeShellRunner(node, run).
            setCaptureOutput(stringBuilder).
                setCaptureStderr(false).
                setLogOutputOnSuccess(false).
                run() != 0) {
            throw new RuntimeException("Failed to get the ssh key file for " + containerName);
        }
        Path sshKeyPath = Paths.get(cluster.env().workingDirectory(),
            containerName + ".id_rsa").toAbsolutePath();
        try (BufferedWriter writer = Files.newBufferedWriter(sshKeyPath)) {
            writer.write(stringBuilder.toString());
        }
        // We have to set the permissions to 0600, or else ssh refuses to use it.
        List<String> chmod = Arrays.asList(new String[] {
            "chmod", "0600", sshKeyPath.toString()
        });
        new NodeShellRunner(node, chmod).
            setCaptureOutput(stringBuilder).
            mustRun();
        return sshKeyPath.toString();
    }

    /**
     * List the containers which are running with our docker network.
     */
    public String[] listContainers(CastleNode node) throws Exception {
        getNetworkCheckFuture(node).get();
        StringBuilder stringBuilder = new StringBuilder();
        new NodeShellRunner(node,
            Arrays.asList(new String[] { "docker", "ps",
                "-f=network=" + NETWORK, "-q", "--format", "{{.Names}}"})).
            setCaptureOutput(stringBuilder).
            setCaptureStderr(false).
            mustRun();
        String[] lines = stringBuilder.toString().trim().split(System.lineSeparator());
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
            List<String> inspect = Arrays.asList(new String[] {
                "docker", "network", "inspect", NETWORK});
            if (new NodeShellRunner(node, inspect).
                    setLogOutputOnSuccess(false).
                    run() == 0) {
                node.log().printf("** %s is running.%n", NETWORK);
                return null;
            }
            node.log().printf("** starting %s.%n", NETWORK);
            List<String> create = Arrays.asList(new String[] {
                "docker", "network", "create", NETWORK});
            if (new NodeShellRunner(node, create).run() == 0) {
                node.log().printf("** successfully created %s.%n", NETWORK);
                return null;
            }
            throw new RuntimeException("Failed to create " + NETWORK + ".");
        }
    }

    public void shutdown(CastleNode node, String containerName) throws Exception {
        List<String> kill = Arrays.asList(new String[] {"docker", "kill", containerName});
        new NodeShellRunner(node, kill).run();
        List<String> rm = Arrays.asList(new String[] {"docker", "rm", containerName});
        new NodeShellRunner(node, rm).run();
    }
}
