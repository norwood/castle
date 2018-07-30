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

package io.confluent.castle.tool;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.confluent.castle.common.CastleUtil;
import io.confluent.castle.common.JsonConfigFile;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import io.confluent.castle.common.JsonTransformer;
import io.confluent.castle.action.ActionRegistry;
import io.confluent.castle.action.ActionScheduler;
import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleClusterSpec;
import io.confluent.castle.common.CastleLog;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 * The castle command.
 */
public final class CastleTool {
    public static final ObjectMapper JSON_SERDE;

    static {
        JSON_SERDE = new ObjectMapper();
        JSON_SERDE.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JSON_SERDE.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        JSON_SERDE.enable(SerializationFeature.INDENT_OUTPUT);
        JSON_SERDE.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    private static final String CASTLE_CLUSTER_INPUT_PATH = "CASTLE_CLUSTER_INPUT_PATH";
    private static final String CASTLE_TARGETS = "CASTLE_TARGETS";
    private static final String CASTLE_WORKING_DIRECTORY = "CASTLE_WORKING_DIRECTORY";
    private static final String CASTLE_VERBOSE = "CASTLE_VERBOSE";
    private static final boolean CASTLE_VERBOSE_DEFAULT = false;
    private static final String CASTLE_PREFIX = "CASTLE_";

    private static final String CASTLE_DESCRIPTION = String.format(
        "The Kafka castle cluster tool.%n" +
        "%n" +
        "Valid targets:%n" +
        "up:                Bring up all nodes.%n" +
        "  init:            Allocate nodes.%n" +
        "  setup:           Set up all nodes.%n" +
        "  start:           Start the system.%n" +
        "%n" +
        "status:            Get the system status.%n" +
        "  daemonStatus:    Get the status of system daemons.%n" +
        "  taskStatus:      Get the status of trogdor tasks.%n" +
        "%n" +
        "down:              Bring down all nodes.%n" +
        "  saveLogs:        Save the system logs.%n" +
        "  stop:            Stop the system.%n" +
        "  destroy:         Deallocate nodes.%n" +
        "%n" +
        "destroyNodes:    Destroy all nodes.%n" +
        "%n" +
        "ssh [nodes] [cmd]: Ssh to the given node(s)%n" +
        "%n");

    private static String getEnv(String name, String defaultValue) {
        String val = System.getenv(name);
        if (val != null) {
            return val;
        }
        return defaultValue;
    }

    private static boolean getEnvBoolean(String name, boolean defaultValue) {
        String val = System.getenv(name);
        if (val != null) {
            try {
                return Boolean.parseBoolean(val);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Unable to parse value " + name +
                    " given for " + name, e);
            }
        }
        return defaultValue;
    }

    public static class CastleSubstituter implements JsonTransformer.Substituter {
        @Override
        public String substitute(String key) {
            if (!key.startsWith(CASTLE_PREFIX)) {
                return null;
            }
            String value = System.getenv(key);
            if (value == null) {
                throw new RuntimeException("You must set the environment variable " + key +
                    " to use this configuration file.");
            }
            return value;
        }
    }

    private static CastleClusterSpec readClusterSpec(String clusterInputPath) throws Throwable {
        JsonNode confNode = new JsonConfigFile(clusterInputPath).jsonNode();
        JsonNode transofmredConfNode = JsonTransformer.transform(confNode, new CastleSubstituter());
        return CastleTool.JSON_SERDE.treeToValue(transofmredConfNode, CastleClusterSpec.class);
    }

    public static void main(String[] args) throws Throwable {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("castle-tool")
            .defaultHelp(true)
            .description(CASTLE_DESCRIPTION);

        parser.addArgument("-c", "--cluster")
            .action(store())
            .type(String.class)
            .dest(CASTLE_CLUSTER_INPUT_PATH)
            .metavar(CASTLE_CLUSTER_INPUT_PATH)
            .setDefault("")
            .help("The cluster file to use.");
        parser.addArgument("-w", "--working-directory")
            .action(store())
            .type(String.class)
            .required(true)
            .dest(CASTLE_WORKING_DIRECTORY)
            .metavar(CASTLE_WORKING_DIRECTORY)
            .help("The output path to store logs, cluster files, and other outputs in.");
        parser.addArgument("-v")
            .action(storeTrue())
            .type(Boolean.class)
            .required(false)
            .dest(CASTLE_VERBOSE)
            .metavar(CASTLE_VERBOSE)
            .setDefault(getEnvBoolean(CASTLE_VERBOSE, CASTLE_VERBOSE_DEFAULT))
            .help("Enable verbose logging.");
        parser.addArgument("target")
            .nargs("*")
            .action(store())
            .required(false)
            .dest(CASTLE_TARGETS)
            .metavar(CASTLE_TARGETS)
            .help("The target action(s) to run.");

        final Namespace res = parser.parseArgsOrFail(args);
        final CastleLog clusterLog = CastleLog.
            fromStdout("cluster", res.getBoolean(CASTLE_VERBOSE));
        CastleShutdownManager shutdownManager = new CastleShutdownManager(clusterLog);
        shutdownManager.install();
        try {
            List<String> targets = res.<String>getList(CASTLE_TARGETS);
            if (targets.isEmpty()) {
                parser.printHelp();
                System.exit(0);
            }
            String workingDirectory = res.getString(CASTLE_WORKING_DIRECTORY);
            String clusterPath = res.getString(CASTLE_CLUSTER_INPUT_PATH);
            Path defaultClusterConfPath = Paths.get(workingDirectory,
                CastleEnvironment.CLUSTER_FILE_NAME);
            if (defaultClusterConfPath.toFile().exists()) {
                if (!clusterPath.isEmpty()) {
                    throw new RuntimeException("A cluster file named " +
                        defaultClusterConfPath.toString() + " exists in your working directory.  " +
                        "You must not specify a cluster path with -c or --cluster.");
                }
                clusterPath = defaultClusterConfPath.toAbsolutePath().toString();
            } else if (clusterPath.isEmpty()) {
                throw new RuntimeException("You must specify a cluster with with -c or --cluster.");
            } else if (!new File(clusterPath).exists()) {
                throw new RuntimeException("The specified cluster path " + clusterPath +
                    " does not exist.");
            }
            Files.createDirectories(Paths.get(workingDirectory));
            CastleEnvironment env = new CastleEnvironment(workingDirectory);
            CastleClusterSpec clusterSpec = readClusterSpec(clusterPath);

            try (CastleCluster cluster = new CastleCluster(env, clusterLog,
                    shutdownManager, clusterSpec)) {
                if (targets.contains(CastleSsh.COMMAND)) {
                    CastleSsh.run(cluster, targets);
                } else {
                    try (ActionScheduler scheduler = cluster.createScheduler(targets,
                        ActionRegistry.INSTANCE.actions(cluster.nodes().keySet()))) {
                        scheduler.await(cluster.conf().globalTimeout(), TimeUnit.SECONDS);
                    }
                }
            }
            shutdownManager.shutdownNormally();
            System.exit(shutdownManager.returnCode().code());
        } catch (Throwable exception) {
            System.out.printf("Exiting with exception: %s%n", CastleUtil.fullStackTrace(exception));
            System.exit(1);
        }
    }
};
