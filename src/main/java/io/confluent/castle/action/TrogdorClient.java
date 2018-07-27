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

package io.confluent.castle.action;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.command.Command;
import io.confluent.castle.command.CommandResultException;
import io.confluent.castle.role.TrogdorCoordinatorRole;
import io.confluent.castle.tool.CastleTool;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A Trogdor client which uses curl to send JSON requests.
 */
public class TrogdorClient {
    private final CastleNode node;

    TrogdorClient(CastleNode node) {
        this.node = node;
    }

    private static String coordinatorUrl(String endpoint) {
        return String.format("http://localhost:%d/coordinator/%s",
            TrogdorCoordinatorRole.PORT, endpoint);
    }

    private JsonNode coordinatorCurl(String endpoint, String op, JsonNode input) throws Exception {
        List<String> cmd = new ArrayList<>(Arrays.asList("curl",
            "-H", "Content-Type:application/json",
            "-w", "_%{http_code}",
            "-X", op, coordinatorUrl(endpoint)
        ));
        if (input != null) {
            cmd.addAll(Arrays.asList("-d", "@-"));
        }
        StringBuilder stringBuilder = new StringBuilder();
        Command command = node.uplink().command().
            argList(cmd).
            captureOutput(stringBuilder).
            setCaptureStderr(false);
        if (input != null) {
            command.setStdin(CastleTool.JSON_SERDE.writeValueAsBytes(input));
        }
        command.mustRun();

        String output = stringBuilder.toString().trim();
        int atIndex = output.lastIndexOf("_");
        if (atIndex < 0) {
            throw new RuntimeException("HTTP status not found on stdout for curl command.");
        }
        String portString = output.substring(atIndex + 1);
        int httpReturnCode;
        try {
            httpReturnCode = Integer.parseInt(portString);
        } catch (NumberFormatException e) {
            throw new RuntimeException(String.format("%s: failed to parse HTTP status " +
                "code for curl command %s", node.nodeName(), Command.joinArgs(cmd)));
        }
        if (httpReturnCode != 200) {
            throw new RuntimeException(String.format("%s: got HTTP error %d when sending: %s%n",
                node.nodeName(), httpReturnCode, input));
        }
        String json = output.substring(0, atIndex);
        try {
            return CastleTool.JSON_SERDE.readTree(json);
        } catch (IOException e) {
            throw new RuntimeException(String.format("%s: JSON parse error when " +
                "handling the return value '%s' from %s", node.nodeName(), json,
                Command.joinArgs(cmd)), e);
        }
    }

    public void createTask(String taskId, JsonNode taskSpec) throws Exception {
        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put("id", taskId);
        node.set("spec", taskSpec);
        coordinatorCurl("task/create", "POST", node);
    }

    public Map<String, JsonNode> getTasks() throws Exception {
        JsonNode result = coordinatorCurl("tasks", "GET", null);
        JsonNode tasks = result.get("tasks");
        if (tasks == null) {
            return Collections.emptyMap();
        }
        TreeMap<String, JsonNode> taskMap = new TreeMap<>();
        for (Iterator<Map.Entry<String, JsonNode>> iter = tasks.fields(); iter.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = iter.next();
            taskMap.put(entry.getKey(), entry.getValue());
        }
        return taskMap;
    }

    public void stopTask(String taskId) throws Exception {
        ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
        node.put("id", taskId);
        coordinatorCurl("task/stop", "PUT", node);
    }
};
