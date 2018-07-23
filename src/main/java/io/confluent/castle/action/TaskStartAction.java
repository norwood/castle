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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.common.JsonTransformer;
import io.confluent.castle.common.CastleUtil;
import io.confluent.castle.common.CastleUtil.CoordinatorFunction;
import io.confluent.castle.role.TaskRole;
import io.confluent.castle.tool.CastleTool;
import org.apache.kafka.trogdor.coordinator.CoordinatorClient;
import org.apache.kafka.trogdor.rest.CreateTaskRequest;
import org.apache.kafka.trogdor.rest.Empty;
import org.apache.kafka.trogdor.rest.JsonRestServer;
import org.apache.kafka.trogdor.task.TaskSpec;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class TaskStartAction extends Action  {
    public final static String TYPE = "taskStart";

    private final Map<String, JsonNode> taskSpecs;

    public TaskStartAction(String scope, TaskRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {
                new TargetId(DaemonStartAction.TYPE),
                new TargetId(TrogdorDaemonType.COORDINATOR.startType())
            },
            new String[] {},
            role.initialDelayMs());
        this.taskSpecs = role.taskSpecs();
    }

    @Override
    public void call(final CastleCluster cluster, CastleNode node) throws Throwable {
        CastleUtil.invokeCoordinator(cluster, node, new CoordinatorFunction<Void>() {
            @Override
            public Void apply(CoordinatorClient coordinatorClient, String endpoint) throws Exception {
                for (Map.Entry<String, JsonNode> entry :
                        createTransformedTaskSpecs(cluster).entrySet()) {
                    // We manipulate the TaskSpec as a raw JsonNode.
                    // If we tried to interpret it here, we'd have to stay in sync with changes to the
                    // TaskSpec fields in Kafka.
                    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
                    node.put("id", entry.getKey());
                    node.set("spec", entry.getValue());
                    JsonRestServer.httpRequest(cluster.clusterLog(), endpoint + "/coordinator/task/create", "POST",
                        node, new TypeReference<Empty>() { }, 3);
                }
                return null;
            }
        });
    }

    /**
     * Get a list of task specs to which transforms have been applied.
     *
     * @param cluster       The castle cluster.
     * @return              The transformed list of task specs.
     */
    private Map<String, JsonNode> createTransformedTaskSpecs(CastleCluster cluster)
            throws Exception {
        Map<String, String> transforms = getTransforms(cluster);
        Map<String, JsonNode> transformedSpecs = new TreeMap<>();
        for (Map.Entry<String, JsonNode> entry : taskSpecs.entrySet()) {
            JsonNode outputNode = JsonTransformer.
                transform(entry.getValue(), new JsonTransformer.MapSubstituter(transforms));
            transformedSpecs.put(entry.getKey(), outputNode);
        }
        return transformedSpecs;
    }

    private Map<String, String> getTransforms(CastleCluster cluster) {
        HashMap<String, String> transforms = new HashMap<>();
        transforms.put("bootstrapServers", cluster.getBootstrapServers());
        return transforms;
    }
};
