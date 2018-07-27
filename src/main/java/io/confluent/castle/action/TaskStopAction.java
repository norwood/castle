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
import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.common.CastleUtil;
import io.confluent.castle.role.TaskRole;
import io.confluent.castle.tool.CastleReturnCode;

import java.util.Map;
import java.util.concurrent.Callable;

public class TaskStopAction extends Action  {
    public final static String TYPE = "tasksStop";

    private final TaskRole role;

    public TaskStopAction(String nodeName, TaskRole role) {
        super(new ActionId(TYPE, nodeName),
            new TargetId[] {},
            new String[] {},
            role.initialDelayMs());
        this.role = role;
    }

    @Override
    public void call(final CastleCluster cluster, final CastleNode node) throws Throwable {
        if (!node.uplink().started()) {
            node.log().printf("*** Skipping %s, because the node is not running.%n", TYPE);
            return;
        }
        if (CastleUtil.getJavaProcessStatus(cluster, node,
                TrogdorDaemonType.COORDINATOR.className()) != CastleReturnCode.SUCCESS) {
            node.log().printf("*** Ignoring TaskStopAction because the Trogdor " +
                "coordinator process does not appear to be running.%n");
            return;
        }
        try {
            TrogdorClient client = new TrogdorClient(node);
            for (String taskId : role.taskSpecs().keySet()) {
                client.stopTask(taskId);
            }
            // Wait for all the tasks to be stopped.
            CastleUtil.waitFor(5, 30000, new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    Map<String, JsonNode> tasks = client.getTasks();
                    for (String taskId : role.taskSpecs().keySet()) {
                        JsonNode state = tasks.get(taskId);
                        if (state != null) {
                            JsonNode stateNode = state.get("state");
                            if (stateNode != null) {
                                if (!stateNode.textValue().equals(TaskStatusAction.DONE)) {
                                    return false;
                                }
                            }
                        }
                    }
                    return true;
                }
            });
        } catch (Throwable e) {
            cluster.clusterLog().info("Error stopping trogdor tasks", e);
            cluster.shutdownManager().changeReturnCode(CastleReturnCode.TOOL_FAILED);
        }
    }
};
