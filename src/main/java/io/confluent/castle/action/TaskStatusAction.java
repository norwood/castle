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
import com.fasterxml.jackson.databind.node.NullNode;
import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.command.CommandResultException;
import io.confluent.castle.role.TaskRole;
import io.confluent.castle.tool.CastleReturnCode;

import java.util.Map;

public class TaskStatusAction extends Action  {
    public final static String TYPE = "taskStatus";

    final static String DONE = "DONE";

    private final TaskRole role;

    public TaskStatusAction(String scope, TaskRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {
                new TargetId(DaemonStatusAction.TYPE)
            },
            new String[] {},
            0);
        this.role = role;
    }

    @Override
    public void call(final CastleCluster cluster, CastleNode node) throws Throwable {
        if (!node.uplink().canLogin()) {
            cluster.clusterLog().printf("%s: can't check task status because we cannot log in.%n",
                node.nodeName());
            cluster.shutdownManager().changeReturnCode(CastleReturnCode.CLUSTER_FAILED);
            return;
        }
        try {
            TrogdorClient client = new TrogdorClient(node);
            Map<String, JsonNode> tasks = client.getTasks();
            for (String taskId : role.taskSpecs().keySet()) {
                JsonNode state = tasks.get(taskId);
                if (state == null) {
                    cluster.clusterLog().printf("** %s: Unable to find task %s%n", node.nodeName(), taskId);
                    cluster.shutdownManager().changeReturnCode(CastleReturnCode.CLUSTER_FAILED);
                    continue;
                }
                JsonNode stateNode = state.get("state");
                if (stateNode == null) {
                    cluster.clusterLog().printf("** Unable to find 'state' field in JSON state data for %s%n",
                        node.nodeName(), taskId);
                    cluster.shutdownManager().changeReturnCode(CastleReturnCode.CLUSTER_FAILED);
                    continue;
                }
                JsonNode statusNode = state.get("status");
                if (statusNode == null) {
                    statusNode = NullNode.instance;
                }
                if (stateNode.textValue().equals(DONE)) {
                    JsonNode errorNode = state.get("error");
                    String error = (errorNode == null) ? "" : errorNode.textValue().trim();
                    if (!error.isEmpty()) {
                        cluster.clusterLog().printf("** %s: Task %s failed with error '%s'%n",
                            node.nodeName(), taskId, error);
                        cluster.shutdownManager().changeReturnCode(CastleReturnCode.CLUSTER_FAILED);
                    } else {
                        cluster.clusterLog().printf("** %s: Task %s succeeded with status %s%n",
                            node.nodeName(), taskId, statusNode);
                    }
                } else {
                    cluster.clusterLog().printf("** %s: Task %s is in progress with status %s%n",
                        node.nodeName(), taskId, statusNode);
                    cluster.shutdownManager().changeReturnCode(CastleReturnCode.IN_PROGRESS);
                }
            }
        } catch (CommandResultException e) {
            if (e.returnCode() == 7) {
                cluster.clusterLog().printf("** %s: Failed to connect to the Trogdor coordinator.%n",
                    node.nodeName());
                cluster.shutdownManager().changeReturnCode(CastleReturnCode.CLUSTER_FAILED);
            } else {
                throw e;
            }
        } catch (Throwable e) {
            cluster.clusterLog().info("Error getting trogdor tasks status", e);
            cluster.shutdownManager().changeReturnCode(CastleReturnCode.TOOL_FAILED);
        }
    }
};
