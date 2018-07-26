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

import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.common.CastleLog;
import io.confluent.castle.role.DockerNodeRole;

/**
 * Checks the status of a node's Docker instance.
 */
public final class DockerCheckAction extends Action {
    public final static String TYPE = "dockerCheck";

    private final DockerNodeRole role;

    public DockerCheckAction(String scope, DockerNodeRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {},
            new String[] {},
            0);
        this.role = role;
    }

    @Override
    public void call(CastleCluster cluster, CastleNode node) throws Throwable {
        String[] ps = new String[] {
            "docker", "ps", "-a", "--no-trunc", "--filter",
            "'name=^/" + role.containerName() + "$'",
            "--format", "'{{.Names}}'"
            };
        if (node.uplink().command().args(ps).run() != 0) {
            CastleLog.printToAll(String.format("*** %s: Failed to run docker ps for %s%n",
                node.nodeName(), role.containerName()),
                node.log(), cluster.clusterLog());
        } else {
            CastleLog.printToAll(String.format("*** %s: %s is running.%n",
                node.nodeName(), role.containerName()),
                node.log(), cluster.clusterLog());
        }
    }
}
