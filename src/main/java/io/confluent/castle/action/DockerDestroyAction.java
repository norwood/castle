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
import io.confluent.castle.role.DockerNodeRole;
import io.confluent.castle.tool.CastleWriteClusterFileHook;

/**
 * Destroys a docker node.
 */
public final class DockerDestroyAction extends Action {
    public final static String TYPE = "dockerDestroy";

    private final DockerNodeRole role;

    public DockerDestroyAction(String scope, DockerNodeRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {},
            new String[] {},
            role.initialDelayMs());
        this.role = role;
    }

    public void call(CastleCluster cluster, CastleNode node) throws Throwable {
        if (!node.uplink().started()) {
            node.log().printf("*** Skipping %s, because the node is not running.%n", TYPE);
            return;
        }
        role.setContainerName("");
        role.setSshIdentityPath("");
        cluster.shutdownManager().addHookIfMissing(new CastleWriteClusterFileHook(cluster));
    }
}
