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

import io.confluent.castle.role.DockerNodeRole;
import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.tool.CastleReturnCode;
import io.confluent.castle.tool.CastleShutdownHook;
import io.confluent.castle.tool.CastleTool;

import java.io.File;

/**
 * Initiates a new Docker node.
 */
public final class DockerInitAction extends Action {
    public final static String TYPE = "dockerInit";

    private final DockerNodeRole role;

    public DockerInitAction(String scope, DockerNodeRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {},
            new String[] {},
            0);
        this.role = role;
    }

    @Override
    public void call(final CastleCluster cluster, final CastleNode node) throws Throwable {
        if (new File(cluster.env().clusterOutputPath()).exists()) {
            throw new RuntimeException("Output cluster path " + cluster.env().clusterOutputPath() +
                " already exists.");
        }
        if (node.uplink().started()) {
            node.log().printf("*** Skipping %s, because the node is already running.%n", TYPE);
            return;
        }

        // Make sure that we don't leak a Docker instance if we shut down unexpectedly.
        cluster.shutdownManager().addHookIfMissing(new DestroyDockerInstancesShutdownHook(cluster));

        // Start up the Docker instance.
        node.uplink().startup();

        // Write out the new cluster file.
        CastleTool.JSON_SERDE.writeValue(new File(cluster.env().clusterOutputPath()), cluster.toSpec());
    }

    /**
     * Destroys Docker instances on shutdown.
     */
    public static final class DestroyDockerInstancesShutdownHook extends CastleShutdownHook {
        private final CastleCluster cluster;

        DestroyDockerInstancesShutdownHook(CastleCluster cluster) {
            super("DestroyDockerInstancesShutdownHook");
            this.cluster = cluster;
        }

        @Override
        public void run(CastleReturnCode returnCode) throws Throwable {
            if (returnCode == CastleReturnCode.SUCCESS) {
                String path = cluster.env().clusterOutputPath();
                try {
                    CastleTool.JSON_SERDE.writeValue(new File(path), cluster.toSpec());
                    cluster.clusterLog().printf("*** Wrote new cluster file to %s%n", path);
                } catch (Throwable e) {
                    cluster.clusterLog().printf("*** Failed to write cluster file to %s%n", path, e);
                    terminateInstances();
                    throw e;
                }
            } else {
                terminateInstances();
            }
        }

        private synchronized void terminateInstances() throws Throwable {
            for (CastleNode node : cluster.nodes().values()) {
                DockerNodeRole dockerRole = node.getRole(DockerNodeRole.class);
                if ((dockerRole != null) && (!dockerRole.containerName().isEmpty())) {
                    node.uplink().shutdown().get();
                }
            }
            cluster.clusterLog().info("*** Terminated docker nodes.");
        }
    }
}
