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
import io.confluent.castle.command.CommandResultException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Rsync the Kafka source directory to the cluster node.
 */
public final class SaveLogsAction extends Action {
    public static final String TYPE = "saveLogs";

    public SaveLogsAction(String scope) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {
                new TargetId(DaemonStopAction.TYPE, scope)
            },
            new String[] {},
            0);
    }

    @Override
    public void call(CastleCluster cluster, CastleNode node) throws Throwable {
        if (!node.uplink().started()) {
            node.log().printf("*** Skipping saveLogs, because the node is not running.%n");
            return;
        }
        Files.createDirectories(Paths.get(cluster.env().workingDirectory(),
            "logs", node.nodeName()));
        int lsStatus = node.uplink().command().args("ls", ActionPaths.LOGS_ROOT).run();
        if (lsStatus == 0) {
            node.uplink().command().
                syncFrom(ActionPaths.LOGS_ROOT + "/",
                    cluster.env().workingDirectory() + "/logs/" + node.nodeName() + "/").
                mustRun();
        } else if ((lsStatus == 1) || (lsStatus == 2)) {
            node.log().printf("*** Skipping saveLogs, because %s was not found.%n", ActionPaths.LOGS_ROOT);
        } else {
            throw new CommandResultException(
                Arrays.asList(new String[]{"ls", ActionPaths.LOGS_ROOT}), lsStatus);
        }
    }
}
