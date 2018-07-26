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
import io.confluent.castle.common.CastleUtil;
import io.confluent.castle.role.CollectdRole;

import static io.confluent.castle.action.ActionPaths.COLLECTD;

/**
 * Stop the collectd monitoring system.
 */
public final class CollectdStopAction extends Action {
    public final static String TYPE = "collectdStop";
    private static final int COLLECTD_STOP_DELAY_MS = 2000;

    public CollectdStopAction(String scope, CollectdRole role) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {},
            new String[] {},
            role.initialDelayMs());
    }

    @Override
    public void call(CastleCluster cluster, CastleNode node) throws Throwable {
        if (!node.uplink().canLogin()) {
            node.log().printf("*** Skipping %s, because the node is not accessible.%n", TYPE);
            return;
        }
        // Flush the collectd cache
        CastleUtil.killProcess(cluster, node, COLLECTD, "SIGUSR1");
        Thread.sleep(COLLECTD_STOP_DELAY_MS);
        CastleUtil.killProcess(cluster, node, COLLECTD);
    }
}
