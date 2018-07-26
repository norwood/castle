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

/**
 * Destroys all nodes.
 */
public final class DestroyNodesAction extends Action {
    public final static String TYPE = "destroyNodes";

    public DestroyNodesAction(String scope) {
        super(new ActionId(TYPE, scope),
            new TargetId[] {},
            new String[] {},
            0);
    }

    public void call(CastleCluster cluster, CastleNode node) throws Throwable {
        node.log().printf("*** %s: Destroying all the nodes for uplink %s.%n",
            node.nodeName(), node.uplink());
        node.uplink().shutdownAll();
    }
}
