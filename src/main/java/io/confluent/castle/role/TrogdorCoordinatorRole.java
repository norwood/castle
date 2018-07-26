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

package io.confluent.castle.role;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.castle.action.Action;
import io.confluent.castle.action.TrogdorDaemonType;
import io.confluent.castle.action.TrogdorStartAction;
import io.confluent.castle.action.TrogdorStatusAction;
import io.confluent.castle.action.TrogdorStopAction;

import java.util.ArrayList;
import java.util.Collection;

public class TrogdorCoordinatorRole implements Role {
    private final int initialDelayMs;

    @JsonCreator
    public TrogdorCoordinatorRole(@JsonProperty("initialDelayMs") int initialDelayMs) {
        this.initialDelayMs = initialDelayMs;
    }

    @JsonProperty
    public int initialDelayMs() {
        return initialDelayMs;
    }

    @Override
    public Collection<Action> createActions(String nodeName) {
        ArrayList<Action> actions = new ArrayList<>();
        actions.add(new TrogdorStartAction(TrogdorDaemonType.COORDINATOR,
            nodeName, initialDelayMs));
        actions.add(new TrogdorStatusAction(TrogdorDaemonType.COORDINATOR,
            nodeName));
        actions.add(new TrogdorStopAction(TrogdorDaemonType.COORDINATOR,
            nodeName, initialDelayMs));
        return actions;
    }
};
