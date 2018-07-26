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

package io.confluent.castle.uplink;

import io.confluent.castle.command.Command;
import io.confluent.castle.command.PortAccessor;

import java.util.concurrent.CompletableFuture;

/**
 * Represents an uplink to a node.
 *
 * The node may be a node in the cloud, or a local docker node.
 *
 * Closing the uplink does not shut down the node.
 * Calling shutdown shuts down the node.
 */
public interface Uplink extends AutoCloseable {
    /**
     * Create a new command that will run on the given node.
     */
    Command command();

    /**
     * Get the internal DNS address of this node.
     * This is the address which other nodes should use to talk to this node.
     * It's probably not accessible from the outside.
     */
    String internalDns();

    /**
     * Return true if the node was started.
     */
    boolean started();

    /**
     * Return true if we can log into the node.
     */
    boolean canLogin();


    /**
     * Get a port accessor for a port.
     */
    PortAccessor openPort(int port) throws Exception;

    /**
     * Start up the node.  Modify the role.
     */
    void startup() throws Exception;

    /**
     * Check the uplink.
     */
    void check() throws Exception;

    /**
     * Shuts down the node we're linked to.  Modify the role.
     */
    CompletableFuture<Void> shutdown() throws Exception;

    /**
     * Destroys all the nodes that we can find via this uplink, even
     * if they appear to have nothing to do with our node.
     */
    void shutdownAll() throws Exception;
}
