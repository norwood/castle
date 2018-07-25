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

package io.confluent.castle.cloud;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * A cache which stores cloud implementations.
 */
public class CloudCache implements AutoCloseable {
    private final Map<String, AutoCloseable> map = new HashMap<>();

    private boolean closed = false;

    /**
     * Get or create a cloud with the given settings.
     *
     * @param description       The cloud description.
     * @param creator           The creator function which is called if the cloud isn't
     *                          found in the cache.
     *
     * @return                  The cloud object.
     */
    @SuppressWarnings("unchecked")
    public synchronized <T extends AutoCloseable> T getOrCreate(String description,
                                                                Function<Void, T> creator) {
        AutoCloseable value = map.get(description);
        if (value != null) {
            return (T) value;
        }
        T returnValue = creator.apply(null);
        map.put(description, returnValue);
        return returnValue;
    }

    /**
     * Shut down the cache and close all clouds.
     */
    @Override
    public synchronized void close() throws Exception {
        if (!closed) {
            closed = true;
            for (AutoCloseable closeable : map.values()) {
                closeable.close();
            }
            map.clear();
        }
    }
}
