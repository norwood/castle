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

import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CloudCacheTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    static class MockCloud implements AutoCloseable {
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private final String name;

        MockCloud(String name) {
            this.name = name;
        }

        @Override
        public void close() throws Exception {
            closed.set(true);
        }
    }

    public MockCloud getOrCreate(CloudCache cache, String name, AtomicInteger cloudsCreated) throws Exception {
        return cache.getOrCreate(name, new Function<Void, MockCloud>() {
            @Override
            public MockCloud apply(Void aVoid) {
                cloudsCreated.incrementAndGet();
                return new MockCloud(name);
            }
        });
    }

    @Test
    public void testCache() throws Exception {
        CloudCache cache = new CloudCache();
        final AtomicInteger cloudsCreated = new AtomicInteger(0);
        for (int i = 0; i < 10; i++) {
            getOrCreate(cache, "foo", cloudsCreated);
        }
        assertEquals(1, cloudsCreated.get());
        MockCloud cloud = getOrCreate(cache, "foo", cloudsCreated);
        assertFalse(cloud.closed.get());
        assertEquals("foo", cloud.name);
        MockCloud cloud2 = getOrCreate(cache, "bar", cloudsCreated);
        assertFalse(cloud2.closed.get());
        assertEquals(2, cloudsCreated.get());
        assertEquals("bar", cloud2.name);
        cache.close();
        assertTrue(cloud.closed.get());
        assertTrue(cloud2.closed.get());
    }
};
