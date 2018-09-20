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

package io.confluent.castle.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JsonMergerTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testNullMerges() throws Exception {
        assertEquals(new TextNode("abc"),
            JsonMerger.merge(null, new TextNode("abc")));
        assertEquals(new TextNode("abc"),
            JsonMerger.merge(new TextNode("abc"), null));
        assertEquals(null,
            JsonMerger.merge(null, null));
    }

    @Test
    public void testObjectMerges() throws Exception {
        ObjectNode a = new ObjectNode(JsonNodeFactory.instance);
        a.set("foo", new TextNode("abcdef"));
        ObjectNode b = new ObjectNode(JsonNodeFactory.instance);
        b.set("bar", new TextNode("ghijkl"));
        b.set("baz", new ObjectNode(JsonNodeFactory.instance));
        JsonNode c = JsonMerger.merge(a, b);
        assertEquals(new TextNode("abcdef"), c.get("foo"));
        assertEquals(new TextNode("ghijkl"), c.get("bar"));
        assertEquals(new ObjectNode(JsonNodeFactory.instance), c.get("baz"));
    }

    @Test
    public void testNullDeltas() throws Exception {
        assertEquals(null,
            JsonMerger.delta(null, null));
        assertEquals(NullNode.instance,
            JsonMerger.delta(new TextNode("abc"), null));
        assertEquals(new TextNode("abc"),
            JsonMerger.delta(null, new TextNode("abc")));
    }

    @Test
    public void testObjectDeltas() throws Exception {
        ObjectNode a = new ObjectNode(JsonNodeFactory.instance);
        a.set("foo", new TextNode("abcdef"));
        ObjectNode b = new ObjectNode(JsonNodeFactory.instance);
        b.set("foo", new TextNode("abcdef"));
        b.set("bar", new TextNode("ghijkl"));
        ObjectNode d0 = new ObjectNode(JsonNodeFactory.instance);
        d0.set("bar", new TextNode("ghijkl"));
        assertEquals(d0, JsonMerger.delta(a, b));
        ObjectNode d1 = new ObjectNode(JsonNodeFactory.instance);
        d1.set("bar", NullNode.instance);
        assertEquals(d1, JsonMerger.delta(b, a));
    }

    @Test
    public void testIdenticalObjectDelta() throws Exception {
        ObjectNode a = new ObjectNode(JsonNodeFactory.instance);
        a.set("foo", new TextNode("abcdef"));
        ObjectNode b = new ObjectNode(JsonNodeFactory.instance);
        b.set("foo", new TextNode("abcdef"));
        assertEquals(null, JsonMerger.delta(b, a));
    }
};
