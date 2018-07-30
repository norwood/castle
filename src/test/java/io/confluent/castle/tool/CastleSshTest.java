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

package io.confluent.castle.tool;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CastleSshTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testArgParse() throws Exception {
        try {
            CastleSsh.parse(Collections.emptySet(), Collections.<String>emptyList());
            fail("expected exception");
        } catch (RuntimeException e) {
            Assert.assertThat(e.getMessage(), CoreMatchers.containsString(
                "Ssh command not found."));
        }

        CastleSsh.CastleSshArgs args = CastleSsh.parse(Collections.singleton("node1"),
                Arrays.asList(new String[] {"ssh", "node1", "ls", "/" }));
        assertEquals(Collections.singletonList("node1"), args.nodeNames());
        assertEquals(Arrays.asList(new String[] {"ls", "/"}), args.command());

        CastleSsh.CastleSshArgs args2 = CastleSsh.parse(
            new TreeSet<String>(Arrays.asList("node0", "node1", "node2")),
                Arrays.asList(new String[] {"ssh", "all", "echo"}));
        assertEquals(Arrays.asList(new String[] {"node0", "node1", "node2"}),
                args2.nodeNames());
        assertEquals(Arrays.asList(new String[] {"echo"}), args2.command());
    }
};
