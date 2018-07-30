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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CastleUtilTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testFullStackTrace() throws Exception {
        RuntimeException inner = new RuntimeException("Foo, bar, and also baz");
        RuntimeException outer = new RuntimeException("quux", inner);
        RuntimeException outer2 = new RuntimeException(null, outer);
        RuntimeException suppressed1Cause = new RuntimeException("cause of suppressed1");
        RuntimeException suppressed1 = new RuntimeException("suppressed1", suppressed1Cause);
        outer2.addSuppressed(suppressed1);
        String exceptionText = "";
        try {
            throw outer2;
        } catch (Throwable e) {
            exceptionText = CastleUtil.fullStackTrace(e);
        }
        String[] exceptionLines = exceptionText.split("\\r?\\n");
        assertEquals("java.lang.RuntimeException: null", exceptionLines[0]);
        assertStartsWith(exceptionLines[1],
            "        io.confluent.castle.common.CastleUtilTest.testFullStackTrace(CastleUtilTest.java:");
        int i = 2;
        while (!exceptionLines[i].startsWith(" Suppressed: java.lang.RuntimeException: suppressed1")) {
            i++;
            assertFalse(i == exceptionLines.length);
        }
        assertStartsWith(exceptionLines[++i],
            "         io.confluent.castle.common.CastleUtilTest.testFullStackTrace(CastleUtilTest.java:");
        while (!exceptionLines[i].startsWith(" Caused by: java.lang.RuntimeException: cause of suppressed1")) {
            i++;
            assertFalse(i == exceptionLines.length);
        }
        while (!exceptionLines[i].startsWith("Caused by: java.lang.RuntimeException: quux")) {
            i++;
            assertFalse(i == exceptionLines.length);
        }
        assertStartsWith(exceptionLines[++i],
            "        io.confluent.castle.common.CastleUtilTest.testFullStackTrace(CastleUtilTest.java:");
        while (!exceptionLines[i].startsWith("Caused by: java.lang.RuntimeException: Foo, bar, and also baz")) {
            i++;
            assertFalse(i == exceptionLines.length);
        }
        assertStartsWith(exceptionLines[++i],
            "        io.confluent.castle.common.CastleUtilTest.testFullStackTrace(CastleUtilTest.java:");
    }

    private static void assertStartsWith(String actual, String expected) {
        assertEquals(expected, actual.substring(0, expected.length()));
    }
}
