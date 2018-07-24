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
import io.confluent.castle.tool.CastleTool;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;

/**
 * Implements a JSON configuration file which supports bash-style comment lines.
 */
public class JsonConfigFile {
    private final String cleanedConfigString;

    public JsonConfigFile(String path) throws Exception {
        if (path == null) {
            throw new RuntimeException("Invalid null configuration file path.");
        } else if (path.isEmpty()) {
            throw new RuntimeException("Invalid empty configuration file path.");
        } else if (!new File(path).exists()) {
            throw new RuntimeException("Unable to locate configuration file " + path);
        }

        StringBuilder cleaned = new StringBuilder();
        Files.lines(Paths.get(path)).forEachOrdered(new Consumer<String>() {
            @Override
            public void accept(String str) {
                boolean inString = false;
                boolean escaped = false;
                StringBuilder out = new StringBuilder();
                for (int i = 0; i < str.length(); i++) {
                    char c = str.charAt(i);
                    switch (c) {
                        case '"':
                            if (!escaped) {
                                inString = !inString;
                            }
                            out.append(c);
                            escaped = false;
                            break;
                        case '#':
                            if (inString) {
                                out.append(c);
                            } else {
                                i = str.length();
                            }
                            escaped = false;
                            break;
                        case '\\':
                            out.append(c);
                            escaped = true;
                            break;
                        default:
                            out.append(c);
                            escaped = false;
                            break;
                    }
                }
                cleaned.append(out.toString()).append(System.lineSeparator());
            }
        });
        cleanedConfigString = cleaned.toString();
    }

    public JsonNode jsonNode() throws Exception {
        return CastleTool.JSON_SERDE.readTree(cleanedConfigString);
    }
};
