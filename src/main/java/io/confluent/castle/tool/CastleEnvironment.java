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

import java.nio.file.Paths;

public final class CastleEnvironment {
    public static final String CLUSTER_FILE_NAME = "cluster.conf";
    private final String clusterPath;
    private final String workingDirectory;

    public CastleEnvironment(String clusterPath,
                           String workingDirectory) {
        this.clusterPath = toAbsolutePath(clusterPath);
        this.workingDirectory = toAbsolutePath(workingDirectory);
    }

    private String toAbsolutePath(String path) {
        if (path == null) {
            path = "";
        }
        return Paths.get(path).toAbsolutePath().toString();
    }

    public String clusterPath() {
        return clusterPath;
    }

    public String workingDirectory() {
        return workingDirectory;
    }

    public String clusterOutputPath() {
        return Paths.get(workingDirectory, CLUSTER_FILE_NAME).toAbsolutePath().toString();
    }
};
