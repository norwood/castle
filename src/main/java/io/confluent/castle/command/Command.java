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

package io.confluent.castle.command;

import java.util.List;

/**
 * A command which the Castle tool needs to run.
 */
public interface Command {
    enum Operation {
        SSH,
        RSYNC_TO,
        RSYNC_FROM;
    }

    /**
     * Set the command arguments.
     *
     * This option is mutually exclusive with syncTo and syncFrom.
     *
     * @param args                  The arguments to use.
     */
    Command args(String... args);

    /**
     * Set the command arguments.
     *
     * This option is mutually exclusive with syncTo and syncFrom.
     *
     * @param args                  The arguments to use.
     */
    Command argList(List<String> args);

    /**
     * Copy files to the remote node.
     *
     * This option is mutually exclusive with args and syncFrom.
     *
     * @param local                 The local path to copy from.
     * @param remote                The remote path to copy to.
     */
    Command syncTo(String local, String remote);

    /**
     * Copy files from the remote node.
     *
     * This option is mutually exclusive with args and syncTo.
     *
     * @param remote                The remote path to copy from.
     * @param local                 The local path to copy to.
     */
    Command syncFrom(String remote, String local);

    /**
     * Capture the output to the given StringBuilder.
     *
     * @param stringBuilder         The stringBuilder which the output will be
     *                              captured to.  By default, the output is not
     *                              captured.
     */
    Command captureOutput(StringBuilder stringBuilder);

    /**
     * Set whether we should capture the command output.
     */
    Command setCaptureStderr(boolean captureStderr);

    /**
     * Sets the stdin to use for the command.
     *
     * @param stdin                 The bytes to send to stdin.
     */
    Command setStdin(byte[] stdin);

    /**
     * Runs the command.
     *
     * @return The exit status of the command.
     */
    int run() throws Exception;

    /**
     * Runs the command, throwing an exception if it fails.
     *
     * @throws Exception    If the command fails.
     */
    void mustRun() throws Exception;

    /**
     * Execute the command.  Exits with the command's return status.
     * The command's stdout and stderr will appear on stdout and stderr.
     *
     * @throws Exception    If the exec fails.
     */
    void exec() throws Exception;

    /**
     * Translate a list of command-line arguments into a human-readable string.
     * Arguments which contain whitespace will be quoted.
     *
     * @param args  The argument list.
     * @return      A human-readable string.
     */
    static String joinArgs(List<String> args) {
        StringBuilder bld = new StringBuilder();
        String prefix = "";
        for (String arg : args) {
            bld.append(prefix);
            prefix = " ";
            if (arg.contains(" ")) {
                bld.append("\"");
            }
            bld.append(arg);
            if (arg.contains(" ")) {
                bld.append("\"");
            }
        }
        return bld.toString();
    }
}
