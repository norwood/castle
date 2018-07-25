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

import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.common.CastleLog;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Runs a shell command for a node and captures the output to a log file, and
 * possibly a stringbuilder.
 */
public class NodeShellRunner {
    private static final int OUTPUT_REDIRECTOR_BUFFER_SIZE = 16384;

    /**
     * A thread which reads from a pipe and writes the result into a file.
     */
    private static final class OutputRedirector implements Runnable {
        private final InputStream output;
        private final StringBuilder stringBuilder;
        private final CastleLog castleLog;

        OutputRedirector(InputStream output, StringBuilder stringBuilder,
                         CastleLog castleLog) {
            this.output = output;
            this.stringBuilder = stringBuilder;
            this.castleLog = castleLog;
        }

        @Override
        public void run() {
            byte[] arr = new byte[OUTPUT_REDIRECTOR_BUFFER_SIZE];
            try {
                while (true) {
                    int ret = output.read(arr);
                    if (ret == -1) {
                        break;
                    }
                    castleLog.write(arr, 0, ret);
                    if (stringBuilder != null) {
                        stringBuilder.append(new String(arr, StandardCharsets.UTF_8));
                    }
                }
            } catch (EOFException e) {
            } catch (IOException e) {
                castleLog.printf("IOException: %s%n", e.getMessage());
            }
        }
    }

    private final CastleNode node;

    private final List<String> commandLine;

    private final StringBuilder stringBuilder;

    private boolean redirectErrorStream = true;

    public NodeShellRunner(CastleNode node, List<String> commandLine, StringBuilder stringBuilder) {
        this.node = node;
        this.commandLine = commandLine;
        this.stringBuilder = stringBuilder;
    }

    public NodeShellRunner setRedirectErrorStream(boolean redirectErrorStream) {
        this.redirectErrorStream = redirectErrorStream;
        return this;
    }

    public int run() throws Exception {
        ProcessBuilder builder = new ProcessBuilder(commandLine);
        builder.redirectErrorStream(redirectErrorStream);
        Process process = null;
        Thread outputRedirectorThread = null;
        int retCode;
        try {
            node.log().printf("** %s: RUNNING %s%n", node.nodeName(),
                Command.joinArgs(commandLine));
            process = builder.start();
            OutputRedirector outputRedirector =
                new OutputRedirector(process.getInputStream(), stringBuilder, node.log());
            outputRedirectorThread = new Thread(outputRedirector, "CastleSsh_" + node.nodeName());
            outputRedirectorThread.start();
            retCode = process.waitFor();
            outputRedirectorThread.join();
            node.log().printf(String.format("** %s: FINISHED %s with RESULT %d%n",
                node.nodeName(), Command.joinArgs(commandLine), retCode));
        } finally {
            if (process != null) {
                process.destroy();
                process.waitFor();
            }
            if (outputRedirectorThread != null) {
                outputRedirectorThread.join();
            }
        }
        return retCode;
    }

    public void mustRun() throws Exception {
        int returnCode = run();
        if (returnCode != 0) {
            throw new CommandResultException(commandLine, returnCode);
        }
    }

    public void exec() throws Exception {
        node.log().printf("** %s: SSH %s%n", node.nodeName(), Command.joinArgs(commandLine));
        ProcessBuilder builder = new ProcessBuilder(commandLine);
        builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        Process process = builder.start();
        System.exit(process.waitFor());
    }
}
