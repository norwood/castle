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
import java.util.ArrayList;
import java.util.List;

/**
 * Runs a shell command for a node and captures the output to a log file, and
 * possibly a stringbuilder.
 */
public class NodeShellRunner {
    private static final int OUTPUT_REDIRECTOR_BUFFER_SIZE = 16384;

    /**
     * A thread which reads the stdout from the process we're running.
     */
    private static final class OutputRedirector implements Runnable {
        private final InputStream output;
        private final List<StringBuilder> stringBuilders;
        private final CastleLog castleLog;

        OutputRedirector(InputStream output, List<StringBuilder> stringBuilders, CastleLog castleLog) {
            this.output = output;
            this.stringBuilders = stringBuilders;
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
                    for (StringBuilder stringBuilder : stringBuilders) {
                        synchronized (stringBuilder) {
                            stringBuilder.append(new String(arr, StandardCharsets.UTF_8));
                        }
                    }
                    if (castleLog != null) {
                        synchronized (castleLog) {
                            castleLog.write(arr, 0, ret);
                        }
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

    private StringBuilder captureOutput = null;

    private boolean captureStderr = true;

    private boolean logOutputOnSuccess = true;

    public NodeShellRunner(CastleNode node, List<String> commandLine) {
        this.node = node;
        this.commandLine = commandLine;
    }

    public NodeShellRunner setCaptureOutput(StringBuilder captureOutput) {
        this.captureOutput = captureOutput;
        return this;
    }

    public NodeShellRunner setCaptureStderr(boolean captureStderr) {
        this.captureStderr = captureStderr;
        return this;
    }

    public NodeShellRunner setLogOutputOnSuccess(boolean logOutputOnSuccess) {
        this.logOutputOnSuccess = logOutputOnSuccess;
        return this;
    }

    public int run() throws Exception {
        ProcessBuilder builder = new ProcessBuilder(commandLine);
        builder.redirectErrorStream(false);
        Process process = null;
        Thread stdoutRedirectorThread = null, stderrRedirectorThread = null;
        int retCode = 1;
        // Set up the string builders which will log the output.
        List<StringBuilder> stdoutBuilders = new ArrayList<>();
        List<StringBuilder> stderrBuilders = new ArrayList<>();
        if (captureOutput != null) {
            stdoutBuilders.add(captureOutput);
            if (captureStderr) {
                stderrBuilders.add(captureOutput);
            }
        }
        StringBuilder errorStringBuilder = null;
        if (!logOutputOnSuccess) {
            errorStringBuilder = new StringBuilder();
            stdoutBuilders.add(errorStringBuilder);
            stderrBuilders.add(errorStringBuilder);
        }
        try {
            node.log().printf("** %s: RUNNING %s%n", node.nodeName(), Command.joinArgs(commandLine));
            process = builder.start();
            OutputRedirector stdoutRedirector =
                new OutputRedirector(process.getInputStream(), stdoutBuilders, node.log());
            OutputRedirector stderrRedirector =
                new OutputRedirector(process.getErrorStream(), stderrBuilders, node.log());
            stdoutRedirectorThread = new Thread(stdoutRedirector, "CastleSshStdout_" + node.nodeName());
            stdoutRedirectorThread.start();
            stderrRedirectorThread = new Thread(stderrRedirector, "CastleSshStderr_" + node.nodeName());
            stderrRedirectorThread.start();
            retCode = process.waitFor();
            stdoutRedirectorThread.join();
            stderrRedirectorThread.join();
            node.log().printf(String.format("** %s: FINISHED %s with RESULT %d%n",
                node.nodeName(), Command.joinArgs(commandLine), retCode));
        } finally {
            if (process != null) {
                process.destroy();
                process.waitFor();
            }
            if (stdoutRedirectorThread != null) {
                stdoutRedirectorThread.join();
            }
            if (stderrRedirectorThread != null) {
                stderrRedirectorThread.join();
            }
            if ((errorStringBuilder != null) && (retCode != 0)) {
                node.log().print(errorStringBuilder.toString());
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
