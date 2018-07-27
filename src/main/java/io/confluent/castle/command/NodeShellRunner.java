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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Runs a shell command for a node and captures the output to a log file, and
 * possibly a stringbuilder.
 */
public class NodeShellRunner {
    private static final int OUTPUT_REDIRECTOR_BUFFER_SIZE = 32768;

    /**
     * A thread which reads the stdout from the process we're running.
     */
    private static final class OutputHandler implements Runnable {
        private final InputStream stream;
        private final List<StringBuilder> stringBuilders;
        private final CastleLog castleLog;
        private final boolean newlineTerminate;

        OutputHandler(InputStream stream, List<StringBuilder> stringBuilders,
                      CastleLog castleLog, boolean newlineTerminate) {
            this.stream = stream;
            this.stringBuilders = stringBuilders;
            this.castleLog = castleLog;
            this.newlineTerminate = newlineTerminate;
        }

        @Override
        public void run() {
            byte[] arr = new byte[OUTPUT_REDIRECTOR_BUFFER_SIZE];
            boolean endedWithNewline = true;
            try {
                while (true) {
                    int ret = stream.read(arr);
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
                        if (ret >= 0) {
                            endedWithNewline = (arr[ret] == '\n');
                        }
                    }
                }
                if (newlineTerminate && (!endedWithNewline)) {
                    synchronized (castleLog) {
                        castleLog.write(new byte[] {'\n'});
                    }
                }
            } catch (EOFException e) {
            } catch (IOException e) {
                castleLog.printf("OutputRedirectory IOException: %s%n", e.getMessage());
            }
        }
    }

    /**
     * A thread which writes to a process' stdin.
     */
    private static final class StdinHandler implements Runnable {
        private final byte[] data;
        private final OutputStream stream;
        private final CastleLog castleLog;

        StdinHandler(OutputStream stream, byte[] data, CastleLog castleLog) {
            this.data = data;
            this.stream = stream;
            this.castleLog = castleLog;
        }

        @Override
        public void run() {
            try {
                stream.write(data);
                stream.close();
            } catch (EOFException e) {
            } catch (IOException e) {
                castleLog.printf("StdinHandler IOException: %s%n", e.getMessage());
            }
        }
    }

    private final CastleNode node;

    private final List<String> commandLine;

    private StringBuilder captureOutput = null;

    private boolean captureStderr = true;

    private boolean logOutputOnSuccess = true;

    private byte[] stdin = null;

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

    public NodeShellRunner setStdin(byte[] stdin) {
        if (stdin == null) {
            this.stdin = null;
        } else {
            this.stdin = Arrays.copyOf(stdin, stdin.length);
        }
        return this;
    }

    public int run() throws Exception {
        ProcessBuilder builder = new ProcessBuilder(commandLine);
        builder.redirectErrorStream(false);
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
        OutputHandler stdoutHandler = null, stderrHandler = null;
        StdinHandler stdinHandler = null;
        StringBuilder errorStringBuilder = null;
        Thread stdoutThread = null, stderrThread = null, stdinThread = null;
        Process process = null;
        try {
            node.log().printf("** %s: RUNNING %s%n", node.nodeName(), Command.joinArgs(commandLine));
            process = builder.start();
            if (stdin != null) {
                stdinHandler = new StdinHandler(process.getOutputStream(), stdin, node.log());
                stdinThread = new Thread(stdinHandler, "CastleSshStdin_" + node.nodeName());
                stdinThread.start();
            }
            if (logOutputOnSuccess) {
                stdoutHandler = new OutputHandler(process.getInputStream(), stdoutBuilders, node.log(), true);
                stderrHandler = new OutputHandler(process.getErrorStream(), stderrBuilders, node.log(), false);
            } else {
                errorStringBuilder = new StringBuilder();
                stdoutBuilders.add(errorStringBuilder);
                stderrBuilders.add(errorStringBuilder);
                stdoutHandler = new OutputHandler(process.getInputStream(), stdoutBuilders, null, false);
                stderrHandler = new OutputHandler(process.getErrorStream(), stderrBuilders, null, false);
            }
            stdoutThread = new Thread(stdoutHandler, "CastleSshStdout_" + node.nodeName());
            stdoutThread.start();
            stderrThread = new Thread(stderrHandler, "CastleSshStderr_" + node.nodeName());
            stderrThread.start();
            retCode = process.waitFor();
            stdoutThread.join();
            stderrThread.join();
            if (stdinThread != null) {
                stdinThread.join();
            }
            node.log().printf("** %s: FINISHED %s with RESULT %d%n",
                node.nodeName(), Command.joinArgs(commandLine), retCode);
        } finally {
            if (process != null) {
                process.destroy();
                process.waitFor();
            }
            if (stdoutThread != null) {
                stdoutThread.join();
            }
            if (stderrThread != null) {
                stderrThread.join();
            }
            if (stdinThread != null) {
                stdinThread.join();
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
