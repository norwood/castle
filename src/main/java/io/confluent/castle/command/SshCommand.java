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
import io.confluent.castle.common.CastleUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A command implementation that uses ssh to contact the node.
 */
public class SshCommand implements Command {
    private final CastleNode node;

    private final String dns;

    private final String sshUser;

    private final int sshPort;

    private final String sshIdentityFile;

    private Operation operation = Operation.SSH;

    private List<String> args = null;

    private String local = null;

    private String remote = null;

    private boolean captureStderr = false;

    private StringBuilder stringBuilder = null;

    private byte[] stdin = null;

    public SshCommand(CastleNode node, String dns, String sshUser, int sshPort, String sshIdentityFile) {
        this.node = node;
        this.dns = dns;
        this.sshUser = sshUser;
        this.sshPort = sshPort;
        this.sshIdentityFile = sshIdentityFile;
    }

    @Override
    public Command args(String... args) {
        return argList(Arrays.asList(args));
    }

    @Override
    public Command argList(List<String> args) {
        this.operation = Operation.SSH;
        this.args = new ArrayList<>(args);
        this.local = null;
        this.remote = null;
        return this;
    }

    @Override
    public Command syncTo(String local, String remote) {
        this.operation = Operation.RSYNC_TO;
        this.args = null;
        this.local = local;
        this.remote = remote;
        return this;
    }

    @Override
    public Command syncFrom(String remote, String local) {
        this.operation = Operation.RSYNC_FROM;
        this.args = null;
        this.local = local;
        this.remote = remote;
        return this;
    }

    @Override
    public Command captureOutput(StringBuilder stringBuilder) {
        this.stringBuilder = stringBuilder;
        return this;
    }

    @Override
    public Command setCaptureStderr(boolean captureStderr) {
        this.captureStderr = captureStderr;
        return this;
    }

    @Override
    public Command setStdin(byte[] stdin) {
        if (stdin == null) {
            this.stdin = null;
        } else {
            this.stdin = Arrays.copyOf(stdin, stdin.length);
        }
        return this;
    }

    @Override
    public int run() throws Exception {
        return new NodeShellRunner(node, makeCommandLine()).
            setCaptureOutput(stringBuilder).
            setCaptureStderr(captureStderr).
            setStdin(stdin).
            run();
    }

    @Override
    public void mustRun() throws Exception {
        new NodeShellRunner(node, makeCommandLine()).
            setCaptureOutput(stringBuilder).
            setCaptureStderr(captureStderr).
            setStdin(stdin).
            mustRun();
    }

    @Override
    public void exec() throws Exception {
        new NodeShellRunner(node, makeCommandLine()).
            setCaptureOutput(stringBuilder).
            setCaptureStderr(captureStderr).
            setStdin(stdin).
            exec();
    }

    private List<String> makeCommandLine() {
        List<String> commandLine = new ArrayList<>();
        if (dns.isEmpty()) {
            throw new RuntimeException("No DNS address configured for " + node.nodeName());
        }
        switch (operation) {
            case SSH:
                if (args == null) {
                    throw new RuntimeException("You must supply ssh arguments.");
                }
                commandLine.addAll(createSshCommandPreamble());
                commandLine.add(dns);
                commandLine.addAll(args);
                break;
            case RSYNC_TO:
                if ((local == null) || (remote == null)) {
                    throw new RuntimeException("The local and remote paths must be non-null.");
                }
                commandLine.add("rsync");
                commandLine.add("-aqi");
                commandLine.add("--delete");
                commandLine.add("-e");
                commandLine.add(CastleUtil.join(createSshCommandPreamble(), " "));
                commandLine.add(local);
                commandLine.add(dns + ":" + remote);
                break;
            case RSYNC_FROM:
                if ((local == null) || (remote == null)) {
                    throw new RuntimeException("The local and remote paths must be non-null.");
                }
                commandLine.add("rsync");
                commandLine.add("-aqi");
                commandLine.add("--delete");
                commandLine.add("-e");
                commandLine.add(CastleUtil.join(createSshCommandPreamble(), " "));
                commandLine.add(dns + ":" + remote);
                commandLine.add(local);
                break;
        }
        return commandLine;
    }

    public List<String> createSshCommandPreamble() {
        List<String> commandLine = new ArrayList<>();
        commandLine.add("ssh");

        // Specify an identity file, if configured.
        if (!sshIdentityFile.isEmpty()) {
            commandLine.add("-i");
            commandLine.add(sshIdentityFile);
        }
        // Set the user to ssh as, if configured.
        if (!sshUser.isEmpty()) {
            commandLine.add("-l");
            commandLine.add(sshUser);
        }

        // Set the port to ssh to, if configured.
        if (sshPort != 0) {
            commandLine.add("-p");
            commandLine.add(Integer.toString(sshPort));
        }

        // Disable strict host-key checking to avoid getting prompted the first time we connect.
        // TODO: can we enable this on subsequent sshes?
        commandLine.add("-o");
        commandLine.add("StrictHostKeyChecking=no");

        return commandLine;
    }

}
