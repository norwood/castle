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

package io.confluent.castle.cloud;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.ResourceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import io.confluent.castle.cluster.CastleCluster;
import io.confluent.castle.cluster.CastleNode;
import io.confluent.castle.common.CastleLog;
import io.confluent.castle.common.CastleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class Ec2Cloud implements AutoCloseable, Runnable {
    private static final Logger log = LoggerFactory.getLogger(Ec2Cloud.class);

    /**
     * The minimum delay to observe between queuing a request and performing it.
     * Having a coalsce delay helps increase the amount of batching we do.
     * By waiting for a short period, we may encounter other requests that could
     * be done in the same batch.
     */
    private final static int COALSCE_DELAY_MS = 20;

    /**
     * The minimum delay to observe between making a request and making a
     * subsequent request.  This helps avoid exceeding the ec2 rate limiting.
     */
    private final static int CALL_DELAY_MS = 500;

    private final static Tag CASTLE_TAG = new Tag("CastleNodeVersion", "1");

    private final Ec2Settings settings;

    private final AmazonEC2 ec2;

    private final Thread thread;

    private final List<CreateInstanceOp> creates = new ArrayList<>();

    private final List<DescribeInstanceOp> describes = new ArrayList<>();

    private final List<DescribeAllInstancesOp> describeAlls = new ArrayList<>();

    private final List<TerminateInstanceOp> terminates = new ArrayList<>();

    private boolean shouldExit = false;

    private boolean shutdownAllInvoked = false;

    private long nextCallTimeMs = 0;

    private final static class CreateInstanceOp {
        private final CompletableFuture<String> future = new CompletableFuture<>();
        private final String instanceType;
        private final String imageId;
        private final int nodeIndex;

        CreateInstanceOp(String instanceType, String imageId, int nodeIndex) {
            this.instanceType = instanceType;
            this.imageId = imageId;
            this.nodeIndex = nodeIndex;
        }
    }

    private static final class DescribeInstanceOp {
        private final CompletableFuture<Ec2InstanceInfo> future = new CompletableFuture<>();
        private final String instanceId;

        DescribeInstanceOp(String instanceId) {
            this.instanceId = instanceId;
        }
    }

    private static final class DescribeAllInstancesOp {
        private final CompletableFuture<Collection<Ec2InstanceInfo>> future =
            new CompletableFuture<>();
    }

    private static final class TerminateInstanceOp {
        private final CompletableFuture<Void> future = new CompletableFuture<>();
        private final String instanceId;

        TerminateInstanceOp(String instanceId) {
            this.instanceId = instanceId;
        }
    }

    public Ec2Cloud(Ec2Settings settings) {
        this.settings = settings;
        AmazonEC2ClientBuilder ec2Builder = AmazonEC2ClientBuilder.standard();
        if (!settings.region().isEmpty()) {
            ec2Builder.setRegion(settings.region());
        }
        this.ec2 = ec2Builder.build();
        this.thread = new Thread(this, "Ec2CloudThread");
        this.thread.start();
    }

    @Override
    public void run() {
        try {
            while (true) {
                synchronized (this) {
                    long delayMs = calculateDelayMs();
                    if (delayMs < 0) {
                        break;
                    } else if (delayMs > 0) {
                        log.trace("Ec2Cloud thread waiting for {} ms.", delayMs);
                        wait(delayMs);
                    } else {
                        makeCalls();
                    }
                }
            }
            log.trace("Ec2Cloud thread exiting");
        } catch (Exception e) {
            log.warn("Ec2Cloud thread exiting with error", e);
            throw new RuntimeException(e);
        } finally {
            synchronized (this) {
                RuntimeException e = new RuntimeException("Ec2Cloud is shutting down.");
                for (CreateInstanceOp op : creates) {
                    op.future.completeExceptionally(e);
                }
                for (DescribeInstanceOp describe : describes) {
                    describe.future.completeExceptionally(e);
                }
                for (DescribeAllInstancesOp describeAll : describeAlls) {
                    describeAll.future.completeExceptionally(e);
                }
                for (TerminateInstanceOp terminate : terminates) {
                    terminate.future.completeExceptionally(e);
                }
            }
        }
    }

    private synchronized long calculateDelayMs() throws Exception {
        if (shouldExit) {
            // Should exit.
            return -1;
        } else if (creates.isEmpty() &&
                    describes.isEmpty() &&
                    describeAlls.isEmpty() &&
                    terminates.isEmpty()) {
            // Nothing to do.
            return Long.MAX_VALUE;
        } else {
            long now = System.currentTimeMillis();
            if (nextCallTimeMs > now) {
                return nextCallTimeMs - now;
            } else {
                return 0;
            }
        }
    }

    private synchronized void makeCalls() throws Exception {
        log.info("Ec2Cloud#makeCalls.  creates.size=" + creates.size());
        if (!creates.isEmpty()) {
            List<CreateInstanceOp> batchCreates = new ArrayList<>();
            Iterator<CreateInstanceOp> iter = creates.iterator();
            CreateInstanceOp firstCreate = iter.next();
            iter.remove();
            batchCreates.add(firstCreate);
            while (iter.hasNext()) {
                CreateInstanceOp runner = iter.next();
                if (runner.equals(firstCreate)) {
                    batchCreates.add(runner);
                    iter.remove();
                }
            }
            Iterator<CreateInstanceOp> runInstanceIterator = batchCreates.iterator();
            Exception failureException = new RuntimeException("Unable to create instance");
            try {
                if (settings.keyPair().isEmpty()) {
                    throw new RuntimeException("You must specify a keypair in " +
                        "order to create a new AWS instance.");
                }
                if (settings.securityGroup().isEmpty()) {
                    throw new RuntimeException("You must specify a security group in " +
                        "order to create a new AWS instance.");
                }
                log.info("Ec2Cloud#makeCalls.  batchCreates.size={}, imageId={}, keyName={}, securityGroups={}",
                    batchCreates.size(), firstCreate.imageId, settings.keyPair(), settings.securityGroup());
                RunInstancesRequest req = new RunInstancesRequest()
                    .withInstanceType(firstCreate.instanceType)
                    .withImageId(firstCreate.imageId)
                    .withMinCount(batchCreates.size())
                    .withMaxCount(batchCreates.size())
                    .withKeyName(settings.keyPair())
                    .withSecurityGroups(settings.securityGroup())
                    .withTagSpecifications(
                        new TagSpecification().withResourceType(ResourceType.Instance).
                            withTags(CASTLE_TAG));

                RunInstancesResult result = ec2.runInstances(req);
                Reservation reservation = result.getReservation();
                Iterator<Instance> instanceIterator = reservation.getInstances().iterator();
                while (runInstanceIterator.hasNext() && instanceIterator.hasNext()) {
                    CreateInstanceOp runInstance = runInstanceIterator.next();
                    Instance instance = instanceIterator.next();
                    runInstance.future.complete(instance.getInstanceId());
                }
            } catch (Exception e) {
                failureException = e;
            }
            while (runInstanceIterator.hasNext()) {
                CreateInstanceOp runInstance = runInstanceIterator.next();
                runInstance.future.completeExceptionally(failureException);
            }
        } else if (!describes.isEmpty()) {
            Map<String, DescribeInstanceOp> idToDescribe = new HashMap<>();
            for (Iterator<DescribeInstanceOp> iter = describes.iterator(); iter.hasNext();
                     iter.remove()) {
                DescribeInstanceOp describe = iter.next();
                idToDescribe.put(describe.instanceId, describe);
            }
            Exception failureException = new RuntimeException("Result did not include instanceID.");
            try {
                DescribeInstancesRequest req = new DescribeInstancesRequest()
                    .withInstanceIds(idToDescribe.keySet());
                DescribeInstancesResult result = ec2.describeInstances(req);
                for (Reservation reservation : result.getReservations()) {
                    for (Instance instance : reservation.getInstances()) {
                        DescribeInstanceOp op = idToDescribe.get(instance.getInstanceId());
                        if (op != null) {
                            Ec2InstanceInfo info =
                                new Ec2InstanceInfo(instance.getInstanceId(),
                                    instance.getPrivateDnsName(),
                                    instance.getPublicDnsName(),
                                    instance.getState().toString());
                            op.future.complete(info);
                            idToDescribe.remove(instance.getInstanceId());
                        }
                    }
                }
            } catch (Exception e) {
                failureException = e;
            }
            for (Map.Entry<String, DescribeInstanceOp> entry : idToDescribe.entrySet()) {
                entry.getValue().future.completeExceptionally(failureException);
            }
        } else if (!describeAlls.isEmpty()) {
            try {
                if (settings.keyPair().isEmpty()) {
                    throw new RuntimeException("You must specify a keypair with --keypair in " +
                        "order to describe all AWS instances.");
                }
                DescribeInstancesRequest req = new DescribeInstancesRequest().withFilters(
                    new Filter("key-name",
                        Collections.singletonList(settings.keyPair())),
                    new Filter("tag:" + CASTLE_TAG.getKey(),
                        Collections.singletonList(CASTLE_TAG.getValue())));
                ArrayList<Ec2InstanceInfo> all = new ArrayList<>();
                DescribeInstancesResult result = ec2.describeInstances(req);
                for (Reservation reservation : result.getReservations()) {
                    for (Instance instance : reservation.getInstances()) {
                        all.add(new Ec2InstanceInfo(instance.getInstanceId(),
                            instance.getPrivateDnsName(),
                            instance.getPublicDnsName(),
                            instance.getState().toString()));
                    }
                }
                for (DescribeAllInstancesOp describeAll : describeAlls) {
                    describeAll.future.complete(all);
                }
            } catch (Exception e) {
                for (DescribeAllInstancesOp describeAll : describeAlls) {
                    describeAll.future.completeExceptionally(e);
                }
            }
        } else if (!terminates.isEmpty()) {
            Map<String, TerminateInstanceOp> idToTerminate = new HashMap<>();
            for (Iterator<TerminateInstanceOp> iter = terminates.iterator(); iter.hasNext();
                     iter.remove()) {
                TerminateInstanceOp op = iter.next();
                idToTerminate.put(op.instanceId, op);
            }
            TerminateInstancesRequest req = new TerminateInstancesRequest()
                .withInstanceIds(idToTerminate.keySet());
            ec2.terminateInstances(req);
            for (TerminateInstanceOp op : idToTerminate.values()) {
                CastleUtil.completeNull(op.future);
            }
        }
        updateNextCallTime(CALL_DELAY_MS);
    }

    private synchronized void updateNextCallTime(long minDelay) {
        nextCallTimeMs = Math.max(nextCallTimeMs, System.currentTimeMillis() + minDelay);
    }

    @Override
    public void close() throws InterruptedException {
        synchronized (this) {
            shouldExit = true;
            notifyAll();
        }
        thread.join();
        ec2.shutdown();
    }

    public synchronized CompletableFuture<String> createInstance(String instanceType,
                String imageId, int nodeIndex) {
        CreateInstanceOp op = new CreateInstanceOp(instanceType, imageId, nodeIndex);
        creates.add(op);
        updateNextCallTime(COALSCE_DELAY_MS);
        notifyAll();
        return op.future;
    }

    public synchronized CompletableFuture<Ec2InstanceInfo> describeInstance(String instanceId)
            throws Exception {
        DescribeInstanceOp op = new DescribeInstanceOp(instanceId);
        describes.add(op);
        updateNextCallTime(COALSCE_DELAY_MS);
        notifyAll();
        return op.future;
    }

    public synchronized CompletableFuture<Collection<Ec2InstanceInfo>> describeAllInstances()
                throws Exception {
        DescribeAllInstancesOp op = new DescribeAllInstancesOp();
        describeAlls.add(op);
        updateNextCallTime(COALSCE_DELAY_MS);
        notifyAll();
        return op.future;
    }

    public synchronized CompletableFuture<Void> terminateInstance(String instanceId) {
        TerminateInstanceOp op = new TerminateInstanceOp(instanceId);
        terminates.add(op);
        updateNextCallTime(COALSCE_DELAY_MS);
        notifyAll();
        return op.future;
    }

    public void destroyAll(CastleCluster cluster, CastleNode node) throws Exception {
        synchronized (this) {
            if (shutdownAllInvoked) {
                return;
            }
            shutdownAllInvoked = true;
        }
        Collection<Ec2InstanceInfo> infos = describeAllInstances().get();
        if (infos.isEmpty()) {
            CastleLog.printToAll(String.format(
                "*** %s: No EC2 instances found.%n", node.nodeName()),
                node.log(), cluster.clusterLog());
            return;
        }
        CastleLog.printToAll(String.format("*** %s: Terminating EC2 instance(s): %s.%n",
            node.nodeName(), String.join(", ",
                infos.stream().map(info -> info.instanceId()).collect(Collectors.toSet()))),
            node.log(), cluster.clusterLog());
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Ec2InstanceInfo info : infos) {
            futures.add(terminateInstance(info.instanceId()));
        }
        for (CompletableFuture<Void> future : futures) {
            future.get();
        }
    }

    @Override
    public String toString() {
        return settings.toString();
    }
}
