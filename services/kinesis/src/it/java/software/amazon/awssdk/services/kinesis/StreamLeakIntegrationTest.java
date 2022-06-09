/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.services.kinesis;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.api.BigIntegerAssert;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;
import software.amazon.awssdk.utils.Pair;

public class StreamLeakIntegrationTest extends AbstractTestCase {
    private static final int NUM_STREAMS = 1; // Re-call setup() if you increase this (current max: 1)
    private static final int NUM_SHARDS_PER_STREAM = 1000; // Re-call setup() if you increase this (current max: 1000)

    private static final int BPS_PRODUCTION_RATE = 4096;
    private static final int RPS_PRODUCTION_RATE = 100;
    private static final Duration TEST_DURATION = Duration.ofHours(3);

    private static final String CONSUMER_NAME_PREFIX = "stream-leak-consumer";
    private static final String STREAM_NAME_PREFIX = "millem-subscribe-to-shard-integ-test";

    private static final byte[] DATA = RandomUtils.nextBytes(BPS_PRODUCTION_RATE / RPS_PRODUCTION_RATE);

    @BeforeClass
    public static void init() throws IOException {
        AbstractTestCase.init(NUM_STREAMS * NUM_SHARDS_PER_STREAM);
    }

    @Test
    public void setup() throws InterruptedException {
        try {
            teardown().forEach(stream -> {
                asyncClient.waiter().waitUntilStreamNotExists(r -> r.streamName(stream)).join();
            });
        } catch (Exception e) {
            // ignore
        }

        AtomicInteger streamNumber = new AtomicInteger(0);
        parallel(NUM_STREAMS, 5, () -> {
            String streamName = STREAM_NAME_PREFIX + "-" + streamNumber.getAndIncrement();
            sleep(1_000); // 5 TPS limit on createStream
            System.out.println("Creating stream " + streamName + ".");
            return asyncClient.createStream(r -> r.streamName(streamName).shardCount(NUM_SHARDS_PER_STREAM))
                              .thenCompose(r -> waitForStreamToBeActive(streamName))
                              .thenRun(() -> System.out.println(streamName + " stream created."));
        });

        // for (int i = 0; i < NUM_STREAMS; i++) {
        //     String streamName = STREAM_NAME_PREFIX + "-" + i;
        //     String streamArn = asyncClient.describeStream(r -> r.streamName(streamName)).join()
        //                                   .streamDescription()
        //                                   .streamARN();
        //
        //     System.out.println("Registering consumer for " + streamName + ".");
        //     parallel(NUM_SHARDS_PER_STREAM, 5,
        //              () -> asyncClient.registerStreamConsumer(r -> r.streamARN(streamArn)
        //                                                             .consumerName(CONSUMER_NAME_PREFIX + "-" + UUID.randomUUID()))
        //                               .thenCompose(registration -> waitForConsumerToBeActive(registration.consumer().consumerARN()))
        //                               .thenRun(() -> System.out.println(streamName + " consumer registered.")));
        // }
    }

    @Test
    public void reproduceSubscribeToShardError() throws InterruptedException {
        List<Pair<software.amazon.awssdk.services.kinesis.model.Consumer, Shard>> work = new ArrayList<>();

        System.out.println("Preparing...");

        for (int i = 0; i < NUM_STREAMS; i++) {
            sleep(1_000 / 5); // 5 TPS limit on listStreamConsumers

            String streamName = STREAM_NAME_PREFIX + "-" + i;
            String streamArn = client.describeStream(r -> r.streamName(streamName))
                                     .streamDescription()
                                     .streamARN();

            List<Shard> shards = client.listShards(r -> r.streamName(streamName)).shards();
            List<software.amazon.awssdk.services.kinesis.model.Consumer> consumers =
                client.listStreamConsumers(r -> r.streamARN(streamArn)).consumers();

            assertThat(shards.size()).isGreaterThanOrEqualTo(NUM_SHARDS_PER_STREAM);
            assertThat(consumers.size()).isGreaterThanOrEqualTo(NUM_SHARDS_PER_STREAM);

            for (int j = 0; j < NUM_SHARDS_PER_STREAM; j++) {
                work.add(Pair.of(consumers.get(j), shards.get(j)));
            }
        }

        AtomicInteger numRecordsReceived = new AtomicInteger(0);
        AtomicInteger numRecordsSent = new AtomicInteger(0);
        AtomicInteger numResponsesReceived = new AtomicInteger(0);
        AtomicInteger numExceptionsReceived = new AtomicInteger(0);
        Map<String, Integer> exceptions = new ConcurrentHashMap<>();

        ScheduledExecutorService backgroundTaskExecutor = Executors.newScheduledThreadPool(2);
        backgroundTaskExecutor.scheduleAtFixedRate(() -> {
            putRecord();
            numRecordsSent.incrementAndGet();
        }, 5000, 1000 / RPS_PRODUCTION_RATE, TimeUnit.MILLISECONDS);

        ExecutorService consumerExecutor = Executors.newCachedThreadPool();

        Instant end = Instant.now().plus(TEST_DURATION);
        backgroundTaskExecutor.scheduleAtFixedRate(() -> {
            Duration timeRemaining = Duration.between(Instant.now(), end);
            String remaining = timeRemaining.toMinutes() + " min " + timeRemaining.getSeconds() % 60 + " sec";
            System.out.println("Remaining time: " + remaining +
                               "; Records: " + numRecordsReceived + "/" + numRecordsSent +
                               "; Responses: " + numResponsesReceived +
                               "; Errors: " + numExceptionsReceived);
        }, 30, 30, TimeUnit.SECONDS);

        System.out.println("Executing tests until " + end + ".");

        AtomicBoolean running = new AtomicBoolean(true);
        Set<CompletableFuture<?>> futures = new HashSet<>(NUM_STREAMS * NUM_SHARDS_PER_STREAM);

        work.forEach(job -> {
            String consumerArn = job.left().consumerARN();
            String shardId = job.right().shardId();

            Consumer<SubscribeToShardEvent> eventConsumer = s -> {
                numRecordsReceived.addAndGet(s.records().size());
            };
            Consumer<SubscribeToShardResponse> responseConsumer = r -> {
                numResponsesReceived.incrementAndGet();
            };

            consumerExecutor.submit(() -> {
                while (running.get()) {
                    Instant lastSuccess = Instant.now();
                    try {
                        CompletableFuture<Void> future =
                            asyncClient.subscribeToShard(r -> r.consumerARN(consumerArn)
                                                               .shardId(shardId)
                                                               .startingPosition(s -> s.type(ShardIteratorType.LATEST)),
                                                         SubscribeToShardResponseHandler.builder()
                                                                                        .onEventStream(p -> p.filter(SubscribeToShardEvent.class)
                                                                                                             .subscribe(eventConsumer))
                                                                                        .onResponse(responseConsumer)
                                                                                        .build());

                        synchronized (futures) {
                            if (running.get()) {
                                futures.add(future);
                            } else {
                                future.cancel(false);
                                return;
                            }
                        }

                        future.join();

                        synchronized (futures) {
                            futures.remove(future);
                        }
                    } catch (Throwable t) {
                        numExceptionsReceived.incrementAndGet();
                        exceptions.merge(t.getMessage(), 1, (k, i) -> i + 1);
                    }

                    long millisSinceSuccess = Duration.between(lastSuccess, Instant.now()).toMillis();
                    if (running.get() && millisSinceSuccess < 10_000) {
                        // Only one (successful) request per arn/shard combo per 5 seconds allowed (use 10 seconds for extra
                        // buffer)
                        sleep(10_000 - Math.toIntExact(millisSinceSuccess));
                    }
                }
            });
        });
        sleep(Math.toIntExact(Duration.between(Instant.now(), end).toMillis()));

        backgroundTaskExecutor.shutdown();
        consumerExecutor.shutdown();
        synchronized (futures) {
            running.set(false);
            futures.forEach(f -> f.cancel(false));
        }
        consumerExecutor.awaitTermination(5, TimeUnit.MINUTES);

        System.out.println("Exceptions encountered:");
        exceptions.forEach((k, v) -> System.out.println(v + ": " + k));

        System.out.println("COMPLETED!!!!");
        sleep(Integer.MAX_VALUE);
    }

    public List<String> teardown() {
        List<String> deletedStreams = new ArrayList<>();

        ListStreamsResponse streams = asyncClient.listStreams().join();
        for (String streamName : streams.streamNames()) {
            if (streamName.contains(STREAM_NAME_PREFIX)) {
                sleep(1_000 / 5); // 5 TPS limit on deleteStream

                asyncClient.deleteStream(r -> r.streamName(streamName)
                                               .enforceConsumerDeletion(true))
                           .join();
                deletedStreams.add(streamName);
            }
        }

        System.out.println("Deleting streams: " + deletedStreams);

        return deletedStreams;
    }

    private static CompletableFuture<Void> waitForConsumerToBeActive(String consumerArn) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        waitForConsumerToBeActive(result, consumerArn);
        return result;
    }

    private static void waitForConsumerToBeActive(CompletableFuture<Void> result, String consumerArn) {
        asyncClient.describeStreamConsumer(r -> r.consumerARN(consumerArn))
                   .thenAccept(r -> {
                       ConsumerStatus status = r.consumerDescription().consumerStatus();
                       if (status == ConsumerStatus.ACTIVE) {
                           result.complete(null);
                       }
                       else {
                           sleep(1_000);
                           waitForConsumerToBeActive(result, consumerArn);
                       }
                   }).exceptionally(t -> {
                       result.completeExceptionally(t);
                       return null;
                   });
    }


    private CompletableFuture<Void> waitForStreamToBeActive(String streamName) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        waitForStreamToBeActive(result, streamName);
        return result;
    }


    private static void waitForStreamToBeActive(CompletableFuture<Void> result, String streamName) {
        sleep(1000 / 10 * 5); // 10 TPS limit on describeStream

        asyncClient.describeStream(r -> r.streamName(streamName))
                   .thenAccept(r -> {
                       StreamStatus status = r.streamDescription().streamStatus();
                       if (status == StreamStatus.ACTIVE) {
                           result.complete(null);
                       }
                       else {
                           waitForStreamToBeActive(result, streamName);
                       }
                   }).exceptionally(t -> {
                       result.completeExceptionally(t);
                       return null;
                   });
    }

    private void putRecord() {
        SdkBytes data = SdkBytes.fromByteArrayUnsafe(DATA);
        client.putRecord(PutRecordRequest.builder()
                                         .streamName(STREAM_NAME_PREFIX + "-" + RandomUtils.nextInt(0, NUM_STREAMS))
                                         .data(data)
                                         .partitionKey(UUID.randomUUID().toString())
                                         .build());
    }

    @Test
    public void cleanup() {
        teardown();
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void parallel(int numCalls, int maxParallel, Supplier<CompletableFuture<?>> function)
        throws InterruptedException {
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        Semaphore lease = new Semaphore(maxParallel);
        for (int i = 0; i < numCalls; i++) {
            lease.acquire();
            try {
                function.get().handle((r, e) -> {
                    if (e != null) {
                        System.out.println("Failure encountered: " + e.getMessage());
                        failures.add(e);
                    }
                    lease.release();
                    return null;
                });
            } catch (Exception e) {
                lease.release();
            }
        }
        lease.acquire(maxParallel);
        if (!failures.isEmpty()) {
            throw new RuntimeException(failures.get(0));
        }
    }
}
