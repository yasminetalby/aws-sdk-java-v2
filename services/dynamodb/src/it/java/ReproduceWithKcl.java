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

import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.KinesisMultiStreamClientFacade;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.fanout.FanOutRecordsPublisher;

public class ReproduceWithKcl {
    private static final int NUM_STREAMS = 1;
    private static final int NUM_SHARDS_PER_STREAM = 1000;

    private static final int BPS_PRODUCTION_RATE = 4096;
    private static final int RPS_PRODUCTION_RATE = 100;
    private static final Duration TEST_DURATION = Duration.ofHours(12);

    private static final String STREAM_NAME_PREFIX = "millem-subscribe-to-shard-integ-test";

    private static final byte[] DATA = RandomUtils.nextBytes(BPS_PRODUCTION_RATE / RPS_PRODUCTION_RATE);

    private KinesisClient syncClient;
    private KinesisAsyncClient asyncClient;
    private DynamoDbAsyncClient dynamoClient;
    private CloudWatchAsyncClient cloudwatchClient;

    private final AtomicInteger numRecordsReceived = new AtomicInteger(0);
    private final AtomicInteger numRecordsSent = new AtomicInteger(0);
    private final AtomicInteger lastSeenLeasedConcurrency = new AtomicInteger(0);
    private final AtomicInteger maxLeasedConcurrency = new AtomicInteger(0);
    private Instant end;
    private SdkAsyncHttpClient nettyClient;

    private final List<Scheduler> schedulers = new ArrayList<>();
    private ScheduledExecutorService backgroundTaskExecutor;

    private void init(int expectedMaxConcurrency) {
        AwsCredentialsProvider credentials = DefaultCredentialsProvider.create();

        syncClient = KinesisClient.builder()
                                  .credentialsProvider(credentials)
                                  .region(Region.US_WEST_2)
                                  .httpClient(ApacheHttpClient.builder()
                                                              .buildWithDefaults(AttributeMap.builder()
                                                                                             .put(TRUST_ALL_CERTIFICATES, Boolean.TRUE)
                                                                                             .build()))
                                  .build();
        nettyClient = NettyNioAsyncHttpClient.builder()
                                             .maxConcurrency(expectedMaxConcurrency * 2 + 1)
                                             .build();
        asyncClient = KinesisAsyncClient.builder()
                                        .credentialsProvider(credentials)
                                        .region(Region.US_WEST_2)
                                        .overrideConfiguration(c -> c.addMetricPublisher(new MetricPublisher() {
                                            @Override
                                            public void publish(MetricCollection metricCollection) {
                                                List<Integer> availableConcurrency =
                                                    metricCollection.metricValues(HttpMetric.AVAILABLE_CONCURRENCY);
                                                List<Integer> leasedConcurrency =
                                                    metricCollection.metricValues(HttpMetric.LEASED_CONCURRENCY);

                                                if (!leasedConcurrency.isEmpty()) {
                                                    int leasedConcurrencyN = leasedConcurrency.get(0);
                                                    lastSeenLeasedConcurrency.set(leasedConcurrencyN);

                                                    int m;
                                                    do {
                                                        m = maxLeasedConcurrency.get();
                                                        if (leasedConcurrencyN < m) {
                                                            break;
                                                        }
                                                    } while (!maxLeasedConcurrency.compareAndSet(m, leasedConcurrencyN));
                                                }
                                                metricCollection.children().forEach(this::publish);
                                            }

                                            @Override
                                            public void close() {
                                            }
                                        }))
                                        .httpClient(nettyClient)
                                        .build();

        dynamoClient = DynamoDbAsyncClient.builder()
                                          .credentialsProvider(credentials)
                                          .region(Region.US_WEST_2)
                                          .build();
        cloudwatchClient = CloudWatchAsyncClient.builder()
                                                .credentialsProvider(credentials)
                                                .region(Region.US_WEST_2)
                                                .build();

        this.backgroundTaskExecutor = Executors.newScheduledThreadPool(2);
        backgroundTaskExecutor.scheduleAtFixedRate(() -> {
            putRecord();
            numRecordsSent.incrementAndGet();
        }, 5000, 1000 / RPS_PRODUCTION_RATE, TimeUnit.MILLISECONDS);

        end = Instant.now().plus(TEST_DURATION);
        backgroundTaskExecutor.scheduleAtFixedRate(() -> {
            Duration timeRemaining = Duration.between(Instant.now(), end);
            String remaining = timeRemaining.toHours() + " hour " +
                               timeRemaining.toMinutes() % 60 + " min " +
                               timeRemaining.getSeconds() % 60 + " sec";
            System.out.println("Remaining time: " + remaining +
                               "; Records: " + numRecordsReceived + "/" + numRecordsSent +
                               "; Leased concurrency: " + lastSeenLeasedConcurrency.get() + " (max: " + maxLeasedConcurrency.get() + ")");
        }, 30, 30, TimeUnit.SECONDS);
    }

    @Test
    public void reproduceErrorWithKcl() {
        init(NUM_STREAMS * NUM_SHARDS_PER_STREAM);

        RecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory();

        for (int i = 0; i < NUM_STREAMS; i++) {
            String streamName = STREAM_NAME_PREFIX + "-" + i;

            ConfigsBuilder configsBuilder = new ConfigsBuilder(streamName,
                                                               streamName,
                                                               asyncClient,
                                                               dynamoClient,
                                                               cloudwatchClient,
                                                               UUID.randomUUID().toString(),
                                                               recordProcessorFactory);

            Scheduler scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig()
            );

            schedulers.add(scheduler);

            Thread thread = new Thread(scheduler);
            thread.setDaemon(true);
            thread.start();
        }

        int millis = Math.toIntExact(Duration.between(Instant.now(), end).toMillis());
        // System.out.println("Sleeping " + millis + " ms.");
        sleep(millis);

        System.out.println("COMPLETE!!!");
        backgroundTaskExecutor.shutdown();
    }

    private void putRecord() {
        SdkBytes data = SdkBytes.fromByteArrayUnsafe(DATA);
        syncClient.putRecord(PutRecordRequest.builder()
                                             .streamName(STREAM_NAME_PREFIX + "-" + RandomUtils.nextInt(0, NUM_STREAMS))
                                             .data(data)
                                             .partitionKey(UUID.randomUUID().toString())
                                             .build());
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private class RecordProcessorFactory implements ShardRecordProcessorFactory {
        @Override
        public ShardRecordProcessor shardRecordProcessor() {
            return new ShardRecordProcessor() {
                @Override
                public void initialize(InitializationInput initializationInput) {
                }

                @Override
                public void processRecords(ProcessRecordsInput processRecordsInput) {
                    numRecordsReceived.addAndGet(processRecordsInput.records().size());
                }

                @Override
                public void leaseLost(LeaseLostInput leaseLostInput) {
                }

                @Override
                public void shardEnded(ShardEndedInput shardEndedInput) {
                    try {
                        shardEndedInput.checkpointer().checkpoint();
                    } catch (InvalidStateException | ShutdownException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
                    try {
                        shutdownRequestedInput.checkpointer().checkpoint();
                    } catch (InvalidStateException | ShutdownException e) {
                        e.printStackTrace();
                    }
                }
            };
        }
    }
}
