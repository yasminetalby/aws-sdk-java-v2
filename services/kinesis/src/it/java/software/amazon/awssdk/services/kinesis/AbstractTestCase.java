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

import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.slf4j.event.Level;
import software.amazon.awssdk.awscore.util.AwsHostNameUtils;
import software.amazon.awssdk.http.Http2Metric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.metrics.LoggingMetricPublisher;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.testutils.service.AwsTestBase;
import software.amazon.awssdk.utils.AttributeMap;

public class AbstractTestCase extends AwsTestBase {
    protected static KinesisClient client;
    protected static KinesisAsyncClient asyncClient;

    public static void init(int expectedMaxConcurrency) throws IOException {
        setUpCredentials();
        KinesisClientBuilder builder = KinesisClient.builder()
                                                    .credentialsProvider(CREDENTIALS_PROVIDER_CHAIN)
                                                    .endpointOverride(URI.create("https://34.223.45.74:443"))
                                                    .httpClient(ApacheHttpClient.builder()
                                                    .buildWithDefaults(AttributeMap.builder()
                                                                                   .put(TRUST_ALL_CERTIFICATES, Boolean.TRUE)
                                                                                   .build()));
        setEndpoint(builder);
        client = builder.build();
        asyncClient = KinesisAsyncClient.builder()
                                        .credentialsProvider(CREDENTIALS_PROVIDER_CHAIN)
                                        .overrideConfiguration(c -> c.addMetricPublisher(new MetricPublisher() {
                                            @Override
                                            public void publish(MetricCollection metricCollection) {
                                                List<Integer> availableConcurrency =
                                                    metricCollection.metricValues(HttpMetric.AVAILABLE_CONCURRENCY);
                                                List<Integer> leasedConcurrency =
                                                    metricCollection.metricValues(HttpMetric.LEASED_CONCURRENCY);

                                                if (!leasedConcurrency.isEmpty()) {
                                                    int leasedConcurrencyN = leasedConcurrency.get(0);
                                                    if (leasedConcurrencyN > expectedMaxConcurrency) {
                                                        System.out.println("Available concurrency: " +
                                                                           availableConcurrency.stream()
                                                                                               .map(Object::toString)
                                                                                               .collect(Collectors.joining(", ")) +
                                                                           "; Leased Concurrency: " +
                                                                           leasedConcurrency.stream()
                                                                                            .map(Object::toString)
                                                                                            .collect(Collectors.joining(", ")));
                                                    }
                                                }
                                                metricCollection.children().forEach(this::publish);
                                            }

                                            @Override
                                            public void close() {
                                            }
                                        }))
                                        .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                                                                                  .maxConcurrency(expectedMaxConcurrency + 10))
                                        .build();
    }

    private static void setEndpoint(KinesisClientBuilder builder) throws IOException {
        File endpointOverrides = new File(
                new File(System.getProperty("user.home")),
                ".aws/awsEndpointOverrides.properties"
        );

        if (endpointOverrides.exists()) {
            Properties properties = new Properties();
            properties.load(new FileInputStream(endpointOverrides));

            String endpoint = properties.getProperty("kinesis.endpoint");

            if (endpoint != null) {
                Region region = AwsHostNameUtils.parseSigningRegion(endpoint, "kinesis")
                                                .orElseThrow(() -> new IllegalArgumentException("Unknown region for endpoint. " +
                                                                                                endpoint));
                builder.region(region)
                       .endpointOverride(URI.create(endpoint));
            }
        }
    }
}
