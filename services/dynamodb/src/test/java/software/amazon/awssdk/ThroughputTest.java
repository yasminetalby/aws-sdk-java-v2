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

package software.amazon.awssdk;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.assertj.core.internal.bytebuddy.implementation.bytecode.Throw;
import org.junit.Test;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class ThroughputTest {
    @Test
    public void test() throws Throwable {
        DynamoDbClient client = DynamoDbClient.builder().httpClientBuilder(ApacheHttpClient.builder().maxConnections(10)).build();
        ExecutorService executor = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 100; i++) {
            System.out.println("Warmup: " + i + "%...");
            runWarmupTests(client, executor);
        }

        Duration runTime = Duration.ofMinutes(1);
        System.out.println("Executing runs... (" + runTime + " each)");
        for (int run = 0; run < 5; run++) {

            int i = 0;

            Instant start = Instant.now();
            Instant end = start.plus(runTime);
            while (Instant.now().isBefore(end)) {
                ++i;
                int v = i % 10;
                client.getItem(r -> r.tableName("millem-throughput")
                                     .key(Collections.singletonMap("key", AttributeValue.builder().s("value" + v).build())));
            }

            double tps = (double) i / runTime.getSeconds();
            System.out.println("Run " + run + " TPS: " + tps);
        }
    }

    private void runWarmupTests(DynamoDbClient client, ExecutorService executor) throws InterruptedException, ExecutionException {
        runRealTests(client, executor);
    }

    private void runRealTests(DynamoDbClient client, ExecutorService executor) throws InterruptedException, ExecutionException {
        List<Future<?>> results = new ArrayList<>();
        for (int value = 0; value < 100; value++) {
            final int v = value;
            results.add(executor.submit(() -> {
                client.getItem(r -> r.tableName("millem-throughput")
                                     .key(Collections.singletonMap("key", AttributeValue.builder().s("value" + v).build())));
            }));
        }
        for (Future<?> result : results) {
            result.get();
        }
    }
}
