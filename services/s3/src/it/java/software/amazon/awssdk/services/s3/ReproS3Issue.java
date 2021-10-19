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

package software.amazon.awssdk.services.s3;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.checksums.Md5Checksum;
import software.amazon.awssdk.http.nio.netty.BadBoyCatchingPublisher;
import software.amazon.awssdk.services.s3.checksums.ByteBufferOrderVerifier;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.utils.BinaryUtils;

public class ReproS3Issue {
    @Test
    public void repro() throws IOException {
        S3AsyncClient s3 = S3AsyncClient.builder()
                                        .endpointOverride(URI.create("http://s3.us-west-2.amazonaws.com"))
                                        .build();
        String bucketKey = "millem-test-bucket-12";

        // try {
        //     s3.createBucket(r -> r.bucket(bucketKey)).join();
        // } catch (Exception e) {
        // }

        byte[] expectedOutputBytes = new byte[1_000_000];
        byte[] outputPattern = "X  ".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < expectedOutputBytes.length; i++) {
            expectedOutputBytes[i] = outputPattern[i % outputPattern.length];
        }
        ByteBufferOrderVerifier.initialize(expectedOutputBytes);
        BadBoyCatchingPublisher.initialize(expectedOutputBytes);

        Md5Checksum md5Checksum = new Md5Checksum();
        md5Checksum.update(expectedOutputBytes, 0, expectedOutputBytes.length);
        System.out.println(BinaryUtils.toHex(md5Checksum.getChecksumBytes()));

        for (int iteration = 1; iteration < 1000; iteration++) {
            List<byte[]> output = new ArrayList<>();
            ByteArrayInputStream bytesInput = new ByteArrayInputStream(expectedOutputBytes);
            while (true) {
                byte[] buffer = new byte[ThreadLocalRandom.current().nextInt(0, 1000)];
                int read = bytesInput.read(buffer);
                if (read < 0) {
                    break;
                }
                else if (read < buffer.length) {
                    buffer = Arrays.copyOfRange(buffer, 0, read);
                }

                output.add(buffer);
            }

            System.out.println("Uploading " + output.size() + " chunks");

            s3.putObject(r -> r.bucket(bucketKey).key(bucketKey).contentLength((long) expectedOutputBytes.length),
                         AsyncRequestBody.fromPublisher(new BadBoyCatchingPublisher(Flux.fromIterable(output).map(ByteBuffer::wrap), "1")))
              .join();

            System.out.println("Downloading " + output.size() + " chunks");

            ResponseBytes<GetObjectResponse> response =
                s3.getObject(r -> r.bucket(bucketKey).key(bucketKey),
                             AsyncResponseTransformer.toBytes()).join();

            assertThat(response.asByteArrayUnsafe()).isEqualTo(expectedOutputBytes);
        }
    }
}
