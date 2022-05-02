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

package software.amazon.awssdk.services;

import org.junit.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;

public class PerfTest {
    @Test
    public void test() {
        S3Client s3 = S3Client.create();
        warm(s3);
        run(s3);
    }

    private void warm(S3Client s3) {
        for (int i = 0; i < 1000; i++) {
            s3.putObject(r -> r.bucket("millem-perf-testing").key("foo"), RequestBody.fromString("Hello!"));
            s3.getObject(r -> r.bucket("millem-perf-testing").key("foo"), ResponseTransformer.toBytes());
        }
    }

    private void run(S3Client s3) {
        for (int i = 0; i < 1000; i++) {
            s3.putObject(r -> r.bucket("millem-perf-testing").key("foo"), RequestBody.fromString("Hello!"));
            s3.getObject(r -> r.bucket("millem-perf-testing").key("foo"), ResponseTransformer.toBytes());
        }
    }
}
