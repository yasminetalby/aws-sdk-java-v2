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

package software.amazon.awssdk.codegen.model.rules.endpoints;

import java.util.List;

public class EndpointTestSuiteModel {

    private List<EndpointTestModel> testCases;

    public List<EndpointTestModel> getTestCases() {
        return testCases;
    }

    public void setTestCases(List<EndpointTestModel> testCases) {
        this.testCases = testCases;
    }
}
