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

package software.amazon.awssdk.internal.http;

import static software.amazon.awssdk.utils.CollectionUtils.deepCopyMap;
import static software.amazon.awssdk.utils.CollectionUtils.unmodifiableMapOfLists;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import software.amazon.awssdk.utils.Lazy;

public class LowCopyListMap {
    private Map<String, List<String>> values = new LinkedHashMap<>();
    private boolean snapshotVended = false;

    public Supplier<Map<String, List<String>>> snapshot() {
        this.snapshotVended = true;
        Map<String, List<String>> snapshotValues = this.values;
        return new Lazy<>(() -> unmodifiableMapOfLists(snapshotValues))::getValue;
    }

    public Map<String, List<String>> values() {
        return unmodifiableMapOfLists(values);
    }

    public void set(Map<String, List<String>> values) {
        this.values = deepCopyMap(values);
        this.snapshotVended = false;
    }

    public void put(String key, List<String> values) {
        copyValuesIfSnapshotVended();
        this.values.put(key, new ArrayList<>(values));
    }

    public void add(String key, String value) {
        copyValuesIfSnapshotVended();
        this.values.computeIfAbsent(key, k -> new ArrayList<>(1)).add(value);
    }

    public void remove(String key) {
        copyValuesIfSnapshotVended();
        this.values.remove(key);
    }

    public void clear() {
        if (snapshotVended) {
            this.values = new LinkedHashMap<>();
        } else {
            this.values.clear();
        }
    }

    private void copyValuesIfSnapshotVended() {
        if (snapshotVended) {
            this.values = deepCopyMap(this.values);
            this.snapshotVended = true;
        }
    }
}
