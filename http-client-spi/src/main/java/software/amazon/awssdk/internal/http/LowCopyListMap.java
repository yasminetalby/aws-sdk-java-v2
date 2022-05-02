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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.awssdk.utils.Lazy;

@SdkInternalApi
public final class LowCopyListMap {
    private LowCopyListMap() {
    }

    public static LowCopyListMap.ForBuilder emptyHeaders() {
        return new LowCopyListMap.ForBuilder(() -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
    }

    public static LowCopyListMap.ForBuilder emptyQueryParameters() {
        return new LowCopyListMap.ForBuilder(LinkedHashMap::new);
    }

    @ThreadSafe
    public static final class ForBuildable {
        private static final long serialVersionUID = 1;

        private final Supplier<Map<String, List<String>>> mapConstructor;
        private final Lazy<Map<String, List<String>>> deeplyUnmodifiableMap;
        private final Map<String, List<String>> map;

        private ForBuildable(ForBuilder forBuilder) {
            this.mapConstructor = forBuilder.mapConstructor;
            this.map = forBuilder.map;
            this.deeplyUnmodifiableMap = new Lazy<>(() -> CollectionUtils.deepUnmodifiableMap(this.map, this.mapConstructor));
        }

        public Map<String, List<String>> forExternalRead() {
            return deeplyUnmodifiableMap.getValue();
        }

        public Map<String, List<String>> forInternalRead() {
            return map;
        }

        public ForBuilder forBuilder() {
            return new ForBuilder(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ForBuildable that = (ForBuildable) o;

            return map.equals(that.map);
        }

        @Override
        public int hashCode() {
            return map.hashCode();
        }
    }

    @NotThreadSafe
    public static final class ForBuilder {
        private static final long serialVersionUID = 1;

        private final Supplier<Map<String, List<String>>> mapConstructor;
        private final List<WeakReference<ForBuildable>> snapshots = new ArrayList<>(1);
        private Map<String, List<String>> map;

        private ForBuilder(Supplier<Map<String, List<String>>> mapConstructor) {
            this.mapConstructor = mapConstructor;
            this.map = mapConstructor.get();
        }

        private ForBuilder(ForBuildable forBuildable) {
            this.mapConstructor = forBuildable.mapConstructor;
            this.map = forBuildable.map;
            this.snapshots.add(new WeakReference<>(forBuildable));
        }

        public void clear() {
            this.map = mapConstructor.get();
            snapshots.clear();
        }

        public void setFromExternal(Map<String, List<String>> map) {
            this.map = deepCopyMap(map, mapConstructor);
            snapshots.clear();
        }

        public Map<String, List<String>> forInternalWrite() {
            for (WeakReference<ForBuildable> snapshotReference : snapshots) {
                ForBuildable snapshot = snapshotReference.get();
                if (snapshot != null) {
                    this.map = deepCopyMap(map, mapConstructor);
                    break;
                }
            }
            snapshots.clear();
            return map;
        }

        public Map<String, List<String>> forInternalRead() {
            return map;
        }

        public ForBuildable forBuildable() {
            ForBuildable newSnapshot = new ForBuildable(this);
            snapshots.add(new WeakReference<>(newSnapshot));
            return newSnapshot;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ForBuilder that = (ForBuilder) o;

            return map.equals(that.map);
        }

        @Override
        public int hashCode() {
            return map.hashCode();
        }
    }
}
