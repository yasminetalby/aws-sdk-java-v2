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

package software.amazon.awssdk.http;

import static software.amazon.awssdk.utils.CollectionUtils.deepCopyMap;
import static software.amazon.awssdk.utils.CollectionUtils.deepUnmodifiableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import software.amazon.awssdk.annotations.Immutable;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.awssdk.utils.Lazy;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.awssdk.utils.ToString;
import software.amazon.awssdk.utils.Validate;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

/**
 * Internal implementation of {@link SdkHttpFullRequest}, buildable via {@link SdkHttpFullRequest#builder()}. Provided to HTTP
 * implementation to execute a request.
 */
@SdkInternalApi
@Immutable
final class DefaultSdkHttpFullRequest implements SdkHttpFullRequest {
    private final String protocol;
    private final String host;
    private final Integer port;
    private final String path;
    private final Map<String, List<String>> queryParametersFromBuilder;
    private final Lazy<Map<String, List<String>>> queryParameters;
    private final SdkHttpMethod httpMethod;
    private final Map<String, List<String>> headersFromBuilder;
    private final Lazy<Map<String, List<String>>> headers;
    private final ContentStreamProvider contentStreamProvider;

    private DefaultSdkHttpFullRequest(Builder builder) {
        this.protocol = standardizeProtocol(builder.protocol);
        this.host = Validate.paramNotNull(builder.host, "host");
        this.port = standardizePort(builder.port);
        this.path = standardizePath(builder.path);
        this.httpMethod = Validate.paramNotNull(builder.httpMethod, "method");
        this.contentStreamProvider = builder.contentStreamProvider;

        this.queryParametersFromBuilder = builder.queryParameters;
        if (builder.queryParametersAreFromToBuilder) {
            this.queryParameters = Lazy.withValue(queryParametersFromBuilder);
        } else {
            this.queryParameters =
                new Lazy<>(() -> deepUnmodifiableMap(queryParametersFromBuilder));
        }

        this.headersFromBuilder = builder.headers;
        if (builder.headersAreFromToBuilder) {
            this.headers = Lazy.withValue(headersFromBuilder);
        } else {
            this.headers = new Lazy<>(() -> deepUnmodifiableMap(headersFromBuilder,
                                                                () -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER)));
        }
    }

    private String standardizeProtocol(String protocol) {
        Validate.paramNotNull(protocol, "protocol");

        String standardizedProtocol = StringUtils.lowerCase(protocol);
        Validate.isTrue(standardizedProtocol.equals("http") || standardizedProtocol.equals("https"),
                        "Protocol must be 'http' or 'https', but was %s", protocol);

        return standardizedProtocol;
    }

    private String standardizePath(String path) {
        if (StringUtils.isEmpty(path)) {
            return "";
        }

        StringBuilder standardizedPath = new StringBuilder();

        // Path must always start with '/'
        if (!path.startsWith("/")) {
            standardizedPath.append('/');
        }

        standardizedPath.append(path);

        return standardizedPath.toString();
    }

    private Integer standardizePort(Integer port) {
        Validate.isTrue(port == null || port >= -1,
                        "Port must be positive (or null/-1 to indicate no port), but was '%s'", port);

        if (port != null && port == -1) {
            return null;
        }

        return port;
    }

    @Override
    public String protocol() {
        return protocol;
    }

    @Override
    public String host() {
        return host;
    }

    @Override
    public int port() {
        return Optional.ofNullable(port).orElseGet(() -> SdkHttpUtils.standardPort(protocol()));
    }

    @Override
    public Map<String, List<String>> headers() {
        return headers.getValue();
    }

    @Override
    public List<String> matchingHeaders(String header) {
        return Collections.unmodifiableList(headersFromBuilder.getOrDefault(header, Collections.emptyList()));
    }

    @Override
    public Optional<String> firstMatchingHeader(String headerName) {
        List<String> headers = headersFromBuilder.get(headerName);
        if (headers == null || headers.isEmpty()) {
            return Optional.empty();
        }

        String header = headers.get(0);
        if (StringUtils.isEmpty(header)) {
            return Optional.empty();
        }

        return Optional.of(header);
    }

    @Override
    public Optional<String> firstMatchingHeader(Collection<String> headersToFind) {
        for (String headerName : headersToFind) {
            Optional<String> header = firstMatchingHeader(headerName);
            if (header.isPresent()) {
                return header;
            }
        }

        return Optional.empty();
    }

    @Override
    public void forEachHeader(BiConsumer<? super String, ? super List<String>> consumer) {
        headersFromBuilder.forEach((k, v) -> consumer.accept(k, Collections.unmodifiableList(v)));
    }

    @Override
    public int numHeaders() {
        return headersFromBuilder.size();
    }

    @Override
    public String encodedPath() {
        return path;
    }

    @Override
    public Map<String, List<String>> rawQueryParameters() {
        return queryParameters.getValue();
    }

    @Override
    public SdkHttpMethod method() {
        return httpMethod;
    }

    @Override
    public Optional<ContentStreamProvider> contentStreamProvider() {
        return Optional.ofNullable(contentStreamProvider);
    }

    @Override
    public SdkHttpFullRequest.Builder toBuilder() {
        return new Builder(this);
    }

    @Override
    public String toString() {
        return ToString.builder("DefaultSdkHttpFullRequest")
                       .add("httpMethod", httpMethod)
                       .add("protocol", protocol)
                       .add("host", host)
                       .add("port", port)
                       .add("encodedPath", path)
                       .add("headers", headers.getValue().keySet())
                       .add("queryParameters", queryParameters.getValue().keySet())
                       .build();
    }

    /**
     * Builder for a {@link DefaultSdkHttpFullRequest}.
     */
    static final class Builder implements SdkHttpFullRequest.Builder {
        private String protocol;
        private String host;
        private Integer port;
        private String path;

        private boolean queryParametersAreFromToBuilder;
        private Map<String, List<String>> queryParameters;

        private SdkHttpMethod httpMethod;

        private boolean headersAreFromToBuilder;
        private Map<String, List<String>> headers;

        private ContentStreamProvider contentStreamProvider;

        Builder() {
            queryParameters = new LinkedHashMap<>();
            queryParametersAreFromToBuilder = false;
            headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            headersAreFromToBuilder = false;
        }

        Builder(DefaultSdkHttpFullRequest request) {
            queryParameters = request.queryParametersFromBuilder;
            queryParametersAreFromToBuilder = true;
            headers = request.headersFromBuilder;
            headersAreFromToBuilder = true;
            protocol = request.protocol;
            host = request.host;
            port = request.port;
            path = request.path;
            httpMethod = request.httpMethod;
            contentStreamProvider = request.contentStreamProvider;
        }

        @Override
        public String protocol() {
            return protocol;
        }

        @Override
        public SdkHttpFullRequest.Builder protocol(String protocol) {
            this.protocol = protocol;
            return this;
        }

        @Override
        public String host() {
            return host;
        }

        @Override
        public SdkHttpFullRequest.Builder host(String host) {
            this.host = host;
            return this;
        }

        @Override
        public Integer port() {
            return port;
        }

        @Override
        public SdkHttpFullRequest.Builder port(Integer port) {
            this.port = port;
            return this;
        }

        @Override
        public DefaultSdkHttpFullRequest.Builder encodedPath(String path) {
            this.path = path;
            return this;
        }

        @Override
        public String encodedPath() {
            return path;
        }

        @Override
        public DefaultSdkHttpFullRequest.Builder putRawQueryParameter(String paramName, List<String> paramValues) {
            copyQueryParamsIfNeeded();
            this.queryParameters.put(paramName, new ArrayList<>(paramValues));
            return this;
        }

        @Override
        public SdkHttpFullRequest.Builder appendRawQueryParameter(String paramName, String paramValue) {
            copyQueryParamsIfNeeded();
            this.queryParameters.computeIfAbsent(paramName, k -> new ArrayList<>()).add(paramValue);
            return this;
        }

        @Override
        public DefaultSdkHttpFullRequest.Builder rawQueryParameters(Map<String, List<String>> queryParameters) {
            this.queryParameters = CollectionUtils.deepCopyMap(queryParameters);
            queryParametersAreFromToBuilder = false;
            return this;
        }

        @Override
        public Builder removeQueryParameter(String paramName) {
            copyQueryParamsIfNeeded();
            this.queryParameters.remove(paramName);
            return this;
        }

        @Override
        public Builder clearQueryParameters() {
            this.queryParameters = new LinkedHashMap<>();
            queryParametersAreFromToBuilder = false;
            return this;
        }

        private void copyQueryParamsIfNeeded() {
            if (queryParametersAreFromToBuilder) {
                queryParametersAreFromToBuilder = false;
                this.queryParameters = deepCopyMap(queryParameters);
            }
        }

        @Override
        public Map<String, List<String>> rawQueryParameters() {
            return CollectionUtils.unmodifiableMapOfLists(queryParameters);
        }

        @Override
        public DefaultSdkHttpFullRequest.Builder method(SdkHttpMethod httpMethod) {
            this.httpMethod = httpMethod;
            return this;
        }

        @Override
        public SdkHttpMethod method() {
            return httpMethod;
        }

        @Override
        public DefaultSdkHttpFullRequest.Builder putHeader(String headerName, List<String> headerValues) {
            copyHeadersIfNeeded();
            this.headers.put(headerName, new ArrayList<>(headerValues));
            return this;
        }

        @Override
        public SdkHttpFullRequest.Builder appendHeader(String headerName, String headerValue) {
            copyHeadersIfNeeded();
            this.headers.computeIfAbsent(headerName, k -> new ArrayList<>()).add(headerValue);
            return this;
        }

        @Override
        public DefaultSdkHttpFullRequest.Builder headers(Map<String, List<String>> headers) {
            this.headers = CollectionUtils.deepCopyMap(headers, () -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
            headersAreFromToBuilder = false;
            return this;
        }

        @Override
        public SdkHttpFullRequest.Builder removeHeader(String headerName) {
            copyHeadersIfNeeded();
            this.headers.remove(headerName);
            return this;
        }

        @Override
        public SdkHttpFullRequest.Builder clearHeaders() {
            this.headers = new LinkedHashMap<>();
            headersAreFromToBuilder = false;
            return this;
        }

        @Override
        public SdkHttpFullRequest.Builder putHeader(String headerName, String headerValue) {
            return SdkHttpFullRequest.Builder.super.putHeader(headerName, headerValue);
        }

        @Override
        public Map<String, List<String>> headers() {
            return CollectionUtils.unmodifiableMapOfLists(this.headers);
        }

        @Override
        public List<String> matchingHeaders(String header) {
            return Collections.unmodifiableList(headers.getOrDefault(header, Collections.emptyList()));
        }

        @Override
        public Optional<String> firstMatchingHeader(String headerName) {
            List<String> headers = this.headers.get(headerName);
            if (headers == null || headers.isEmpty()) {
                return Optional.empty();
            }

            String header = headers.get(0);
            if (StringUtils.isEmpty(header)) {
                return Optional.empty();
            }

            return Optional.of(header);
        }

        @Override
        public Optional<String> firstMatchingHeader(Collection<String> headersToFind) {
            for (String headerName : headersToFind) {
                Optional<String> header = firstMatchingHeader(headerName);
                if (header.isPresent()) {
                    return header;
                }
            }

            return Optional.empty();
        }

        @Override
        public void forEachHeader(BiConsumer<? super String, ? super List<String>> consumer) {
            headers.forEach((k, v) -> consumer.accept(k, Collections.unmodifiableList(v)));
        }

        @Override
        public int numHeaders() {
            return headers.size();
        }

        private void copyHeadersIfNeeded() {
            if (headersAreFromToBuilder) {
                headersAreFromToBuilder = false;
                this.headers = deepCopyMap(headers);
            }
        }

        @Override
        public DefaultSdkHttpFullRequest.Builder contentStreamProvider(ContentStreamProvider contentStreamProvider) {
            this.contentStreamProvider = contentStreamProvider;
            return this;
        }

        @Override
        public ContentStreamProvider contentStreamProvider() {
            return contentStreamProvider;
        }

        @Override
        public SdkHttpFullRequest.Builder copy() {
            return build().toBuilder();
        }

        @Override
        public SdkHttpFullRequest.Builder applyMutation(Consumer<SdkHttpRequest.Builder> mutator) {
            mutator.accept(this);
            return this;
        }

        @Override
        public DefaultSdkHttpFullRequest build() {
            return new DefaultSdkHttpFullRequest(this);
        }
    }

}
