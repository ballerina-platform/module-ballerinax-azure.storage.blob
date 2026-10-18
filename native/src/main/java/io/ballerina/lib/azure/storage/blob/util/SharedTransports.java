/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.azure.storage.blob.util;

import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobServiceClient;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import reactor.netty.resources.ConnectionProvider;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * The transport stacks listeners share with the {@code Caller}s they build. A listener registers
 * its stack under the one read-only {@code ClientConfiguration} instance every one of its
 * Callers is constructed from, so {@code Client} initialization can adopt the stack by the
 * configuration's identity instead of building another connection pool per Caller.
 */
public final class SharedTransports {

    // One listener's shared stack.
    public record Shared(HttpClient httpClient, ConnectionProvider provider, BlobServiceClient serviceClient,
                         String sasSignature) {
    }

    private static final Map<BMap<BString, Object>, Shared> REGISTRY =
            Collections.synchronizedMap(new IdentityHashMap<>());

    private SharedTransports() {
    }

    public static void register(BMap<BString, Object> config, Shared shared) {
        REGISTRY.put(config, shared);
    }

    public static void unregister(BMap<BString, Object> config) {
        REGISTRY.remove(config);
    }

    public static Shared lookup(BMap<BString, Object> config) {
        return REGISTRY.get(config);
    }
}
