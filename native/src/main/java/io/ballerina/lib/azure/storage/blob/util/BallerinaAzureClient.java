/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.azure.storage.blob.util;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.queue.models.QueueStorageException;
import io.ballerina.lib.azure.storage.blob.observability.BlobMetricsUtil;
import io.ballerina.lib.azure.storage.blob.observability.BlobTracingUtil;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.util.function.Supplier;

/**
 * Shared plumbing for the native operations: runs the blocking SDK call off the Ballerina
 * scheduler via {@link Environment#yieldAndRun}, converts every failure to a typed Ballerina
 * error, and fetches the SDK clients stored on the Ballerina client objects.
 */
public final class BallerinaAzureClient {

    // Keys under which the SDK clients are stored on client objects.
    public static final String NATIVE_SERVICE_CLIENT = "serviceClient";
    public static final String NATIVE_CONTAINER_CLIENT = "containerClient";
    // The SAS query a SAS-authenticated client was built with; a same-account copy source carries it.
    public static final String NATIVE_SAS_SIGNATURE = "sasSignature";
    public static final String NATIVE_REMOTE_URL = "remoteUrl";
    public static final String NATIVE_PROTOCOL = "protocol";

    private static final int MAX_CAUSE_DEPTH = 8;

    private BallerinaAzureClient() {
    }

    /**
     * Runs a blocking operation body and maps its outcome to a Ballerina value.
     *
     * @param env  the Ballerina runtime environment
     * @param body the operation body; its return value is passed through verbatim
     * @return the body's result, or the mapped Ballerina error on failure
     */
    public static Object invoke(Environment env, Supplier<Object> body) {
        return env.yieldAndRun(() -> {
            try {
                return body.get();
            } catch (Exception e) {
                return mapFailure(e);
            }
        });
    }

    /**
     * Runs an operation body as {@link #invoke(Environment, Supplier)} does, tagging the call's
     * own span with the operation's identity before it runs and its outcome after.
     *
     * @param env           the Ballerina runtime environment
     * @param self          the client object the operation runs on
     * @param operationType the observability operation type, {@code get}, {@code put} or {@code manage}
     * @param path          the blob or container path the operation addresses, or {@code null}
     * @param destination   the destination path of a copy or transfer, or {@code null}
     * @param body          the operation body
     * @return the body's result, or the mapped Ballerina error on failure
     */
    public static Object invoke(Environment env, BObject self, String operationType, String path,
                                String destination, Supplier<Object> body) {
        BlobTracingUtil.sendMetricsData(env, getRemoteUrl(self), getProtocol(self), operationType, path, destination);
        return BlobTracingUtil.sendTraces(invoke(env, body), env);
    }

    /**
     * Counts bytes a client operation moved, against the client's endpoint identity.
     *
     * @param self          the client object the operation ran on
     * @param operationType {@code get} or {@code put}
     * @param bytes         the byte count
     */
    public static void reportBytes(BObject self, String operationType, long bytes) {
        BlobMetricsUtil.reportBytesTransferred(getRemoteUrl(self), getProtocol(self), BlobMetricsUtil.CONTEXT_CLIENT,
                operationType, bytes);
    }

    /**
     * Returns the blob endpoint host stored on a client object for observability.
     *
     * @param self the Ballerina client object
     * @return the host, or {@code null} when not set
     */
    public static String getRemoteUrl(BObject self) {
        return (String) self.getNativeData(NATIVE_REMOTE_URL);
    }

    /**
     * Returns the wire protocol stored on a client object for observability.
     *
     * @param self the Ballerina client object
     * @return the protocol, or {@code null} when not set
     */
    public static String getProtocol(BObject self) {
        return (String) self.getNativeData(NATIVE_PROTOCOL);
    }

    /**
     * Stores the observability identity of an endpoint on a client object.
     *
     * @param self       the Ballerina client object
     * @param accountUrl the blob service URL the client was built with
     */
    public static void storeEndpointIdentity(BObject self, String accountUrl) {
        self.addNativeData(NATIVE_REMOTE_URL, extractHost(accountUrl));
        self.addNativeData(NATIVE_PROTOCOL, extractProtocol(accountUrl));
    }

    /**
     * Extracts the host, with the port when explicit, from a URL.
     *
     * @param url the URL
     * @return the host, or {@code null} when the URL does not parse
     */
    public static String extractHost(String url) {
        try {
            java.net.URI uri = java.net.URI.create(url);
            if (uri.getHost() == null) {
                return null;
            }
            return uri.getPort() == -1 ? uri.getHost() : uri.getHost() + ":" + uri.getPort();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Extracts the scheme from a URL.
     *
     * @param url the URL
     * @return the scheme, or {@code null} when the URL does not parse
     */
    public static String extractProtocol(String url) {
        try {
            return java.net.URI.create(url).getScheme();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Maps a failure to the module's typed error: Azure service failures go through the
     * code-keyed mapper, Ballerina errors pass through, and anything else becomes the
     * generic client-side {@code Error}.
     *
     * @param e the failure
     * @return the mapped Ballerina error
     */
    public static BError mapFailure(Throwable e) {
        if (e instanceof BError bError) {
            return bError;
        }
        // A service failure raised mid-stream reaches us wrapped: the SDK's input stream rethrows
        // it inside a RuntimeException. Its origin is still the service, so it keeps its status
        // and error code rather than collapsing to the client-side Error. Bounded and
        // cycle-guarded, since a cause chain can loop.
        Throwable current = e;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (current instanceof BlobStorageException storageException) {
                return ErrorMapper.toBError(storageException);
            }
            if (current instanceof QueueStorageException queueException) {
                return ErrorMapper.toBError(queueException);
            }
            Throwable cause = current.getCause();
            current = cause == current ? null : cause;
        }
        return BlobErrorCreator.clientError(describe(e), e);
    }

    /** Builds a human-readable message for an unexpected local exception. */
    public static String describe(Throwable t) {
        return t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
    }

    /**
     * Returns the {@code BlobServiceClient} stored on a client object.
     *
     * @param self the Ballerina client object
     * @return the SDK service client
     */
    public static BlobServiceClient getServiceClient(BObject self) {
        return (BlobServiceClient) self.getNativeData(NATIVE_SERVICE_CLIENT);
    }

    /**
     * Returns the {@code BlobContainerClient} stored on a container-bound client object.
     *
     * @param self the Ballerina client object
     * @return the SDK container client
     */
    public static BlobContainerClient getContainerClient(BObject self) {
        return (BlobContainerClient) self.getNativeData(NATIVE_CONTAINER_CLIENT);
    }

    /**
     * Normalizes a container-relative blob path for the SDK: removes leading slashes and
     * rejects an empty result. Everything else, blanks included, is part of the blob name.
     *
     * @param path the slash-delimited blob path
     * @return the blob name the wire carries, without a leading slash
     */
    public static String blobPath(BString path) {
        String p = path.getValue();
        while (p.startsWith("/")) {
            p = p.substring(1);
        }
        if (p.isEmpty()) {
            throw BlobErrorCreator.clientError("the path must name a blob", null);
        }
        return p;
    }
}
