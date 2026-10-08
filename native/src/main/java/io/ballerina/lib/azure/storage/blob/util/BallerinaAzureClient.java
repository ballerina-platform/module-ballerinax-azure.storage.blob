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
     * Normalizes a container-relative blob path for the SDK: strips a leading slash and
     * rejects an empty result.
     *
     * @param path the slash-delimited blob path
     * @return the blob name the wire carries, without a leading slash
     */
    public static String blobPath(BString path) {
        String p = path.getValue().strip();
        while (p.startsWith("/")) {
            p = p.substring(1);
        }
        if (p.isEmpty()) {
            throw BlobErrorCreator.clientError("the path must name a blob", null);
        }
        return p;
    }
}
