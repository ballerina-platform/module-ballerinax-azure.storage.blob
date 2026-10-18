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

import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

/**
 * Creates the typed Ballerina errors declared in {@code errors.bal}. Service-raised errors carry
 * a {@code ServiceErrorDetail} record; client-side failures are the generic root {@code Error}
 * with no detail. The type name string must match the Ballerina error type exactly.
 */
public final class BlobErrorCreator {

    private static final String GENERIC_ERROR = "Error";
    private static final String SERVICE_ERROR_DETAIL = "ServiceErrorDetail";
    private static final BString HTTP_STATUS = StringUtils.fromString("httpStatus");
    private static final BString ERROR_CODE = StringUtils.fromString("errorCode");
    private static final String CONTENT_BINDING_ERROR = "ContentBindingError";
    private static final String CONTENT_BINDING_ERROR_DETAIL = "ContentBindingErrorDetail";
    private static final BString CONTAINER_NAME = StringUtils.fromString("containerName");
    private static final BString PATH = StringUtils.fromString("path");
    private static final BString CONTENT = StringUtils.fromString("content");

    private BlobErrorCreator() {
    }

    /**
     * Creates a typed error for a failure returned by the Azure service.
     *
     * @param typeName   the Ballerina error type name
     * @param message    the human-readable message
     * @param httpStatus the HTTP status returned by the service
     * @param errorCode  the Azure error code
     * @param cause      the originating Java exception
     * @return the Ballerina error
     */
    public static BError storageError(String typeName, String message, int httpStatus, String errorCode,
                                      Throwable cause) {
        BMap<BString, Object> detail = ValueCreator.createRecordValue(ModuleUtils.getModule(), SERVICE_ERROR_DETAIL);
        detail.put(HTTP_STATUS, (long) httpStatus);
        detail.put(ERROR_CODE, StringUtils.fromString(errorCode));
        return ErrorCreator.createError(ModuleUtils.getModule(), typeName,
                StringUtils.fromString(message == null ? "" : message), toCause(cause), detail);
    }

    /**
     * Creates a client-side error as the generic root {@code Error}. No server exchange
     * produced it, so it carries no detail.
     *
     * @param message the human-readable message
     * @param cause   the originating Java exception
     * @return the Ballerina error
     */
    public static BError clientError(String message, Throwable cause) {
        return ErrorCreator.createError(ModuleUtils.getModule(), GENERIC_ERROR,
                StringUtils.fromString(message == null ? "" : message), toCause(cause), null);
    }

    /**
     * Creates the listener's {@code ContentBindingError}: the blob whose content failed to bind,
     * with the content itself only when it was already read into memory.
     *
     * @param message       the human-readable message
     * @param cause         the binding failure
     * @param containerName the blob's container
     * @param path          the blob's container-relative path
     * @param content       the content bytes, or {@code null} when nothing was read into memory
     * @return the Ballerina error
     */
    public static BError contentBindingError(String message, BError cause, String containerName, String path,
                                             byte[] content) {
        BMap<BString, Object> detail = ValueCreator.createRecordValue(ModuleUtils.getModule(),
                CONTENT_BINDING_ERROR_DETAIL);
        detail.put(CONTAINER_NAME, StringUtils.fromString(containerName));
        detail.put(PATH, StringUtils.fromString(path));
        if (content != null) {
            detail.put(CONTENT, ValueCreator.createReadonlyArrayValue(content));
        }
        return ErrorCreator.createError(ModuleUtils.getModule(), CONTENT_BINDING_ERROR,
                StringUtils.fromString(message == null ? "" : message), cause, detail);
    }

    /**
     * Re-creates an error of the same type and detail with another message, for the places the
     * listener prefixes context to a service's own description.
     *
     * @param original the error
     * @param message  the new message
     * @return the Ballerina error
     */
    @SuppressWarnings("unchecked")
    public static BError withMessage(BError original, String message) {
        Object details = original.getDetails();
        return ErrorCreator.createError(ModuleUtils.getModule(), original.getType().getName(),
                StringUtils.fromString(message), original.getCause(),
                details instanceof BMap ? (BMap<BString, Object>) details : null);
    }

    private static BError toCause(Throwable cause) {
        if (cause == null) {
            return null;
        }
        String message = cause.getMessage() == null ? cause.getClass().getName() : cause.getMessage();
        return ErrorCreator.createError(StringUtils.fromString(message));
    }
}
