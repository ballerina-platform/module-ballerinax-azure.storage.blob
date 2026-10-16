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

    private static BError toCause(Throwable cause) {
        if (cause == null) {
            return null;
        }
        String message = cause.getMessage() == null ? cause.getClass().getName() : cause.getMessage();
        return ErrorCreator.createError(StringUtils.fromString(message));
    }
}
