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

import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.queue.models.QueueStorageException;
import io.ballerina.runtime.api.values.BError;

import java.util.Set;

/**
 * Maps an Azure {@link BlobStorageException} to a typed Ballerina error, keyed on the Azure error
 * code string rather than the HTTP status alone: an archived blob, a blob-type mismatch and a
 * state conflict are all HTTP 409 but map to different types.
 */
public final class ErrorMapper {

    private static final Set<String> NOT_FOUND_CODES =
            Set.of("BlobNotFound", "ContainerNotFound", "ResourceNotFound", "QueueNotFound", "MessageNotFound");
    private static final Set<String> ARCHIVED_CODES = Set.of("BlobArchived", "BlobBeingRehydrated");
    private static final Set<String> INVALID_BLOB_TYPE_CODES = Set.of("InvalidBlobType");
    private static final Set<String> CONFLICT_CODES =
            Set.of("ContainerAlreadyExists", "BlobAlreadyExists", "ContainerBeingDeleted", "SnapshotsPresent",
                    "PendingCopyOperation", "NoPendingCopyOperation", "CopyIdMismatch", "BlockCountExceedsLimit",
                    "LeaseAlreadyPresent", "LeaseAlreadyBroken", "LeaseNotPresentWithLeaseOperation",
                    "LeaseIdMismatchWithLeaseOperation", "LeaseIsBreakingAndCannotBeAcquired",
                    "LeaseIsBreakingAndCannotBeChanged", "LeaseIsBrokenAndCannotBeRenewed");
    private static final Set<String> AUTHORIZATION_CODES =
            Set.of("AuthenticationFailed", "AuthorizationFailure", "AuthorizationPermissionMismatch",
                    "AuthorizationSourceIPMismatch", "AuthorizationProtocolMismatch", "AuthorizationServiceMismatch",
                    "AuthorizationResourceTypeMismatch", "InsufficientAccountPermissions", "AccountIsDisabled",
                    "UnauthorizedBlobOverwrite");
    private static final Set<String> PRECONDITION_CODES =
            Set.of("LeaseIdMissing", "LeaseIdMismatchWithBlobOperation", "LeaseIdMismatchWithContainerOperation",
                    "LeaseNotPresentWithBlobOperation", "LeaseNotPresentWithContainerOperation", "LeaseLost",
                    "ConditionNotMet", "TargetConditionNotMet", "SourceConditionNotMet",
                    "InfiniteLeaseDurationRequired");
    private static final Set<String> RANGE_CODES = Set.of("InvalidRange", "InvalidPageRange");

    private ErrorMapper() {
    }

    /**
     * Converts a service exception into the matching typed Ballerina error.
     *
     * @param e the Azure service exception
     * @return the Ballerina error
     */
    public static BError toBError(BlobStorageException e) {
        String code = e.getErrorCode() == null ? "" : e.getErrorCode().toString();
        String message = e.getServiceMessage() == null ? e.getMessage() : e.getServiceMessage();
        return BlobErrorCreator.storageError(typeName(code), message, e.getStatusCode(), code, e);
    }

    /**
     * Converts a queue service exception into the matching typed Ballerina error; the queue
     * codes share the blob catalogue's names where they overlap.
     *
     * @param e the Azure queue service exception
     * @return the Ballerina error
     */
    public static BError toBError(QueueStorageException e) {
        String code = e.getErrorCode() == null ? "" : e.getErrorCode().toString();
        String message = e.getServiceMessage() == null ? e.getMessage() : e.getServiceMessage();
        return BlobErrorCreator.storageError(typeName(code), message, e.getStatusCode(), code, e);
    }

    private static String typeName(String code) {
        if (NOT_FOUND_CODES.contains(code)) {
            return "NotFoundError";
        }
        if (ARCHIVED_CODES.contains(code)) {
            return "ArchivedBlobError";
        }
        if (INVALID_BLOB_TYPE_CODES.contains(code)) {
            return "InvalidBlobTypeError";
        }
        if (CONFLICT_CODES.contains(code)) {
            return "ConflictError";
        }
        if (AUTHORIZATION_CODES.contains(code)) {
            return "AuthorizationError";
        }
        if (PRECONDITION_CODES.contains(code)) {
            return "PreconditionFailedError";
        }
        if (RANGE_CODES.contains(code)) {
            return "RangeNotSatisfiableError";
        }
        return "ServiceError";
    }
}
