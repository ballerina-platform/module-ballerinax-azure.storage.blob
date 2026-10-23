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

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * The error-code table of the mapper (spec 6): every catalogued code maps to its leaf type, and
 * anything else, a missing code included, stays a plain {@code ServiceError}.
 */
public class ErrorMapperTest {

    @DataProvider
    public Object[][] codes() {
        return new Object[][] {
            {"BlobNotFound", "NotFoundError"}, {"ContainerNotFound", "NotFoundError"},
            {"ResourceNotFound", "NotFoundError"}, {"QueueNotFound", "NotFoundError"},
            {"MessageNotFound", "NotFoundError"},
            {"BlobArchived", "ArchivedBlobError"}, {"BlobBeingRehydrated", "ArchivedBlobError"},
            {"InvalidBlobType", "InvalidBlobTypeError"},
            {"ContainerAlreadyExists", "ConflictError"}, {"BlobAlreadyExists", "ConflictError"},
            {"ContainerBeingDeleted", "ConflictError"}, {"SnapshotsPresent", "ConflictError"},
            {"PendingCopyOperation", "ConflictError"}, {"NoPendingCopyOperation", "ConflictError"},
            {"CopyIdMismatch", "ConflictError"}, {"BlockCountExceedsLimit", "ConflictError"},
            {"LeaseAlreadyPresent", "ConflictError"}, {"LeaseAlreadyBroken", "ConflictError"},
            {"LeaseNotPresentWithLeaseOperation", "ConflictError"},
            {"LeaseIdMismatchWithLeaseOperation", "ConflictError"},
            {"LeaseIsBreakingAndCannotBeAcquired", "ConflictError"},
            {"LeaseIsBreakingAndCannotBeChanged", "ConflictError"},
            {"LeaseIsBrokenAndCannotBeRenewed", "ConflictError"},
            {"AuthenticationFailed", "AuthorizationError"}, {"AuthorizationFailure", "AuthorizationError"},
            {"AuthorizationPermissionMismatch", "AuthorizationError"},
            {"AuthorizationSourceIPMismatch", "AuthorizationError"},
            {"AuthorizationProtocolMismatch", "AuthorizationError"},
            {"AuthorizationServiceMismatch", "AuthorizationError"},
            {"AuthorizationResourceTypeMismatch", "AuthorizationError"},
            {"InsufficientAccountPermissions", "AuthorizationError"}, {"AccountIsDisabled", "AuthorizationError"},
            {"UnauthorizedBlobOverwrite", "AuthorizationError"},
            {"LeaseIdMissing", "PreconditionFailedError"},
            {"LeaseIdMismatchWithBlobOperation", "PreconditionFailedError"},
            {"LeaseIdMismatchWithContainerOperation", "PreconditionFailedError"},
            {"LeaseNotPresentWithBlobOperation", "PreconditionFailedError"},
            {"LeaseNotPresentWithContainerOperation", "PreconditionFailedError"},
            {"LeaseLost", "PreconditionFailedError"}, {"ConditionNotMet", "PreconditionFailedError"},
            {"TargetConditionNotMet", "PreconditionFailedError"},
            {"SourceConditionNotMet", "PreconditionFailedError"},
            {"InfiniteLeaseDurationRequired", "PreconditionFailedError"},
            {"InvalidRange", "RangeNotSatisfiableError"}, {"InvalidPageRange", "RangeNotSatisfiableError"},
        };
    }

    @Test(dataProvider = "codes")
    public void everyCataloguedCodeMapsToItsLeaf(String code, String typeName) {
        assertEquals(ErrorMapper.typeName(code), typeName);
    }

    @Test
    public void anyOtherCodeStaysAServiceError() {
        assertEquals(ErrorMapper.typeName("InternalError"), "ServiceError");
        assertEquals(ErrorMapper.typeName("blobnotfound"), "ServiceError", "codes match case-sensitively");
        assertEquals(ErrorMapper.typeName(""), "ServiceError");
    }
}
