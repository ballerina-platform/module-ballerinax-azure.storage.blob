// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

# Structured detail carried by every error the Azure service raised.
public type ServiceErrorDetail record {|
    # The HTTP status code returned by Azure
    int httpStatus;
    # The Azure error code (e.g. `BlobNotFound`)
    string errorCode;
|};

# The root error type. A client-side failure is this type and carries no detail, except the
# listener's `ContentBindingError`; an error raised by the Azure service is a `ServiceError`.
public type Error distinct error;

# An error raised by the Azure service, carrying the HTTP status and the Azure error code. A
# code that maps to no subtype stays this type.
public type ServiceError distinct (Error & error<ServiceErrorDetail>);

# The requested container, blob, queue or message was not found (HTTP 404).
public type NotFoundError distinct ServiceError;

# The operation conflicts with the current state of the resource, e.g. creating a container
# that already exists, or deleting a blob whose snapshots were not directed (HTTP 409).
public type ConflictError distinct ServiceError;

# Authentication or authorization failed, e.g. an invalid key or insufficient SAS
# permissions (HTTP 403).
public type AuthorizationError distinct ServiceError;

# A precondition such as a lease-id requirement on a write was not met (HTTP 412).
public type PreconditionFailedError distinct ServiceError;

# The requested byte range cannot be satisfied for the target blob, including a misaligned
# page range (HTTP 416).
public type RangeNotSatisfiableError distinct ServiceError;

# The blob's content is unavailable because of the archive tier, either still archived or
# currently rehydrating (HTTP 409).
public type ArchivedBlobError distinct ServiceError;

# The operation applies to another blob type, e.g. appending to a block blob or writing
# pages to an append blob (HTTP 409).
public type InvalidBlobTypeError distinct ServiceError;

# Returned by a handler to move the message to the poison queue at once.
public type DeadLetterError distinct Error;

# The detail of a `RetryError`.
public type RetryErrorDetail record {|
    # The least time, in seconds, before the message is redelivered
    decimal delay;
|};

# Returned by a handler to have the message redelivered after `delay`.
public type RetryError distinct (Error & error<RetryErrorDetail>);

# The blob whose content failed to bind.
public type ContentBindingErrorDetail record {|
    # The blob's container
    string containerName;
    # The blob's container-relative path
    string path;
    # The content, when it was already read into memory
    byte[] content?;
|};

# A blob's content could not be bound to the handler's parameter type.
public type ContentBindingError distinct (Error & error<ContentBindingErrorDetail>);
