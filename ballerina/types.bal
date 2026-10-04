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

import ballerina/time;

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

# Shared Key authentication with the storage account name and one of its access keys.
public type SharedKeyConfig record {|
    # The storage account name, used to sign requests and to derive the service URL
    string accountName;
    # A base64-encoded access key of the storage account
    string accountKey;
    # The blob service endpoint URL, including the scheme. Omit to use the default
    # `https://{accountName}.blob.core.windows.net`
    string serviceUrl?;
|};

# Shared Access Signature (SAS) authentication with a bare SAS token, as issued by
# `az storage container generate-sas` or the SAS-generation operations.
public type SasConfig record {|
    # The name of the storage account the token belongs to (determines the service URL)
    string accountName;
    # A SAS token scoped to the required resources and permissions
    string sasToken;
|};

# Shared Access Signature (SAS) authentication with a full SAS URL, which carries the service
# URL and the SAS token in one string, as issued by the Azure portal.
public type SasUrlConfig record {|
    # A full blob-service SAS URL, including the scheme and the SAS query string
    # (e.g. `https://{account}.blob.core.windows.net/?sv=...&sig=...`)
    string sasUrl;
|};

# Connection-string authentication. The connection string carries the account name, the
# credential (an account key or a SAS token), and the service endpoints.
public type ConnectionStringConfig record {|
    # An Azure Storage connection string, as issued by the Azure portal, the Azure CLI, or
    # infrastructure tooling
    string connectionString;
|};

# The Microsoft Entra ID credential chains.
public enum EntraIdKind {
    # The default credential chain: the environment, a managed identity, then developer
    # sign-ins (Azure CLI, IDE accounts), in turn
    DEFAULT_AZURE_CREDENTIAL = "default",
    # An Azure managed identity, for workloads on Azure compute (VMs, App Service, AKS, Functions)
    MANAGED_IDENTITY = "managed-identity"
}

# Microsoft Entra ID authentication through a credential chain.
public type EntraIdChainConfig record {|
    # The credential chain to use
    EntraIdKind kind;
    # The storage account name (determines the service URL unless `serviceUrl` overrides it)
    string accountName;
    # The client ID of a user-assigned managed identity. Omit to use the system-assigned one
    string clientId?;
    # The blob service endpoint URL, including the scheme. Omit to use the default
    # `https://{accountName}.blob.core.windows.net`
    string serviceUrl?;
|};

# Microsoft Entra ID authentication as a service principal with a client secret.
public type ClientSecretConfig record {|
    # The storage account name (determines the service URL unless `serviceUrl` overrides it)
    string accountName;
    # The Entra ID tenant the service principal belongs to
    string tenantId;
    # The application (client) ID of the service principal
    string clientId;
    # The client secret issued for the service principal
    string clientSecret;
    # The blob service endpoint URL, including the scheme. Omit to use the default
    # `https://{accountName}.blob.core.windows.net`
    string serviceUrl?;
|};

# Microsoft Entra ID authentication as a service principal with a client certificate.
public type ClientCertificateConfig record {|
    # The storage account name (determines the service URL unless `serviceUrl` overrides it)
    string accountName;
    # The Entra ID tenant the service principal belongs to
    string tenantId;
    # The application (client) ID of the service principal
    string clientId;
    # Path to the certificate file: a PEM file, or a PFX file when `certificatePassword` is set
    string certificatePath;
    # The password protecting a PFX certificate. Omit for an unencrypted PEM certificate
    string certificatePassword?;
    # The blob service endpoint URL, including the scheme. Omit to use the default
    # `https://{accountName}.blob.core.windows.net`
    string serviceUrl?;
|};

# Microsoft Entra ID workload-identity authentication, for Kubernetes workloads federated
# with Entra ID.
public type WorkloadIdentityConfig record {|
    # The storage account name (determines the service URL unless `serviceUrl` overrides it)
    string accountName;
    # The Entra ID tenant the workload identity belongs to
    string tenantId;
    # The application (client) ID of the federated identity
    string clientId;
    # Path to the projected service-account token file
    string tokenFilePath;
    # The blob service endpoint URL, including the scheme. Omit to use the default
    # `https://{accountName}.blob.core.windows.net`
    string serviceUrl?;
|};

# Microsoft Entra ID authentication: a credential chain, or a service principal identified by
# its client secret, its certificate, or a federated token.
public type EntraIdConfig EntraIdChainConfig|ClientSecretConfig|ClientCertificateConfig|WorkloadIdentityConfig;

# The authentication configuration: exactly one credential-artifact record, selected by the
# fields present.
public type AuthConfig SharedKeyConfig|SasConfig|SasUrlConfig|ConnectionStringConfig|EntraIdConfig;

// ---------------------------------------------------------------------------
// Client configuration
// ---------------------------------------------------------------------------

# How the delay between retries grows.
public enum RetryPolicyType {
    # The delay grows exponentially with each try
    EXPONENTIAL = "exponential",
    # The delay is the same before every try
    FIXED_INTERVAL = "fixed"
}

# The proxy protocol.
public enum ProxyType {
    # An HTTP proxy
    HTTP,
    # A SOCKS4 proxy
    SOCKS4,
    # A SOCKS5 proxy
    SOCKS5
}

# Retry behaviour for service requests. Omit the record to keep the connector's defaults.
public type RetryConfig record {|
    # How the delay between tries grows
    RetryPolicyType retryPolicyType = EXPONENTIAL;
    # The maximum number of tries, counting the first attempt
    int maxTries = 4;
    # The timeout applied to each individual try, in seconds
    decimal tryTimeoutSeconds = 60;
    # The base delay between tries, in seconds
    decimal retryDelaySeconds = 4;
    # The upper bound on the delay between tries, in seconds
    decimal maxRetryDelaySeconds = 120;
    # A secondary endpoint to retry reads against, for geo-redundant accounts
    string secondaryHostUrl?;
|};

# A PKCS12 or JKS certificate store and the password that opens it.
public type CertStore record {|
    # Path to the store file
    string path;
    # The store password
    string password;
|};

# A certificate and private key pair identifying the client for mutual TLS.
public type CertKey record {|
    # Path to the client certificate file
    string certFile;
    # Path to the client private key file
    string keyFile;
    # The password protecting the private key, when it is encrypted
    string keyPassword?;
|};

# TLS configuration for the connector's HTTPS traffic.
public type SecureSocket record {|
    # The trusted CA certificates: a PEM file path, or a truststore with its password
    CertStore|string cert?;
    # The client identity for mutual TLS: a certificate and key pair, or a keystore with its
    # password
    CertKey|CertStore key?;
    # The TLS protocol versions offered during the handshake (e.g. `["TLSv1.3", "TLSv1.2"]`)
    string[] protocolVersions?;
    # The cipher suites offered during the handshake
    string[] ciphers?;
    # Whether the server's host name is verified against its certificate
    boolean verifyHostName = true;
    # Whether TLS sessions may be resumed
    boolean sessionResumption?;
    # Whether the server certificate's revocation status is checked
    boolean validateRevocation?;
    # The Server Name Indication host name sent during the handshake
    string sniHostName?;
    # The TLS handshake timeout, in seconds
    decimal handshakeTimeoutSeconds?;
    # The TLS session timeout, in seconds
    decimal sessionTimeoutSeconds?;
|};

# Proxy settings for the connector's traffic.
public type ProxyConfig record {|
    # The proxy protocol
    ProxyType proxyType = HTTP;
    # The proxy host name or address
    string host;
    # The proxy port
    int port;
    # The user name, when the proxy requires authentication
    string username?;
    # The password, when the proxy requires authentication
    string password?;
    # Host names that bypass the proxy
    string[] nonProxyHosts?;
|};

# Connection-pool settings for the connector's HTTP transport.
public type ConnectionPoolConfig record {|
    # The maximum number of concurrent connections
    int maxConnections = 50;
    # How long an idle connection is kept before being closed, in seconds
    decimal idleTimeoutSeconds = 60;
    # The timeout for establishing a connection, in seconds
    decimal connectTimeoutSeconds = 10;
    # The timeout for reading a response, in seconds
    decimal readTimeoutSeconds = 60;
|};

# HTTP transport settings: proxying, connection pooling, and TLS.
public type TransportConfig record {|
    # Route traffic through this proxy
    ProxyConfig proxy?;
    # Connection-pool tuning
    ConnectionPoolConfig connectionPool = {};
    # Custom TLS settings (trust and key material, verification)
    SecureSocket secureSocket?;
|};

# Configuration for an `azure.storage.blob` client (`Client` or `AdminClient`).
public type ClientConfiguration record {|
    # The authentication configuration (see `AuthConfig`)
    AuthConfig auth;
    # Retry behaviour for service requests; omit for the service defaults
    RetryConfig retryConfig?;
    # HTTP transport settings (proxy, connection pool, TLS)
    TransportConfig transportConfig = {};
|};

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

# The access tiers this module sets on a blob. The tier a blob reports is a plain `string`,
# because the service's tier set is open-ended.
public enum AccessTier {
    # Optimised for frequent access
    HOT = "Hot",
    # Optimised for infrequent access, held at least 30 days
    COOL = "Cool",
    # Optimised for rare access, held at least 90 days
    COLD = "Cold",
    # Offline storage, cheapest to hold; content must be rehydrated before it can be read
    ARCHIVE = "Archive"
}

# How quickly an archived blob is rehydrated.
public enum RehydratePriority {
    # Standard priority, documented at up to fifteen hours
    STANDARD = "Standard",
    # High priority, typically under one hour
    HIGH = "High"
}

# A container's anonymous access level.
public enum PublicAccess {
    # No anonymous access
    PRIVATE = "private",
    # Anonymous read access to blobs
    BLOB = "blob",
    # Anonymous read access to blobs, and anonymous listing of the container
    CONTAINER = "container"
}

# What happens to a blob's snapshots when the blob is deleted.
public enum DeleteSnapshotsOption {
    # Delete the blob together with its snapshots
    INCLUDE = "include",
    # Delete only the snapshots, keeping the blob
    ONLY_SNAPSHOTS = "only"
}

# The serialization format applied to structured content.
public enum FileFormat {
    # A JSON document
    JSON,
    # An XML document
    XML,
    # CSV rows
    CSV
}

# The blob lifecycle event types this module dispatches.
public enum BlobEventType {
    # A blob's content was fully committed, on creation or on replacement
    BLOB_CREATED,
    # A blob was deleted
    BLOB_DELETED
}

# The state of a copy operation.
public enum CopyStatus {
    # The copy is in progress
    PENDING = "pending",
    # The copy completed
    SUCCESS = "success",
    # The copy was aborted
    ABORTED = "aborted",
    # The copy failed
    FAILED = "failed"
}

# The lease state of a container or blob.
public enum LeaseState {
    # No lease is held
    AVAILABLE = "available",
    # A lease is held
    LEASED = "leased",
    # A fixed-duration lease ran out
    EXPIRED = "expired",
    # The lease is breaking
    BREAKING = "breaking",
    # The lease was broken
    BROKEN = "broken"
}

# Whether a container or blob is locked by a lease.
public enum LeaseStatus {
    # A lease is held
    LOCKED = "locked",
    # No lease is held
    UNLOCKED = "unlocked"
}

# Whether a lease runs for a fixed time or indefinitely.
public enum LeaseDuration {
    # The lease never expires until released or broken
    INFINITE = "infinite",
    # The lease expires after its duration unless renewed
    FIXED = "fixed"
}

# The protocols a request presenting a SAS token may use.
public enum SasProtocol {
    # HTTPS only
    HTTPS = "https",
    # HTTPS or HTTP
    HTTPS_HTTP = "https,http"
}

// ---------------------------------------------------------------------------
// Account-level results
// ---------------------------------------------------------------------------

# A container as returned by the account-level listings.
public type ContainerInfo record {|
    # The container name
    string name;
    # When the container was last modified
    time:Utc lastModified;
    # The container's entity tag
    string eTag;
    # The anonymous access level
    PublicAccess publicAccess;
    # The lease state
    LeaseState leaseState;
    # Whether the container is locked by a lease
    LeaseStatus leaseStatus;
    # Whether a held lease is fixed-duration or infinite, or `()` when no lease is held
    LeaseDuration? leaseDuration;
    # The container's metadata; present only when the listing requested it
    map<string> metadata?;
    # Whether the container is soft-deleted; present only on soft-deleted entries
    boolean isDeleted?;
    # The restore version of a soft-deleted container; pass to `AdminClient.undeleteContainer`
    string deletedVersion?;
    # When the container was deleted; present only on soft-deleted entries
    time:Utc deletedTime?;
    # Days remaining before the soft-deleted container is purged
    int remainingRetentionDays?;
|};

# The result of a container listing.
public type ContainerList record {|
    # The containers returned
    ContainerInfo[] containers;
    # The marker to resume the listing from; present only when more containers remain
    string nextMarker?;
|};

# How long soft-deleted blobs, or metrics and log data, are retained.
public type RetentionPolicy record {|
    # Whether the retention policy is enabled
    boolean enabled;
    # The number of days a deleted resource is retained
    int days?;
|};

# Request-metrics collection for the blob service.
public type MetricsProperties record {|
    # The metrics configuration version
    string version;
    # Whether metrics collection is enabled
    boolean enabled;
    # Whether per-API metrics are collected
    boolean includeApis?;
    # How long the metrics data is retained
    RetentionPolicy retentionPolicy?;
|};

# Classic request logging for the blob service.
public type LoggingProperties record {|
    # The logging configuration version
    string version;
    # Whether read requests are logged
    boolean read;
    # Whether write requests are logged
    boolean write;
    # Whether delete requests are logged
    boolean delete;
    # How long the log data is retained
    RetentionPolicy retentionPolicy?;
|};

# One cross-origin resource sharing rule.
public type CorsRule record {|
    # The origins allowed to make cross-origin requests
    string[] allowedOrigins;
    # The HTTP methods allowed in cross-origin requests
    string[] allowedMethods;
    # The request headers the origin may specify
    string[] allowedHeaders;
    # The response headers exposed to the client
    string[] exposedHeaders;
    # How long a browser may cache the preflight response, in seconds
    int maxAgeInSeconds;
|};

# The account's static website settings.
public type StaticWebsiteProperties record {|
    # Whether static website hosting is enabled
    boolean enabled;
    # The blob served for a directory request
    string indexDocument?;
    # The blob served when a request matches nothing
    string errorDocument404Path?;
    # The default index document path
    string defaultIndexDocumentPath?;
|};

# The account's blob service configuration. A group present in the record replaces that group
# whole; a group absent is left unchanged.
public type ServiceProperties record {|
    # Hourly request-metrics collection
    MetricsProperties hourMetrics?;
    # Per-minute request-metrics collection
    MetricsProperties minuteMetrics?;
    # Classic request logging
    LoggingProperties logging?;
    # The CORS rules; an empty array deletes every rule
    CorsRule[] cors?;
    # The blob soft-delete retention policy, the prerequisite for `undeleteBlob`
    RetentionPolicy deleteRetentionPolicy?;
    # The static website settings
    StaticWebsiteProperties staticWebsite?;
    # The service version applied to requests that do not name one
    string defaultServiceVersion?;
|};

# The storage account's SKU, kind, and namespace type.
public type AccountInfo record {|
    # The account's SKU name (e.g. `Standard_LRS`)
    string skuName;
    # The account kind (e.g. `StorageV2`)
    string accountKind;
    # Whether the account has a hierarchical namespace (Azure Data Lake Storage Gen2), on which
    # directories are real and renames exist through a different endpoint
    boolean isHierarchicalNamespaceEnabled;
|};

# A key for signing user-delegation SAS tokens, obtained from `AdminClient.getUserDelegationKey`.
public type UserDelegationKey record {|
    # The object ID of the Entra ID identity the key was issued to
    string signedObjectId;
    # The tenant the identity belongs to
    string signedTenantId;
    # When the key becomes valid
    time:Utc signedStart;
    # When the key expires; tokens signed with it cannot outlive this
    time:Utc signedExpiry;
    # The service the key applies to
    string signedService;
    # The service version that issued the key
    string signedVersion;
    # The key value used to sign tokens
    string value;
|};

// ---------------------------------------------------------------------------
// Container-level results
// ---------------------------------------------------------------------------

# The bound container's properties and metadata.
public type ContainerProperties record {|
    # When the container was last modified
    time:Utc lastModified;
    # The container's entity tag
    string eTag;
    # The anonymous access level
    PublicAccess publicAccess;
    # The container's complete metadata set
    map<string> metadata;
    # The lease state
    LeaseState leaseState;
    # Whether the container is locked by a lease
    LeaseStatus leaseStatus;
    # Whether a held lease is fixed-duration or infinite, or `()` when no lease is held
    LeaseDuration? leaseDuration;
    # Whether an immutability policy is set on the container
    boolean hasImmutabilityPolicy;
    # Whether a legal hold is set on the container
    boolean hasLegalHold;
|};

# One stored access policy: a validity window and a permission string under an identifier.
public type SignedIdentifier record {|
    # The identifier a SAS token references to inherit this policy
    string id;
    # When the policy becomes valid
    time:Utc startTime?;
    # When the policy expires
    time:Utc expiryTime?;
    # The permissions the policy grants, as the wire's permission string
    string permissions?;
|};

# A container's anonymous access level and its stored access policies, as the wire carries
# them: together.
public type ContainerAccessPolicy record {|
    # The anonymous access level
    PublicAccess access;
    # The stored access policies, at most five
    SignedIdentifier[] identifiers;
|};

// ---------------------------------------------------------------------------
// Blob results
// ---------------------------------------------------------------------------

# One entry of a blob listing. In the hierarchical mode an entry whose `isPrefix` is set is a
# collapsed group of names rather than a blob.
public type BlobEntry record {|
    # The blob's container-relative path, which feeds the path-taking operations directly
    string path;
    # Whether this entry is a collapsed prefix rather than a blob
    boolean isPrefix = false;
    # The blob's size in bytes; absent on prefix entries
    int contentLength?;
    # When the blob was last modified
    time:Utc lastModified?;
    # The blob's entity tag
    string eTag?;
    # The blob type (`BlockBlob`, `AppendBlob`, or `PageBlob`)
    string blobType?;
    # The blob's access tier, as the wire's string
    string accessTier?;
    # The blob's metadata; present only when the listing requested it
    map<string> metadata?;
    # The blob's index tags; present only when the listing requested it
    map<string> tags?;
    # The snapshot identifier; present on snapshot entries
    string snapshotId?;
    # Whether the blob is soft-deleted; present on soft-deleted entries
    boolean isDeleted?;
|};

# One page of a blob listing.
public type BlobList record {|
    # The blobs returned
    BlobEntry[] blobs;
    # The marker to resume the listing from; present only when more blobs remain
    string nextMarker?;
|};

# The state of a blob's most recent copy operation.
public type CopyStatusInfo record {|
    # The identifier of the copy operation
    string copyId;
    # The copy's state
    CopyStatus copyStatus;
    # The URL the content was copied from
    string copySource;
    # The bytes copied so far, as `copied/total`
    string copyProgress?;
    # When the copy completed
    time:Utc copyCompletionTime?;
    # The service's description of a failed or aborted copy
    string copyStatusDescription?;
|};

# A blob's properties, metadata, and the state of its most recent copy.
public type BlobProperties record {|
    # When the blob was last modified
    time:Utc lastModified;
    # When the blob was created
    time:Utc createdTime;
    # The blob's entity tag
    string eTag;
    # The blob's size in bytes
    int contentLength;
    # The blob's content headers
    ContentHeaders contentHeaders;
    # The blob's complete metadata set
    map<string> metadata;
    # The blob type (`BlockBlob`, `AppendBlob`, or `PageBlob`)
    string blobType;
    # The access tier, as the wire's string, since the service's tier set is open-ended
    string accessTier?;
    # Whether the tier was inferred rather than set explicitly; present whenever the blob has a tier
    boolean accessTierInferred?;
    # The rehydration state, set while a rehydration from the archive tier is pending
    string archiveStatus?;
    # The lease state
    LeaseState leaseState;
    # Whether the blob is locked by a lease
    LeaseStatus leaseStatus;
    # Whether a held lease is fixed-duration or infinite, or `()` when no lease is held
    LeaseDuration? leaseDuration;
    # The most recent copy operation; absent when the blob was never a copy destination
    CopyStatusInfo copyStatus?;
    # The page blob's write sequence marker; present only on page blobs
    int blobSequenceNumber?;
    # The append blob's committed block count; present only on append blobs
    int committedBlockCount?;
|};

# The identifier and state of a copy at the moment the service accepted it.
public type CopyInfo record {|
    # The identifier of the copy operation, which `abortCopy` cancels
    string copyId;
    # The copy's state at acceptance, which may already be `SUCCESS` for a small copy
    CopyStatus copyStatus;
|};

# One blob matched by an index-tag query.
public type TaggedBlobEntry record {|
    # The blob's container-relative path
    string path;
    # The blob's index tags
    map<string> tags;
|};

# One written byte range of a page blob.
public type PageRange record {|
    # The range's start offset in bytes
    int offset;
    # The range's length in bytes
    int length;
|};

# One block of a block blob.
public type BlockInfo record {|
    # The block's base64 identifier
    string blockId;
    # The block's size in bytes
    int sizeBytes;
|};

# A block blob's committed and staged blocks.
public type BlockList record {|
    # The blocks that make up the blob's current content
    BlockInfo[] committedBlocks;
    # The blocks staged but not yet committed
    BlockInfo[] uncommittedBlocks;
|};

// ---------------------------------------------------------------------------
// Listener event
// ---------------------------------------------------------------------------

# A blob lifecycle event, as delivered through Azure Event Grid. The fields describe the blob
# at the moment the event fired.
public type BlobEvent record {|
    # Whether the blob was created (or replaced) or deleted
    BlobEventType eventType;
    # The container the blob belongs to, derived from the event subject
    string containerName;
    # The blob's container-relative path, derived from the event subject
    string path;
    # The blob's full URL, which `Caller.copyBlobFromUrl` takes as its source
    string url;
    # When the event fired
    time:Utc eventTime;
    # The storage operation that caused the event (e.g. `PutBlob`, `PutBlockList`, `CopyBlob`)
    string api;
    # The blob's content type, as provided by the event
    string contentType?;
    # The blob's size in bytes, as provided by the event
    int contentLength?;
    # The blob type as the event reports it. The event schema documents only `BlockBlob` and
    # `PageBlob`, so an append blob's events may not identify their type
    string blobType?;
    # The blob's entity tag, as provided by the event
    string eTag?;
    # An opaque string whose ordering is comparable for one blob path, for detecting stale
    # events about the same blob
    string sequencer;
|};
