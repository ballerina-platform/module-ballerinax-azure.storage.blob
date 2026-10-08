# Specification: Ballerina Azure Blob Storage Library

_Owners_: @YasanPunch \
_Reviewers_: @niveathika \
_Created_: 2026/08/12 \
_Updated_: 2026/09/27 \
_Edition_: Swan Lake

## Introduction

This specification describes the Azure Blob Storage connector library for the Ballerina
programming language, enabling applications to manage Microsoft Azure blob containers and the
blobs within them, and to react to blob lifecycle events. The library definition has progressed
over time and may undergo further refinement. Previous versions are accessible via their
corresponding GitHub tags.

For feedback or suggestions regarding this library, please open a discussion through a
[GitHub issue](https://github.com/ballerina-platform/ballerina-library/issues) or participate in
the [Discord server](https://discord.gg/ballerinalang). Community input drives potential updates
to both specification and implementation.

The official implementation aligns with this specification. Any deviation qualifies as a defect.

This connector replaces the blobs sub-module of the deprecated `ballerinax/azure_storage_service`
package. Every capability of that module has a path in this library.

## Contents

1. [Overview](#1-overview)
2. [Configuration](#2-configuration)
   * 2.1 [Authentication](#21-authentication)
   * 2.2 [Client Configuration](#22-client-configuration)
   * 2.3 [Retry Configuration](#23-retry-configuration)
   * 2.4 [Transport Configuration](#24-transport-configuration)
3. [AdminClient](#3-adminclient)
   * 3.1 [Initializing the AdminClient](#31-initializing-the-adminclient)
   * 3.2 [Container Management Operations](#32-container-management-operations)
   * 3.3 [Service Configuration Operations](#33-service-configuration-operations)
   * 3.4 [Account Information](#34-account-information)
   * 3.5 [User Delegation Key and Account SAS](#35-user-delegation-key-and-account-sas)
4. [Client](#4-client)
   * 4.1 [Initializing the Client](#41-initializing-the-client)
   * 4.2 [Container Operations](#42-container-operations)
   * 4.3 [Blob Operations](#43-blob-operations)
   * 4.4 [Transfer Operations](#44-transfer-operations)
   * 4.5 [Copy Operations](#45-copy-operations)
   * 4.6 [Access Tier Operations](#46-access-tier-operations)
   * 4.7 [Blob Index Tag Operations](#47-blob-index-tag-operations)
   * 4.8 [Snapshot Operations](#48-snapshot-operations)
   * 4.9 [Lease Operations](#49-lease-operations)
   * 4.10 [Append Blob Operations](#410-append-blob-operations)
   * 4.11 [Page Blob Operations](#411-page-blob-operations)
   * 4.12 [Block Operations](#412-block-operations)
   * 4.13 [SAS Generation](#413-sas-generation)
5. [The Listener and Caller](#5-the-listener-and-caller)
   * 5.1 [The Event Delivery Path](#51-the-event-delivery-path)
   * 5.2 [Initializing the Listener](#52-initializing-the-listener)
   * 5.3 [Services and the Container Attach Point](#53-services-and-the-container-attach-point)
   * 5.4 [Handlers and Routing](#54-handlers-and-routing)
   * 5.5 [Content Binding](#55-content-binding)
   * 5.6 [The Blob Event](#56-the-blob-event)
   * 5.7 [Delivery Semantics and Poison Messages](#57-delivery-semantics-and-poison-messages)
   * 5.8 [Error Notification](#58-error-notification)
   * 5.9 [The Caller](#59-the-caller)
6. [Errors](#6-errors)

## 1. Overview

[Azure Blob Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction)
is the object storage service of Azure Storage, holding unstructured data as blobs inside
containers. The `ballerinax/azure.storage.blob` module provides an idiomatic Ballerina API for
the service.

The public surface is four types:

* The `AdminClient` operates at the storage account level. It creates, lists, deletes, and
  restores containers, manages the account's blob service configuration, reads the account's
  information, and mints account level SAS tokens.
* The `Client` is bound to a single container at initialization and carries every operation
  inside that container: blob listing, transfers, copies, access tiers, index tags, snapshots,
  leases, append blob and page blob writes, block operations, access policies, and SAS
  generation.
* The `Listener` consumes the blob events (created, deleted) that an Azure Event Grid
  subscription delivers to an Azure Storage queue. It binds each new blob's content and
  dispatches it to the service attached for the blob's container.
* The `Caller` is passed to each listener handler. It forwards a subset of the `Client`
  operations, bound to the service's container, so a handler can act on the event's blob
  without constructing a separate client.

Blob operations address a blob by its container relative name, a slash delimited path such as
`2026/07/invoice.pdf`. Blob storage has no real directories; the slashes are part of the blob
name, and the hierarchical listing mode groups names by their slash segments. A leading slash
on a supplied path is removed. Where two paths co-occur, they are named `sourcePath` and
`destinationPath`, in source first order.

Blobs come in three types. Block blobs hold ordinary content and are what every transfer
operation of this module creates. Append blobs grow by appending blocks. Page blobs have a
fixed capacity, are written in 512 byte aligned pages, and back virtual disks. A blob's type is
fixed for its lifetime and is reported in `BlobProperties.blobType`. A type specific operation
applied to a blob of another type fails with an `InvalidBlobTypeError`, and so does an upload
over an existing append or page blob. Changing a blob's type means deleting it and writing a
new one.

Every operation that calls the service is a remote method, invoked with `->`. Methods that make
no service call are ordinary methods, invoked with `.`: the `Listener`'s lifecycle methods and
the SAS generation methods, which sign tokens locally. The clients hold no releasable resources
and have no close method.

Leases gate writes throughout the module by one rule: every operation that writes a leased
resource carries an optional `leaseId` in its options record, reads never require one, and a
write against a leased resource without the current lease id fails with a
`PreconditionFailedError` (section 4.9).

## 2. Configuration

### 2.1 Authentication

The authentication configuration is a union in which each member is one credential artifact,
the thing the Azure portal, CLI, or infrastructure tooling hands the user:

```ballerina
public type AuthConfig SharedKeyConfig|SasConfig|SasUrlConfig|ConnectionStringConfig|EntraIdConfig;
```

Every member has a unique required field or field combination, so both the compiler and
`Config.toml` select the right member by structural matching.

###### Example: Selecting an Authentication Mode

```ballerina
// The fields present select the union member:
blob:AuthConfig sharedKey = {accountName: "myacct", accountKey: "..."};
blob:AuthConfig sasToken = {accountName: "myacct", sasToken: "sv=..."};
blob:AuthConfig sasUrl = {sasUrl: "https://myacct.blob.core.windows.net/?sv=..."};
blob:AuthConfig connectionString = {connectionString: "..."};
blob:AuthConfig defaultChain = {kind: "default", accountName: "myacct"};
blob:AuthConfig managedIdentity = {kind: "managed-identity", accountName: "myacct"};
blob:AuthConfig servicePrincipal = {accountName: "myacct", tenantId: "...", clientId: "...", clientSecret: "..."};
```

The five modes:

* **Shared key** (`SharedKeyConfig`): authenticates with the storage account name and one of
  its access keys. The required `accountName` is the signing identity and derives the service
  URL; the required `accountKey` is a base64 encoded access key. The optional `serviceUrl`
  overrides the endpoint, defaulting to `https://{accountName}.blob.core.windows.net`.
* **SAS token** (`SasConfig`): authenticates with a bare shared access signature token, as
  issued by `az storage container generate-sas` or the SAS generation methods of this module.
  Requires `accountName` (which determines the service URL) and `sasToken`.
* **SAS URL** (`SasUrlConfig`): authenticates with a full SAS URL, which carries the service
  URL and the SAS token in one string, as issued by the Azure portal ("Blob service SAS URL").
  Requires `sasUrl`, including the scheme and the SAS query string.
* **Connection string** (`ConnectionStringConfig`): authenticates with a storage account
  connection string, which bundles the account name, the credential (an account key or a SAS
  token), and the service endpoints. Requires `connectionString`.
* **Microsoft Entra ID** (`EntraIdConfig`): itself a union of four records.
  `EntraIdChainConfig` selects a credential chain through its `kind`, an `EntraIdKind`:
  `DEFAULT_AZURE_CREDENTIAL`
  (`"default"`) tries the environment, a managed identity, and developer sign-ins in turn, and
  `MANAGED_IDENTITY` (`"managed-identity"`) uses the managed identity alone. Its optional
  `clientId` selects a user assigned managed identity under either kind; omitted, the system
  assigned identity is used. `ClientSecretConfig`, `ClientCertificateConfig`, and
  `WorkloadIdentityConfig` authenticate as a service principal and are distinguished by their
  unique required field: `clientSecret`, `certificatePath` (PEM, or PFX when
  `certificatePassword` is set), or `tokenFilePath` (the federated service account token of a
  Kubernetes workload). All four require `accountName` and accept an optional `serviceUrl`
  override; the service principal records also require `tenantId` and `clientId`.

Every auth mode is validated at `init` with local computation and no call to Azure: connection
strings are parsed by the SDK, which refuses one it cannot derive a blob endpoint from and
accepts the development storage shorthand, and the explicit records get non empty,
base64, and URL scheme checks. A malformed credential fails at `init` with a specific error.

An Entra ID identity authorizes data operations through Azure RBAC. `Storage Blob Data Reader`
covers reads and listings, `Storage Blob Data Contributor` adds writes and deletes, and
`Storage Blob Data Owner` adds the access policy, index tag, and tag query operations. `Storage
Blob Delegator` permits obtaining a user delegation key; it must be assigned at storage account,
resource group, or subscription scope, and a container scoped assignment does not grant it.
Container lifecycle and service configuration (the `AdminClient` surface) authorize against the
storage account's management role actions, carried by roles such as `Contributor`, which grant
no blob data access on their own. A `Listener` authenticating with Entra ID additionally needs
`Storage Queue Data Contributor`: it receives, updates, and deletes queue messages and creates
the poison queue, and the narrower `Storage Queue Data Message Processor` cannot create a queue.

The `Listener` authenticates with the same union. Its credential must cover both the queue
service (receiving, updating, and deleting messages, and creating the poison queue) and the
blob operations its handlers perform. A SAS credential for a listener must be an account SAS
spanning the queue and blob services, as minted by `AdminClient.generateAccountSas`
(section 3.5); a container or blob scoped SAS cannot poll a queue.

### 2.2 Client Configuration

Both clients take the same `ClientConfiguration` record: the required `auth` (an `AuthConfig`
member, section 2.1), an optional `retryConfig` (section 2.3), and `transportConfig`
(section 2.4), which always applies and defaults to the values listed there. The configuration
is an included record parameter on both `init` methods, so callers pass its fields as named
arguments, for example `new (auth = {accountName, accountKey})`.

### 2.3 Retry Configuration

The optional `RetryConfig` record shapes the retry behaviour of service requests; omitting it
keeps the service's defaults, which are the values listed below. Its fields:

* `retryPolicyType`: how the delay between tries grows, `EXPONENTIAL` or `FIXED_INTERVAL`.
  Defaults to `EXPONENTIAL`.
* `maxTries`: the maximum number of tries, counting the first attempt. Defaults to 4.
* `tryTimeoutSeconds`: the timeout applied to each individual try. Defaults to 60.
* `retryDelaySeconds`: the base delay between tries. Defaults to 4.
* `maxRetryDelaySeconds`: the upper bound on the delay between tries. Defaults to 120.
* `secondaryHostUrl`: a secondary endpoint to retry reads against, for geo redundant accounts.
  No default.

### 2.4 Transport Configuration

The `TransportConfig` record covers the HTTP transport. Its fields:

* `proxy`: routes the connector's traffic through an HTTP, SOCKS4, or SOCKS5 proxy, with
  optional credentials and a bypass list. No default.
* `connectionPool`: tunes the connection pool. Its `maxConnections` defaults to 50,
  `idleTimeoutSeconds` to 60, `connectTimeoutSeconds` to 10, and `readTimeoutSeconds` to 60.
  These values apply whenever the pool is not configured.
* `secureSocket`: configures custom TLS: trust material (a PEM certificate path, or a
  PKCS12 or JKS truststore with its password), a client identity for mutual TLS (a certificate
  and key pair, or a keystore with its password), the offered TLS versions and cipher suites,
  host name verification, session reuse, revocation checking, an SNI host name, and handshake
  and session timeouts. No default.

## 3. AdminClient

The `AdminClient` manages the containers within a storage account. Use it for container
lifecycle management, the account's blob service configuration, account information, and
account level SAS tokens. For operations scoped to a single container, use the `Client`.

### 3.1 Initializing the AdminClient

The constructor takes the client configuration (section 2.2) as an included record parameter
and validates the credential locally, with no call to Azure.

###### Example: Initializing the AdminClient

```ballerina
blob:AdminClient admin = check new (auth = {accountName: "myacct", accountKey: "..."});
```

### 3.2 Container Management Operations

* `hasContainer(containerName)`: returns whether the named container exists. The result is
  `false` only when Azure confirms absence (HTTP 404); an `Error` means the check itself could
  not complete, so an auth problem is never misreported as a missing container.
* `listContainers(options)`: lists the containers of the account as a `ContainerList`, whose
  `containers` array holds the results. `ContainerListOptions` offers a name `prefix`, toggles
  for including metadata and soft deleted containers, a `'limit` on the number of results, and
  a `marker` to resume from. Without a limit every container is returned and `nextMarker` is
  absent; with one, up to that many are returned and `nextMarker` is present when more remain,
  to be passed back as the next call's `marker`.
* `createContainer(containerName, options)`: creates a container. `ContainerCreateOptions`
  accepts metadata and a public access level (`BLOB` grants anonymous reads of blobs,
  `CONTAINER` additionally grants anonymous listing; when absent, the container is private).
  Anonymous access also requires the storage account to permit it.
* `deleteContainer(containerName, options)`: deletes a container and every blob in it.
  `DeleteContainerOptions.leaseId` passes the active lease when the container is leased. When
  container soft delete is enabled on the storage account, the container is retained for the
  configured period and can be restored with `undeleteContainer`.
* `undeleteContainer(containerName, deletedContainerVersion)`: restores a soft deleted
  container. Soft deleted containers are listed by `listContainers({includeDeleted: true})`,
  where each carries the `deletedVersion` this operation takes.

###### Example: Creating a Container When Absent

```ballerina
if !(check admin->hasContainer("invoices")) {
    check admin->createContainer("invoices");
}
```

### 3.3 Service Configuration Operations

* `getServiceProperties()`: reads the account's blob service configuration as a
  `ServiceProperties` record. The record models the full configuration document: request
  metrics collection, classic logging, CORS rules, the blob soft delete retention policy,
  the static website settings, and the default service version.
* `setServiceProperties(properties)`: writes the configuration. Each configuration group is
  an optional field of the record. A group present in the record replaces that group as a
  whole; a group absent from the record leaves the service's current setting untouched. An
  empty `cors` array therefore deletes every CORS rule, while omitting the `cors` field
  preserves them. To change one group, read the current configuration, modify it, and pass
  the result back.

Enabling the blob soft delete retention policy here is the prerequisite for `undeleteBlob`.
Container soft delete is a storage account setting, enabled on the account rather than through
the service properties, and is the prerequisite for `undeleteContainer`. The static website
settings configure the service side feature; this module offers no operations against the
static website endpoint itself.

### 3.4 Account Information

* `getAccountInfo()`: reads the account's SKU name, account kind, and whether the account has
  a hierarchical namespace, as an `AccountInfo` record. On hierarchical namespace accounts
  (Azure Data Lake Storage Gen2) directories are real and renames exist through a different
  endpoint; this module models neither.

### 3.5 User Delegation Key and Account SAS

* `getUserDelegationKey(startTime, expiryTime)`: obtains a `UserDelegationKey` for signing user
  delegation SAS tokens (section 4.13). Requires a client authenticated with Microsoft Entra ID
  whose identity holds the `Storage Blob Delegator` role. The key is valid at most 7 days.
* `generateAccountSas(values)`: mints an account level SAS token. This is an ordinary method,
  invoked with `.`: it signs the token locally with the account key and makes no service call.
  It requires a client authenticated with a shared key (or a connection string carrying an
  account key). The signature values select the covered services (`blob`, `queue`), the
  resource types (`'service`, `container`, `'object`), the permissions, and the validity
  window, and optionally a start time, the permitted protocol set, and an IP range. The
  account level permissions are one boolean per account permission: `read`, `write`,
  `delete`, `list`, `add`, `create`, `update`, `process`, `tag`, and `filter`; `update` and
  `process` are queue operations, and `tag` and `filter` cover the index tag operations and
  tag queries. A SAS covering the queue service with `read`, `add`, `create`, `update`, and
  `process` is the credential a `Listener` needs (section 2.1). Rotating the account key
  revokes every SAS minted from it.

## 4. Client

The `Client` is bound to a single container at initialization and operates on that container
and the blobs within it.

### 4.1 Initializing the Client

The constructor takes the container name and the client configuration (section 2.2). Binding
is lazy: `init` makes no call to Azure, so initializing against a container that does not exist
succeeds, and the first operation on it fails with a `NotFoundError`. The up front existence
check is `AdminClient.hasContainer`.

###### Example: Initializing the Client

```ballerina
blob:Client invoices = check new ("invoices", auth = {accountName: "myacct", accountKey: "..."});
```

### 4.2 Container Operations

* `getContainerProperties()`: reads the bound container's properties, including its metadata,
  public access level, lease state, and the immutability and legal hold flags, as a
  `ContainerProperties` record.
* `setContainerMetadata(metadata, options)`: replaces the container's complete metadata set.
  Metadata is free form, user defined annotation; Azure stores and returns it verbatim, and it
  is read back through `getContainerProperties`.
* `getContainerAccessPolicy()`: reads the container's public access level and its stored
  access policies together, as a `ContainerAccessPolicy` record carrying the access level and
  a `SignedIdentifier` array; a private container reads back as `PRIVATE`.
* `setPublicAccess(access, options)`: sets the public access level (`PRIVATE`, `BLOB`, or
  `CONTAINER`) and leaves the stored access policies unchanged.
* `setContainerAccessPolicy(identifiers, options)`: replaces the stored access policies (at
  most five, validated locally; `[]` removes them all) and leaves the public access level
  unchanged.

The service writes the access level and the policies together. Each of the two setters
therefore reads the current access control list first and resends the half it does not
change, so each call is two requests.

A stored access policy carries a validity window and a permission string under an identifier.
Container SAS tokens minted against a policy (via the `identifier` field of the signature
values) inherit its window and permissions, so removing or editing a policy immediately
revokes or changes every SAS minted against it.

### 4.3 Blob Operations

* `listBlobs(options)`: returns the container's blobs as one lazy `BlobEntry` stream.
  `BlobListOptions` offers a name `prefix`, a `delimiter` that switches to the hierarchical
  mode, and toggles for including metadata, index tags, snapshots, and soft deleted blobs. In
  the flat mode (no delimiter) every blob of the container is returned regardless of the
  slashes in its name. In the hierarchical mode the listing covers one level: names extending
  past the next delimiter collapse into a single entry whose `isPrefix` flag is set, and such a
  prefix entry is fed back as the `prefix` of a further listing to descend one level.
* `listBlobsPage(options)`: returns one page of the listing as a `BlobList`, whose `blobs`
  array holds the page and whose `nextMarker` is present when more blobs follow. The options
  extend `BlobListOptions` with a `pageSize` (up to the service maximum of 5,000) and the
  `marker` to resume from. The marker is plain data, so a listing can be checkpointed and
  resumed across process restarts.
* `hasBlob(path)`: returns whether the blob exists, with the same semantics as `hasContainer`:
  `false` only on a confirmed 404, an `Error` when the check itself fails.
* `deleteBlob(path, options)`: deletes a blob. A blob that has snapshots cannot be deleted
  without directing what happens to them: `DeleteBlobOptions.deleteSnapshots` deletes them
  along with the blob (`INCLUDE`) or deletes only the snapshots and keeps the blob
  (`ONLY_SNAPSHOTS`); `snapshotId` deletes one snapshot instead of the blob; `leaseId` passes
  the active lease when the blob is leased. When the account's blob soft delete retention
  policy is enabled, the deleted blob is retained for the configured period.
* `undeleteBlob(path)`: restores a soft deleted blob, together with any soft deleted snapshots
  it had. Find restorable blobs with `listBlobs({includeDeleted: true})`.
* `getBlobProperties(path)`: reads the blob's properties as a `BlobProperties` record: the
  entity tag and timestamps, the content length and content headers, user metadata, the blob
  type, the access tier, whether that tier was inferred rather than set (`accessTierInferred`)
  and any archive rehydration status, the lease state, the state of the most recent copy
  operation as a `CopyStatusInfo`, and the type specific fields, present only for their type:
  the page blob's `blobSequenceNumber` and the append blob's `committedBlockCount`. The access
  tier is reported as the wire's string; the tiers this module sets (section 4.6) are its well
  known values.
* `setBlobMetadata(path, metadata, options)`: replaces the blob's complete metadata set.
* `setContentHeaders(path, headers, options)`: replaces the blob's complete content header set
  (`Content-Type`, `Cache-Control`, and the other standard headers). Any header omitted from
  the record is cleared on the blob.

Properties are read whole and written through these narrower operations: the content headers
through `setContentHeaders`, the metadata through `setBlobMetadata`, and the access tier
through `setAccessTier` (section 4.6). The remaining properties are maintained by the service.

There is no rename operation. Azure Blob Storage has no rename, so relocating a blob is a copy
to the new name followed by a delete of the old one, and the two steps are individually
observable.

###### Example: Listing With a Prefix

```ballerina
stream<blob:BlobEntry, blob:Error?> entries = check invoices->listBlobs({prefix: "2026/"});
check entries.forEach(function(blob:BlobEntry entry) {
    io:println(entry.path);
});
```

###### Example: Resuming a Listing Across Restarts

```ballerina
blob:BlobList page = check invoices->listBlobsPage({prefix: "2026/", pageSize: 500});
// process page.blobs, persist page.nextMarker, and later:
blob:BlobList next = check invoices->listBlobsPage({prefix: "2026/", pageSize: 500, marker: savedMarker});
```

### 4.4 Transfer Operations

* `uploadFromFile(sourcePath, destinationPath, options)`: uploads a local file as a block
  blob. Both paths are full paths including the file name, the local path first. The source is
  not deleted, and no format detection or type conversion takes place.
* `upload(content, destinationPath, options)`: uploads a Ballerina value (section 4.4.1).
* `download(sourcePath, destinationPath, options)`: downloads a blob to a local path, the
  blob path first. The download fails with a client side `Error` when a local file already
  exists at the destination.
* `getBlob(path, options, targetType)`: retrieves the blob's content in the form the target
  type selects (section 4.4.2).

Every upload creates a block blob. An existing block blob at the destination is replaced, and
its snapshots are retained. An existing append or page blob fails with an
`InvalidBlobTypeError`; delete it first, then upload. An archived blob fails with an
`ArchivedBlobError` until it is rehydrated or deleted (section 4.6). There is no `overwrite`
option.

Content larger than a single request threshold is transferred as parallel chunks in every
transfer direction, so memory stays bounded. `UploadOptions`, which `uploadFromFile` takes,
carries content headers, metadata, index tags, an access tier for the new blob, and the lease
id when the destination is leased. `UploadContentOptions`, which `upload` takes, adds the
`fileFormat` override described below. The retrieval options carry a `ByteRange`, a
`snapshotId` to read from a blob snapshot (section 4.8), and the `fileFormat` override for
record shaped targets.

#### 4.4.1 Upload Content

`upload` takes its content as the `UploadContent` union, whose members mirror the `getBlob`
targets (section 4.4.2), so anything written is readable back in the same type:

```ballerina
public type UploadContent byte[]|string|json|xml|record {}|record {}[]|
    stream<byte[], error?>|stream<record {}, error?>;
```

The serialization format resolves first: the explicit `UploadContentOptions.fileFormat`
override wins, else the destination path's extension decides. The value is then checked
against the resolved format.

| Format | Selected by | Members serialized |
|---|---|---|
| none | no override and no `.json`, `.xml`, or `.csv` extension | `byte[]`, `string`, `stream<byte[], error?>` |
| JSON | `JSON`, or `.json` | `json`, `record {}` |
| XML | `XML`, or `.xml` | `xml`, `record {}` |
| CSV | `CSV`, or `.csv` | `record {}[]`, `stream<record {}, error?>` |

`byte[]`, `string`, and a byte stream pass through untouched under every format; content the
caller already serialized is never re-encoded. A byte stream is staged as blocks and committed
at the end of the stream, with no content length needed up front. Until that commit the
destination blob does not exist, so a failed stream upload leaves no partial blob and an
existing blob at the destination is replaced only by a successful commit. A source stream
failure aborts with a client side `Error`, and every failure closes the source stream. Each
stream upload stages its blocks under an id of its own, so two stream uploads to one path at
the same time never blend: the first commit wins, and the service refuses the other with
`InvalidBlockList`, since a commit discards every block it does not list.

The structured members serialize as follows:

* **JSON**: a `json` value or a record (which includes any map of `anydata` members) becomes a
  JSON document. A record array is never JSON; it is written as CSV.
* **XML**: an `xml` value is written in its textual form. A record becomes a single element
  document whose root element is named `root`; the `@xmldata:Name` annotation renames the
  member elements, not the root. A record array is never XML.
* **CSV**: a record array becomes rows headed by the union of every record's field names in
  first seen order, with nil or absent members as empty cells. Fields containing a comma,
  quote, backslash, or line break are quoted so that the CSV reads bind back. A record stream
  is written row by row as it is pulled, so its header is the first record's field names, and
  a later record's extra field is not added to a header already written. An empty record array
  or an exhausted record stream writes an empty blob with no header row.

Positional or headerless CSV has no union member: write the text and upload it as a `string`
or `byte[]`. A structured value whose format resolves to neither an override nor a known
extension, and any value to format pairing outside the table, are refused with a client side
`Error`.

When the connector chose the serialization (a `json` value, a record, a record array, a record
stream, or an `xml` value) and the caller set no explicit `contentType`, the uploaded blob's
content type is set to match: `application/json`, `application/xml`, or `text/csv`. An
explicit content type in the upload options always wins. Content the connector did not
serialize (`byte[]`, `string`, byte streams, and disk uploads) gets no automatic type.

###### Example: Uploading Records

```ballerina
type Metric record {
    string quarter;
    int revenue;
};

// The .json extension selects the JSON serialization.
Metric q1 = {quarter: "q1", revenue: 1250000};
check invoices->upload(q1, "2026/q1/metrics.json");

// A record array is CSV; the override beats the extension when they disagree.
Metric[] quarters = [{quarter: "q1", revenue: 1250000}, {quarter: "q2", revenue: 1310000}];
check invoices->upload(quarters, "2026/summary.dat", {fileFormat: blob:CSV});
```

#### 4.4.2 Content Retrieval

`getBlob` returns the blob's content in the form the target type selects. The target type is a
member of the `RetrievableType` union, whose members mirror `UploadContent`:

```ballerina
public type RetrievableType byte[]|string|json|xml|record {}|record {}[]|
    stream<byte[], error?>|stream<record {}, error?>;
```

* `byte[]`: the raw content, materialized in one call.
* `string`: the content decoded as UTF-8 text; content that is not valid UTF-8 fails with a
  client side `Error`.
* `json`: the content parsed as a JSON document.
* `xml`: the content parsed as an XML document. A subtype such as `xml:Element` takes only a
  document of that shape; any other document fails with a client side `Error`.
* `record {}` or `record {}[]`: the content bound to the record shape per a resolved format.
  The explicit `GetBlobOptions.fileFormat` override wins, else the path's extension (`.json`,
  `.xml`, `.csv`) decides. A single record binds from JSON or XML (never CSV), a record array
  binds from a JSON array or CSV rows (never XML), and a format that resolves to neither an
  override nor a known extension is refused with a client side `Error`. CSV binding consumes
  the header row for field names; positional or headerless CSV is read as `string` or
  `byte[]`.
* A union of these members binds through the JSON parser, so it serves JSON content and
  json-shaped targets such as `json|()`; a union target with content whose format resolves
  to XML or CSV is refused with a client side `Error`. A readonly intersection such as
  `Person & readonly` binds as its underlying member and yields a readonly value.
* `stream<byte[], error?>`: a lazy byte stream, so memory stays bounded for any blob size.
* `stream<record {}, error?>`: CSV rows bound lazily, one record per pull; a row that fails to
  bind surfaces as the error entry of that pull.

The materialized targets download the full content before binding. Binding is strict: content
that does not match the target type fails with a client side `Error`.

###### Example: Retrieving Content by Target Type

```ballerina
byte[] raw = check invoices->getBlob("2026/q1/report.pdf");

Person[] people = check invoices->getBlob("2026/q1/people.csv");

stream<byte[], error?> chunks = check invoices->getBlob("2026/q1/large.bin");
```

### 4.5 Copy Operations

* `copyBlob(sourcePath, destinationPath, options)`: copies a blob within the bound container
  under this client's credentials, returning a `CopyInfo`. The service authorizes the copy
  source separately from the request, so a client authenticated with a SAS attaches that SAS
  to the source URL; a shared key authorizes a source in the same account on its own.
* `copyBlobFromUrl(sourceUrl, destinationPath, options)`: copies from any Azure Storage URL
  the service can read: a blob in another container or account, or a file in Azure Files. A
  source in the same storage account is authorized by this client's own credential. A source
  elsewhere that is not publicly readable must carry its own authorization in the URL,
  typically a SAS token.
* `abortCopy(path, copyId, options)`: cancels a pending copy. `AbortCopyOptions.leaseId`
  passes the destination's active lease.

Copies are asynchronous. Inspect the returned `CopyInfo.copyStatus` and, if pending, observe
progress through `getBlobProperties`, whose result carries the state of the most recent copy
operation, or cancel with `abortCopy`. `CopyOptions` accepts destination metadata, index tags,
an access tier for the destination blob, and the lease id when the destination is leased. The
service accepts a copy over a leased destination only when its lease is infinite. Copying an
archived blob to a new destination with an online tier is also the way to rehydrate without
touching the original (section 4.6).

### 4.6 Access Tier Operations

* `setAccessTier(path, accessTier, options)`: moves the blob to another access tier: `HOT`,
  `COOL`, or `COLD` (the online tiers), or `ARCHIVE` (offline). `SetAccessTierOptions` carries
  the `rehydratePriority` and the lease id when the blob is leased.

An archived blob's content cannot be read or replaced until it is rehydrated: reads of an
archived blob fail with an `ArchivedBlobError`, while its properties, metadata, and index tags
stay readable. Rehydration is `setAccessTier` back to an online tier and takes hours. The
`rehydratePriority` (`STANDARD`, up to fifteen hours, or `HIGH`, typically under one) applies
to that move, and the pending rehydration is observable in `BlobProperties.archiveStatus`. A
copy to a new online tier destination (section 4.5) rehydrates while keeping the archived
original.

### 4.7 Blob Index Tag Operations

* `setTags(path, tags, options)`: replaces the blob's complete index tag set.
* `getTags(path)`: reads the blob's index tags.
* `findBlobsByTags(query)`: returns the blobs of the bound container whose tags match the
  query, as a lazy stream of `TaggedBlobEntry` (the blob's path and its tags).

Index tags differ from metadata: the service indexes them, so they answer queries without
listing, and they carry their own SAS permission (`tag`, and `filter` for queries). Tags are
set and read whole. The tag index is eventually consistent: a `findBlobsByTags` query may
briefly lag a `setTags` write, while `getTags` always reads the blob's current tags.

A blob carries at most ten tags. A tag key is 1 to 128 characters and a value 0 to 256, both
case sensitive, and both admit only letters, digits, and the characters ` +-.:=_/`. `setTags`
checks these rules locally before any request.

The query is the service's filter expression: tag keys in double quotes, values in single
quotes, the operators `=`, `>`, `>=`, `<`, and `<=`, and conditions joined by `AND`, for
example `"status" = 'processed' AND "year" >= '2026'`. Before any request the connector checks
that the quotes balance, that only those operators appear, and that the quoted parts use the
tag character set, and it adds the `@container` clause for the bound container; an expression
that supplies its own `@container` is refused. A quote cannot appear in a key or a value, so
there is no escaping.

### 4.8 Snapshot Operations

* `createSnapshot(path, options)`: creates a point in time, read only copy of the blob and
  returns the snapshot id. `CreateSnapshotOptions` carries metadata for the snapshot and the
  lease id when the blob is leased.

Snapshot contents are read through the regular read operations: pass the snapshot id in the
options of `download` or `getBlob` to read the snapshot instead of the live blob. Snapshots
are listed with `listBlobs({includeSnapshots: true})`, where each snapshot entry carries its
`snapshotId`, and deleted through `deleteBlob` (one snapshot via `snapshotId`, or all of a
blob's snapshots via `deleteSnapshots`).

###### Example: Freezing and Reading a Version

```ballerina
string snapshotId = check invoices->createSnapshot("2026/07/invoice.pdf");
byte[] frozen = check invoices->getBlob("2026/07/invoice.pdf", {snapshotId});
```

### 4.9 Lease Operations

Blob leases lock a blob against writes and deletion by anyone not holding the lease id; reads
stay open to all.

* `acquireLease(path, leaseDurationSeconds, proposedLeaseId)`: acquires a lease, fixed duration
  (15 to 60 seconds) or infinite (-1), returning the lease id. The duration is validated
  locally. The proposed lease id is optional; the service generates one when it is absent.
* `renewLease(path, leaseId)`: keeps a fixed duration lease alive.
* `releaseLease(path, leaseId)`: releases the lease.
* `breakLease(path, breakPeriodSeconds)`: reclaims the lease without its id. With no break
  period, a fixed duration lease runs out its remaining time and an infinite lease breaks
  immediately; an explicit period (0 to 60) overrides. The operation returns the seconds until
  the lease is actually free.
* `changeLease(path, leaseId, proposedLeaseId)`: changes the lease id, handing the lease over
  without a release window in which another client could acquire it. Returns the new lease id.

While a blob is leased, the write operations of this module pass the lease id through their
options; a write without the current lease id fails with a `PreconditionFailedError`. The
index tag operations are the exception: their lease violations surface as an
`AuthorizationError` (HTTP 403). Failures of the lease operations themselves, against a stale
id or the wrong lease state, surface as a `ConflictError`; the error code in the detail names
the exact condition. The module offers no container lease operations, but every container
write honors an externally held container lease the same way (sections 3.2 and 4.2).

### 4.10 Append Blob Operations

* `createAppendBlob(path, options)`: creates an empty append blob. The options carry content
  headers, metadata, and index tags for the new blob, and the lease id.
* `appendBlock(path, content, options)`: appends a block of content, a `byte[]`, at the blob's
  end. Blocks append strictly in arrival order; each block may be at most 4 MiB, and a blob
  holds at most 50,000 blocks. `AppendBlockOptions.leaseId` passes the active lease.
* `appendBlockFromUrl(path, sourceUrl, options)`: appends content read from a URL the service
  can read, with the same authorization rules as `copyBlobFromUrl` (section 4.5).

An append blob grows only through these operations. Appending to a blob of another type fails
with an `InvalidBlobTypeError`. The `committedBlockCount` of `BlobProperties` reports an append
blob's progress. On flat namespace accounts, appending fires no blob created event
(section 5.6).

###### Example: Accumulating a Log

```ballerina
check invoices->createAppendBlob("logs/2026-08.log");
check invoices->appendBlock("logs/2026-08.log", line1);
check invoices->appendBlock("logs/2026-08.log", line2);
```

### 4.11 Page Blob Operations

* `createPageBlob(path, sizeInBytes, options)`: creates an empty page blob of the given fixed
  capacity, a multiple of 512 bytes up to 8 TiB. The options carry content headers, metadata,
  index tags, and the lease id. The capacity is reserved sparsely: unwritten pages read as
  zeros and only written pages are billed.
* `uploadPages(path, offset, content, options)`: writes content, a `byte[]`, at the given
  offset. The offset must be 512 byte aligned and the content length a multiple of 512, at
  most 4 MiB per call.
* `clearPages(path, offset, length, options)`: resets a 512 byte aligned range to zeros,
  releasing its storage.
* `listPageRanges(path, options)`: lists the written ranges as a `PageRange` array (each an
  `offset` and a `length`); the options restrict the listing to a byte range and select a
  snapshot.

Page writes to a blob of another type fail with an `InvalidBlobTypeError`, and misaligned
offsets or lengths are refused with a client side `Error` before any request is made. The `blobSequenceNumber` of
`BlobProperties` is the page blob's write sequence marker. The page write operations carry the
lease id in their options when the blob is leased.

### 4.12 Block Operations

The block operations expose block blob composition directly. They are not the path for
ordinary large uploads, which the transfer operations of section 4.4 chunk internally. They
serve composing a blob from remote pieces, and staging content across process restarts before
committing it.

* `stageBlock(path, blockId, content, options)`: stages a block of content, a `byte[]` of at
  most 4,000 MiB, under a base64 block id, without changing the visible blob. The caller
  supplies the id already base64 encoded; an invalid encoding fails with a client side `Error`
  before any request is made.
* `stageBlockFromUrl(path, blockId, sourceUrl, options)`: stages a block read from a URL the
  service can read, with the same source rules as `copyBlobFromUrl` (section 4.5); the
  content never passes through the caller. `StageBlockFromUrlOptions.sourceRange` stages one
  byte range of the source instead of its whole content.
* `commitBlockList(path, blockIds, options)`: commits an ordered list of staged block ids as
  the blob's new content, creating or replacing the blob in one atomic step. The options carry
  content headers, metadata, index tags, an access tier, and the lease id. All block ids of
  one blob must be equal in length; an unequal set fails with a client side `Error` before any
  request is made.
* `listBlocks(path)`: reports the blob's committed blocks and its staged, uncommitted blocks
  as a `BlockList` record, each block a `BlockInfo` carrying its id and size.

Staged blocks expire seven days after the blob's most recent successful staging. Until a
commit, the visible blob (if any) is unchanged; a producer that restarts resumes by listing its
uncommitted blocks and continuing.

###### Example: Composing a Blob From Remote Pieces

```ballerina
check archive->stageBlockFromUrl("2026/combined.bin", blockId1, part1Url);
check archive->stageBlockFromUrl("2026/combined.bin", blockId2, part2Url);
check archive->commitBlockList("2026/combined.bin", [blockId1, blockId2]);
```

### 4.13 SAS Generation

The SAS generation methods are ordinary methods, invoked with `.`: signing happens locally
with the credential the client holds, and no call is made to Azure.

* `generateContainerSas(values)`: mints a SAS token scoped to the whole container.
* `generateSas(path, values)`: mints a SAS token scoped to a single blob.
* `generateContainerUserDelegationSas(values, key)`: the container scoped user delegation
  variant.
* `generateUserDelegationSas(path, values, key)`: the blob scoped user delegation variant.

`generateContainerSas` and `generateSas` sign with the account key, so the client must be
authenticated with a shared key (or a connection string carrying an account key); rotating the
account key revokes every SAS minted from it. The signature values carry the validity window
and the permissions, and optionally a start time, the permitted protocol set (HTTPS only, or
HTTPS and HTTP), an IP range, and a stored access policy `identifier`. A parameter may be
carried by the stored access policy or by the token, but not both; a parameter set in both
places fails at use time with HTTP 400. Generation validates locally what is knowable at
signing time, and fails with a client side `Error` when no `identifier` is supplied and the
`expiryTime` or the `permissions` are missing.

The signature values and their permissions record come in two shapes, one per scope, so a
token cannot ask for a permission its scope does not carry. The container scoped methods take
`ContainerSasSignatureValues`, whose `ContainerSasPermissions` carries one boolean per wire
permission whose operation this module offers: `read`, `add` (appending, section 4.10),
`create`, `write`, `delete`, `list`, `tag` (index tags, section 4.7), and `filter` (tag
queries, section 4.7). The blob scoped methods take `BlobSasSignatureValues`, whose
`BlobSasPermissions` carries the same set without `list` and `filter`.

The user delegation variants sign with a `UserDelegationKey` (from
`AdminClient.getUserDelegationKey`) instead of the account key, so no storage key is handled.
They are valid at most 7 days (the key's lifetime), and stored access policies do not apply to
them: the user delegation variants reject an `identifier` and require an explicit `expiryTime`
and `permissions`.

###### Example: Minting a Read Only SAS for One Blob

```ballerina
import ballerina/time;

string sasToken = check invoices.generateSas("2026/07/invoice.pdf", {
    expiryTime: time:utcAddSeconds(time:utcNow(), 3600),
    permissions: {read: true}
});
```

## 5. The Listener and Caller

### 5.1 The Event Delivery Path

Azure Blob Storage publishes blob events through Azure Event Grid, and Event Grid delivers to
an Azure Storage queue as a pull destination. The listener consumes that queue: delivery is
near real time, deletions are observable, and the container is never listed.

The wiring is created once, outside the application: a storage queue in the account, and an
Event Grid subscription on the storage account with the queue as its destination. The
subscription's filters choose which events arrive: the event types (blob created, blob
deleted) and, through a subject prefix such as `/blobServices/default/containers/invoices/`,
the containers covered. The listener consumes whatever reaches its queue.

###### Example: Reacting to New Blobs

```ballerina
import ballerinax/azure.storage.blob;

type Invoice record {|
    string id;
    decimal total;
|};

listener blob:Listener blobListener = check new ("blob-events",
    auth = {accountName: "myacct", accountKey: "..."}
);

// The container is the attach point; a .json blob created in it is bound to the record.
service /invoices on blobListener {
    remote function onBlobJson(Invoice invoice, blob:BlobEvent event, blob:Caller caller) returns error? {
        // Process the invoice. The event is acknowledged when the handler returns normally.
        check caller->setTags(event.path, {status: "processed"});
    }
}
```

### 5.2 Initializing the Listener

The constructor takes the queue name and the listener configuration as an included record
parameter:

* `auth`: the authentication configuration (section 2.1). The credential must cover both the
  queue (receive, update, and delete messages, and create the poison queue) and the blob
  operations the handlers perform; a SAS credential must be an account SAS spanning both
  services. Required.
* `maxPollingIntervalSeconds`: the ceiling of the empty queue polling backoff. When the queue
  is empty, polls back off with randomized exponential delays up to this ceiling. Defaults
  to 60.
* `batchSize`: how many messages one receive fetches, 1 to 32. Defaults to 16.
* `newBatchThreshold`: the next batch is fetched when the number of events still being
  handled drops to this value, so concurrent handling is bounded by
  `batchSize + newBatchThreshold`. Ranges from 0 to `batchSize`. Defaults to half the batch
  size.
* `redeliveryDelaySeconds`: how long a message stays invisible after its handler fails,
  before it is redelivered. Defaults to 0 (immediate redelivery).
* `maxDeliveryCount`: how many deliveries a message gets before it is moved to the poison
  queue (section 5.7). Defaults to 5.
* `retryConfig` and `transportConfig`: as for the clients (sections 2.3 and 2.4).
* `laxDataBinding`: relaxes the typed content handlers' binding (section 5.5). Defaults to
  `false`.
* `queueServiceUrl`: overrides the queue endpoint, which otherwise derives from the account
  name as `https://{accountName}.queue.core.windows.net`. For a SAS URL credential the queue
  endpoint is instead the SAS URL's host with its blob service label replaced by the queue one,
  and `queueServiceUrl` is required when that host carries none; a connection string derives
  it from its queue endpoint or its account name, unless `queueServiceUrl` overrides it.

Message visibility is not configuration. A received message is hidden for a fixed window, and
the listener extends that window for as long as its handler runs, so a slow handler never
loses its message to redelivery. A process that dies mid handling leaves its messages hidden
until the window expires, within ten minutes.

The lifecycle methods return `error?`. `attach` follows the rules of section 5.3; `detach` of a
service that is not attached fails; `'start` on a listener that is already running fails.
`gracefulStop` stops polling and waits for the handlers already running to finish.
`immediateStop` stops polling and returns without waiting; messages whose handlers were still
running reappear on the queue when their visibility window expires.

### 5.3 Services and the Container Attach Point

One listener consumes one queue, and several services attach to it, one per container. The
container is the service's attach point: `service /invoices on blobListener` handles the
events of the `invoices` container. The name is one segment satisfying Azure's container name
rule, or `$root` or `$logs`; a leading slash is removed. Routing is an exact match on the
event's container. A service with no attach point receives the events of every container no
named service claims. An event for a container no service claims is acknowledged and logged at
debug level. Attaching a second service for the same container, or a second service with no
attach point, fails.

A service is declared against the module's `Service` type, a marker the service object is
attached by; it carries no members of its own. A service declares at least one of the six
dispatchable handlers of section 5.4; `onError` does not satisfy the requirement. The handler
set, each handler's parameter types, and the `error?` return are validated at compile time by
the module's compiler plugin, which also rejects resource functions and unknown remote methods.

### 5.4 Handlers and Routing

A service declares its handlers by name:

* **`onBlob`**: the raw bytes catch all for created blobs. Takes its content as `byte[]` or as
  `stream<byte[], error?>`.
* **`onBlobText`**: takes a `string`.
* **`onBlobJson`**: takes a `json` value or a record. A `json` parameter receives any parsed
  root as is; a record binds an object root by projection.
* **`onBlobXml`**: takes an `xml` document or a record whose fields bind from the document's
  elements.
* **`onBlobCsv`**: takes a string matrix (`string[][]`), a record array, a
  `stream<string[], error?>`, or a `stream<record {}, error?>`. The record forms map each
  row's fields through the blob's first row, the header; the string forms keep every row, the
  header row included.
* **`onBlobDeleted`**: invoked for each deleted blob. Takes the `BlobEvent`.
* **`onError`**: the error notification handler (section 5.8).

A content handler takes its content first, then optionally the `BlobEvent` (section 5.6), then
optionally the `Caller` (section 5.9), in that order; the listener passes only what the
handler declares. `onBlobDeleted` takes the `BlobEvent` and, optionally, the `Caller`.

Created events are routed by the blob name's extension: `txt` to `onBlobText`, `json` to
`onBlobJson`, `xml` to `onBlobXml`, `csv` to `onBlobCsv`, and everything else to `onBlob`. A
name with no extension is routed by the event's content type (`text/*`, `application/json`,
`application/xml` or `text/xml`, `text/csv`), else to `onBlob`. A per handler
`@blob:FunctionConfig` overrides the routing: its `namePattern` is a regular expression
matched against the blob name (the last path segment), and its `contentTypePattern` one
matched against the event's content type without its parameters. When more than one handler
matches, the winner is
fixed: handlers are checked in the order `onBlobText`, `onBlobJson`, `onBlobXml`, `onBlobCsv`,
then `onBlob`. A blob whose routing names an undeclared typed handler falls back to `onBlob`.
A created event whose routing finds no declared handler is acknowledged and logged at debug
level.

Metadata, property, and index tag changes fire no event, and on flat namespace accounts
neither does appending to an append blob. A process that must observe those changes polls the
relevant read operations instead.

###### Example: Routing by Name Pattern

```ballerina
service /reports on blobListener {
    @blob:FunctionConfig {namePattern: "daily-.*\\.csv"}
    remote function onBlobCsv(stream<Reading, error?> rows) returns error? {
        // Rows are bound lazily, one record per pull.
    }

    remote function onBlob(stream<byte[], error?> content, blob:BlobEvent event) returns error? {
        // Every other created blob in the container.
    }
}
```

### 5.5 Content Binding

A content handler receives the blob's content as it is at fetch time, which is after the event
fired. A blob replaced in between is delivered with its current content, and its own later
event follows; `event.eTag` lets a handler detect the change. Content is fetched only for
created events and only when a content handler is declared for the blob.

Binding is strict by default. Setting `laxDataBinding` on the listener relaxes it: JSON and
CSV record binding treat a null value as an optional field and an absent member as a nilable
field, and XML record binding tolerates elements the record does not declare.

A fetch or binding failure is disposed of by its cause:

* The blob is not found (HTTP 404): `onError` is notified and the message is acknowledged; the
  blob's own deleted event follows.
* The blob is archived (`ArchivedBlobError`): `onError` is notified and the message is
  acknowledged.
* Content that fails to bind to the handler's parameter type: `onError` is notified with a
  client side `Error` and the message is acknowledged.
* Any other failure: `onError` is notified and the message is left to redeliver
  (section 5.7).

### 5.6 The Blob Event

The `BlobEvent` record carries what the storage event provides, in both the Event Grid and the
CloudEvents delivery schemas (the listener detects the schema per message):

* `eventType`: `BLOB_CREATED` or `BLOB_DELETED`.
* `containerName` and `path`: the blob's container and its container relative name, derived
  from the event subject.
* `url`: the blob's full URL, which `Caller.copyBlobFromUrl` takes as its source.
* `eventTime`: when the event fired.
* `api`: the storage operation that caused the event (for example `PutBlob`, `PutBlockList`,
  `CopyBlob`, `DeleteBlob`).
* `sequencer`: always present. An opaque string whose ordering is comparable per blob path,
  for detecting stale events about the same blob.
* `contentType`, `contentLength`, `blobType`, `eTag`: as provided by the event. The event
  schema documents only `BlockBlob` and `PageBlob` as blob types, so an append blob's events
  may not identify their type.

A created event fires when a blob's content is fully committed: an upload, a committed block
list, or a completed copy. Creation and replacement are the same event; overwriting an
existing blob fires it again, and the event does not say which happened (the `api` field
carries the causing operation). The event describes the blob at the moment it fired; the blob
may have changed or been deleted since.

The service publishes two further event types, `BlobTierChanged` and
`AsyncOperationInitiated`. The listener acknowledges them without dispatching; subscriptions
should filter to the created and deleted events.

### 5.7 Delivery Semantics and Poison Messages

Delivery is at least once. Up to `batchSize + newBatchThreshold` events are handled
concurrently, and a message stays invisible to other consumers while its handler runs
(section 5.2).

When the handler returns normally, the listener acknowledges the event by deleting the queue
message; it is never delivered again. When the handler returns an error or panics, the message
is made visible again after `redeliveryDelaySeconds` and redelivered. A message that reaches
`maxDeliveryCount` deliveries without an acknowledgement is moved to the poison queue, named
`{queueName}-poison` and created on demand, and an error is logged; processing of other
messages continues. Poison messages never expire; the poison queue holds them until someone
inspects and removes them. Duplicate delivery is always possible, so handlers must be
idempotent; the event's `sequencer` supports ignoring stale duplicates about one blob. A
handler that writes into a container the subscription covers fires further created and
deleted events, which come back to the same service; such a handler must recognise its own
output, for example by path, or the subscription's subject filter must exclude it.

Delivery has two independent time bounds, and raising one does not extend the other.

* An event that reaches the queue is subject to the queue service's message time to live,
  seven days by default, so events waiting for a listener that stays down longer are lost.
  The Event Grid subscription's message time to live setting raises it.
* An event that never reaches the queue is subject to Event Grid's own retry policy, which
  stops at whichever of its two limits is hit first: thirty delivery attempts, or an event
  time to live of at most 1,440 minutes (24 hours), which is also its default. A queue
  unreachable for a day loses those events regardless of any queue side setting. Event Grid
  then drops the event unless the subscription names a dead letter storage container, which
  is off by default.

Both are subscription level setup, outside this module's configuration.

### 5.8 Error Notification

A service may declare `onError`, taking the `Error` and, optionally, the `Caller`:
`remote function onError(blob:Error err, blob:Caller caller) returns error?`. It is notified
when a poll of the queue fails (with the mapped typed error, for example an
`AuthorizationError` when the credential cannot receive messages), when a message cannot be
parsed as a storage event (with a client side `Error`; the message follows the redelivery and
poison path), and when a content fetch or binding fails (section 5.5). It is not an event
handler and does not satisfy the at least one handler requirement. An error returned by
`onError` itself is logged. Errors returned by the event handlers do not notify `onError`; they
drive redelivery (section 5.7).

A failed poll belongs to no event, so every attached service's `onError` is notified of it;
a catch-all service's `onError` that takes a `Caller` is skipped for it, since no container
binds one. A failed poll also logs its error, and polling keeps its backoff schedule, so the
next poll tries again.

### 5.9 The Caller

A `Caller` is passed to each handler so it can act on the event's blob without constructing a
separate client; it cannot be created by user code. It is bound to the service's container and
forwards seven operations of the `Client` with identical signatures: `getBlob`,
`getBlobProperties`, `download`, `upload`, `deleteBlob`, `copyBlobFromUrl`, and `setTags`,
each with the semantics of its `Client` counterpart. Handlers pass the event's path
explicitly, for example `caller->deleteBlob(event.path)`. `copyBlobFromUrl` copies into the
service's own container; copying to another container from a handler uses a `Client` bound to
that container.

###### Example: Marking and Consuming on Event

```ballerina
remote function onBlobJson(Invoice invoice, blob:BlobEvent event, blob:Caller caller) returns error? {
    check caller->upload(invoice, string `processed/${event.path}`);
    check caller->deleteBlob(event.path);
}
```

## 6. Errors

Every error raised by an operation of this module is a subtype of the distinct `Error` type.
The hierarchy splits by origin: an error the Azure service raised is a `ServiceError` carrying
the HTTP status and the Azure error code of the failed request in its detail, while a client
side failure is the generic `Error` with no detail. The service's description becomes the
Ballerina error's `message()`.

* **`Error`**: the root type, and the type of every client side failure.
* **`ServiceError`**: any error raised by the Azure service, carrying `httpStatus` and
  `errorCode` in its `ServiceErrorDetail`. A service failure whose Azure error code maps to
  none of the subtypes below stays this generic type.
  * **`NotFoundError`**: the requested container or blob was not found (HTTP 404;
    `BlobNotFound`, `ContainerNotFound`), or, for a `Listener`, its queue or a message on it
    (`QueueNotFound`, `MessageNotFound`). Queue service failures reach `onError` through the
    same mapping.
  * **`ConflictError`**: the operation conflicts with the current state of the resource, for
    example creating a container that already exists, deleting a blob whose snapshots were
    not directed, or a lease operation against the wrong lease state (HTTP 409).
  * **`AuthorizationError`**: authentication or authorization failed, for example an invalid
    key or insufficient SAS permissions (HTTP 403).
  * **`PreconditionFailedError`**: a precondition such as a lease id requirement was not met
    (HTTP 412; `LeaseIdMissing`, `LeaseIdMismatchWithBlobOperation`, `LeaseLost`).
  * **`RangeNotSatisfiableError`**: the requested byte range cannot be satisfied for the
    target blob, including misaligned page ranges (HTTP 416; `InvalidRange`,
    `InvalidPageRange`).
  * **`ArchivedBlobError`**: the blob's content is unavailable because of the archive tier,
    either still archived or currently rehydrating (HTTP 409; `BlobArchived`,
    `BlobBeingRehydrated`).
  * **`InvalidBlobTypeError`**: the operation applies to another blob type, for example
    appending to a block blob or writing pages to an append blob (HTTP 409;
    `InvalidBlobType`).

The mapping keys on the Azure error code, not the HTTP status alone: `BlobArchived`,
`BlobBeingRehydrated`, and `InvalidBlobType` (each HTTP 409) map to their own types, distinct
from the state conflicts (also HTTP 409) that map to `ConflictError`. Check the more specific
error types before the more general ones.

###### Example: Handling a Specific Failure

```ballerina
blob:BlobProperties|blob:Error properties = invoices->getBlobProperties("2026/07/invoice.pdf");
if properties is blob:NotFoundError {
    // The blob is absent; create it, or skip.
} else if properties is blob:Error {
    return properties;
}
```
