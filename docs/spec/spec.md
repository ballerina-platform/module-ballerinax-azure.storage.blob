# Specification: Ballerina Azure Blob Storage Library

_Owners_: @YasanPunch \
_Reviewers_: @niveathika \
_Created_: 2026/08/12 \
_Updated_: 2026/10/16 \
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
   * 5.1 [The Event Source](#51-the-event-source)
   * 5.2 [Initializing the Listener](#52-initializing-the-listener)
   * 5.3 [Services and Their Queues](#53-services-and-their-queues)
   * 5.4 [Message Visibility](#54-message-visibility)
   * 5.5 [Handlers and Routing](#55-handlers-and-routing)
   * 5.6 [Content Binding](#56-content-binding)
   * 5.7 [The Blob Event](#57-the-blob-event)
   * 5.8 [The Order of Checks](#58-the-order-of-checks)
   * 5.9 [Delivery and Settlement](#59-delivery-and-settlement)
   * 5.10 [The Poison Queue, Logging, and Monitoring](#510-the-poison-queue-logging-and-monitoring)
   * 5.11 [Delivery Time Bounds](#511-delivery-time-bounds)
   * 5.12 [Error Notification](#512-error-notification)
   * 5.13 [The Caller](#513-the-caller)
   * 5.14 [Parsing Events Outside the Listener](#514-parsing-events-outside-the-listener)
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
* The `Listener` consumes the blob events (created, deleted) that Azure Event Grid
  subscriptions deliver to Azure Storage queues. Each attached service consumes the queue named
  in its `@blob:ServiceConfig` annotation; the listener binds each new blob's content and
  dispatches it to that service's handlers.
* The `Caller` is passed to each listener handler. It forwards a subset of the `Client`
  operations, bound to the event's container, so a handler can act on the event's blob
  without constructing a separate client.

The public `parseEvent` function turns a blob event received by other means, such as through
Azure Service Bus, into the same `BlobEvent` record the listener's handlers receive
(section 5.14).

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
the SAS generation methods, which sign tokens locally. The clients have no close method: a
client holds its connections for as long as it is reachable, and they are freed once it is not.
Create a client once and reuse it.

Leases gate writes throughout the module by one rule: every operation that writes a leased
resource carries an optional `leaseId` in its options record, and reads never require one. A
write to a leased blob without the current lease id, and a delete of a leased container
without it, fail with a `PreconditionFailedError`; a lease id that is given is always checked
(section 4.9). The one exception is `createSnapshot`, which succeeds on a leased blob without
the lease id (section 4.8).

## 2. Configuration

### 2.1 Authentication

The authentication configuration, `AuthConfig`, is exactly one of `SharedKeyConfig`,
`SasConfig`, `SasUrlConfig`, `ConnectionStringConfig`, or `EntraIdConfig`. Each member is one
credential artifact, the thing the Azure portal, CLI, or infrastructure tooling hands the user.
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
`Storage Blob Data Owner` adds the access policy, index tag, and tag query operations. Each is
assignable at container scope or wider; creating a container needs an assignment at storage account
scope or wider. Obtaining a user delegation key needs the `generateUserDelegationKey` action at
storage account scope or wider (resource group or subscription), and a container scoped assignment
does not grant it. `Storage Blob Delegator` is the least privileged built in role carrying it, and
any of the blob data roles above assigned at that scope also does. Creating and deleting containers
and setting container metadata need only `Storage Blob Data Contributor`. Reading and setting the
blob service properties need the storage account's `blobServices/read` and `blobServices/write`
actions, whose least privileged built in role is `Storage Account Contributor`. A `Listener`
authenticating with Entra ID additionally needs `Storage Queue Data Contributor`: it receives,
updates, sends, and deletes queue messages and creates any poison queue that is missing, and the
narrower `Storage Queue Data Message Processor` can neither update nor send messages, nor create a
queue.

The `Listener` authenticates with the same union. One credential serves every queue, every
poison queue, and every `Caller` of a listener, so it must cover both the queue service
(receiving, updating, and deleting messages, sending to the poison queues, and creating any that
is missing) and the blob operations its handlers perform. A SAS credential for a listener must be an account SAS
spanning the queue and blob services, as minted by `AdminClient.generateAccountSas`
(section 3.5); a container or blob scoped SAS cannot poll a queue. `init` rejects a listener SAS
that carries `sr` (a service SAS) or whose `ss` lacks `b` or `q`, with an error naming the
missing part, and applies the same check to the `SharedAccessSignature` of a connection string; the
permission letters are not checked locally, and a missing one fails at the first call that needs it
with an `AuthorizationError`. Such a SAS covers the blob and queue services, the container and
object resource types, and the permissions read, add, create, update, and process (`ss=bq`,
`srt=co`, `sp=racup`): read for Get Blob, add for Put Message to a poison queue, create for Create
Queue when a poison queue is missing, update for Update Message, and process for Get Messages and
Delete Message. The container resource type and the create permission serve only the creation of
poison queues, so with pre-created poison queues `srt=o` and `sp=raup` suffice. Each `Caller`
operation the handlers use adds its own permission (`w` for `upload` and `copyBlobFromUrl`, `d` for
`deleteBlob`, `t` for `setTags` and for an upload or copy that sets tags).

### 2.2 Client Configuration

Both clients take the same `ClientConfiguration` record: the required `auth` (an `AuthConfig`
member, section 2.1), an optional `retryConfig` (section 2.3), and `transportConfig`
(section 2.4), which always applies and defaults to the values listed there. The configuration
is an included record parameter on both `init` methods, so callers pass its fields as named
arguments, for example `new (auth = {accountName, accountKey})`.

### 2.3 Retry Configuration

The optional `RetryConfig` record shapes the retry behaviour of service requests; omitting it
keeps the connector's defaults, which are the values listed below. Its fields:

* `retryPolicyType`: how the delay between tries grows, `EXPONENTIAL` or `FIXED_INTERVAL`.
  Defaults to `EXPONENTIAL`.
* `maxTries`: the maximum number of tries, counting the first attempt. Defaults to 4.
* `tryTimeout`: the timeout applied to each individual try. Defaults to 60.
* `retryDelay`: the base delay between tries. Defaults to 4.
* `maxRetryDelay`: the upper bound on the delay between tries. Defaults to 120.
* `secondaryHostUrl`: a secondary endpoint to retry reads against, for geo redundant accounts.
  On a listener it applies to the blob requests only; the queue requests never retry against a
  secondary. No default.

### 2.4 Transport Configuration

The `TransportConfig` record covers the HTTP transport. Its fields:

* `proxy`: routes the connector's traffic through an HTTP, SOCKS4, or SOCKS5 proxy, with
  optional credentials and a bypass list. No default.
* `connectionPool`: tunes the connection pool. Its `maxConnections` defaults to 50,
  `idleTimeout` to 60, `connectTimeout` to 10, and `readTimeout` to 60.
  These values apply whenever the pool is not configured. On a listener the connection pool
  is shared by every service and every `Caller`, and it applies per remote address (host and
  port): the blob endpoint and the queue endpoint each get a pool of `maxConnections`, and so
  do an emulator's blob and queue ports on one host.
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
  whose identity holds the `generateUserDelegationKey` action at storage account scope or
  wider, which `Storage Blob Delegator` and any blob data role assigned there grant
  (section 2.1). The key is valid at most 7 days.
* `generateAccountSas(values)`: mints an account level SAS token. This is an ordinary method,
  invoked with `.`: it signs the token locally with the account key and makes no service call.
  It requires a client authenticated with a shared key (or a connection string carrying an
  account key). The signature values select the covered services (`blob`, `queue`), the
  resource types (`'service`, `container`, `'object`), the permissions, and the validity
  window, and optionally a start time, the permitted protocol set, and an IP range. The
  account level permissions are one boolean per account permission: `read`, `write`,
  `delete`, `list`, `add`, `create`, `update`, `process`, `tag`, and `filter`; `update` and
  `process` are queue operations, and `tag` and `filter` cover the index tag operations and
  tag queries. An account SAS covering the blob and queue services, the `container` and
  `'object` resource types, and `read`, `add`, `create`, `update`, and `process` is the
  credential a `Listener` needs (section 2.1). Rotating the account key
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
change, so each call is two requests, and two processes changing the two halves at the same
time can overwrite each other: the last writer wins. An application that changes both
concurrently serialises its own calls.

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

The content an upload accepts, `UploadContent`, is one of: `byte[]`, `string`, `json`, `xml`,
a record (`record {}`), an array of records (`record {}[]`), a stream of byte arrays
(`stream<byte[], error?>`), or a stream of records (`stream<record {}, error?>`). These members
mirror the `getBlob` targets (section 4.4.2), so anything written is readable back in the same
type.

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

`getBlob` returns the blob's content in the form the target type selects. The target type,
`RetrievableType`, is one of the same members an upload accepts (section 4.4.1), each bound as
follows:

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
destination inherits the source's metadata unless the copy sets metadata, but it carries no
index tags unless the copy sets them. The
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
set and read whole. The service refuses index tags on an account with a hierarchical
namespace, so the three operations fail there with the service's error. The tag index is
eventually consistent: a `findBlobsByTags` query may briefly lag a `setTags` write, while
`getTags` always reads the blob's current tags.

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
  returns the snapshot id. `CreateSnapshotOptions` carries metadata for the snapshot and an
  optional lease id. A snapshot of a leased blob succeeds without the lease id, while a lease
  id that is given is checked and a wrong one fails with a `PreconditionFailedError`.

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
  immediately; an explicit period (0 to 60) can only shorten the break, never lengthen it. The operation returns the seconds until
  the lease is actually free.
* `changeLease(path, leaseId, proposedLeaseId)`: changes the lease id, handing the lease over
  without a release window in which another client could acquire it. Returns the new lease id.

While a blob is leased, the write operations of this module pass the lease id through their
options; a write without the current lease id, or with a wrong one, fails with a
`PreconditionFailedError`, the index tag operations and `setAccessTier` included. The one
exception is `createSnapshot`, which succeeds without the lease id and checks only one that is
given (section 4.8). Failures of the lease operations
themselves, against a stale id or the wrong lease state, surface as a `ConflictError`; the
error code in the detail names the exact condition. The module offers no container lease
operations, but it honors an externally held container lease (sections 3.2 and 4.2). Such a
lease gates only the container's deletion, which fails with a `PreconditionFailedError` without
the current lease id. Other container writes succeed without it, while a wrong lease id passed
to them fails with a `PreconditionFailedError`. A blob write's lease id always refers to the
blob's own lease: the container's lease id is checked only by container operations.

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
(section 5.5).

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

### 5.1 The Event Source

Azure Blob Storage publishes blob events through Azure Event Grid, and Event Grid delivers to
an Azure Storage queue as a pull destination. The listener consumes such queues: delivery is
near real time, deletions are observable, and the container is never listed.

The wiring is created once, outside the application: for each queue, a storage queue in the
account and an Event Grid subscription on the storage account with that queue as its
destination. The subscription does all filtering. Its event types (`includedEventTypes`), its
subject filters (`subjectBeginsWith`, such as `/blobServices/default/containers/invoices/`,
and `subjectEndsWith`, such as `.json`), and its advanced filters choose which containers,
name prefixes, name suffixes, and event types reach the queue. Advanced filters compare
ignoring case; the values of one filter are alternatives, and every filter must hold; a
subscription takes up to 25 advanced filters and 25 values. A filter on a key an event may
lack, such as `data.api` or `data.blobType`, uses `StringNotIn`, which keeps the events that
lack it; the prefix and suffix operators (`StringNotBeginsWith`, `StringNotEndsWith`,
`StringEndsWith`) are used only on `subject`, which every event carries. The listener adds no
filtering of its own: each service consumes whatever reaches its queue. Splitting the events into
separate streams, by container or by suffix, means one subscription and one queue per stream,
and one service per queue (section 5.3).

###### Example: Reacting to New Blobs

```ballerina
import ballerinax/azure.storage.blob;

type Invoice record {|
    string id;
    decimal total;
|};

listener blob:Listener blobListener = check new (auth = {accountName: "myacct", accountKey: "..."});

// The service consumes the queue its annotation names; a .json blob is bound to the record.
@blob:ServiceConfig {queueName: "invoice-events"}
isolated service on blobListener {
    isolated remote function onBlobJson(Invoice invoice, blob:BlobEvent event, blob:Caller caller) returns error? {
        // Process the invoice. The event is acknowledged when the handler returns normally.
        check caller->setTags(event.path, {status: "processed"});
    }
}
```

### 5.2 Initializing the Listener

The constructor takes the listener configuration as an included record parameter. It names no
queue: each service names its own (section 5.3). The configuration's fields:

* `auth`: the authentication configuration (section 2.1). One credential serves every queue,
  every poison queue, and every `Caller` of the listener, so it must cover both the queue
  service (receive, update, and delete messages, send to the poison queues, and create any that
  is missing) and the blob operations the handlers perform; a SAS credential must be an account SAS spanning
  both services. Required.
* `laxDataBinding`: relaxes the typed content handlers' binding (section 5.6). Defaults to
  `false`.
* `retryConfig` and `transportConfig`: as for the clients (sections 2.3 and 2.4). The
  listener's connection pool is shared by every service and every `Caller`, per remote address
  (section 2.4), and `retryConfig.secondaryHostUrl` applies to its blob requests only: the
  queue requests never retry against a secondary.
* `queueServiceUrl`: overrides the queue endpoint. When it is omitted and the credential
  carries a `serviceUrl`, the queue endpoint is that host with its `.blob.` label replaced by
  `.queue.`, and `init` fails, asking for `queueServiceUrl`, when the host carries no such
  label (a custom domain, an IP address, or an emulator address). Without a `serviceUrl` the
  queue endpoint derives from the account name as `https://{accountName}.queue.core.windows.net`.
  For a SAS URL credential the queue endpoint is the SAS URL's host with its blob service label
  replaced by the queue one, and `queueServiceUrl` is required when that host carries none.
  For a connection string the queue endpoint is its `QueueEndpoint` when present; otherwise,
  when it names an explicit `BlobEndpoint`, that host with `.blob.` replaced by `.queue.`, and
  `init` fails, asking for `queueServiceUrl`, when that host carries no such label; otherwise it
  derives from its `AccountName`, `DefaultEndpointsProtocol`, and `EndpointSuffix`, so sovereign
  clouds are honoured. A connection string from which no queue endpoint derives fails at
  `init`. `queueServiceUrl` overrides each of these.

The lifecycle methods return `error?`. Module level services attach before the listener
starts; attaching and detaching on a running listener arise from imperative code.

* `attach(Service s, string[]|string? name = ())` takes the attach point in the form the
  language's listener type expects. It rejects a non-empty attach point at runtime, as the compiler plugin
  does for a declared service, since an imperative `attach` call never passes through the
  plugin. It validates the service and its annotation (section 5.3), and on a running listener
  starts polling that service's queue at once. It logs a warning when the attached
  services' combined concurrency, the sum of each one's `batchSize + newBatchThreshold`, exceeds
  `transportConfig.connectionPool.maxConnections`. The warning prints the sum and
  `maxConnections` and states that the sum is the most blob fetches that can be in flight, an
  upper bound rather than connections held: stream reads hold no connection between chunks,
  and the queue traffic has its own pool. Three services at the default settings raise it; a
  larger `maxConnections` or a smaller `batchSize` removes it. Attaching to a stopped listener
  fails.
* `'start` begins polling the queue of every attached service. Starting a listener that is
  already running fails, and a stopped listener cannot be started again.
* `detach` on a running listener stops polling that service's queue and returns at once.
  Handlers already running keep their messages hidden (section 5.4) and settle normally when
  they finish, and their `Caller` keeps working. Detaching a service that is not attached
  fails.
* `gracefulStop` stops polling and waits for the handlers still running, those of detached
  services included, for up to ten minutes. Past that bound it returns with a logged warning;
  in a process that keeps running, the remaining handlers continue and settle normally.
* `immediateStop` stops polling and returns at once. It never releases a message in flight:
  handlers still running keep their messages hidden and settle normally.

A receive that completes after its service is detached or the listener is stopped dispatches
nothing: each message it returned is made visible again at once, which spends one of its
deliveries.

Stopping a listener that was never started fails, and stopping a stopped listener again does
nothing. The listener's shared connections are released once it is stopped and its last
running handler has finished. A process that exits first leaves its messages hidden until
their visibility window expires, within ten minutes (section 5.4).

On SIGTERM or SIGINT the runtime calls each listener's `gracefulStop` in turn and then exits.
Handlers still running when the process exits, after `gracefulStop` returns or when the
platform kills it at the end of its grace period (30 seconds by default on Kubernetes), do not
settle, and their messages reappear
within the visibility window; a long wait in one listener delays the stop of the next.

### 5.3 Services and Their Queues

Several services attach to one listener. Each names the queue it consumes in its
`@blob:ServiceConfig` annotation and has its own polling, its own consumption settings, and
its own poison queue. The annotation is required, and the service's attach point must be
empty: `isolated service on blobListener`. The compiler plugin rejects a declared service with
an attach point, and `attach` rejects a non-empty one at runtime (section 5.2). The
annotation's fields:

* `queueName`: the queue the service consumes. Required.
* `poisonQueueName`: the queue failed messages move to (section 5.10). Defaults to
  `{queueName}-poison`.
* `batchSize`: how many messages one receive fetches, 1 to 32. Defaults to 16.
* `newBatchThreshold`: the next batch is fetched when the number of events still being
  handled drops to this value, so a service has at most `batchSize + newBatchThreshold`
  messages in flight, whether or not it is isolated (section 5.9).
  Ranges from 0 to `batchSize`. Defaults to half the batch size, rounded down (0 for a batch size of 1).
* `maxPollingInterval`: the ceiling of the empty queue polling backoff. When the queue
  is empty, polls back off with randomized exponential delays up to this ceiling. Must be
  above 0. Defaults to 60.
* `redeliveryDelay`: how long a message stays invisible after its handler fails,
  before it is redelivered, 0 to 604,800 (seven days), rounded up to whole seconds. Defaults
  to 0 (immediate redelivery).
* `maxDeliveryCount`: how many deliveries a message gets before it is moved to the poison
  queue (section 5.10), at least 1. Defaults to 5. With 1, any delivery that does not settle,
  such as one cut short by a crash or by a routine shutdown or redeploy (section 5.2), a lost
  pop receipt, or a batch released at a stop, sends the message to the poison queue on its next
  receipt.

Both queue names follow Azure's queue name rules: 3 to 63 characters of lowercase letters,
digits, and hyphens, beginning and ending with a letter or a digit, with no consecutive
hyphens. The default poison queue name adds seven characters, so a service whose `queueName`
exceeds 56 characters must set `poisonQueueName`, and attaching it without one fails with an
error naming that field. Consuming a poison queue whose source queue's name exceeds 49
characters (so its own name exceeds 56) therefore also needs an explicit `poisonQueueName`. A
`poisonQueueName` equal to the service's own `queueName` is refused, and so is a service naming a
queue that another service of the same listener already consumes. A service may name another
attached service's queue as its poison queue, or consume another service's poison queue, for
inspection and replay. Several services may share one poison queue, which keeps a single queue to
monitor. Replay can then route a message back by its event's subject only while those services'
subscription filters do not overlap on a container; otherwise the source queue in the move's error
line (section 5.10) is the only record of where it came from. When the
poison queues of one listener's attached services would form a cycle (one service's poison
queue is another's queue and that one's poison queue is the first's, or a longer chain),
`attach` refuses the service that closes it, naming the queues. A cycle across listeners or
processes cannot be detected, so the deployment must avoid one. Only attached services count
toward the duplicate and cycle checks, so a detached service's queue may be reused. These
checks, and the ranges of the consumption settings above, run at `attach`, and at compile time
for values given as literals.

Several processes or listeners consuming one queue are competing consumers, a valid way to
scale out: each message is held by one of them at a time. Competing consumers must all read
from the same account: a queue fed by several accounts cannot be shared by consumers of
different accounts, since each acknowledges the others' events as foreign (section 5.8), so
such consumers need separate queues.

A service is declared against the module's `Service` type, a marker the service object is
attached by; it carries no members of its own. A service declares at least one of the six
dispatchable handlers of section 5.5; `onError` does not satisfy the requirement. The handler
set, each handler's parameter types, and the `error?` return are validated at compile time by
the module's compiler plugin, which also rejects resource functions and unknown remote methods.

### 5.4 Message Visibility

Message visibility is not configuration. A received message is hidden for a ten minute window,
and the listener renews that window every five minutes for as long as its handler, or an
`onError` call for that message (section 5.12), runs, so a slow handler keeps its message while
renewal succeeds. After a transient renewal failure (a server side error, a network error, or a
timeout) the next renewal comes sooner: the interval halves, from five minutes to two and a
half and then one and a quarter, down to a floor of one minute, and each failure logs a
warning. After a successful renewal the interval returns to five minutes. Each attempt is
scheduled from the start of the previous one, not its end. Renewal stops when the message is
no longer the listener's to hold (its pop receipt no longer matches, the message or the queue
is not found, the queue is being deleted, or the service refuses a visibility past the
message's expiry), when the expiry margin below is reached, or when the message is disposed
of, including after a failed or skipped settlement step (section 5.10). Renewal never hides a message past its expiry (section 5.11): each renewal
is clamped to the message's remaining time less a safety margin of 60 seconds. When that
clamped value is not positive, renewal stops and an error is logged that the message is bound
to expire while its handler still runs; a visibility of zero or less is never sent, since it
would release a running message to other consumers. During that last minute the message is
visible, so any consumer, this listener included, may receive it once more before it expires;
that delivery is one of the duplicates section 5.9 asks handlers to tolerate.

Each renewal call runs without blocking and times out at the smaller of 30 seconds and the
hold time remaining, so one hung call cannot delay the renewal of other messages. The calls
whose failures the listener interprets itself run without the SDK's retries: renewals, which
the halving schedule retries, and the hold check that opens a move to the poison queue
(section 5.10). The other settlement calls, among them the redelivery delay update, the
release at a stop, and the poison send, keep `retryConfig` (section 2.3). Renewal and settlement of
one message never overlap, and settlement always uses the newest pop receipt. One duplicate remains
possible: a renewal that succeeds on the service but times out on the client leaves no usable pop
receipt, so the message reappears when the hold ends and is handled again.

A warning is logged at each full window (10, 20, 30 minutes, and so on) that a handler or
`onError` is still running. A hung handler keeps its message hidden and occupies one of its
service's concurrency slots for as long as it runs.

A process that dies mid handling leaves its messages hidden until the window expires, within
ten minutes.

### 5.5 Handlers and Routing

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
* **`onBlobDelete`**: invoked for each deleted blob. Takes the `BlobEvent`.
* **`onError`**: the error notification handler (section 5.12).

A content handler takes its content first, then optionally the `BlobEvent` (section 5.7), then
optionally the `Caller` (section 5.13), in that order; the listener passes only what the
handler declares. `onBlobDelete` takes the `BlobEvent` and, optionally, the `Caller`. Every
event handler returns `error?`; a handler steers the outcome by returning a `DeadLetterError`
or `RetryError` value, both subtypes of `error` (section 5.9). Declaring them in the return
type, as in `returns blob:DeadLetterError|blob:RetryError|error?`, is legal but makes the
language type a bare `error(...)` constructed in the handler as the first of them, a
`DeadLetterError`; declare `error?` and construct the settlement errors explicitly.

Created events are routed by the blob name's extension, compared ignoring case: `txt` to
`onBlobText`, `json` to `onBlobJson`, `xml` to `onBlobXml`, `csv` to `onBlobCsv`, and
everything else to `onBlob`. A name with no extension is routed by the event's content type
without its parameters: `application/json` to `onBlobJson`, `application/xml` or `text/xml` to
`onBlobXml`, `text/csv` to `onBlobCsv`, any other `text/*` to `onBlobText`, else to `onBlob`.
A per handler `@blob:FunctionConfig` takes precedence over that routing: its `namePattern` is a
regular expression matched against the whole blob name (the last path segment), and its
`contentTypePattern` one matched against the whole content type without its parameters; a
handler matches when either of its patterns does. Handlers with patterns are checked first, in
the fixed order `onBlobText`, `onBlobJson`, `onBlobXml`, `onBlobCsv`, then `onBlob`, and the
first match wins. When none matches, the extension or content type routing applies to every
declared handler, those with patterns included. A blob whose routing names an undeclared typed
handler falls back to `onBlob`.

An event the service has no handler for is never its to handle: a created event whose routing
finds no declared handler and no `onBlob`, and a deleted event on a service that declares no
`onBlobDelete`. The listener acknowledges it without dispatching. The first such event of each
kind on a queue logs a warning naming the queue and the subscription filter that keeps such
events out of it; repeats log at debug level with a running count. An unrouted created event is
keyed by its queue alone, and its warning names the first extension seen and an advanced filter
on `subject`: `StringNotEndsWith` with that extension, or `StringEndsWith` with the handled
ones (`subjectEndsWith` when one suffix suffices). Other kinds are keyed by the event type,
naming `includedEventTypes`, and a no-content created event (section 5.7) is keyed by its
operation or blob type. For an operation that commits no content the warning names
`StringNotIn` on `data.api` with `CreateFile`, `SftpCreate`, and `SftpWrite`, which keeps events without an
`api`; for an append or page blob create it names `StringNotIn` on `data.blobType` with
`AppendBlob` and `PageBlob`, and notes that this filter also drops the copies and deletes of
such blobs, since a filter cannot single out their creation. Section 5.8 places these checks in
order.

Metadata, property, and index tag changes fire no event, and on flat namespace accounts
neither does appending to an append blob. A process that must observe those changes polls the
relevant read operations instead.

###### Example: Routing by Name Pattern

```ballerina
@blob:ServiceConfig {queueName: "report-events"}
isolated service on blobListener {
    @blob:FunctionConfig {namePattern: "daily-.*\\.csv"}
    isolated remote function onBlobCsv(stream<Reading, error?> rows) returns error? {
        // Rows are bound lazily, one record per pull.
    }

    isolated remote function onBlob(stream<byte[], error?> content, blob:BlobEvent event) returns error? {
        // Every other created blob the queue carries.
    }
}
```

### 5.6 Content Binding

A content handler receives the blob's content as it is at fetch time, which is after the event
fired. A blob replaced in between is delivered with its current content, and its own later
event follows; `event.eTag` lets a handler detect the change. Content is fetched only for
created events and only when a content handler is declared for the blob. Whether a blob is
empty is decided by the fetched content, never by the event's `contentLength`. An empty blob
binds to empty bytes, an empty string, or a stream that ends at once; a JSON, XML, or CSV
target (a record, a record array, or `string[][]`) fails binding like any invalid content.

Binding is strict by default. Setting `laxDataBinding` on the listener relaxes it: JSON and
CSV record binding treat a null value as an optional field and an absent member as a nilable
field, and XML record binding tolerates elements the record does not declare.

A fetch or binding failure notifies `onError` (section 5.12) and is disposed of by its cause:

* The blob is not found (HTTP 404 with `BlobNotFound`), deleted before the fetch: the message
  is acknowledged. The blob's own deleted event follows only when the subscription admits
  deleted events and the blob was deleted individually; deleting its container, or a
  recursive directory delete on an account with a hierarchical namespace, publishes no per
  blob deleted event. Any other 404, such as `ContainerNotFound` or `ResourceNotFound`, is an
  ordinary failure, below.
* The blob is archived (`ArchivedBlobError`): the message moves to the poison queue at once,
  to be replayed after rehydration.
* Content that fails to bind to the handler's parameter type: the message moves to the poison
  queue at once, and `onError` receives a `ContentBindingError` (section 6) naming the
  container and the path.
* Any other failure, such as throttling, a network error, or an authorization failure: the
  message is made visible again for redelivery, as for a failed handler (section 5.9).

The stream forms bind as the handler pulls, so a row that fails to bind surfaces inside the
handler as the error entry of that pull. The handler's return then decides the outcome. A
returned `DeadLetterError` or `RetryError` is honoured as always (section 5.9). When the
handler returns the stream's own binding error, the outcome is that of a binding failure: the
message moves to the poison queue and `onError` receives a `ContentBindingError` without
content. Any other returned error, or a panic, follows the ordinary failure rule
(section 5.9). A handler that returns `()` after such an entry has handled it, and the message
is acknowledged. A transport failure in the middle of a stream is an ordinary failure
(section 5.9).

Stream content is read in ranged chunks, and no connection is held between chunks. The read is
locked to the version of the blob it started on: a blob replaced while its stream is being
read fails the next chunk with a `PreconditionFailedError` (HTTP 412), the handler's returned
error makes the message visible again for redelivery, and the new content may then be
processed twice, by that redelivery and by the blob's own created event. A query over the
stream (`from ... in rows do {}`) surfaces a copy of an error entry that keeps its message but
not its type; a handler that tests an entry's type pulls the stream with `next()`.

### 5.7 The Blob Event

The `BlobEvent` record carries what the storage event provides, in both the Event Grid and the
CloudEvents delivery schemas (the listener detects the schema per message):

* `eventType`: `BLOB_CREATED` or `BLOB_DELETED`.
* `accountName`: the storage account the event comes from, the last segment of the event's
  `topic` (Event Grid schema) or `source` (CloudEvents), `.../storageAccounts/{name}`.
* `containerName` and `path`: the blob's container and its container relative name, derived
  from the event subject.
* `url`: the blob's full URL on the blob endpoint, which `Caller.copyBlobFromUrl` takes as its
  source. An event raised through the Data Lake API carries the URL of the `dfs` endpoint,
  `{account}.dfs.core.windows.net/{file system}/{path}`; the listener and `parseEvent`
  (section 5.14) give `url` the same path on the blob endpoint, with `.dfs.` replaced by
  `.blob.`.
* `eventTime`: when the event fired.
* `api`: the storage operation that caused the event (for example `PutBlob`, `PutBlockList`,
  `CopyBlob`, `DeleteBlob`).
* `sequencer`: always present. An opaque string whose ordering is comparable per blob path,
  for detecting stale events about the same blob.
* `contentType`, `contentLength`, `blobType`, `eTag`: as provided by the event. The service
  reports `blobType` as `BlockBlob`, `PageBlob`, or `AppendBlob`, though the event schema
  documents only the first two.

A created event fires when a blob's content is committed: an upload, a committed block list, a
completed copy, a Data Lake flush with close, or an SFTP commit (the `api` values `PutBlob`,
`PutBlockList`, `CopyBlob`, `FlushWithClose`, and `SftpCommit`). On a hierarchical namespace
account a copy that does not finish at once publishes an `AsyncOperationInitiated` event first
and the created event when the copy commits. On hierarchical namespace (Data Lake) and SFTP
enabled accounts the created event also fires for operations that commit no content:
`CreateFile` (an empty file), `SftpCreate` (an empty blob opened for an upload), and, with
resumable SFTP uploads, `SftpWrite` in the middle of an upload. Creating an append or a page
blob fires a created event with the `api` value `PutBlob` and the `blobType` `AppendBlob` or
`PageBlob`; it carries no written content either (an empty append blob, or a page blob of
zeros), and the appends and page writes that follow fire no event. The listener recognises an
append or page blob create only when its `blobType` is present and is `AppendBlob` or
`PageBlob`: a `PutBlob` event whose `blobType` is absent or carries another value is
dispatched, as is a `CopyBlob` of an append or page blob. Both kinds, a created event from an
operation that commits no content (`CreateFile`, `SftpCreate`, `SftpWrite`) and a recognised
append or page blob create, are called a no-content created event. The listener treats a
no-content created event as never the service's to handle (section 5.8), and its first time
warning names the subscription filter of section 5.5 for its kind. Every other created event is
dispatched. Creation and replacement are the same event; overwriting an
existing blob fires it again, and the event does not say which happened (the `api` field
carries the causing operation). The event describes the blob at the moment it fired; the blob
may have changed or been deleted since.

Besides the created and deleted events the service publishes the blob event types
`BlobTierChanged`, `AsyncOperationInitiated`, `BlobRenamed`, `DirectoryCreated`,
`DirectoryRenamed`, `DirectoryDeleted`, `BlobInventoryPolicyCompleted`, and
`LifecyclePolicyCompleted`. `AsyncOperationInitiated` fires on any account for an archive
rehydration (Set Blob Tier, or Copy Blob from an archived blob), and on a hierarchical
namespace account for a copy that does not finish at once. The listener does not dispatch them (section 5.8); a subscription's
`includedEventTypes` should name only the created and deleted events.

### 5.8 The Order of Checks

Every message passes the same checks in a fixed order: the listener checks its delivery
count, decodes it, parses the event envelope, checks the event type, checks the account, and
only then routes the event (section 5.5).

* **Delivery limit**: a message received with a dequeue count above `maxDeliveryCount` has
  been delivered that many times and never settled, for example because a process stopped in
  the middle of its handler or a move to the poison queue failed. It moves to the poison queue
  verbatim without dispatching, logged at error level with the reason "delivery limit exceeded
  on receipt". This holds for every message past the limit, including those the checks below
  would acknowledge. The owning service's `onError` is notified before the move, with the
  error "delivery limit exceeded on receipt", and once more when the move then fails or is
  skipped after a transient failure of its hold check (section 5.10). It gets the event and a
  `Caller` only when the message parses as a created event that is not a no-content created
  event, or a deleted event, from this account; routing is not consulted. Otherwise both are
  `()`: for a message that does not parse, an event type that is not dispatched, a no-content
  created event (section 5.7), or an event from another account. A past limit message that does not parse
  gets no separate parse error notification.
* **Decoding and parsing**: a message that cannot be decoded or parsed as an event in either
  delivery schema moves to the poison queue at once, since the same bytes fail the same way on
  every delivery. The owning service's `onError` is notified with a client side `Error` naming
  the queue and the message id.
* **Event type**: an event of any type other than blob created and blob deleted is never the
  service's to handle. It is acknowledged with the first time warning of section 5.5, naming
  `includedEventTypes`. So is a no-content created event (section 5.7), with a warning naming
  the advanced filter of section 5.5 for its kind.
* **Account**: a blob created or deleted event must name its storage account in its `topic`
  or `source`; one that does not is handled as an unparseable message. Blob events relayed
  through an Event Grid custom topic lose the storage account when the topic uses the Event
  Grid schema, since Event Grid stamps the custom topic as the event's `topic`, or when a
  CloudEvents relay rewrites `source`; the listener moves such events to the poison queue as
  unparseable and `parseEvent` (section 5.14) returns an `Error` for them. A CloudEvents relay
  that keeps the original `source` parses normally; otherwise subscribe the storage account's system
  topic to the queue directly. The event's account is
  compared with the account the listener's `Caller` reads from, resolved in order from the
  credential's explicit `accountName`, a connection string's `AccountName`, or the blob
  endpoint's first host label (used only when the host carries the `.blob.` service label) or
  the account segment of an emulator address's path. Otherwise the account is unknown: the
  check is skipped, and a one time warning states that the listener assumes its queues carry
  the events of a single account. The comparison ignores case. An event from another account is
  acknowledged without dispatching and logged at error level the first time each foreign account is
  seen, then at debug level with a running count. The level is error, not warning, because such an
  event signals misrouting that drops another account's events, not a filter to tune. The listener
  keeps one small entry per distinct foreign account seen.

The account is always taken from the event's `topic` or `source`, never from the host of its
blob URL. Without the check, a queue fed by two accounts would read the other account's
`invoices/a.json` from this account's container of the same name.

### 5.9 Delivery and Settlement

Delivery is at least once. A message stays invisible to other consumers while its handler
runs (section 5.4). A service declared `isolated service` whose handlers are isolated runs
its handlers fully in parallel, up to `batchSize + newBatchThreshold` at once. A service that
is not isolated runs them in turn at the runtime's lock, interleaving while one waits on I/O,
which every connector call does, so it still receives, holds (renewed) and has in flight up to
`batchSize + newBatchThreshold` messages; only its Ballerina code is serialised. Such a service
should be declared isolated, or lower `batchSize` to leave messages to competing consumers.
Renewal and settlement work the same either way.

The handler's return settles the message:

* Returning `()`: the listener acknowledges the event by deleting the queue message; it is
  never delivered again.
* Returning a `DeadLetterError`: the handler gives up, and the message moves to the poison
  queue at once, with the error's message logged as "dead-lettered by the handler: …". For a
  failure known to be permanent, this saves the remaining deliveries, each a full fetch of the
  blob.
* Returning a `RetryError`: the message is made visible again after the `delay` of the
  error's detail instead of `redeliveryDelay`, for example to honour a downstream
  service's retry hint. The delay is a requested minimum, rounded up to whole seconds; a
  negative delay counts as 0. It is capped at 604,800 seconds (seven days), the queue
  service's limit and the one case where the delay applied is shorter than requested. A delay
  that would end at or within one visibility window of the message's expiry moves the message
  to the poison queue instead (section 5.11).
* Returning any other error, or panicking: the message is made visible again after
  `redeliveryDelay` and redelivered.

A delivery ended by a `RetryError` still counts toward `maxDeliveryCount`. A message whose
final delivery ends in a handler failure moves to the poison queue (section 5.10) with the
reason "delivery limit reached", and one
received past that limit is moved there on receipt without dispatching (section 5.8); a
`RetryError` returned on the final delivery moves it there too, and its delay is ignored.
Processing of other messages continues.

The two settlement errors are recognised only when the handler returns them as they are: an
error that carries one as its cause is an ordinary failure, and so is a panic, whatever its
error value. Recognition is by the module's own types, never by an error's name. They apply in
all six event handlers, `onBlobDelete` included. `onError` never settles a message
(section 5.12).

Duplicate delivery is always possible, so handlers must be idempotent; the event's `sequencer`
supports ignoring stale duplicates about one blob. A handler that writes into a container the
subscription covers fires further created and deleted events, which come back to the same
service; such a handler must recognise its own output, for example by path, or the
subscription must exclude it with the advanced filter `StringNotBeginsWith` on `subject`.

###### Example: Steering the Outcome

```ballerina
@blob:ServiceConfig {queueName: "invoice-events"}
isolated service on blobListener {
    isolated remote function onBlobJson(Invoice invoice, blob:BlobEvent event) returns error? {
        if invoice.total < 0d {
            // Retrying cannot help; park the message for inspection.
            return error blob:DeadLetterError(string `negative total in ${event.path}`);
        }
        boolean accepted = check postToLedger(invoice);
        if !accepted {
            // The ledger is busy; try again in two minutes.
            return error blob:RetryError("ledger busy", delay = 120);
        }
    }
}
```

### 5.10 The Poison Queue, Logging, and Monitoring

A message moved to the poison queue named by the service's `poisonQueueName` (section 5.3) is
copied there verbatim and then deleted from its source queue. The move first updates the held
message, keeping it hidden, which checks that the listener still holds it and yields the newest
pop receipt for the delete. When that update fails because the pop receipt no longer matches or
the message is not found, the move is skipped and logged: another consumer holds the message,
or it is gone. When it fails transiently (a server side error, a network error, or a timeout),
the move is skipped and logged too; `onError` is notified as for a failed move (below) and
runs under renewal (section 5.12); when it returns, renewal stops and the message returns at
the end of its current window. The next delivery handles it with a fresh pop receipt, and the
delivery limit catches a repeat. That update is timed like a renewal (section 5.4): it times
out at the smaller of 30 seconds and the hold time remaining. Its visibility is a renewal's:
the ten-minute window, clamped to end 60 seconds before the message's expiry; when no positive
value remains, the update is skipped, and the copy is sent and the source message deleted with
the current pop receipt, accepting the rare duplicate a stale receipt can cause in that last
minute. Otherwise, after the check, the move sends the copy, creates the poison queue only when
that send fails because the queue does not exist (`QueueNotFound`), then sends again, and
finally deletes the source message with the newest pop receipt. Poison messages
never expire; the poison queue holds them until someone inspects, replays, or removes them. A
process that stops between the copy and the delete leaves the message in both queues, and a
poison send retried after a try that already reached the service leaves two copies, so the
poison queue may hold an occasional duplicate. A message's dequeue count starts again in the
poison queue.

A failed settlement step is logged at error level with the message id. Its outcome:

* A failed acknowledgement leaves the message to be redelivered, unless it fails as not found
  (HTTP 404 with `MessageNotFound`), which means the message is already gone.
* A failed redelivery delay update, or a failed release at a stop (section 5.2), has an
  unknown outcome, since a retried call may already have applied: the message returns either
  as that call set or when its current window expires.
* A failed move to the poison queue also notifies `onError` (section 5.12) with an error naming
  the poison queue, and with the message's event and `Caller`, or `()` where it carries none.
  So does a move skipped after a transient failure of its hold check; a move skipped because
  another consumer holds the message, or because it is gone, is only logged. For a message
  past its delivery limit this is a second call, after the limit call (section 5.8), with the
  same event and `Caller`. The message returns when its visibility window expires. Until its
  delivery count passes `maxDeliveryCount`, each later delivery repeats what led to the move.
  When the move followed the handler's return (a `DeadLetterError`, a stream's own binding
  error it returned, or a failure whose redelivery would outlive the message), the handler
  reruns. Otherwise the handler never ran, and the parse or fetch, the bind, the `onError`
  call, and the move repeat: for an unparseable message, an archived blob, a binding failure,
  or a fetch or `Caller` failure near expiry. Near expiry the message usually expires first, or
  its last minute move is sent without the check. Past the limit the move is retried on receipt
  without dispatching (section 5.8), with both notifications on every receipt, so a cause such
  as a credential that cannot create a missing poison queue gives two `onError` calls per
  visibility window until it is fixed.

The listener logs through the `log` module. Every handler failure, a returned error or a panic,
logs a warning naming the handler, the blob path, the delivery number out of
`maxDeliveryCount` ("delivery n of max"), and the error's message; for a returned `RetryError`
the warning also states the delay applied, after rounding and the cap, or, when the listener
cannot honour it, "delay not applied: " followed by the reason the poison move's line carries
("retry requested on the final delivery" or "would expire before redelivery"), so the two lines
correlate. A returned `DeadLetterError` logs no warning: its one line is the poison move's,
with the reason "dead-lettered by the handler: …". A failure that would notify `onError` also
logs a warning when the service declares no `onError`. Every move to the poison queue logs at error
level with the source queue, the original message id, the new poison message id, the blob
path, the delivery count, and the reason, a `reason` key value field holding one of eight fixed
strings:

* "delivery limit reached": the retries are exhausted after failures (section 5.9).
* "dead-lettered by the handler: <message>": the handler returned a `DeadLetterError`.
* "retry requested on the final delivery": the handler returned a `RetryError` on the final
  delivery.
* "would expire before redelivery": the redelivery delay would outlive the message
  (section 5.11).
* "delivery limit exceeded on receipt": the message was received past its delivery limit
  (section 5.8).
* "unparseable message": the message cannot be decoded or parsed as an event (section 5.8).
* "archived blob": the blob is archived (section 5.6).
* "content binding failed": the content fails to bind before dispatch, or the handler
  returned a stream's own binding error (section 5.6).

The two delivery limit reasons differ after their shared start, so a substring query separates
them. The reason is never written into the message, which stays verbatim for replay.

Azure Monitor's queue metrics carry no per queue dimension, so they cannot alert on one poison
queue's depth. A periodic check of the poison queue's approximate message count, through the
queue service's Get Queue Metadata operation, serves that purpose instead.

### 5.11 Delivery Time Bounds

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

The listener reads each message's expiry time when it receives it. The queue service's Update
Message operation cannot hide a message past its expiry, so a message whose redelivery delay, `redeliveryDelay`
or a `RetryError`'s, would end at or within one visibility window of that expiry moves to the
poison queue at once, with the reason "would expire before redelivery", instead of expiring
unrecorded. The rule is inactive when the subscription's queue message time to live is
infinite.

### 5.12 Error Notification

A service may declare `onError`:
`isolated remote function onError(blob:Error err, blob:BlobEvent? event, blob:Caller? caller) returns error?`.
Its first parameter takes the error, declared as `blob:Error` or `error`. The event and
`Caller` parameters are optional, must be declared nilable, and are identified by their types,
so `onError` takes `(Error)`, `(Error, BlobEvent?)`, `(Error, Caller?)`, or
`(Error, BlobEvent?, Caller?)`; a parameter declared as a non nilable `BlobEvent` or `Caller` is
a compile error.

`onError` is notified when:

* A poll of the service's queue fails, with the mapped typed error, for example an
  `AuthorizationError` when the credential cannot receive messages; the error's message names
  the queue. Only the service that owns the queue is notified, and the queue keeps its backoff
  schedule, so the next poll tries again. A fault that affects every queue, such as an expired
  credential, reaches each owning service through its own poll.
* A message cannot be parsed as an event (section 5.8).
* A content fetch or binding fails (section 5.6).
* The `Caller` for the event's container cannot be built; the message is then made visible
  again for redelivery, as for a failed handler (section 5.9).
* A move to the poison queue fails, or is skipped after a transient failure of its hold check
  (section 5.10).
* A message is received past its delivery limit and moved to the poison queue (section 5.8);
  a failure of that move, or a skip after a transient failure of its hold check, notifies a second
  time.

For a failure bound to an event, `event` carries the event and `caller` a `Caller` bound to
its container, or `()` when the `Caller` could not be built. For a failed poll or an
unparseable message, both are `()`, as they are for a message past its delivery limit
unless it parses as a created event that is not a no-content created event, or a deleted
event, from this account (section 5.8).

`onError` runs to completion before the message is disposed of, so it can, for example, mark a
failing blob before the message moves to the poison queue. When to run it is decided by the
hold, not by the event parameter. A call for a held message, one that is unparseable, past its
delivery limit, or bound to an event, runs in that message's concurrency slot while its
visibility is renewed, with the warning at each full window (section 5.4). Only a call with no
held message, after a failed poll, runs before its queue's next poll, so the queue stays
paused until it returns, and a warning naming the queue is logged at each full ten minutes it
is still running. `gracefulStop` waits for a running `onError` as for any handler. It never settles
the message: an error it returns does not change the outcome and is logged as a warning. It is not
an event handler and does not satisfy the at least one handler requirement. Errors returned by the
event handlers do not notify `onError`; they drive settlement (section 5.9). The one exception is a
stream handler returning the stream's own binding error, which notifies `onError` with a
`ContentBindingError` (section 5.6).

###### Example: Marking an Unreadable Blob

```ballerina
isolated remote function onError(blob:Error err, blob:BlobEvent? event, blob:Caller? caller) returns error? {
    if err is blob:ContentBindingError && event is blob:BlobEvent && caller is blob:Caller {
        // Runs before the message moves to the poison queue; tag changes fire no event.
        check caller->setTags(event.path, {status: "unreadable"});
    }
}
```

### 5.13 The Caller

A `Caller` is passed to each handler so it can act on the event's blob without constructing a
separate client; it cannot be created by user code. A queue may carry the events of several
containers, so each `Caller` is bound to the event's container, one per container. It forwards
seven operations of the `Client` with identical signatures: `getBlob`, `getBlobProperties`,
`download`, `upload`, `deleteBlob`, `copyBlobFromUrl`, and `setTags`, each with the semantics
of its `Client` counterpart. Handlers pass the event's path explicitly, for example
`caller->deleteBlob(event.path)`. `copyBlobFromUrl` copies into the event's container; copying
to another container from a handler uses a `Client` bound to that container. Every `Caller` of
a listener uses the listener's one credential and its shared connection pool (section 5.2).
The listener keeps one `Caller` for each container it has seen events from, for its whole
life, one small entry per distinct container.

###### Example: Marking and Consuming on Event

```ballerina
isolated remote function onBlobJson(Invoice invoice, blob:BlobEvent event, blob:Caller caller) returns error? {
    check caller->upload(invoice, string `processed/${event.path}`);
    check caller->deleteBlob(event.path);
}
```

The copy lands in the event's container, so the subscription for this queue must exclude the
`processed/` prefix with the advanced filter `StringNotBeginsWith` on `subject`, or the handler
must write elsewhere; otherwise its own output triggers it
again (section 5.9).

### 5.14 Parsing Events Outside the Listener

The public function `parseEvent(message)` turns one delivered blob event into a `BlobEvent`,
for applications that receive blob events by another route, such as an Event Grid subscription
delivering to Azure Service Bus, consumed with the `ballerinax/asb` connector. It is a pure
function with no service call. The message may be bytes, a string, or parsed JSON, base64
encoded or raw, in either delivery schema, and a single event or a one element array. It
returns a `BlobEvent` for a blob created or deleted event, `()` for a valid event of another
type or a no-content created event (section 5.7), which the caller skips, and an
`Error` for anything malformed or unsupported. The `accountName` field is derived as for the
listener (section 5.7); a consumer whose queue may carry the events of several accounts compares it
with the account its client reads from.

Webhook delivery, with its validation handshake, is out of scope.

###### Example: Consuming Blob Events From Service Bus

```ballerina
import ballerinax/asb;
import ballerinax/azure.storage.blob;

configurable string connectionString = ?;
configurable string accountKey = ?;

final blob:Client invoices = check new ("invoices", auth = {accountName: "myacct", accountKey});

listener asb:Listener asbListener = check new (
    connectionString = connectionString,
    entityConfig = {queueName: "blob-events"}
);

isolated service on asbListener {
    isolated remote function onMessage(asb:Message message) returns error? {
        blob:BlobEvent? event = check blob:parseEvent(message.body);
        if event is () {
            // A valid event of another type, or a no-content created event.
            return;
        }
        // The queue may carry several accounts' events, and the subscription admits one container.
        if event.accountName != "myacct" || event.eventType != blob:BLOB_CREATED {
            return;
        }
        Invoice invoice = check invoices->getBlob(event.path);
        // Process the invoice.
    }
}
```

## 6. Errors

Every error raised by an operation of this module is a subtype of the distinct `Error` type.
The hierarchy splits by origin: an error the Azure service raised is a `ServiceError` carrying
the HTTP status and the Azure error code of the failed request in its detail, while a client
side failure is the generic `Error` with no detail, except for the three listener subtypes
below, which carry their own. The service's description becomes the Ballerina error's
`message()`, except that for a failed receive the listener prefixes the queue's name to it
(section 5.12).

* **`Error`**: the root type, and the type of every client side failure other than the
  listener subtypes below.
* **`ContentBindingError`**: a listener failed to bind a blob's content to a handler's
  parameter type (section 5.6). Its detail carries the `containerName` and the `path` of the
  blob, and the `content` as bytes only when the content was already read into memory.
* **`DeadLetterError`**: returned by a listener handler to move its message to the poison
  queue at once (section 5.9). It carries no detail.
* **`RetryError`**: returned by a listener handler to have its message redelivered later
  (section 5.9). Its detail carries `delay`, the requested minimum delay before the
  message is visible again.
* **`ServiceError`**: any error raised by the Azure service, carrying `httpStatus` and
  `errorCode` in its `ServiceErrorDetail`. A service failure whose Azure error code maps to
  none of the subtypes below stays this generic type.
  * **`NotFoundError`**: the requested container or blob was not found (HTTP 404;
    `BlobNotFound`, `ContainerNotFound`, `ResourceNotFound`), or, for a `Listener`, its
    queue or a message on it (`QueueNotFound`, `MessageNotFound`). Queue service failures
    reach `onError` through the same mapping.
  * **`ConflictError`**: the operation conflicts with the current state of the resource, for
    example creating a container that already exists, deleting a blob whose snapshots were
    not directed, or a lease operation against the wrong lease state (HTTP 409;
    `ContainerAlreadyExists`, `BlobAlreadyExists`, `ContainerBeingDeleted`,
    `SnapshotsPresent`, `PendingCopyOperation`, `NoPendingCopyOperation`, `CopyIdMismatch`,
    `BlockCountExceedsLimit`, `LeaseAlreadyPresent`, `LeaseAlreadyBroken`,
    `LeaseNotPresentWithLeaseOperation`, `LeaseIdMismatchWithLeaseOperation`,
    `LeaseIsBreakingAndCannotBeAcquired`, `LeaseIsBreakingAndCannotBeChanged`,
    `LeaseIsBrokenAndCannotBeRenewed`).
  * **`AuthorizationError`**: authentication or authorization failed, for example an invalid
    key or insufficient SAS permissions (HTTP 403; `AuthenticationFailed`,
    `AuthorizationFailure`, `AuthorizationPermissionMismatch`,
    `AuthorizationSourceIPMismatch`, `AuthorizationProtocolMismatch`,
    `AuthorizationServiceMismatch`, `AuthorizationResourceTypeMismatch`,
    `InsufficientAccountPermissions`, `AccountIsDisabled`, `UnauthorizedBlobOverwrite`).
  * **`PreconditionFailedError`**: a precondition such as a lease id requirement was not met
    (HTTP 412; `LeaseIdMissing`, `LeaseIdMismatchWithBlobOperation`,
    `LeaseIdMismatchWithContainerOperation`, `LeaseNotPresentWithBlobOperation`,
    `LeaseNotPresentWithContainerOperation`, `LeaseLost`, `ConditionNotMet`,
    `TargetConditionNotMet`, `SourceConditionNotMet`, `InfiniteLeaseDurationRequired`).
  * **`RangeNotSatisfiableError`**: the requested byte range cannot be satisfied for the
    target blob, including misaligned page ranges (HTTP 416; `InvalidRange`,
    `InvalidPageRange`).
  * **`ArchivedBlobError`**: the blob's content is unavailable because of the archive tier,
    either still archived or currently rehydrating (HTTP 409; `BlobArchived`,
    `BlobBeingRehydrated`).
  * **`InvalidBlobTypeError`**: the operation applies to another blob type, for example
    appending to a block blob or writing pages to an append blob (HTTP 409;
    `InvalidBlobType`).

`ContentBindingError`, `DeadLetterError`, and `RetryError` are direct subtypes of `Error` and
are never mapped from an Azure error code: the listener raises the first, and handlers return
the other two.

The mapping keys on the Azure error code, not the HTTP status alone: `BlobArchived`,
`BlobBeingRehydrated`, and `InvalidBlobType` (each HTTP 409) map to their own types, distinct
from the state conflicts (also HTTP 409) that map to `ConflictError`. Every other code,
including `AppendPositionConditionNotMet` and every 5xx failure, stays the generic
`ServiceError` with its status and code intact. More specific types are checked before more
general ones, since every subtype is a `ServiceError` and every `ServiceError` is an `Error`;
check them in the same order when handling a failure.

###### Example: Handling a Specific Failure

```ballerina
blob:BlobProperties|blob:Error properties = invoices->getBlobProperties("2026/07/invoice.pdf");
if properties is blob:NotFoundError {
    // The blob is absent; create it, or skip.
} else if properties is blob:Error {
    return properties;
}
```
