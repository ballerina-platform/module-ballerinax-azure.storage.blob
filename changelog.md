# Changelog

This file documents all notable changes to the Ballerina Azure Blob Storage package. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Container-scoped `Client` and account-level `AdminClient`: container lifecycle, listing, and access policies, service configuration, account information, and the user delegation key
- Blob operations: existence, properties, metadata, content headers, deletion and restore, copies, access tiers, index tags with tag queries, snapshots, and leases
- Transfers with typed data binding following the shared file-modules databinding contract: matching `UploadContent` and `RetrievableType` unions (`byte[]`, `string`, `json`, `xml`, records, record arrays, and the byte and CSV record stream forms), format resolution from a `fileFormat` override or the path extension, records-only CSV binding, and an automatic content type for connector-serialized content
- Append blobs, page blobs, and block staging
- Container, blob, user-delegation, and account SAS generation
- Shared key, SAS token, SAS URL, connection string, and Microsoft Entra ID authentication, with configurable retry, proxy, connection-pool, and TLS transport settings
- `Listener` whose services each consume the storage queue an Event Grid subscription delivers blob events to, named in `@blob:ServiceConfig` with the service's own batch, polling, redelivery, and delivery-limit settings: typed content handlers (`onBlob`, `onBlobText`, `onBlobJson`, `onBlobXml`, `onBlobCsv`) with `@blob:FunctionConfig` routing overrides, `onBlobDelete`, and `onError` with the event and `Caller` where the failure carries them; settlement by the handler's return, with `DeadLetterError` and `RetryError` to steer it; visibility renewal while a handler runs; a poison queue per service, created on demand; the account check and the ordered checks every message passes; and both the Event Grid and CloudEvents message schemas
- `parseEvent`, parsing a delivered blob event for applications that receive it by another route, such as Azure Service Bus
- `Caller` bound to the event's container and handed to the handlers, with the content, property, upload, copy, delete, and tag operations an event handler needs
- A compiler plugin validating listener services: the empty attach point, the `@blob:ServiceConfig` values given as literals, the queues of one listener's services, the handler set, and every handler's parameters and return type
- A test suite that runs against the Azurite emulator without credentials, and against a live storage account when credentials are configured
- Examples: folder archive, download link, blob event processor, tag search, and blob events from Service Bus
- Standard observability support shared with the file connectors: the connector's tags on every client operation's span, the `file_active_connections`, `file_bytes_transferred_total`, `file_events_total`, `file_databinding_duration_seconds`, and `file_resource_execution_duration_seconds` metrics, and a per-message trace of the listener's stages and settlement

### Changed

- The `file_databinding_duration_seconds` and `file_resource_execution_duration_seconds` gauges carry `watched.path`, the service's queue name, and the `file-error` span carries the type of the error `onError` is called with

### Fixed

- An omitted `retryConfig` now applies the connector's retry defaults, bounding each try at 60 seconds, instead of the SDK's unbounded try timeout
- `copyBlobFromUrl`, `appendBlockFromUrl`, and `stageBlockFromUrl` attach a SAS-authenticated client's SAS to a same-account source URL that carries none, so `Caller.copyBlobFromUrl(event.url)` works under a SAS listener
