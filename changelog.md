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
- A test suite that runs against the Azurite emulator without credentials, and against a live storage account when credentials are configured

### Changed

### Fixed
