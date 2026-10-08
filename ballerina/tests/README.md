# Running the tests

There is one suite and two interchangeable backends, and a run uses exactly one. When both `liveAccountName` and `liveAccountKey` are configured, the whole suite runs against that real storage account; otherwise it runs against the [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite) emulator (no Azure account, no network beyond localhost, works on every machine with Docker). CI without secrets, including fork pull requests, is an Azurite run.

```sh
docker compose -p azure-storage-blob-tests -f ballerina/tests/resources/docker/compose.yaml up -d --wait
cd ballerina
bal test
```

## Backend selection

`backend.bal` reads `liveAccountName`/`liveAccountKey` from Config.toml, or from the `LIVE_ACCOUNT_NAME`/`LIVE_ACCOUNT_KEY` environment variables (a Config.toml entry takes precedence). A live run never silently falls back to the emulator. To force an emulator run on a machine that has credentials, move `tests/Config.toml` aside for that run.

A few tests need what the emulator cannot provide and enable themselves only in a live run, each stating its reason in a comment: blob and container undelete, Append Block From URL and Put Block From URL (Azurite 3.36 answers 501 `APINotImplemented`), and the user-delegation SAS test, which additionally needs the Entra credentials below. Two emulator differences are tolerated by the tests rather than hidden: a wrong account key answers `AuthorizationFailure` where the service answers `AuthenticationFailed` (both map to `AuthorizationError`), and a page write past the end of a page blob answers 416 with a code outside the service catalogue, which the code-keyed mapping leaves as a plain `ServiceError`. One service difference is tolerated the same way: the service deletes a container asynchronously, so an immediate restore answers 409 `ContainerBeingDeleted` until the deletion completes (the restore test retries), and a repeated delete of the same container races that deletion, so the not-found check uses a name that never existed.

## The emulator

`tests/resources/docker/compose.yaml` runs `mcr.microsoft.com/azure-storage/azurite` pinned to 3.36.0 with `--skipApiVersionCheck`, publishing the blob endpoint on `127.0.0.1:10000`. The suite connects with Azurite's published development account (`devstoreaccount1`) through the path-style URL `http://127.0.0.1:10000/devstoreaccount1`, which also exercises the connector's explicit `serviceUrl` and port handling. The Gradle build starts and stops the container around `bal test` on its own.

## Live runs

### One-time Azure setup

1. In the [Azure portal](https://portal.azure.com), create a general-purpose v2 **storage account** (Standard performance, LRS redundancy). Keep **Allow storage account key access** enabled (the default); the tests authenticate with the account key.
2. Under **Data protection**, keep **soft delete for blobs** enabled (the suite enables it itself when it is off) and enable **soft delete for containers**; the undelete test exercises both.
3. After deployment, open **Security + networking** > **Access keys** and copy the storage account name and the key1 value.

### Configure and run

Create `ballerina/tests/Config.toml` (gitignored; never commit it) with the following values:

```toml
liveAccountName = "<storage account name>"
liveAccountKey = "<key1>"

# optional: enables the user-delegation SAS test; the app registration needs a client
# secret and the Storage Blob Data Contributor and Storage Blob Delegator roles on the account
# liveEntraTenantId = "<tenant id>"
# liveEntraClientId = "<application id>"
# liveEntraClientSecret = "<client secret>"
```

Note the location: for `bal test`, configurable values are read from `Config.toml` inside the `tests/` directory, not the package root. Role assignments can take a few minutes to propagate; if a freshly configured Entra test fails with an authorization error, wait and rerun.

The credential values can also be supplied as environment variables: `LIVE_ACCOUNT_NAME`, `LIVE_ACCOUNT_KEY`, `LIVE_ENTRA_TENANT_ID`, `LIVE_ENTRA_CLIENT_ID`, `LIVE_ENTRA_CLIENT_SECRET`.

### Container naming, cost, and cleanup

Each test creates its own container under a per-run prefix, `azbt-<run id>-` on GitHub Actions or `azbt-<epoch seconds>-` locally, so reruns never collide with leftovers from an interrupted run. Containers are swept at suite end by prefix. With container soft delete enabled the deleted containers sit in the retention window at negligible cost for test-sized data. Containers abandoned by a crashed run keep their run's prefix and can be listed for manual sweeping:

```sh
az storage container list --account-name <account-name> --prefix azbt- --include-deleted
```

The access tier test moves one small blob to the archive tier and asks for rehydration; the blob is deleted with its container, so no archive storage lingers.

Treat the account key as a development-only secret: it can be regenerated at any time under **Access keys**, which immediately invalidates the old value.

## Gradle

`./gradlew build` (or `./gradlew test`) runs the same `bal test` inside the Ballerina Docker image, starting Azurite before the tests and stopping it afterwards. The same credential rule applies: without credentials the run is emulator-backed.
