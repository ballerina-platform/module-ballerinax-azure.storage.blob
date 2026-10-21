# Running the tests

There is one suite and two interchangeable backends, and a run uses exactly one. When both `liveAccountName` and `liveAccountKey` are configured, the whole suite runs against that real storage account; otherwise it runs against the [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite) emulator (no Azure account, no network beyond localhost, works on every machine with Docker). CI without secrets, including fork pull requests, is an Azurite run.

```sh
docker compose -p azure-storage-blob-tests -f ballerina/tests/resources/docker/compose.yaml up -d --wait
cd ballerina
bal test
```

## Backend selection

`backend.bal` reads `liveAccountName`/`liveAccountKey` from the configuration files a run is given, or from the `LIVE_ACCOUNT_NAME`/`LIVE_ACCOUNT_KEY` environment variables (a configured entry takes precedence). A live run never silently falls back to the emulator. To force an emulator run on a machine that has credentials, run without the live configuration file (below).

## Observability

`tests/Config.toml` is committed and holds no secrets: it enables metrics and the Prometheus reporter on `127.0.0.1:9797`, so the suite runs with observability on and the tests in `observability_tests.bal` can read back what the connector records (`ballerinax/prometheus` is a test-only dependency). `bal test --disable-groups observability` skips those tests, for a run with observability off, for example with `BAL_CONFIG_FILES` pointing at an empty file.

A few tests need what the emulator cannot provide and enable themselves only in a live run, each stating its reason in a comment: blob and container undelete, Append Block From URL and Put Block From URL (Azurite 3.36 answers 501 `APINotImplemented`), and the user-delegation SAS test, which additionally needs the Entra credentials below. Two emulator differences are tolerated by the tests rather than hidden: a wrong account key answers `AuthorizationFailure` where the service answers `AuthenticationFailed` (both map to `AuthorizationError`), and a page write past the end of a page blob answers 416 with a code outside the service catalogue, which the code-keyed mapping leaves as a plain `ServiceError`. One service difference is tolerated the same way: the service deletes a container asynchronously, so an immediate restore answers 409 `ContainerBeingDeleted` until the deletion completes (the restore test retries), and a repeated delete of the same container races that deletion, so the not-found check uses a name that never existed.

## The emulator

`tests/resources/docker/compose.yaml` runs `mcr.microsoft.com/azure-storage/azurite` pinned to 3.36.0 with `--skipApiVersionCheck`, publishing the blob endpoint on `127.0.0.1:10000` and the queue endpoint on `127.0.0.1:10001`. The suite connects with Azurite's published development account (`devstoreaccount1`) through the path-style URLs `http://127.0.0.1:10000/devstoreaccount1` and `http://127.0.0.1:10001/devstoreaccount1`, which also exercise the connector's explicit `serviceUrl`, `queueServiceUrl`, and port handling. The Gradle build starts and stops the container around `bal test` on its own.

## The listener tests

The connector has no queue client of its own, so `queue_backend.bal` talks to the queue REST API directly, authorized by an account SAS the suite mints with the connector's `AdminClient.generateAccountSas` (queue and blob services): it creates a per-run queue for each test, puts synthetic Event Grid messages on it (both the Event Grid and the CloudEvents schema, base64-encoded and raw), and peeks at what the listener left on the queue and its poison queue. The message shapes follow the published Blob Storage event samples. No Event Grid subscription is involved; the end-to-end delivery from a real subscription is a manual probe against a live account (the blob event processor example is the harness for it). Queues are swept at suite end by the run prefix, like the containers.

## Live runs

### One-time Azure setup

1. In the [Azure portal](https://portal.azure.com), create a general-purpose v2 **storage account** (Standard performance, LRS redundancy). Keep **Allow storage account key access** enabled (the default); the tests authenticate with the account key.
2. Under **Data protection**, keep **soft delete for blobs** enabled (the suite enables it itself when it is off) and enable **soft delete for containers**; the undelete test exercises both.
3. After deployment, open **Security + networking** > **Access keys** and copy the storage account name and the key1 value.

### Configure and run

Create `ballerina/tests/live/Config.toml` (gitignored; never commit it) with the following values:

```toml
liveAccountName = "<storage account name>"
liveAccountKey = "<key1>"

# optional: enables the user-delegation SAS test; the app registration needs a client
# secret and the Storage Blob Data Contributor and Storage Blob Delegator roles on the account
# liveEntraTenantId = "<tenant id>"
# liveEntraClientId = "<application id>"
# liveEntraClientSecret = "<client secret>"
```

Then run the suite with both configuration files, the committed one first:

```sh
cd ballerina
BAL_CONFIG_FILES=tests/Config.toml:tests/live/Config.toml bal test
```

`BAL_CONFIG_FILES` replaces the default lookup of `tests/Config.toml`, so the committed file has to be listed too. Role assignments can take a few minutes to propagate; if a freshly configured Entra test fails with an authorization error, wait and rerun.

The credential values can also be supplied as environment variables, with a plain `bal test`: `LIVE_ACCOUNT_NAME`, `LIVE_ACCOUNT_KEY`, `LIVE_ENTRA_TENANT_ID`, `LIVE_ENTRA_CLIENT_ID`, `LIVE_ENTRA_CLIENT_SECRET`.

### Container naming, cost, and cleanup

Each test creates its own container under a per-run prefix, `azbt-<run id>-<attempt>-` on GitHub Actions or `azbt-<epoch seconds>-` locally, so reruns never collide with leftovers from an interrupted run. Containers are swept at suite end by prefix. With container soft delete enabled the deleted containers sit in the retention window at negligible cost for test-sized data. Containers abandoned by a crashed run keep their run's prefix and can be listed for manual sweeping:

```sh
az storage container list --account-name <account-name> --prefix azbt- --include-deleted
```

The access tier test moves one small blob to the archive tier and asks for rehydration; the blob is deleted with its container, so no archive storage lingers.

Treat the account key as a development-only secret: it can be regenerated at any time under **Access keys**, which immediately invalidates the old value.

## Gradle

`./gradlew build` (or `./gradlew test`) runs the same `bal test` inside the Ballerina Docker image, starting Azurite before the tests and stopping it afterwards. The same credential rule applies: without credentials the run is emulator-backed.
