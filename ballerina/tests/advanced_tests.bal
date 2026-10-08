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

// Copies, tiers, index tags, snapshots, leases, append and page blobs, blocks, SAS, and the
// error mapping. The live-only behaviours (undelete, the from-URL block operations, user
// delegation) enable themselves when the run has what they need.

import ballerina/lang.runtime;
import ballerina/test;
import ballerina/time;

// ---------------------------------------------------------------------------
// Copies
// ---------------------------------------------------------------------------

@test:Config {}
function testCopyWithinContainerAndFromUrl() returns error? {
    Client blobClient = check readyClient("copies");
    string container = testContainer("copies");
    check blobClient->upload("original", "src/a.txt", {metadata: {k: "v"}});
    CopyInfo copy = check blobClient->copyBlob("src/a.txt", "dst/a.txt", {tags: {copied: "yes"}});
    test:assertNotEquals(copy.copyId, "");
    BlobProperties destination = check awaitCopy(blobClient, "dst/a.txt");
    test:assertEquals(destination.metadata, {k: "v"}, "the destination inherits the source metadata");
    string content = check blobClient->getBlob("dst/a.txt");
    test:assertEquals(content, "original");
    CopyStatusInfo? status = destination.copyStatus;
    test:assertTrue(status is CopyStatusInfo);
    if status is CopyStatusInfo {
        test:assertEquals(status.copyId, copy.copyId);
        test:assertEquals(status.copyStatus, SUCCESS);
    }
    map<string> tags = check blobClient->getTags("dst/a.txt");
    test:assertEquals(tags, {copied: "yes"});

    // A URL source: the same blob addressed by its full URL with a read SAS.
    string sas = check blobClient.generateSas("src/a.txt", {
        expiryTime: time:utcAddSeconds(time:utcNow(), 600),
        permissions: {read: true}
    });
    string sourceUrl = string `${serviceUrl()}/${container}/src/a.txt?${sas}`;
    CopyInfo fromUrl = check blobClient->copyBlobFromUrl(sourceUrl, "dst/b.txt", {metadata: {fresh: "1"}});
    test:assertNotEquals(fromUrl.copyId, "");
    BlobProperties b = check awaitCopy(blobClient, "dst/b.txt");
    test:assertEquals(b.metadata, {fresh: "1"}, "explicit metadata replaces the inherited set");

    Error? notPending = blobClient->abortCopy("dst/a.txt", copy.copyId);
    test:assertTrue(notPending is ConflictError, "aborting a finished copy is a conflict");
    if notPending is ConflictError {
        test:assertEquals(notPending.detail().errorCode, "NoPendingCopyOperation");
    }
}

// ---------------------------------------------------------------------------
// Tiers
// ---------------------------------------------------------------------------

@test:Config {}
function testAccessTiers() returns error? {
    Client blobClient = check readyClient("tiers");
    check blobClient->upload("t", "tiered.txt", {accessTier: COOL});
    BlobProperties properties = check blobClient->getBlobProperties("tiered.txt");
    test:assertEquals(properties.accessTier, "Cool");
    check blobClient->setAccessTier("tiered.txt", HOT);
    properties = check blobClient->getBlobProperties("tiered.txt");
    test:assertEquals(properties.accessTier, "Hot");
    test:assertEquals(properties.accessTierInferred, false);
    check blobClient->setAccessTier("tiered.txt", ARCHIVE);
    properties = check blobClient->getBlobProperties("tiered.txt");
    test:assertEquals(properties.accessTier, "Archive");
    string|Error archived = blobClient->getBlob("tiered.txt");
    test:assertTrue(archived is ArchivedBlobError, "reading an archived blob is refused");
    if archived is ArchivedBlobError {
        test:assertEquals(archived.detail().httpStatus, 409);
    }
    // Rehydration is accepted; completion takes hours on the service.
    check blobClient->setAccessTier("tiered.txt", COOL, {rehydratePriority: HIGH});
    properties = check blobClient->getBlobProperties("tiered.txt");
    test:assertTrue(properties.accessTier == "Cool" || properties.archiveStatus is string);
}

// ---------------------------------------------------------------------------
// Index tags
// ---------------------------------------------------------------------------

@test:Config {}
function testTagsAndQueries() returns error? {
    Client blobClient = check readyClient("tags");
    check blobClient->upload("1", "one.txt");
    check blobClient->upload("2", "two.txt");
    check blobClient->setTags("one.txt", {status: "done", priority: "05"});
    check blobClient->setTags("two.txt", {status: "done", priority: "09"});
    map<string> tags = check blobClient->getTags("one.txt");
    test:assertEquals(tags, {status: "done", priority: "05"});
    // Replace-whole.
    check blobClient->setTags("one.txt", {status: "open"});
    tags = check blobClient->getTags("one.txt");
    test:assertEquals(tags, {status: "open"});

    // The index is eventually consistent; poll briefly for the expected matches.
    TaggedBlobEntry[] matches = [];
    foreach int attempt in 0 ..< 20 {
        stream<TaggedBlobEntry, Error?> found = check blobClient->findBlobsByTags("\"status\" = 'done' AND \"priority\" >= '05'");
        matches = check from TaggedBlobEntry entry in found select entry;
        if matches.length() == 1 {
            break;
        }
        runtime:sleep(0.5);
    }
    test:assertEquals(matches.length(), 1);
    test:assertEquals(matches[0].path, "two.txt");
    test:assertEquals(matches[0].tags["status"], "done");

    // Local validation, before any request.
    string[] badQueries = ["", "status = 'done'", "\"status\" = done", "@container = 'x' AND \"a\" = 'b'",
            "\"status\" = 'done' OR \"a\" = 'b'", "\"status\" != 'done'", "\"sta\"tus\" = 'x'"];
    foreach string bad in badQueries {
        stream<TaggedBlobEntry, Error?>|Error refused = blobClient->findBlobsByTags(bad);
        test:assertTrue(refused is Error && refused !is ServiceError, "query should be refused: " + bad);
    }
    map<string> tooMany = {};
    foreach int i in 0 ..< 11 {
        tooMany[string `k${i}`] = "v";
    }
    Error? tooManyTags = blobClient->setTags("one.txt", tooMany);
    test:assertTrue(tooManyTags is Error && tooManyTags !is ServiceError);
    Error? badChar = blobClient->setTags("one.txt", {"bad key!": "v"});
    test:assertTrue(badChar is Error && badChar !is ServiceError);
    Error? badValue = blobClient->setTags("one.txt", {k: "v#"});
    test:assertTrue(badValue is Error && badValue !is ServiceError);
}

// ---------------------------------------------------------------------------
// Snapshots and deletion modes
// ---------------------------------------------------------------------------

@test:Config {}
function testSnapshots() returns error? {
    Client blobClient = check readyClient("snapshots");
    check blobClient->upload("v1", "doc.txt", {metadata: {gen: "1"}});
    string snapshotId = check blobClient->createSnapshot("doc.txt");
    test:assertNotEquals(snapshotId, "");
    check blobClient->upload("v2", "doc.txt");
    string live = check blobClient->getBlob("doc.txt");
    test:assertEquals(live, "v2");
    string old = check blobClient->getBlob("doc.txt", {snapshotId});
    test:assertEquals(old, "v1");

    string withMetadata = check blobClient->createSnapshot("doc.txt", {metadata: {gen: "snap"}});
    test:assertNotEquals(withMetadata, snapshotId);

    stream<BlobEntry, Error?> listed = check blobClient->listBlobs({includeSnapshots: true});
    BlobEntry[] entries = check from BlobEntry entry in listed select entry;
    test:assertEquals(entries.length(), 3);
    test:assertEquals(entries.filter(e => e.hasKey("snapshotId")).length(), 2);

    Error? blocked = blobClient->deleteBlob("doc.txt");
    test:assertTrue(blocked is ConflictError, "a blob with snapshots needs a snapshot directive");
    if blocked is ConflictError {
        test:assertEquals(blocked.detail().errorCode, "SnapshotsPresent");
    }
    check blobClient->deleteBlob("doc.txt", {snapshotId});
    check blobClient->deleteBlob("doc.txt", {deleteSnapshots: ONLY_SNAPSHOTS});
    boolean stillThere = check blobClient->hasBlob("doc.txt");
    test:assertTrue(stillThere);
    _ = check blobClient->createSnapshot("doc.txt");
    check blobClient->deleteBlob("doc.txt", {deleteSnapshots: INCLUDE});
    boolean gone = check blobClient->hasBlob("doc.txt");
    test:assertFalse(gone);
}

// ---------------------------------------------------------------------------
// Leases
// ---------------------------------------------------------------------------

@test:Config {}
function testLeases() returns error? {
    Client blobClient = check readyClient("leases");
    check blobClient->upload("locked", "lease.txt");
    string|Error badDuration = blobClient->acquireLease("lease.txt", 5);
    test:assertTrue(badDuration is Error && badDuration !is ServiceError);

    string leaseId = check blobClient->acquireLease("lease.txt", 15);
    BlobProperties properties = check blobClient->getBlobProperties("lease.txt");
    test:assertEquals(properties.leaseState, LEASED);
    test:assertEquals(properties.leaseStatus, LOCKED);
    test:assertEquals(properties.leaseDuration, FIXED);

    // Writes need the lease id; reads never do.
    Error? blocked = blobClient->setBlobMetadata("lease.txt", {k: "v"});
    test:assertTrue(blocked is PreconditionFailedError);
    if blocked is PreconditionFailedError {
        test:assertEquals(blocked.detail().errorCode, "LeaseIdMissing");
        test:assertEquals(blocked.detail().httpStatus, 412);
    }
    check blobClient->setBlobMetadata("lease.txt", {k: "v"}, {leaseId});
    string content = check blobClient->getBlob("lease.txt");
    test:assertEquals(content, "locked");

    check blobClient->renewLease("lease.txt", leaseId);
    string proposed = "12345678-1234-1234-1234-123456789012";
    string changed = check blobClient->changeLease("lease.txt", leaseId, proposed);
    test:assertEquals(changed, proposed);
    string|Error again = blobClient->acquireLease("lease.txt", -1);
    test:assertTrue(again is ConflictError, "a leased blob cannot be leased again");
    if again is ConflictError {
        test:assertEquals(again.detail().errorCode, "LeaseAlreadyPresent");
    }
    int remaining = check blobClient->breakLease("lease.txt", 0);
    test:assertEquals(remaining, 0);
    properties = check blobClient->getBlobProperties("lease.txt");
    test:assertEquals(properties.leaseState, BROKEN);

    string infinite = check blobClient->acquireLease("lease.txt", -1);
    properties = check blobClient->getBlobProperties("lease.txt");
    test:assertEquals(properties.leaseDuration, INFINITE);
    check blobClient->releaseLease("lease.txt", infinite);
    properties = check blobClient->getBlobProperties("lease.txt");
    test:assertEquals(properties.leaseState, AVAILABLE);
    test:assertEquals(properties.leaseDuration, ());
}

// ---------------------------------------------------------------------------
// Append blobs
// ---------------------------------------------------------------------------

@test:Config {}
function testAppendBlobs() returns error? {
    Client blobClient = check readyClient("append");
    check blobClient->createAppendBlob("log.txt", {contentHeaders: {contentType: "text/plain"}});
    check blobClient->appendBlock("log.txt", "line one\n".toBytes());
    check blobClient->appendBlock("log.txt", "line two\n".toBytes());
    string content = check blobClient->getBlob("log.txt");
    test:assertEquals(content, "line one\nline two\n");
    BlobProperties properties = check blobClient->getBlobProperties("log.txt");
    test:assertEquals(properties.blobType, "AppendBlob");
    test:assertEquals(properties.committedBlockCount, 2);
    test:assertEquals(properties.contentHeaders.contentType, "text/plain");

    check blobClient->upload("block", "block.txt");
    Error? wrongType = blobClient->appendBlock("block.txt", "x".toBytes());
    test:assertTrue(wrongType is InvalidBlobTypeError);
    if wrongType is InvalidBlobTypeError {
        test:assertEquals(wrongType.detail().errorCode, "InvalidBlobType");
        test:assertEquals(wrongType.detail().httpStatus, 409);
    }

}

// Azurite 3.36 does not implement Append Block From URL (501 APINotImplemented).
@test:Config {enable: liveRun}
function testAppendBlockFromUrl() returns error? {
    Client blobClient = check readyClient("appendurl");
    string container = testContainer("appendurl");
    check blobClient->createAppendBlob("log.txt");
    check blobClient->upload("block", "block.txt");
    string sas = check blobClient.generateSas("block.txt", {
        expiryTime: time:utcAddSeconds(time:utcNow(), 600),
        permissions: {read: true}
    });
    check blobClient->appendBlockFromUrl("log.txt", string `${serviceUrl()}/${container}/block.txt?${sas}`);
    string content = check blobClient->getBlob("log.txt");
    test:assertEquals(content, "block");
}

// ---------------------------------------------------------------------------
// Page blobs
// ---------------------------------------------------------------------------

@test:Config {}
function testPageBlobs() returns error? {
    Client blobClient = check readyClient("pages");
    Error? unaligned = blobClient->createPageBlob("disk.vhd", 1000);
    test:assertTrue(unaligned is Error && unaligned !is ServiceError);
    check blobClient->createPageBlob("disk.vhd", 4096, {metadata: {kind: "disk"}});
    BlobProperties properties = check blobClient->getBlobProperties("disk.vhd");
    test:assertEquals(properties.blobType, "PageBlob");
    test:assertEquals(properties.contentLength, 4096);
    test:assertEquals(properties.blobSequenceNumber, 0);

    byte[] page = [];
    foreach int i in 0 ..< 512 {
        page.push(<byte>(i % 256));
    }
    check blobClient->uploadPages("disk.vhd", 512, page);
    check blobClient->uploadPages("disk.vhd", 2048, page);
    PageRange[] ranges = check blobClient->listPageRanges("disk.vhd");
    test:assertEquals(ranges, [{offset: 512, length: 512}, {offset: 2048, length: 512}]);
    check blobClient->clearPages("disk.vhd", 512, 512);
    ranges = check blobClient->listPageRanges("disk.vhd", {range: {startByte: 0, endByte: 4095}});
    test:assertEquals(ranges, [{offset: 2048, length: 512}]);
    byte[] readBack = check blobClient->getBlob("disk.vhd", {range: {startByte: 2048, endByte: 2559}});
    test:assertEquals(readBack, page);

    // The service answers InvalidPageRange (416); the emulator answers 416 with a code outside
    // the catalogue, which the code-keyed mapping leaves as a plain ServiceError.
    Error? outOfRange = blobClient->uploadPages("disk.vhd", 8192, page);
    test:assertTrue(outOfRange is ServiceError, "writing past the end fails, got " + describe(outOfRange));
    if outOfRange is ServiceError {
        test:assertEquals(outOfRange.detail().httpStatus, 416);
        if liveRun {
            test:assertTrue(outOfRange is RangeNotSatisfiableError);
        }
    }
}

// ---------------------------------------------------------------------------
// Blocks
// ---------------------------------------------------------------------------

@test:Config {}
function testBlocks() returns error? {
    Client blobClient = check readyClient("blocks");
    string id1 = "YmxvY2stMDAwMQ==";
    string id2 = "YmxvY2stMDAwMg==";
    check blobClient->stageBlock("assembled.txt", id1, "first ".toBytes());
    check blobClient->stageBlock("assembled.txt", id2, "second".toBytes());
    boolean visible = check blobClient->hasBlob("assembled.txt");
    test:assertFalse(visible, "staged blocks do not create the blob");
    BlockList staged = check blobClient->listBlocks("assembled.txt");
    test:assertEquals(staged.committedBlocks, []);
    test:assertEquals(staged.uncommittedBlocks.map(b => b.blockId), [id1, id2]);
    test:assertEquals(staged.uncommittedBlocks[0].sizeBytes, 6);

    Error? badId = blobClient->stageBlock("assembled.txt", "not base64!", "x".toBytes());
    test:assertTrue(badId is Error && badId !is ServiceError);
    Error? unevenIds = blobClient->commitBlockList("assembled.txt", [id1, "c2hvcnQ="]);
    test:assertTrue(unevenIds is Error && unevenIds !is ServiceError);

    check blobClient->commitBlockList("assembled.txt", [id2, id1], {contentHeaders: {contentType: "text/plain"}});
    string content = check blobClient->getBlob("assembled.txt");
    test:assertEquals(content, "secondfirst ");
    BlockList committed = check blobClient->listBlocks("assembled.txt");
    test:assertEquals(committed.committedBlocks.map(b => b.blockId), [id2, id1]);
    test:assertEquals(committed.uncommittedBlocks, []);

    // A later commit replaces the blob with exactly the listed blocks.
    check blobClient->stageBlock("assembled.txt", id1, "only".toBytes());
    check blobClient->commitBlockList("assembled.txt", [id1]);
    content = check blobClient->getBlob("assembled.txt");
    test:assertEquals(content, "only", "the commit replaces the blob with the listed blocks");
}

// Azurite 3.36 does not implement Put Block From URL (501 APINotImplemented).
@test:Config {enable: liveRun}
function testStageBlockFromUrl() returns error? {
    Client blobClient = check readyClient("blockurl");
    string container = testContainer("blockurl");
    check blobClient->upload("source", "src.txt");
    string sas = check blobClient.generateSas("src.txt", {
        expiryTime: time:utcAddSeconds(time:utcNow(), 600),
        permissions: {read: true}
    });
    string id = "YmxvY2stMDAwMw==";
    check blobClient->stageBlockFromUrl("assembled.txt", id, string `${serviceUrl()}/${container}/src.txt?${sas}`,
            {sourceRange: {startByte: 0, endByte: 2}});
    check blobClient->commitBlockList("assembled.txt", [id]);
    string content = check blobClient->getBlob("assembled.txt");
    test:assertEquals(content, "sou");
}

// ---------------------------------------------------------------------------
// SAS
// ---------------------------------------------------------------------------

@test:Config {}
function testSasTokens() returns error? {
    Client blobClient = check readyClient("sas");
    check blobClient->upload("secret", "s.txt");
    string container = testContainer("sas");

    string containerSas = check blobClient.generateContainerSas({
        expiryTime: time:utcAddSeconds(time:utcNow(), 600),
        permissions: {read: true, list: true},
        protocol: HTTPS_HTTP
    });
    Client viaSas = check newSasContainerClient(container, containerSas);
    string content = check viaSas->getBlob("s.txt");
    test:assertEquals(content, "secret");
    stream<BlobEntry, Error?> listed = check viaSas->listBlobs();
    BlobEntry[] entries = check from BlobEntry entry in listed select entry;
    test:assertEquals(entries.length(), 1);
    Error? denied = viaSas->upload("x", "w.txt");
    test:assertTrue(denied is AuthorizationError, "a read-only SAS cannot write");
    if denied is AuthorizationError {
        test:assertEquals(denied.detail().httpStatus, 403);
    }

    string|Error neither = blobClient.generateSas("s.txt", {startTime: time:utcNow()});
    test:assertTrue(neither is Error && neither !is ServiceError, "an identifier or an expiry is required");

    check blobClient->setContainerAccessPolicy([{id: "readers", permissions: "r",
            expiryTime: time:utcAddSeconds(time:utcNow(), 3600)}]);
    string policySas = check blobClient.generateSas("s.txt", {identifier: "readers"});
    test:assertTrue(policySas.includes("si=readers"));

    AdminClient admin = check newAdmin();
    string accountSas = check admin.generateAccountSas({
        expiryTime: time:utcAddSeconds(time:utcNow(), 600),
        permissions: {read: true, list: true},
        services: {blob: true, queue: true},
        resourceTypes: {'service: true, container: true, 'object: true}
    });
    test:assertTrue(accountSas.includes("ss=bq") || accountSas.includes("ss=qb"));
    Client viaAccountSas = check newSasContainerClient(container, accountSas);
    content = check viaAccountSas->getBlob("s.txt");
    test:assertEquals(content, "secret");
}

@test:Config {enable: liveEntraEnabled}
function testUserDelegationSas() returns error? {
    Client blobClient = check readyClient("udk");
    string container = testContainer("udk");
    check blobClient->upload("delegated", "d.txt");
    AdminClient entraAdmin = check new (auth = {
        accountName: liveAccountName,
        tenantId: liveEntraTenantId,
        clientId: liveEntraClientId,
        clientSecret: liveEntraClientSecret
    });
    time:Utc now = time:utcNow();
    UserDelegationKey key = check entraAdmin->getUserDelegationKey(now, time:utcAddSeconds(now, 3600));
    Client entraClient = check new (container, auth = {
        accountName: liveAccountName,
        tenantId: liveEntraTenantId,
        clientId: liveEntraClientId,
        clientSecret: liveEntraClientSecret
    });
    string sas = check entraClient.generateUserDelegationSas("d.txt", {
        expiryTime: time:utcAddSeconds(now, 600),
        permissions: {read: true}
    }, key);
    Client viaSas = check newSasContainerClient(container, sas);
    string content = check viaSas->getBlob("d.txt");
    test:assertEquals(content, "delegated");
    string|Error withPolicy = entraClient.generateUserDelegationSas("d.txt", {identifier: "readers"}, key);
    test:assertTrue(withPolicy is Error && withPolicy !is ServiceError);
}

// ---------------------------------------------------------------------------
// Live-only behaviours
// ---------------------------------------------------------------------------

@test:Config {enable: liveRun}
function testUndeleteBlobAndContainer() returns error? {
    AdminClient admin = check newAdmin();
    ServiceProperties current = check admin->getServiceProperties();
    RetentionPolicy? retention = current.deleteRetentionPolicy;
    if retention is () || !retention.enabled {
        check admin->setServiceProperties({deleteRetentionPolicy: {enabled: true, days: 1}});
    }
    Client blobClient = check readyClient("undelete");
    check blobClient->upload("keep me", "restore.txt");
    check blobClient->deleteBlob("restore.txt");
    boolean gone = check blobClient->hasBlob("restore.txt");
    test:assertFalse(gone);
    stream<BlobEntry, Error?> deleted = check blobClient->listBlobs({includeDeleted: true});
    BlobEntry[] entries = check from BlobEntry entry in deleted select entry;
    test:assertEquals(entries.length(), 1);
    test:assertEquals(entries[0].isDeleted, true);
    check blobClient->undeleteBlob("restore.txt");
    string content = check blobClient->getBlob("restore.txt");
    test:assertEquals(content, "keep me");
    // Restoring a live blob is a wire no-op.
    check blobClient->undeleteBlob("restore.txt");

    // Container soft delete is an account setting; when it is on, a deleted container lists
    // with its deleted version and comes back.
    string container = testContainer("undelete");
    check admin->deleteContainer(container);
    ContainerList listed = check admin->listContainers({prefix: container, includeDeleted: true});
    if listed.containers.length() == 1 && listed.containers[0].deletedVersion is string {
        string deletedVersion = listed.containers[0].deletedVersion ?: "";
        // The deletion completes asynchronously; until it does, a restore answers
        // ContainerBeingDeleted, so the restore is retried for a while.
        check await(function() returns boolean|error {
            Error? restored = admin->undeleteContainer(container, deletedVersion);
            if restored is ConflictError && restored.detail().errorCode == "ContainerBeingDeleted" {
                return false;
            }
            return restored is () ? true : restored;
        }, timeoutSeconds = 120, intervalSeconds = 5);
        check await(function() returns boolean|error {
            boolean restored = check admin->hasContainer(container);
            return restored;
        }, timeoutSeconds = 60);
    }
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

@test:Config {}
function testErrorMapping() returns error? {
    Client blobClient = check readyClient("errors");
    string container = testContainer("errors");
    BlobProperties|Error notFound = blobClient->getBlobProperties("absent.txt");
    test:assertTrue(notFound is NotFoundError);
    if notFound is NotFoundError {
        test:assertEquals(notFound.detail().errorCode, "BlobNotFound");
        test:assertEquals(notFound.detail().httpStatus, 404);
        test:assertTrue(notFound.message().length() > 0);
    }
    check blobClient->upload("small", "small.txt");
    byte[]|Error badRange = blobClient->getBlob("small.txt", {range: {startByte: 100, endByte: 200}});
    test:assertTrue(badRange is RangeNotSatisfiableError, "expected a range error, got " + describe(badRange));
    byte[]|Error localRange = blobClient->getBlob("small.txt", {range: {startByte: 5, endByte: 1}});
    test:assertTrue(localRange is Error && localRange !is ServiceError, "an inverted range is refused locally");

    Client wrongKey = check new (container, auth = {
        accountName: sharedKeyAuth().accountName,
        accountKey: "d3Jvbmcta2V5LXdyb25nLWtleS13cm9uZy1rZXk=",
        serviceUrl: sharedKeyAuth().serviceUrl
    });
    BlobProperties|Error unauthorized = wrongKey->getBlobProperties("small.txt");
    test:assertTrue(unauthorized is AuthorizationError, "a bad key is an authorization error, got "
            + describe(unauthorized));
    if unauthorized is AuthorizationError {
        test:assertEquals(unauthorized.detail().httpStatus, 403);
        // The service answers AuthenticationFailed; the emulator AuthorizationFailure.
        test:assertTrue(["AuthenticationFailed", "AuthorizationFailure"].indexOf(unauthorized.detail().errorCode) != ());
    }
}

// Polls the destination until its copy finishes; same-account copies complete quickly.
function awaitCopy(Client blobClient, string path) returns BlobProperties|error {
    foreach int attempt in 0 ..< 60 {
        BlobProperties properties = check blobClient->getBlobProperties(path);
        CopyStatusInfo? status = properties.copyStatus;
        if status is CopyStatusInfo && status.copyStatus == SUCCESS {
            return properties;
        }
        runtime:sleep(0.5);
    }
    return error("the copy did not complete in time");
}

// The blob service URL of the active backend, for building source URLs.
isolated function serviceUrl() returns string => liveRun
    ? string `https://${liveAccountName}.blob.core.windows.net`
    : AZURITE_URL;

// ---------------------------------------------------------------------------
// Review round 1: typed-read target shapes, stream upload isolation, account SAS tags
// ---------------------------------------------------------------------------

type Author record {|
    string name;
    int books;
|};

@test:Config {}
function testGetBlobXmlSubtypeTargets() returns error? {
    Client blobClient = check readyClient("xmlsub");
    check blobClient->upload("<author><name>Ann</name><books>3</books></author>", "a.xml");
    xml:Element element = check blobClient->getBlob("a.xml");
    test:assertEquals(element.getName(), "author");
    xml<xml:Element> elements = check blobClient->getBlob("a.xml");
    test:assertEquals(elements.length(), 1);
    // A document that is not one element (a leading comment) is refused for an element target.
    check blobClient->upload("<!-- c --><author/>", "c.xml");
    xml:Element|Error refused = blobClient->getBlob("c.xml");
    test:assertTrue(refused is Error && refused !is ServiceError, "a comment-led document is not an xml:Element");
    xml whole = check blobClient->getBlob("c.xml");
    test:assertEquals(whole.length(), 2, "the plain xml target takes the whole document");
}

@test:Config {}
function testGetBlobReadonlyIntersectionTargets() returns error? {
    Client blobClient = check readyClient("readonly");
    Author ann = {name: "Ann", books: 3};
    check blobClient->upload(ann, "a.json");
    check blobClient->upload("<author><name>Ann</name><books>3</books></author>", "a.xml");
    check blobClient->upload("name,books\nAnn,3\nBob,1\n", "a.csv");
    Author & readonly fromJson = check blobClient->getBlob("a.json");
    test:assertEquals(fromJson, ann);
    test:assertTrue(fromJson is readonly);
    Author & readonly fromXml = check blobClient->getBlob("a.xml");
    test:assertEquals(fromXml, ann);
    (Author & readonly)[] fromCsv = check blobClient->getBlob("a.csv");
    test:assertEquals(fromCsv.length(), 2);
    test:assertEquals(fromCsv[1].name, "Bob");
    test:assertTrue(fromCsv[0] is readonly);
    stream<Author & readonly, error?> rows = check blobClient->getBlob("a.csv");
    (Author & readonly)[] pulled = check from Author & readonly row in rows select row;
    test:assertEquals(pulled.length(), 2);
}

@test:Config {}
function testGetBlobUnionTargets() returns error? {
    Client blobClient = check readyClient("unions");
    Author ann = {name: "Ann", books: 3};
    check blobClient->upload(ann, "a.json");
    check blobClient->upload("name,books\nAnn,3\n", "a.csv");
    Author|Metric fromJson = check blobClient->getBlob("a.json");
    test:assertEquals(fromJson, ann);
    json|() maybe = check blobClient->getBlob("a.json");
    test:assertEquals(maybe, {name: "Ann", books: 3});
    // A union target has no CSV or XML binding.
    string|Author|Error refused = blobClient->getBlob("a.csv");
    test:assertTrue(refused is Error && refused !is ServiceError, "a union target binds from JSON only");
    if refused is Error {
        test:assertTrue(refused.message().includes("union"), refused.message());
    }
}

@test:Config {}
function testStreamUploadsToOnePathDoNotMix() returns error? {
    Client blobClient = check readyClient("race");
    // Two uploads to one path, each spanning two blocks, run at the same time; the blob must
    // end up as one of them whole, never as blocks of both.
    byte[] first = [];
    byte[] second = [];
    foreach int i in 0 ..< (STREAM_BLOCK_BYTES + 1000) {
        first.push(<byte>(i % 251));
        second.push(<byte>(i % 241));
    }
    stream<byte[], error?> firstSource = [first.slice(0, STREAM_BLOCK_BYTES), first.slice(STREAM_BLOCK_BYTES)].toStream();
    stream<byte[], error?> secondSource = [second.slice(0, STREAM_BLOCK_BYTES), second.slice(STREAM_BLOCK_BYTES)].toStream();
    future<Error?> a = start blobClient->upload(firstSource, "race.bin");
    future<Error?> b = start blobClient->upload(secondSource, "race.bin");
    Error? firstResult = wait a;
    Error? secondResult = wait b;
    // A commit discards every uncommitted block it does not list, so when the two overlap the
    // second commit is refused by the service rather than blending the two uploads; when they
    // do not overlap, both commit and the later one is the blob.
    test:assertTrue(firstResult is () || secondResult is (), "one of the uploads must succeed");
    foreach Error? outcome in [firstResult, secondResult] {
        if outcome is Error {
            test:assertTrue(outcome is ServiceError && outcome.detail().errorCode == "InvalidBlockList",
                    "the losing upload fails with InvalidBlockList: " + outcome.message());
        }
    }
    byte[] stored = check blobClient->getBlob("race.bin");
    if firstResult is Error {
        test:assertEquals(stored, second, "the stored content is the winning upload in full");
    } else if secondResult is Error {
        test:assertEquals(stored, first, "the stored content is the winning upload in full");
    } else {
        test:assertTrue(stored == first || stored == second, "the stored content is one upload in full");
    }
}

@test:Config {}
function testStreamUploadAcceptsReadonlyChunks() returns error? {
    Client blobClient = check readyClient("rochunks");
    byte[] first = [1, 2, 3];
    byte[] second = [4, 5, 6];
    readonly & byte[] head = first.cloneReadOnly();
    readonly & byte[] tail = second.cloneReadOnly();
    stream<readonly & byte[], error?> chunks = [head, tail].toStream();
    check blobClient->upload(chunks, "ro.bin");
    byte[] stored = check blobClient->getBlob("ro.bin");
    test:assertEquals(stored, [1, 2, 3, 4, 5, 6]);
}

@test:Config {}
function testAccountSasTagPermission() returns error? {
    Client blobClient = check readyClient("sastags");
    string container = testContainer("sastags");
    check blobClient->upload("t", "t.txt");
    AdminClient admin = check newAdmin();
    string tagging = check admin.generateAccountSas({
        expiryTime: time:utcAddSeconds(time:utcNow(), 600),
        permissions: {read: true, write: true, tag: true, filter: true},
        services: {blob: true},
        resourceTypes: {container: true, 'object: true}
    });
    test:assertTrue(tagging.includes("sp="), tagging);
    Client viaSas = check newSasContainerClient(container, tagging);
    check viaSas->setTags("t.txt", {status: "tagged"});
    map<string> tags = check viaSas->getTags("t.txt");
    test:assertEquals(tags, {status: "tagged"});
}

@test:Config {}
function testUploadXmlRequiresXmlFormat() returns error? {
    Client blobClient = check readyClient("xmlfmt");
    xml doc = xml `<note>hi</note>`;
    Error? asJson = blobClient->upload(doc, "note.json");
    test:assertTrue(asJson is Error && asJson !is ServiceError, "xml content under a .json path is refused");
    Error? bare = blobClient->upload(doc, "note.txt");
    test:assertTrue(bare is Error && bare !is ServiceError, "xml content under no format is refused");
    check blobClient->upload(doc, "note.dat", {fileFormat: XML});
    xml back = check blobClient->getBlob("note.dat");
    test:assertEquals(back, doc);
}

@test:Config {}
function testCopyBlobOverSas() returns error? {
    Client blobClient = check readyClient("sascopy");
    string container = testContainer("sascopy");
    check blobClient->upload("src", "src.txt");
    // The service authorizes the copy source separately, so the client's SAS travels with it.
    string sas = check blobClient.generateContainerSas({
        expiryTime: time:utcAddSeconds(time:utcNow(), 600),
        permissions: {read: true, write: true, create: true}
    });
    Client viaSas = check newSasContainerClient(container, sas);
    CopyInfo copied = check viaSas->copyBlob("src.txt", "dst.txt");
    test:assertTrue(copied.copyId != "");
    check await(function() returns boolean|error {
        BlobProperties props = check blobClient->getBlobProperties("dst.txt");
        return props.copyStatus?.copyStatus == SUCCESS;
    });
    string content = check blobClient->getBlob("dst.txt");
    test:assertEquals(content, "src");
}
