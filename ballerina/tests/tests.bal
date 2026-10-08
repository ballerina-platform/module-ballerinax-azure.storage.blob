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

// Client and AdminClient tests: container lifecycle, service configuration, blob operations,
// the transfers, and the data binding matrix. Every test derives from a specification claim.

import ballerina/file;
import ballerina/io;
import ballerina/test;

type Metric record {
    string quarter;
    int revenue;
};

type Note record {
    string title;
    string body?;
};

// ---------------------------------------------------------------------------
// Backend and initialization
// ---------------------------------------------------------------------------

@test:Config {}
function testBackendIsExplicit() {
    io:println(liveRun ? "backend: live Azure" : "backend: Azurite at " + AZURITE_URL);
    test:assertEquals(liveRun, liveAccountName != "" && liveAccountKey != "");
}

@test:Config {}
function testInitMakesNoCall() returns error? {
    // A client bound to a container that does not exist initializes; the first call fails.
    Client blobClient = check newContainerClient(testContainer("missing"));
    BlobProperties|Error properties = blobClient->getBlobProperties("nothing.txt");
    test:assertTrue(properties is NotFoundError, "expected NotFoundError, got " + describe(properties));
    if properties is NotFoundError {
        test:assertEquals(properties.detail().errorCode, "ContainerNotFound");
        test:assertEquals(properties.detail().httpStatus, 404);
    }
    // The existence check reads any confirmed 404 as absence.
    boolean exists = check blobClient->hasBlob("nothing.txt");
    test:assertFalse(exists);
}

@test:Config {}
function testInitValidatesLocally() {
    AdminClient|Error badKey = new (auth = {accountName: "acct", accountKey: "not base64!"});
    test:assertTrue(badKey is Error && badKey !is ServiceError);
    AdminClient|Error badUrl = new (auth = {sasUrl: "https://acct.blob.core.windows.net/?sv=2024"});
    test:assertTrue(badUrl is Error && badUrl !is ServiceError);
    Client|Error emptyContainer = new ("", auth = sharedKeyAuth());
    test:assertTrue(emptyContainer is Error && emptyContainer !is ServiceError);
}

// ---------------------------------------------------------------------------
// AdminClient: containers
// ---------------------------------------------------------------------------

@test:Config {}
function testContainerLifecycle() returns error? {
    AdminClient admin = check newAdmin();
    string container = testContainer("lifecycle");
    boolean before = check admin->hasContainer(container);
    test:assertFalse(before);
    check admin->createContainer(container, {metadata: {owner: "tests"}});
    boolean after = check admin->hasContainer(container);
    test:assertTrue(after);

    Error? again = admin->createContainer(container);
    test:assertTrue(again is ConflictError);
    if again is ConflictError {
        test:assertEquals(again.detail().errorCode, "ContainerAlreadyExists");
        test:assertEquals(again.detail().httpStatus, 409);
    }

    ContainerList listed = check admin->listContainers({prefix: container, includeMetadata: true});
    test:assertEquals(listed.containers.length(), 1);
    ContainerInfo info = listed.containers[0];
    test:assertEquals(info.name, container);
    test:assertEquals(info.publicAccess, PRIVATE);
    test:assertEquals(info.leaseState, AVAILABLE);
    test:assertEquals(info.leaseStatus, UNLOCKED);
    test:assertEquals(info.leaseDuration, ());
    test:assertEquals(info.metadata, {owner: "tests"});
    test:assertFalse(listed.hasKey("nextMarker"));

    check admin->deleteContainer(container);
    // A never-created name: the service deletes asynchronously, so deleting the same container
    // again races the first deletion and may be accepted, conflict, or be not found.
    Error? missing = admin->deleteContainer(testContainer("lifecycle-absent"));
    test:assertTrue(missing is NotFoundError, "deleting a container that does not exist must be NotFoundError");
}

@test:Config {}
function testListContainersLimitAndMarker() returns error? {
    AdminClient admin = check newAdmin();
    string prefix = testContainer("page");
    foreach int i in 0 ..< 3 {
        check createTestContainer(admin, string `${prefix}-${i}`);
    }
    ContainerList first = check admin->listContainers({prefix, 'limit: 2});
    test:assertEquals(first.containers.length(), 2);
    test:assertTrue(first.hasKey("nextMarker"));
    ContainerList second = check admin->listContainers({prefix, 'limit: 2, marker: first.nextMarker});
    test:assertEquals(second.containers.length(), 1);
    test:assertFalse(second.hasKey("nextMarker"));
    // A marker without a limit resumes the whole remaining listing.
    ContainerList rest = check admin->listContainers({prefix, marker: first.nextMarker});
    test:assertEquals(rest.containers.length(), 1);
    test:assertFalse(rest.hasKey("nextMarker"));

    ContainerList all = check admin->listContainers({prefix});
    test:assertEquals(all.containers.length(), 3);
    test:assertFalse(all.hasKey("nextMarker"));
}

@test:Config {}
function testCreateContainerWithPublicAccess() returns error? {
    AdminClient admin = check newAdmin();
    string container = testContainer("public");
    check admin->createContainer(container, {publicAccess: BLOB});
    Client blobClient = check newContainerClient(container);
    ContainerProperties properties = check blobClient->getContainerProperties();
    test:assertEquals(properties.publicAccess, BLOB);
}

// ---------------------------------------------------------------------------
// AdminClient: service configuration and account information
// ---------------------------------------------------------------------------

@test:Config {}
function testServicePropertiesRoundTrip() returns error? {
    AdminClient admin = check newAdmin();
    ServiceProperties before = check admin->getServiceProperties();
    CorsRule rule = {
        allowedOrigins: ["https://example.test"],
        allowedMethods: ["GET", "PUT"],
        allowedHeaders: ["x-ms-meta-*"],
        exposedHeaders: ["x-ms-request-id"],
        maxAgeInSeconds: 60
    };
    check admin->setServiceProperties({cors: [rule]});
    ServiceProperties after = check admin->getServiceProperties();
    test:assertEquals(after.cors, [rule]);
    // A group absent from the write is left untouched.
    test:assertEquals(after.hourMetrics, before.hourMetrics);
    test:assertEquals(after.logging, before.logging);
    // An empty array deletes every rule.
    check admin->setServiceProperties({cors: []});
    ServiceProperties cleared = check admin->getServiceProperties();
    test:assertEquals(cleared.cors ?: [], []);
}

@test:Config {}
function testAccountInfo() returns error? {
    AdminClient admin = check newAdmin();
    AccountInfo info = check admin->getAccountInfo();
    test:assertNotEquals(info.skuName, "");
    test:assertNotEquals(info.accountKind, "");
}

// ---------------------------------------------------------------------------
// Client: container operations
// ---------------------------------------------------------------------------

@test:Config {}
function testContainerMetadataAndProperties() returns error? {
    AdminClient admin = check newAdmin();
    string container = testContainer("meta");
    check createTestContainer(admin, container);
    Client blobClient = check newContainerClient(container);
    check blobClient->setContainerMetadata({team: "storage", env: "test"});
    ContainerProperties properties = check blobClient->getContainerProperties();
    test:assertEquals(properties.metadata, {team: "storage", env: "test"});
    test:assertEquals(properties.publicAccess, PRIVATE);
    test:assertEquals(properties.leaseState, AVAILABLE);
    test:assertFalse(properties.hasLegalHold);
    // Replace-whole: the new set drops the omitted key.
    check blobClient->setContainerMetadata({team: "blob"});
    properties = check blobClient->getContainerProperties();
    test:assertEquals(properties.metadata, {team: "blob"});
}

@test:Config {}
function testAccessPolicySettersKeepTheOtherHalf() returns error? {
    AdminClient admin = check newAdmin();
    string container = testContainer("acl");
    check createTestContainer(admin, container);
    Client blobClient = check newContainerClient(container);

    SignedIdentifier policy = {id: "readers", permissions: "rl", expiryTime: [4102444800, 0]};
    check blobClient->setContainerAccessPolicy([policy]);
    ContainerAccessPolicy acl = check blobClient->getContainerAccessPolicy();
    test:assertEquals(acl.access, PRIVATE);
    test:assertEquals(acl.identifiers.length(), 1);
    test:assertEquals(acl.identifiers[0].id, "readers");
    test:assertEquals(acl.identifiers[0].permissions, "rl");

    check blobClient->setPublicAccess(CONTAINER);
    acl = check blobClient->getContainerAccessPolicy();
    test:assertEquals(acl.access, CONTAINER);
    test:assertEquals(acl.identifiers.length(), 1, "setPublicAccess must keep the stored policies");

    check blobClient->setContainerAccessPolicy([]);
    acl = check blobClient->getContainerAccessPolicy();
    test:assertEquals(acl.access, CONTAINER, "setContainerAccessPolicy must keep the access level");
    test:assertEquals(acl.identifiers.length(), 0);

    check blobClient->setPublicAccess(PRIVATE);
    acl = check blobClient->getContainerAccessPolicy();
    test:assertEquals(acl.access, PRIVATE);

    SignedIdentifier[] six = [];
    foreach int i in 0 ..< 6 {
        six.push({id: string `p${i}`, permissions: "r"});
    }
    Error? tooMany = blobClient->setContainerAccessPolicy(six);
    test:assertTrue(tooMany is Error && tooMany !is ServiceError);
}

// ---------------------------------------------------------------------------
// Client: blob operations and transfers
// ---------------------------------------------------------------------------

@test:Config {}
function testUploadReplacesAndGetBlobBinds() returns error? {
    Client blobClient = check readyClient("basic");
    check blobClient->upload("hello", "docs/greeting.txt");
    boolean present = check blobClient->hasBlob("docs/greeting.txt");
    test:assertTrue(present);
    boolean absent = check blobClient->hasBlob("docs/other.txt");
    test:assertFalse(absent);
    string text = check blobClient->getBlob("docs/greeting.txt");
    test:assertEquals(text, "hello");
    byte[] raw = check blobClient->getBlob("docs/greeting.txt");
    test:assertEquals(raw, "hello".toBytes());

    // A second upload replaces the blob; a leading slash is stripped.
    check blobClient->upload("hello again".toBytes(), "/docs/greeting.txt");
    text = check blobClient->getBlob("docs/greeting.txt");
    test:assertEquals(text, "hello again");

    BlobProperties properties = check blobClient->getBlobProperties("docs/greeting.txt");
    test:assertEquals(properties.contentLength, 11);
    test:assertEquals(properties.blobType, "BlockBlob");
    test:assertEquals(properties.leaseState, AVAILABLE);
    test:assertFalse(properties.hasKey("copyStatus"));
    // The connector sets no type for raw content; the service's own default is octet-stream.
    test:assertEquals(properties.contentHeaders.contentType ?: "application/octet-stream", "application/octet-stream");
}

@test:Config {}
function testStructuredUploadsSetContentType() returns error? {
    Client blobClient = check readyClient("structured");
    Metric q1 = {quarter: "q1", revenue: 1250000};
    check blobClient->upload(q1, "2026/q1/metrics.json");
    BlobProperties jsonProperties = check blobClient->getBlobProperties("2026/q1/metrics.json");
    test:assertEquals(jsonProperties.contentHeaders.contentType, "application/json");
    Metric back = check blobClient->getBlob("2026/q1/metrics.json");
    test:assertEquals(back, q1);

    check blobClient->upload(q1, "2026/q1/metrics.xml");
    BlobProperties xmlProperties = check blobClient->getBlobProperties("2026/q1/metrics.xml");
    test:assertEquals(xmlProperties.contentHeaders.contentType, "application/xml");
    Metric fromXml = check blobClient->getBlob("2026/q1/metrics.xml");
    test:assertEquals(fromXml, q1);
    xml document = check blobClient->getBlob("2026/q1/metrics.xml");
    test:assertEquals((document/<quarter>).data(), "q1");

    Metric[] quarters = [{quarter: "q1", revenue: 1250000}, {quarter: "q2", revenue: 1310000}];
    check blobClient->upload(quarters, "2026/summary.csv");
    BlobProperties csv = check blobClient->getBlobProperties("2026/summary.csv");
    test:assertEquals(csv.contentHeaders.contentType, "text/csv");
    Metric[] rows = check blobClient->getBlob("2026/summary.csv");
    test:assertEquals(rows, quarters);
    string csvText = check blobClient->getBlob("2026/summary.csv");
    test:assertEquals(csvText, "quarter,revenue\nq1,1250000\nq2,1310000");

    // The override beats the extension, and an explicit content type wins.
    check blobClient->upload(quarters, "2026/summary.dat",
            {fileFormat: CSV, contentHeaders: {contentType: "text/plain"}});
    BlobProperties dat = check blobClient->getBlobProperties("2026/summary.dat");
    test:assertEquals(dat.contentHeaders.contentType, "text/plain");
    Metric[] datRows = check blobClient->getBlob("2026/summary.dat", {fileFormat: CSV});
    test:assertEquals(datRows, quarters);

    json value = {items: [1, 2, 3], ok: true};
    check blobClient->upload(value, "2026/value.json");
    json readBack = check blobClient->getBlob("2026/value.json");
    test:assertEquals(readBack, value);
}

@test:Config {}
function testFormatRefusals() returns error? {
    Client blobClient = check readyClient("refusals");
    Metric q1 = {quarter: "q1", revenue: 1};
    Metric[] quarters = [q1];
    Error? r1 = blobClient->upload(q1, "no-extension");
    test:assertTrue(r1 is Error, "record without a format");
    Error? r2 = blobClient->upload(q1, "rows.csv");
    test:assertTrue(r2 is Error, "record is never CSV");
    Error? r3 = blobClient->upload(quarters, "rows.json");
    test:assertTrue(r3 is Error, "record array is never JSON");
    Error? r4 = blobClient->upload(quarters, "rows.xml");
    test:assertTrue(r4 is Error, "record array is never XML");
    json array = [1, 2];
    Error? r5 = blobClient->upload(array, "array.csv");
    test:assertTrue(r5 is Error, "json is never CSV");

    check blobClient->upload("a,b\n1,2", "rows.csv");
    Metric|Error notCsv = blobClient->getBlob("rows.csv");
    test:assertTrue(notCsv is Error && notCsv !is ServiceError, "a record never binds from CSV");
    check blobClient->upload("{\"quarter\": \"q1\", \"revenue\": \"x\"}", "bad.json");
    Metric|Error strict = blobClient->getBlob("bad.json");
    test:assertTrue(strict is Error && strict !is ServiceError, "binding is strict");
    Metric|Error unresolvable = blobClient->getBlob("no-extension-either");
    test:assertTrue(unresolvable is Error, "a record target without a format is refused");
}

@test:Config {}
function testCsvWriterUnionHeaderAndQuoting() returns error? {
    Client blobClient = check readyClient("csvwriter");
    Note[] notes = [{title: "plain"}, {title: "with, comma", body: "says \"hi\"\nbye"}];
    check blobClient->upload(notes, "notes.csv");
    string csv = check blobClient->getBlob("notes.csv");
    test:assertEquals(csv, "title,body\nplain,\n\"with, comma\",\"says \\\"hi\\\"\nbye\"");
    Note[] back = check blobClient->getBlob("notes.csv");
    test:assertEquals(back[1].title, "with, comma");
    test:assertEquals(back[1].body, "says \"hi\"\nbye");

    Note[] none = [];
    check blobClient->upload(none, "empty.csv");
    BlobProperties empty = check blobClient->getBlobProperties("empty.csv");
    test:assertEquals(empty.contentLength, 0);
}

@test:Config {}
function testFileTransfers() returns error? {
    Client blobClient = check readyClient("files");
    string sourceFile = check file:createTemp("-source.bin");
    byte[] payload = [];
    foreach int i in 0 ..< 300000 {
        payload.push(<byte>(i % 251));
    }
    check io:fileWriteBytes(sourceFile, payload);
    check blobClient->uploadFromFile(sourceFile, "bin/payload.bin", {metadata: {origin: "disk"}});
    BlobProperties properties = check blobClient->getBlobProperties("bin/payload.bin");
    test:assertEquals(properties.contentLength, payload.length());
    test:assertEquals(properties.metadata, {origin: "disk"});

    string target = check file:createTemp("-target.bin");
    Error? exists = blobClient->download("bin/payload.bin", target);
    test:assertTrue(exists is Error && exists !is ServiceError, "download must not overwrite a local file");
    check file:remove(target);
    check blobClient->download("bin/payload.bin", target);
    test:assertEquals(check io:fileReadBytes(target), payload);

    string partial = check file:createTemp("-partial.bin");
    check file:remove(partial);
    check blobClient->download("bin/payload.bin", partial, {range: {startByte: 10, endByte: 19}});
    test:assertEquals(check io:fileReadBytes(partial), payload.slice(10, 20));
    byte[] ranged = check blobClient->getBlob("bin/payload.bin", {range: {startByte: 0, endByte: 3}});
    test:assertEquals(ranged, payload.slice(0, 4));

    Error? missingLocal = blobClient->uploadFromFile("/definitely/not/here.bin", "x.bin");
    test:assertTrue(missingLocal is Error && missingLocal !is ServiceError);
    check file:remove(sourceFile);
    check file:remove(target);
    check file:remove(partial);
}

@test:Config {}
function testStreamUploads() returns error? {
    Client blobClient = check readyClient("streams");
    // Chunks of uneven size that coalesce into blocks: two full blocks plus a remainder.
    byte[][] chunks = [];
    int total = 0;
    foreach int i in 0 ..< 5 {
        byte[] chunk = [];
        foreach int j in 0 ..< (2 * 1024 * 1024 + i * 1000) {
            chunk.push(<byte>((i + j) % 256));
        }
        total += chunk.length();
        chunks.push(chunk);
    }
    stream<byte[], error?> sourceStream = chunks.toStream();
    check blobClient->upload(sourceStream, "stream/big.bin");
    BlobProperties properties = check blobClient->getBlobProperties("stream/big.bin");
    test:assertEquals(properties.contentLength, total);
    // The connector sets no type for a byte stream; the service's own default is octet-stream.
    test:assertEquals(properties.contentHeaders.contentType ?: "application/octet-stream", "application/octet-stream");
    stream<byte[], error?> back = check blobClient->getBlob("stream/big.bin");
    int read = 0;
    check from byte[] chunk in back
        do {
            read += chunk.length();
        };
    test:assertEquals(read, total);

    // An empty stream writes an empty blob.
    byte[][] nothing = [];
    check blobClient->upload(nothing.toStream(), "stream/empty.bin");
    BlobProperties empty = check blobClient->getBlobProperties("stream/empty.bin");
    test:assertEquals(empty.contentLength, 0);

    // A record stream writes CSV rows as pulled, headed by the first record's fields.
    Note[] notes = [{title: "a"}, {title: "b", body: "extra"}];
    stream<record {}, error?> rows = notes.toStream();
    check blobClient->upload(rows, "stream/notes.csv");
    string csv = check blobClient->getBlob("stream/notes.csv");
    test:assertEquals(csv, "title\na\nb");
    BlobProperties csvProperties = check blobClient->getBlobProperties("stream/notes.csv");
    test:assertEquals(csvProperties.contentHeaders.contentType, "text/csv");
    stream<Note, error?> lazyRows = check blobClient->getBlob("stream/notes.csv");
    Note[] bound = check from Note note in lazyRows select note;
    test:assertEquals(bound, [{title: "a"}, {title: "b"}]);

    // A failing source leaves no blob behind and closes the source.
    FailingSource failing = new;
    stream<byte[], error?> broken = new (failing);
    Error? failed = blobClient->upload(broken, "stream/never.bin");
    test:assertTrue(failed is Error && failed !is ServiceError);
    test:assertTrue(failing.closed);
    boolean neverExists = check blobClient->hasBlob("stream/never.bin");
    test:assertFalse(neverExists);
}

class FailingSource {
    boolean closed = false;
    private int pulls = 0;

    public isolated function next() returns record {|byte[] value;|}|error? {
        lock {
            self.pulls += 1;
            if self.pulls > 1 {
                return error("source broke");
            }
        }
        return {value: [1, 2, 3]};
    }

    public isolated function close() returns error? {
        lock {
            self.closed = true;
        }
    }
}

@test:Config {}
function testMetadataHeadersAndDelete() returns error? {
    Client blobClient = check readyClient("headers");
    check blobClient->upload("x", "a.txt", {contentHeaders: {contentType: "text/plain", cacheControl: "no-cache"},
            metadata: {k: "v"}, tags: {env: "test"}});
    BlobProperties properties = check blobClient->getBlobProperties("a.txt");
    test:assertEquals(properties.contentHeaders.contentType, "text/plain");
    test:assertEquals(properties.contentHeaders.cacheControl, "no-cache");
    test:assertEquals(properties.metadata, {k: "v"});
    map<string> tags = check blobClient->getTags("a.txt");
    test:assertEquals(tags, {env: "test"});

    check blobClient->setBlobMetadata("a.txt", {k2: "v2"});
    check blobClient->setContentHeaders("a.txt", {contentLanguage: "en"});
    properties = check blobClient->getBlobProperties("a.txt");
    test:assertEquals(properties.metadata, {k2: "v2"});
    test:assertEquals(properties.contentHeaders.contentLanguage, "en");
    test:assertFalse(properties.contentHeaders.hasKey("cacheControl"), "setContentHeaders clears omitted headers");

    check blobClient->deleteBlob("a.txt");
    boolean stillThere = check blobClient->hasBlob("a.txt");
    test:assertFalse(stillThere);
    Error? gone = blobClient->deleteBlob("a.txt");
    test:assertTrue(gone is NotFoundError);
    if gone is NotFoundError {
        test:assertEquals(gone.detail().errorCode, "BlobNotFound");
    }
    Error? emptyPath = blobClient->deleteBlob("/");
    test:assertTrue(emptyPath is Error && emptyPath !is ServiceError);
}

@test:Config {}
function testListBlobsFlatHierarchicalAndPaged() returns error? {
    Client blobClient = check readyClient("listing");
    foreach string path in ["2026/01/a.txt", "2026/02/b.txt", "2027/c.txt", "root.txt"] {
        check blobClient->upload(path, path, {metadata: {p: path}, tags: {year: path.substring(0, 4)}});
    }
    stream<BlobEntry, Error?> all = check blobClient->listBlobs();
    string[] paths = check from BlobEntry entry in all select entry.path;
    test:assertEquals(paths, ["2026/01/a.txt", "2026/02/b.txt", "2027/c.txt", "root.txt"]);

    stream<BlobEntry, Error?> prefixed = check blobClient->listBlobs({prefix: "2026/", includeMetadata: true,
            includeTags: true});
    BlobEntry[] entries = check from BlobEntry entry in prefixed select entry;
    test:assertEquals(entries.length(), 2);
    test:assertEquals(entries[0].metadata, {p: "2026/01/a.txt"});
    test:assertEquals(entries[0].tags, {year: "2026"});
    test:assertEquals(entries[0].blobType, "BlockBlob");
    test:assertEquals(entries[0].contentLength, "2026/01/a.txt".length());

    stream<BlobEntry, Error?> grouped = check blobClient->listBlobs({delimiter: "/"});
    BlobEntry[] top = check from BlobEntry entry in grouped select entry;
    // The service orders prefixes and blobs together by name; the emulator lists blobs first.
    test:assertEquals(top.map(e => e.path).sort(), ["2026/", "2027/", "root.txt"]);
    test:assertEquals(top.filter(e => e.isPrefix).map(e => e.path).sort(), ["2026/", "2027/"]);
    test:assertEquals(top.filter(e => !e.isPrefix).map(e => e.path), ["root.txt"]);

    BlobList page = check blobClient->listBlobsPage({pageSize: 3});
    test:assertEquals(page.blobs.length(), 3);
    test:assertTrue(page.hasKey("nextMarker"));
    BlobList rest = check blobClient->listBlobsPage({pageSize: 3, marker: page.nextMarker});
    test:assertEquals(rest.blobs.length(), 1);
    test:assertFalse(rest.hasKey("nextMarker"));
}

// Renders a result for an assertion message, whatever it holds.
isolated function describe(any|error value) returns string => value is error ? value.message() : value.toString();

// A client bound to a fresh container of this run.
function readyClient(string base) returns Client|error {
    AdminClient admin = check newAdmin();
    string container = testContainer(base);
    check createTestContainer(admin, container);
    return newContainerClient(container);
}

// ---------------------------------------------------------------------------
// Connection strings: the SDK's parser decides, including the development shorthand
// ---------------------------------------------------------------------------

@test:Config {}
function testConnectionStringForms() returns error? {
    // Whitespace around the pairs is the SDK's to trim.
    SharedKeyConfig key = sharedKeyAuth();
    string padded = liveRun
        ? string `DefaultEndpointsProtocol=https; AccountName=${key.accountName}; AccountKey=${key.accountKey}; EndpointSuffix=core.windows.net`
        : string `DefaultEndpointsProtocol=http; AccountName=${key.accountName}; AccountKey=${key.accountKey}; BlobEndpoint=${AZURITE_URL}`;
    AdminClient admin = check new (auth = {connectionString: padded});
    boolean absent = check admin->hasContainer(testContainer("never-created"));
    test:assertFalse(absent);

    // A string the SDK cannot derive a blob endpoint from fails at init, before any call.
    AdminClient|Error queueOnly = new (auth = {connectionString: "QueueEndpoint=https://acct.queue.core.windows.net;SharedAccessSignature=sv=2024-11-04&sig=abc"});
    test:assertTrue(queueOnly is Error && queueOnly !is ServiceError, "a queue-only connection string must fail at init");
    AdminClient|Error garbage = new (auth = {connectionString: "not a connection string"});
    test:assertTrue(garbage is Error && garbage !is ServiceError);

    // The development storage shorthand resolves to the emulator's endpoints, port included.
    if !liveRun {
        AdminClient dev = check new (auth = {connectionString: "UseDevelopmentStorage=true"});
        boolean devAbsent = check dev->hasContainer(testContainer("never-created"));
        test:assertFalse(devAbsent);
    }
}
