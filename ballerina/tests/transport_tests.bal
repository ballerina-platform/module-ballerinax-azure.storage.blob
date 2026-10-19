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

// Transport configuration: the timeouts, the proxy, and the TLS material reach the HTTP
// client the connector builds. The TLS fixtures are a self-signed certificate with its key
// in the three PEM forms (PKCS#8, encrypted PKCS#8, and PKCS#1).

import ballerina/jballerina.java;
import ballerina/test;
import ballerina/time;

const string TLS_DIR = "tests/resources/tls";
const string FIXTURE_CERT = TLS_DIR + "/cert.pem";

// A non-routable address: the connect attempt gets no answer and runs into the timeout.
const string BLACKHOLE_URL = "http://10.255.255.1:10000/devstoreaccount1";

@test:Config {}
function testConnectTimeoutIsHonoured() returns error? {
    AdminClient admin = check new (
        auth = {accountName: AZURITE_ACCOUNT, accountKey: AZURITE_KEY, serviceUrl: BLACKHOLE_URL},
        retryConfig = {maxTries: 1},
        transportConfig = {connectionPool: {connectTimeoutSeconds: 1}}
    );
    time:Utc started = time:utcNow();
    AccountInfo|Error info = admin->getAccountInfo();
    decimal elapsed = time:utcDiffSeconds(time:utcNow(), started);
    test:assertTrue(info is Error && info !is ServiceError,
            "a connect timeout is a client-side error, got " + describe(info));
    test:assertTrue(elapsed < 10d, string `the 1 s connect timeout took ${elapsed} s`);
}

@test:Config {}
function testProxySettingsAreApplied() returns error? {
    SharedKeyConfig auth = sharedKeyAuth();
    string backendHost = liveRun ? string `${auth.accountName}.blob.core.windows.net` : "127.0.0.1";
    // The backend host is on the bypass list, so the unreachable proxy is never contacted.
    AdminClient bypassing = check new (auth = auth, transportConfig = {
        proxy: {host: "proxy.invalid", port: 3128, username: "user", password: "secret", nonProxyHosts: [backendHost]}
    });
    _ = check bypassing->getAccountInfo();
    // Without the bypass every request goes to the proxy, whose name does not resolve.
    AdminClient proxied = check new (auth = auth, retryConfig = {maxTries: 1}, transportConfig = {
        proxy: {host: "proxy.invalid", port: 3128, username: "user", password: "secret"}
    });
    AccountInfo|Error info = proxied->getAccountInfo();
    test:assertTrue(info is Error && info !is ServiceError,
            "an unreachable proxy fails the request locally, got " + describe(info));
    // A SOCKS proxy is accepted at initialization.
    _ = check new AdminClient(auth = auth, transportConfig = {
        proxy: {proxyType: SOCKS5, host: "proxy.invalid", port: 1080}
    });
}

@test:Config {}
function testSecureSocketBuildsFromPemMaterial() returns error? {
    _ = check new Client("tls", auth = sharedKeyAuth(), transportConfig = {secureSocket: {
        cert: FIXTURE_CERT,
        key: {certFile: FIXTURE_CERT, keyFile: TLS_DIR + "/key-pkcs8.pem"},
        protocolVersions: ["TLSv1.3", "TLSv1.2"],
        ciphers: ["TLS_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"],
        sniHostName: "fixture.example",
        sessionTimeoutSeconds: 300,
        handshakeTimeoutSeconds: 15,
        sessionResumption: true,
        verifyHostName: true,
        validateRevocation: true
    }});
    _ = check new Client("tls", auth = sharedKeyAuth(), transportConfig = {secureSocket: {
        key: {certFile: FIXTURE_CERT, keyFile: TLS_DIR + "/key-encrypted.pem", keyPassword: "fixture"}
    }});
    // The traditional PKCS#1 form loads too.
    _ = check new Client("tls", auth = sharedKeyAuth(), transportConfig = {secureSocket: {
        key: {certFile: FIXTURE_CERT, keyFile: TLS_DIR + "/key-pkcs1.pem"}
    }});
}

@test:Config {}
function testSecureSocketRefusesBadMaterial() {
    Client|Error missing = new ("tls", auth = sharedKeyAuth(), transportConfig = {secureSocket: {
        cert: TLS_DIR + "/absent.pem"
    }});
    assertRefused(missing, "does not point to a readable file");
    Client|Error noPassword = new ("tls", auth = sharedKeyAuth(), transportConfig = {secureSocket: {
        key: {certFile: FIXTURE_CERT, keyFile: TLS_DIR + "/key-encrypted.pem"}
    }});
    assertRefused(noPassword, "does not contain valid private key");
    Client|Error wrongPassword = new ("tls", auth = sharedKeyAuth(), transportConfig = {secureSocket: {
        key: {certFile: FIXTURE_CERT, keyFile: TLS_DIR + "/key-encrypted.pem", keyPassword: "wrong"}
    }});
    assertRefused(wrongPassword, "does not contain valid private key");
    Client|Error revocationWithoutTrust = new ("tls", auth = sharedKeyAuth(), transportConfig = {secureSocket: {
        validateRevocation: true
    }});
    assertRefused(revocationWithoutTrust, "validateRevocation");
}

// The service's certificate chains to a public CA, which a trust store holding only the
// fixture certificate rejects; the emulator speaks plain HTTP, so only a live run can show
// that the configured trust material governs the handshake.
@test:Config {enable: liveRun}
function testSecureSocketTrustMaterialGovernsTheHandshake() returns error? {
    AdminClient admin = check new (auth = sharedKeyAuth(), retryConfig = {maxTries: 1},
            transportConfig = {secureSocket: {cert: FIXTURE_CERT}});
    AccountInfo|Error info = admin->getAccountInfo();
    test:assertTrue(info is Error && info !is ServiceError,
            "a handshake against untrusted material fails locally, got " + describe(info));
}

// The SDK's Netty version advisory stays out of the program's log. The transport checks its
// logger for INFO before it scans the classpath, and the connector leaves that logger at WARNING.
@test:Config {}
function testNettyVersionAdvisoryIsSuppressed() returns error? {
    _ = check new AdminClient(auth = sharedKeyAuth());
    test:assertFalse(loggerAdmitsInfo("com.azure.core.http.netty.implementation.NettyUtility"),
            "the Netty version advisory logger admits INFO");
}

// Whether a JUL logger would publish an INFO record, which is the check the SDK transport
// makes before it scans the classpath for Netty versions.
function loggerAdmitsInfo(string loggerName) returns boolean {
    handle logger = getLogger(java:fromString(loggerName));
    return isLoggable(logger, infoLevel());
}

function getLogger(handle name) returns handle = @java:Method {
    'class: "java.util.logging.Logger",
    paramTypes: ["java.lang.String"]
} external;

function infoLevel() returns handle = @java:FieldGet {
    'class: "java.util.logging.Level",
    name: "INFO"
} external;

function isLoggable(handle logger, handle level) returns boolean = @java:Method {
    'class: "java.util.logging.Logger"
} external;

function assertRefused(Client|Error result, string messagePart) {
    test:assertTrue(result is Error && result !is ServiceError, "expected a client-side error, got " + describe(result));
    if result is Error {
        test:assertTrue(result.message().includes(messagePart),
                string `expected "${messagePart}" in: ${result.message()}`);
    }
}
