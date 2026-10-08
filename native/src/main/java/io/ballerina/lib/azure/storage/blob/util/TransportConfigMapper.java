/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.azure.storage.blob.util;

import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertPathBuilder;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.PKIXRevocationChecker;
import java.security.cert.X509CertSelector;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

/**
 * Maps the connector's {@code RetryConfig} and {@code TransportConfig} onto the SDK's retry
 * policy and its Netty transport. TLS settings ({@code SecureSocket}) are mapped with full
 * fidelity: trust material (a {@code CertStore} or a PEM file), client key material (a store or
 * cert-and-key files), TLS versions, cipher suites, hostname verification, SNI override,
 * session flags and timeouts, and certificate revocation checking through the JDK's PKIX
 * checker.
 */
public final class TransportConfigMapper {

    // Ballerina RetryPolicyType and ProxyType enum values matched against the config.
    private static final String RETRY_POLICY_FIXED = "fixed";
    private static final String PROXY_TYPE_SOCKS4 = "SOCKS4";
    private static final String PROXY_TYPE_SOCKS5 = "SOCKS5";

    // Field names of the retry, connection-pool, proxy, and TLS records (read only here).
    private static final BString RETRY_POLICY_TYPE = StringUtils.fromString("retryPolicyType");
    private static final BString MAX_TRIES = StringUtils.fromString("maxTries");
    private static final BString TRY_TIMEOUT_SECONDS = StringUtils.fromString("tryTimeoutSeconds");
    private static final BString RETRY_DELAY_SECONDS = StringUtils.fromString("retryDelaySeconds");
    private static final BString MAX_RETRY_DELAY_SECONDS = StringUtils.fromString("maxRetryDelaySeconds");
    private static final BString SECONDARY_HOST_URL = StringUtils.fromString("secondaryHostUrl");
    private static final BString CONNECTION_POOL = StringUtils.fromString("connectionPool");
    private static final BString MAX_CONNECTIONS = StringUtils.fromString("maxConnections");
    private static final BString IDLE_TIMEOUT_SECONDS = StringUtils.fromString("idleTimeoutSeconds");
    private static final BString CONNECT_TIMEOUT_SECONDS = StringUtils.fromString("connectTimeoutSeconds");
    private static final BString READ_TIMEOUT_SECONDS = StringUtils.fromString("readTimeoutSeconds");
    private static final BString SECURE_SOCKET = StringUtils.fromString("secureSocket");
    private static final BString PROXY = StringUtils.fromString("proxy");
    private static final BString PROXY_TYPE = StringUtils.fromString("proxyType");
    private static final BString HOST = StringUtils.fromString("host");
    private static final BString PORT = StringUtils.fromString("port");
    private static final BString USERNAME = StringUtils.fromString("username");
    private static final BString PASSWORD = StringUtils.fromString("password");
    private static final BString NON_PROXY_HOSTS = StringUtils.fromString("nonProxyHosts");
    private static final BString CERT = StringUtils.fromString("cert");
    private static final BString KEY = StringUtils.fromString("key");
    private static final BString PROTOCOL_VERSIONS = StringUtils.fromString("protocolVersions");
    private static final BString CIPHERS = StringUtils.fromString("ciphers");
    private static final BString VERIFY_HOST_NAME = StringUtils.fromString("verifyHostName");
    private static final BString SESSION_RESUMPTION = StringUtils.fromString("sessionResumption");
    private static final BString VALIDATE_REVOCATION = StringUtils.fromString("validateRevocation");
    private static final BString HANDSHAKE_TIMEOUT_SECONDS = StringUtils.fromString("handshakeTimeoutSeconds");
    private static final BString SESSION_TIMEOUT_SECONDS = StringUtils.fromString("sessionTimeoutSeconds");
    private static final BString SNI_HOST_NAME = StringUtils.fromString("sniHostName");
    // CertStore and CertKey fields.
    private static final BString STORE_PATH = StringUtils.fromString("path");
    private static final BString CERT_FILE = StringUtils.fromString("certFile");
    private static final BString KEY_FILE = StringUtils.fromString("keyFile");
    private static final BString KEY_PASSWORD = StringUtils.fromString("keyPassword");

    private TransportConfigMapper() {
    }

    public static RequestRetryOptions retryOptions(BMap<BString, Object> retry) {
        String policy = retry.getStringValue(RETRY_POLICY_TYPE).getValue();
        return new RequestRetryOptions(
                RETRY_POLICY_FIXED.equals(policy) ? RetryPolicyType.FIXED : RetryPolicyType.EXPONENTIAL,
                Math.toIntExact((Long) retry.get(MAX_TRIES)),
                seconds(retry.get(TRY_TIMEOUT_SECONDS)),
                seconds(retry.get(RETRY_DELAY_SECONDS)),
                seconds(retry.get(MAX_RETRY_DELAY_SECONDS)),
                ValueUtils.optString(retry, SECONDARY_HOST_URL));
    }

    @SuppressWarnings("unchecked")
    public static com.azure.core.http.HttpClient httpClient(BMap<BString, Object> transport) {
        BMap<BString, Object> pool = (BMap<BString, Object>) transport.get(CONNECTION_POOL);
        ConnectionProvider provider = ConnectionProvider.builder("azure-storage-blob")
                .maxConnections(Math.toIntExact((Long) pool.get(MAX_CONNECTIONS)))
                .maxIdleTime(seconds(pool.get(IDLE_TIMEOUT_SECONDS)))
                .build();
        HttpClient reactorClient = HttpClient.create(provider);

        BMap<BString, Object> secureSocket = (BMap<BString, Object>) transport.get(SECURE_SOCKET);
        if (secureSocket != null) {
            reactorClient = applyTls(reactorClient, secureSocket);
        }

        NettyAsyncHttpClientBuilder builder = new NettyAsyncHttpClientBuilder(reactorClient)
                .connectTimeout(seconds(pool.get(CONNECT_TIMEOUT_SECONDS)))
                .readTimeout(seconds(pool.get(READ_TIMEOUT_SECONDS)));

        BMap<BString, Object> proxy = (BMap<BString, Object>) transport.get(PROXY);
        if (proxy != null) {
            builder.proxy(proxyOptions(proxy));
        }
        return builder.build();
    }

    private static ProxyOptions proxyOptions(BMap<BString, Object> proxy) {
        String type = proxy.getStringValue(PROXY_TYPE).getValue();
        ProxyOptions.Type proxyType = switch (type) {
            case PROXY_TYPE_SOCKS4 -> ProxyOptions.Type.SOCKS4;
            case PROXY_TYPE_SOCKS5 -> ProxyOptions.Type.SOCKS5;
            default -> ProxyOptions.Type.HTTP;
        };
        ProxyOptions options = new ProxyOptions(proxyType, new InetSocketAddress(
                proxy.getStringValue(HOST).getValue(),
                Math.toIntExact((Long) proxy.get(PORT))));
        String username = ValueUtils.optString(proxy, USERNAME);
        String password = ValueUtils.optString(proxy, PASSWORD);
        if (username != null && password != null) {
            options.setCredentials(username, password);
        }
        BArray nonProxyHosts = (BArray) proxy.get(NON_PROXY_HOSTS);
        if (nonProxyHosts != null && nonProxyHosts.size() > 0) {
            options.setNonProxyHosts(String.join("|", nonProxyHosts.getStringArray()));
        }
        return options;
    }

    private static HttpClient applyTls(HttpClient reactorClient, BMap<BString, Object> secureSocket) {
        try {
            SslContextBuilder sslBuilder = SslContextBuilder.forClient();
            configureTrust(sslBuilder, secureSocket);
            configureKey(sslBuilder, secureSocket);

            BArray protocolVersions = (BArray) secureSocket.get(PROTOCOL_VERSIONS);
            if (protocolVersions != null && protocolVersions.size() > 0) {
                sslBuilder.protocols(protocolVersions.getStringArray());
            }
            BArray ciphers = (BArray) secureSocket.get(CIPHERS);
            if (ciphers != null && ciphers.size() > 0) {
                sslBuilder.ciphers(Arrays.asList(ciphers.getStringArray()));
            }
            Object sessionTimeout = secureSocket.get(SESSION_TIMEOUT_SECONDS);
            if (sessionTimeout != null) {
                sslBuilder.sessionTimeout(seconds(sessionTimeout).toSeconds());
            }
            SslContext sslContext = sslBuilder.build();

            boolean verifyHostName = secureSocket.getBooleanValue(VERIFY_HOST_NAME);
            // The two optional flags leave the engine's defaults alone when absent.
            boolean disableSessions = Boolean.FALSE.equals(secureSocket.get(SESSION_RESUMPTION));
            String sniHostName = ValueUtils.optString(secureSocket, SNI_HOST_NAME);
            Object handshakeTimeout = secureSocket.get(HANDSHAKE_TIMEOUT_SECONDS);

            return reactorClient.secure(spec -> {
                reactor.netty.tcp.SslProvider.Builder providerBuilder = spec.sslContext(sslContext)
                        .handlerConfigurator(handler -> {
                            SSLEngine engine = handler.engine();
                            SSLParameters parameters = engine.getSSLParameters();
                            if (!verifyHostName) {
                                parameters.setEndpointIdentificationAlgorithm(null);
                            }
                            if (sniHostName != null) {
                                parameters.setServerNames(List.of(new SNIHostName(sniHostName)));
                            }
                            engine.setSSLParameters(parameters);
                            if (disableSessions) {
                                engine.setEnableSessionCreation(false);
                            }
                        });
                if (handshakeTimeout != null) {
                    providerBuilder.handshakeTimeout(seconds(handshakeTimeout));
                }
            });
        } catch (GeneralSecurityException | IOException e) {
            throw BlobErrorCreator.clientError(
                    "invalid secureSocket configuration: " + BallerinaAzureClient.describe(e), e);
        }
    }

    /*
     * Applies the trust side: a CertStore or a PEM certificate file, optionally with revocation
     * checking. Revocation runs through the JDK PKIX checker, which consumes stapled OCSP
     * responses when the server sends them and falls back to OCSP/CRL fetching.
     */
    private static void configureTrust(SslContextBuilder sslBuilder, BMap<BString, Object> secureSocket)
            throws GeneralSecurityException, IOException {
        Object cert = secureSocket.get(CERT);
        boolean validateRevocation = Boolean.TRUE.equals(secureSocket.get(VALIDATE_REVOCATION));
        if (cert == null && validateRevocation) {
            throw BlobErrorCreator.clientError(
                    "validateRevocation requires trust material (`cert`) to validate against", null);
        }
        if (cert == null) {
            return;
        }
        KeyStore trustStore;
        if (cert instanceof BString pemPath) {
            requireFile(pemPath.getValue(), "cert");
            if (!validateRevocation) {
                sslBuilder.trustManager(new File(pemPath.getValue()));
                return;
            }
            trustStore = pemToKeyStore(pemPath.getValue());
        } else {
            @SuppressWarnings("unchecked")
            BMap<BString, Object> store = (BMap<BString, Object>) cert;
            trustStore = loadKeyStore(store.getStringValue(STORE_PATH).getValue(),
                    store.getStringValue(PASSWORD).getValue());
        }
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("PKIX");
        if (validateRevocation) {
            CertPathBuilder certPathBuilder = CertPathBuilder.getInstance("PKIX");
            PKIXRevocationChecker revocationChecker = (PKIXRevocationChecker) certPathBuilder.getRevocationChecker();
            PKIXBuilderParameters pkixParameters = new PKIXBuilderParameters(trustStore, new X509CertSelector());
            pkixParameters.addCertPathChecker(revocationChecker);
            trustManagerFactory.init(new CertPathTrustManagerParameters(pkixParameters));
        } else {
            trustManagerFactory.init(trustStore);
        }
        sslBuilder.trustManager(trustManagerFactory);
    }

    private static void configureKey(SslContextBuilder sslBuilder, BMap<BString, Object> secureSocket)
            throws GeneralSecurityException, IOException {
        Object key = secureSocket.get(KEY);
        if (key == null) {
            return;
        }
        @SuppressWarnings("unchecked")
        BMap<BString, Object> keyRecord = (BMap<BString, Object>) key;
        if (keyRecord.containsKey(CERT_FILE)) {
            String certFile = keyRecord.getStringValue(CERT_FILE).getValue();
            String keyFile = keyRecord.getStringValue(KEY_FILE).getValue();
            requireFile(certFile, "key.certFile");
            requireFile(keyFile, "key.keyFile");
            sslBuilder.keyManager(new File(certFile), new File(keyFile), ValueUtils.optString(keyRecord, KEY_PASSWORD));
            return;
        }
        String password = keyRecord.getStringValue(PASSWORD).getValue();
        KeyStore keyStore = loadKeyStore(keyRecord.getStringValue(STORE_PATH).getValue(), password);
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, password.toCharArray());
        sslBuilder.keyManager(keyManagerFactory);
    }

    private static KeyStore loadKeyStore(String path, String password) throws GeneralSecurityException, IOException {
        requireFile(path, "store path");
        for (String type : new String[] {"PKCS12", "JKS"}) {
            KeyStore store = KeyStore.getInstance(type);
            try (InputStream input = new FileInputStream(path)) {
                store.load(input, password.toCharArray());
                return store;
            } catch (IOException e) {
                // Wrong store format (or password); try the next type before giving up.
            }
        }
        throw BlobErrorCreator.clientError(
                "cannot load the certificate store at " + path + " as PKCS12 or JKS (check the password)", null);
    }

    private static KeyStore pemToKeyStore(String pemPath) throws GeneralSecurityException, IOException {
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
        store.load(null, null);
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        try (InputStream input = new FileInputStream(pemPath)) {
            Collection<? extends Certificate> certificates = factory.generateCertificates(input);
            if (certificates.isEmpty()) {
                throw BlobErrorCreator.clientError("no certificates found in " + pemPath, null);
            }
            int index = 0;
            for (Certificate certificate : certificates) {
                store.setCertificateEntry("cert-" + index++, certificate);
            }
        }
        return store;
    }

    private static void requireFile(String path, String fieldName) {
        if (!new File(path).isFile()) {
            throw BlobErrorCreator.clientError(fieldName + " does not point to a readable file: " + path, null);
        }
    }

    private static Duration seconds(Object decimalValue) {
        return Duration.ofMillis((long) (((BDecimal) decimalValue).floatValue() * 1000));
    }
}
