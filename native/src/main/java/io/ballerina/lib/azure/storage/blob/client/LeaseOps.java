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

package io.ballerina.lib.azure.storage.blob.client;

import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import io.ballerina.lib.azure.storage.blob.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.blob.util.BlobErrorCreator;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

/**
 * Native implementations of the blob lease operations. Lease durations are checked locally
 * against the service's rule: 15 to 60 seconds, or -1 for an infinite lease.
 */
public final class LeaseOps {

    private static final int MIN_LEASE_SECONDS = 15;
    private static final int MAX_LEASE_SECONDS = 60;
    private static final int INFINITE_LEASE = -1;

    private LeaseOps() {
    }

    /** Acquires a lease on a blob and returns the lease id. */
    public static Object acquireLease(Environment env, BObject self, BString path, long leaseDurationSeconds,
                                      Object proposedLeaseId) {
        return BallerinaAzureClient.invoke(env, () -> {
            if (leaseDurationSeconds != INFINITE_LEASE
                    && (leaseDurationSeconds < MIN_LEASE_SECONDS || leaseDurationSeconds > MAX_LEASE_SECONDS)) {
                throw BlobErrorCreator.clientError("leaseDurationSeconds must be between " + MIN_LEASE_SECONDS
                        + " and " + MAX_LEASE_SECONDS + ", or -1 for an infinite lease", null);
            }
            String proposed = proposedLeaseId == null ? null : ((BString) proposedLeaseId).getValue();
            return StringUtils.fromString(leaseClient(self, path, proposed).acquireLease((int) leaseDurationSeconds));
        });
    }

    /** Renews an active lease. */
    public static Object renewLease(Environment env, BObject self, BString path, BString leaseId) {
        return BallerinaAzureClient.invoke(env, () -> {
            leaseClient(self, path, leaseId.getValue()).renewLease();
            return null;
        });
    }

    /** Releases an active lease. */
    public static Object releaseLease(Environment env, BObject self, BString path, BString leaseId) {
        return BallerinaAzureClient.invoke(env, () -> {
            leaseClient(self, path, leaseId.getValue()).releaseLease();
            return null;
        });
    }

    /** Breaks the current lease and returns the seconds until it is fully broken. */
    public static Object breakLease(Environment env, BObject self, BString path, Object breakPeriodSeconds) {
        return BallerinaAzureClient.invoke(env, () -> {
            Integer breakPeriod = breakPeriodSeconds == null ? null : Math.toIntExact((Long) breakPeriodSeconds);
            Integer remaining = leaseClient(self, path, null)
                    .breakLeaseWithResponse(breakPeriod, null, null, null).getValue();
            return remaining == null ? 0L : remaining.longValue();
        });
    }

    /** Changes the id of an active lease and returns the new id. */
    public static Object changeLease(Environment env, BObject self, BString path, BString leaseId,
                                     BString proposedLeaseId) {
        return BallerinaAzureClient.invoke(env, () -> StringUtils.fromString(
                leaseClient(self, path, leaseId.getValue()).changeLease(proposedLeaseId.getValue())));
    }

    private static BlobLeaseClient leaseClient(BObject self, BString path, String leaseId) {
        BlobLeaseClientBuilder builder = new BlobLeaseClientBuilder().blobClient(BlobOps.blobClient(self, path));
        if (leaseId != null) {
            builder.leaseId(leaseId);
        }
        return builder.buildClient();
    }
}
