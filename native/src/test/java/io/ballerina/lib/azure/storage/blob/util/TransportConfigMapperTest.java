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

import com.azure.storage.common.policy.RequestRetryOptions;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.Duration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * The retry options built from a {@code RetryConfig} record (spec 2.3): the record's values,
 * the connector's defaults included, reach the SDK, and the retry-free form keeps the try
 * timeout with a single try.
 */
public class TransportConfigMapperTest {

    @Test
    public void theRecordValuesReachTheSdk() {
        RequestRetryOptions options = TransportConfigMapper.retryOptions(retryConfig("exponential", 4, 60, 4, 120));
        assertEquals(options.getMaxTries(), 4);
        assertEquals(options.getTryTimeoutDuration(), Duration.ofSeconds(60));
        assertEquals(options.getRetryDelay(), Duration.ofSeconds(4));
        assertEquals(options.getMaxRetryDelay(), Duration.ofSeconds(120));
        assertNull(options.getSecondaryHost());
    }

    @Test
    public void aSecondaryHostIsMappedForTheBlobClientsOnly() {
        BMap<BString, Object> retry = retryConfig("fixed", 2, 15, 1, 30);
        retry.put(StringUtils.fromString("secondaryHostUrl"),
                StringUtils.fromString("https://acct-secondary.blob.core.windows.net"));
        RequestRetryOptions options = TransportConfigMapper.retryOptions(retry);
        assertEquals(options.getMaxTries(), 2);
        assertEquals(options.getTryTimeoutDuration(), Duration.ofSeconds(15));
        assertEquals(options.getSecondaryHost(), "https://acct-secondary.blob.core.windows.net");
        assertNull(TransportConfigMapper.retryOptions(retry, false).getSecondaryHost(),
                "the queue clients never retry against a secondary");
    }

    @Test
    public void theRetryFreeFormKeepsTheTryTimeout() {
        RequestRetryOptions single = TransportConfigMapper.singleTry(retryConfig("exponential", 4, 60, 4, 120));
        assertEquals(single.getMaxTries(), 1);
        assertEquals(single.getTryTimeoutDuration(), Duration.ofSeconds(60));
    }

    private static BMap<BString, Object> retryConfig(String policy, long maxTries, long tryTimeout, long retryDelay,
                                                     long maxRetryDelay) {
        BMap<BString, Object> retry = ValueCreator.createMapValue();
        retry.put(StringUtils.fromString("retryPolicyType"), StringUtils.fromString(policy));
        retry.put(StringUtils.fromString("maxTries"), maxTries);
        retry.put(StringUtils.fromString("tryTimeout"), seconds(tryTimeout));
        retry.put(StringUtils.fromString("retryDelay"), seconds(retryDelay));
        retry.put(StringUtils.fromString("maxRetryDelay"), seconds(maxRetryDelay));
        return retry;
    }

    private static BDecimal seconds(long value) {
        return ValueCreator.createDecimalValue(BigDecimal.valueOf(value));
    }
}
