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

package io.ballerina.lib.azure.storage.blob.server;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;

/**
 * The redelivery delay arithmetic: a requested delay is a minimum, rounded up to whole seconds,
 * never negative, and capped at the queue service's seven-day limit; a delay that would end at
 * or within one hold window of the message's expiry cannot be honoured.
 */
final class Delays {

    /** The queue service's longest visibility timeout, seven days. */
    static final long MAX_SECONDS = 604_800;

    private Delays() {
    }

    static long seconds(BigDecimal requested) {
        if (requested == null || requested.signum() <= 0) {
            return 0;
        }
        long seconds = requested.setScale(0, RoundingMode.CEILING).longValueExact();
        return Math.min(seconds, MAX_SECONDS);
    }

    static boolean wouldExpire(Instant now, long delaySeconds, Instant expiry, Duration hold) {
        if (expiry == null) {
            return false;
        }
        Instant redelivery = now.plusSeconds(delaySeconds).plus(hold);
        return !redelivery.isBefore(expiry);
    }
}
