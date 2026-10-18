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

import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * The redelivery delay rules of spec 5.9 and 5.11: a requested minimum rounded up to whole
 * seconds, never negative, capped at seven days; and the expiry rule with its one-window margin.
 */
public class DelaysTest {

    private static final Duration HOLD = Duration.ofMinutes(10);
    private static final Instant NOW = Instant.parse("2026-10-18T10:00:00Z");

    @Test
    public void requestedDelaysRoundUpAndNeverGoNegative() {
        assertEquals(Delays.seconds(new BigDecimal("2.5")), 3);
        assertEquals(Delays.seconds(new BigDecimal("2.0")), 2);
        assertEquals(Delays.seconds(new BigDecimal("0.001")), 1);
        assertEquals(Delays.seconds(BigDecimal.ZERO), 0);
        assertEquals(Delays.seconds(new BigDecimal("-5")), 0);
        assertEquals(Delays.seconds(null), 0);
    }

    @Test
    public void requestedDelaysAreCappedAtSevenDays() {
        assertEquals(Delays.seconds(new BigDecimal("604800")), 604_800);
        assertEquals(Delays.seconds(new BigDecimal("604800.5")), 604_800);
        assertEquals(Delays.seconds(new BigDecimal("99999999")), 604_800);
    }

    @Test
    public void aDelayEndingAtOrWithinOneWindowOfTheExpiryCannotBeHonoured() {
        Instant expiry = NOW.plus(Duration.ofMinutes(30));
        assertFalse(Delays.wouldExpire(NOW, 0, expiry, HOLD), "immediate redelivery, 20 minutes to spare");
        assertFalse(Delays.wouldExpire(NOW, 19 * 60 + 59, expiry, HOLD), "ends one second before the window");
        assertTrue(Delays.wouldExpire(NOW, 20 * 60, expiry, HOLD), "ends exactly one window before expiry");
        assertTrue(Delays.wouldExpire(NOW, 25 * 60, expiry, HOLD));
        assertTrue(Delays.wouldExpire(NOW, 604_800, expiry, HOLD));
    }

    @Test
    public void theRuleIsInactiveWithoutAnExpiry() {
        assertFalse(Delays.wouldExpire(NOW, 604_800, null, HOLD));
    }
}
