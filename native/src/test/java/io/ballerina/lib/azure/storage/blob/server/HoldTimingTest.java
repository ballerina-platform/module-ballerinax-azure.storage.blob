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
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * The time constants of spec 5.4 derived from the ten-minute hold, the renewal call timeout,
 * the test seam's bounds, and the connection pool warning of spec 5.2.
 */
public class HoldTimingTest {

    @Test
    public void theConstantsDeriveFromTheHold() {
        HoldTiming timing = new HoldTiming(HoldTiming.DEFAULT_HOLD);
        assertEquals(timing.hold(), Duration.ofMinutes(10));
        assertEquals(timing.renewal(), Duration.ofMinutes(5));
        assertEquals(timing.floor(), Duration.ofMinutes(1));
        assertEquals(timing.margin(), Duration.ofSeconds(60));
        assertEquals(timing.window(), Duration.ofMinutes(10));
        assertEquals(timing.gracefulBound(), Duration.ofMinutes(10));
        assertEquals(timing.pausedQueueWarning(), Duration.ofMinutes(10));
        assertFalse(timing.scaled());
    }

    @Test
    public void theRenewalCallTimesOutAtTheSmallerOf30SecondsAndTheHoldRemaining() {
        HoldTiming timing = new HoldTiming(HoldTiming.DEFAULT_HOLD);
        assertEquals(timing.callTimeout(Duration.ofMinutes(5)), Duration.ofSeconds(30));
        assertEquals(timing.callTimeout(Duration.ofSeconds(12)), Duration.ofSeconds(12));
        assertEquals(timing.callTimeout(Duration.ZERO), Duration.ofSeconds(1), "never a zero timeout");
    }

    @Test
    public void theSeamAcceptsOnlyItsRange() {
        String previous = System.getProperty(HoldTiming.PROPERTY);
        try {
            System.clearProperty(HoldTiming.PROPERTY);
            assertEquals(HoldTiming.fromSystemProperty().hold(), HoldTiming.DEFAULT_HOLD);
            System.setProperty(HoldTiming.PROPERTY, "20");
            HoldTiming scaled = HoldTiming.fromSystemProperty();
            assertEquals(scaled.hold(), Duration.ofSeconds(20));
            assertEquals(scaled.renewal(), Duration.ofSeconds(10));
            assertEquals(scaled.margin(), Duration.ofSeconds(2));
            assertTrue(scaled.scaled());
            for (String rejected : new String[]{"9", "601", "abc", "-20", ""}) {
                System.setProperty(HoldTiming.PROPERTY, rejected);
                assertEquals(HoldTiming.fromSystemProperty().hold(), HoldTiming.DEFAULT_HOLD, rejected);
            }
        } finally {
            if (previous == null) {
                System.clearProperty(HoldTiming.PROPERTY);
            } else {
                System.setProperty(HoldTiming.PROPERTY, previous);
            }
        }
    }

    @Test
    public void thePoolWarningFiresAboveMaxConnectionsAndPrintsBothNumbers() {
        ServiceSettings defaults = ServiceSettings.of("events", null, 16, null, BigDecimal.valueOf(60),
                BigDecimal.ZERO, 5);
        assertNull(Listener.poolWarning(List.of(defaults, defaults), 50), "two default services fit the pool");
        String warning = Listener.poolWarning(List.of(defaults, defaults, defaults), 50);
        assertTrue(warning.contains("up to 72 blob fetches") && warning.contains("maxConnections (50)"), warning);
        assertTrue(warning.contains("upper bound") && warning.contains("lower batchSize"), warning);
        assertNull(Listener.poolWarning(List.of(defaults, defaults, defaults), 72), "the bound itself is fine");
    }
}
