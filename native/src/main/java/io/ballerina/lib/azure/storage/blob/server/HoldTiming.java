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

import java.time.Duration;

/**
 * The time constants of message handling, all derived from the ten-minute hold a received
 * message is hidden for: the renewal period is half of it, the retry floor and the expiry margin
 * a tenth, and the full-window warning, the graceful stop bound and the paused-queue warning
 * one hold each. A JVM system property shortens the hold for the module's own test suite, which
 * cannot otherwise exercise these behaviours within a test's lifetime; it is not part of the
 * module's contract.
 */
final class HoldTiming {

    static final String PROPERTY = "io.ballerina.lib.azure.storage.blob.listener.holdSeconds";
    static final Duration DEFAULT_HOLD = Duration.ofMinutes(10);
    /** The queue service's longest server timeout, the cap of every renewal call. */
    static final Duration CALL_TIMEOUT_CAP = Duration.ofSeconds(30);
    private static final Duration CALL_TIMEOUT_FLOOR = Duration.ofSeconds(1);
    private static final long MIN_PROPERTY_SECONDS = 10;
    private static final long MAX_PROPERTY_SECONDS = 600;

    private final Duration hold;

    HoldTiming(Duration hold) {
        this.hold = hold;
    }

    /** The default timing, or the shortened hold the system property names when it is set. */
    static HoldTiming fromSystemProperty() {
        String value = System.getProperty(PROPERTY);
        if (value == null || value.isBlank()) {
            return new HoldTiming(DEFAULT_HOLD);
        }
        try {
            long seconds = Long.parseLong(value.strip());
            if (seconds >= MIN_PROPERTY_SECONDS && seconds <= MAX_PROPERTY_SECONDS) {
                return new HoldTiming(Duration.ofSeconds(seconds));
            }
        } catch (NumberFormatException ignored) {
            // An unusable value keeps the default.
        }
        return new HoldTiming(DEFAULT_HOLD);
    }

    boolean scaled() {
        return !DEFAULT_HOLD.equals(hold);
    }

    Duration hold() {
        return hold;
    }

    Duration renewal() {
        return hold.dividedBy(2);
    }

    Duration floor() {
        return hold.dividedBy(10);
    }

    Duration margin() {
        return hold.dividedBy(10);
    }

    Duration window() {
        return hold;
    }

    Duration gracefulBound() {
        return hold;
    }

    Duration pausedQueueWarning() {
        return hold;
    }

    /** A duration in minutes, or in seconds under a minute. */
    static String describe(Duration duration) {
        return duration.toSeconds() < 60 ? duration.toSeconds() + " seconds" : duration.toMinutes() + " minutes";
    }

    /** A renewal call's timeout: the smaller of the service's cap and the hold time remaining. */
    Duration callTimeout(Duration holdRemaining) {
        Duration remaining = holdRemaining.compareTo(CALL_TIMEOUT_FLOOR) < 0 ? CALL_TIMEOUT_FLOOR : holdRemaining;
        return remaining.compareTo(CALL_TIMEOUT_CAP) < 0 ? remaining : CALL_TIMEOUT_CAP;
    }
}
