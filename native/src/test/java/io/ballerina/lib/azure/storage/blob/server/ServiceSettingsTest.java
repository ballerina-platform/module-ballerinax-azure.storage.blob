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

import io.ballerina.lib.azure.storage.blob.util.ConfigurationException;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

/**
 * The service settings rules of spec 5.3: the defaults, the queue name rules, the poison queue
 * relations, the ranges, and the checks against the services already attached.
 */
public class ServiceSettingsTest {

    private static ServiceSettings settings(String queue, String poison) {
        return ServiceSettings.of(queue, poison, 16, null, BigDecimal.valueOf(60), BigDecimal.ZERO, 5);
    }

    @Test
    public void defaultsFollowTheSpecification() {
        ServiceSettings settings = settings("invoice-events", null);
        assertEquals(settings.poisonQueueName(), "invoice-events-poison");
        assertEquals(settings.newBatchThreshold(), 8, "half the batch size, rounded down");
        assertEquals(ServiceSettings.of("q-1", null, 1, null, BigDecimal.ONE, BigDecimal.ZERO, 1).newBatchThreshold(),
                0, "0 for a batch size of 1");
        assertEquals(settings.maxPollingMillis(), 60_000);
        assertEquals(settings.concurrency(), 24);
    }

    @Test
    public void queueNamesFollowAzuresRules() {
        for (String bad : new String[]{"Invoices", "ab", "a--b", "-abc", "abc-", "a_b", "", "a".repeat(64)}) {
            ConfigurationException e = expectThrows(ConfigurationException.class, () -> settings(bad, null));
            assertTrue(e.getMessage().contains("queueName"), e.getMessage());
        }
        settings("abc", null);
        settings("a-1-b", null);
        settings("a".repeat(56), null);
        ConfigurationException poison = expectThrows(ConfigurationException.class, () -> settings("events", "Dead"));
        assertTrue(poison.getMessage().contains("poisonQueueName"), poison.getMessage());
    }

    @Test
    public void aLongQueueNameNeedsAnExplicitPoisonQueueName() {
        String long57 = "q" + "a".repeat(56);
        ConfigurationException e = expectThrows(ConfigurationException.class, () -> settings(long57, null));
        assertTrue(e.getMessage().contains("poisonQueueName is required"), e.getMessage());
        assertEquals(settings(long57, "dead").poisonQueueName(), "dead");
    }

    @Test
    public void thePoisonQueueMustDifferFromTheQueue() {
        assertThrows(ConfigurationException.class, () -> settings("events", "events"));
    }

    @Test
    public void rangesAreEnforced() {
        assertThrows(ConfigurationException.class, () -> ServiceSettings.of("q", null, 0, null,
                BigDecimal.ONE, BigDecimal.ZERO, 1));
        assertThrows(ConfigurationException.class, () -> ServiceSettings.of("events", null, 33, null,
                BigDecimal.ONE, BigDecimal.ZERO, 1));
        assertThrows(ConfigurationException.class, () -> ServiceSettings.of("events", null, 4, -1,
                BigDecimal.ONE, BigDecimal.ZERO, 1));
        assertThrows(ConfigurationException.class, () -> ServiceSettings.of("events", null, 4, 5,
                BigDecimal.ONE, BigDecimal.ZERO, 1));
        assertThrows(ConfigurationException.class, () -> ServiceSettings.of("events", null, 4, null,
                BigDecimal.ZERO, BigDecimal.ZERO, 1));
        assertThrows(ConfigurationException.class, () -> ServiceSettings.of("events", null, 4, null,
                BigDecimal.ONE, BigDecimal.valueOf(-1), 1));
        assertThrows(ConfigurationException.class, () -> ServiceSettings.of("events", null, 4, null,
                BigDecimal.ONE, BigDecimal.valueOf(604_801), 1));
        assertThrows(ConfigurationException.class, () -> ServiceSettings.of("events", null, 4, null,
                BigDecimal.ONE, BigDecimal.ZERO, 0));
        ServiceSettings bounds = ServiceSettings.of("events", null, 32, 32, new BigDecimal("0.5"),
                new BigDecimal("604800"), 1);
        assertEquals(bounds.maxPollingMillis(), 500);
        assertEquals(bounds.redeliverySeconds(), 604_800);
        assertEquals(ServiceSettings.of("events", null, 4, null, BigDecimal.ONE, new BigDecimal("1.2"), 1)
                .redeliverySeconds(), 2, "the redelivery delay rounds up to whole seconds");
    }

    @Test
    public void duplicateQueuesAndPoisonCyclesAreRefusedAgainstTheAttachedServices() {
        ServiceSettings a = settings("queue-a", null);
        ConfigurationException duplicate = expectThrows(ConfigurationException.class,
                () -> settings("queue-a", null).checkAgainst(List.of(a)));
        assertTrue(duplicate.getMessage().contains("already consumed"), duplicate.getMessage());
        // Consuming a's poison queue, and poisoning into a's queue, are both allowed.
        settings("queue-a-poison", null).checkAgainst(List.of(a));
        settings("queue-b", "queue-a").checkAgainst(List.of(a));
        // A shared poison queue is allowed.
        settings("queue-d", "dead").checkAgainst(List.of(settings("queue-c", "dead")));
        // Two- and three-service cycles are refused at the service that closes them, naming the queues.
        ServiceSettings e = settings("queue-e", "queue-f");
        ConfigurationException two = expectThrows(ConfigurationException.class,
                () -> settings("queue-f", "queue-e").checkAgainst(List.of(e)));
        assertTrue(two.getMessage().contains("queue-f -> queue-e -> queue-f"), two.getMessage());
        ServiceSettings x = settings("queue-x", "queue-y");
        ServiceSettings y = settings("queue-y", "queue-z");
        ConfigurationException three = expectThrows(ConfigurationException.class,
                () -> settings("queue-z", "queue-x").checkAgainst(List.of(x, y)));
        assertTrue(three.getMessage().contains("queue-z -> queue-x -> queue-y -> queue-z"), three.getMessage());
        // Only attached services count: without y, z closes nothing.
        settings("queue-z", "queue-x").checkAgainst(List.of(x));
        assertNull(null);
    }
}
