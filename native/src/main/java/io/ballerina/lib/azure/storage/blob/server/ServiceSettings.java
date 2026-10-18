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
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

// A service's consumption settings, read from its @blob:ServiceConfig annotation and validated
// against the rules the specification fixes (names, ranges, and the relations between the
// attached services' queues). The compiler plugin applies the same rules to literal values.
record ServiceSettings(String queueName, String poisonQueueName, int batchSize, int newBatchThreshold,
                       long maxPollingMillis, long redeliverySeconds, int maxDeliveryCount) {

    static final String POISON_SUFFIX = "-poison";
    static final int MAX_QUEUE_NAME_WITH_DEFAULT_POISON = 56;
    static final int MAX_BATCH_SIZE = 32;
    static final Pattern QUEUE_NAME = Pattern.compile("^(?=.{3,63}$)[a-z0-9]+(?:-[a-z0-9]+)*$");
    static final String QUEUE_NAME_RULE = "3 to 63 lowercase letters, digits and single hyphens, beginning and "
            + "ending with a letter or digit";

    private static final BString QUEUE_NAME_FIELD = StringUtils.fromString("queueName");
    private static final BString POISON_QUEUE_NAME_FIELD = StringUtils.fromString("poisonQueueName");
    private static final BString BATCH_SIZE_FIELD = StringUtils.fromString("batchSize");
    private static final BString NEW_BATCH_THRESHOLD_FIELD = StringUtils.fromString("newBatchThreshold");
    private static final BString MAX_POLLING_INTERVAL_SECONDS_FIELD = StringUtils.fromString(
            "maxPollingIntervalSeconds");
    private static final BString REDELIVERY_DELAY_SECONDS_FIELD = StringUtils.fromString("redeliveryDelaySeconds");
    private static final BString MAX_DELIVERY_COUNT_FIELD = StringUtils.fromString("maxDeliveryCount");

    /**
     * Reads and validates the annotation value.
     *
     * @param annotation the {@code ServiceConfiguration} record, or {@code null} when absent
     * @return the settings
     * @throws ConfigurationException when the annotation is missing or a value breaks a rule
     */
    static ServiceSettings fromAnnotation(BMap<BString, Object> annotation) {
        if (annotation == null) {
            throw new ConfigurationException("a service needs the '@blob:ServiceConfig' annotation naming its queue");
        }
        Object threshold = annotation.get(NEW_BATCH_THRESHOLD_FIELD);
        Object poison = annotation.get(POISON_QUEUE_NAME_FIELD);
        return of(annotation.getStringValue(QUEUE_NAME_FIELD).getValue(),
                poison == null ? null : ((BString) poison).getValue(),
                Math.toIntExact(annotation.getIntValue(BATCH_SIZE_FIELD)),
                threshold == null ? null : Math.toIntExact((Long) threshold),
                ((BDecimal) annotation.get(MAX_POLLING_INTERVAL_SECONDS_FIELD)).decimalValue(),
                ((BDecimal) annotation.get(REDELIVERY_DELAY_SECONDS_FIELD)).decimalValue(),
                Math.toIntExact(annotation.getIntValue(MAX_DELIVERY_COUNT_FIELD)));
    }

    /**
     * Validates the given values and applies the defaults.
     *
     * @param queueName                 the queue the service consumes
     * @param poisonQueueName           the poison queue, or {@code null} for the default
     * @param batchSize                 messages per receive
     * @param newBatchThreshold         the refill trigger, or {@code null} for the default
     * @param maxPollingIntervalSeconds the empty-queue backoff ceiling
     * @param redeliveryDelaySeconds    the invisibility after a handler failure
     * @param maxDeliveryCount          deliveries before the poison queue
     * @return the settings
     * @throws ConfigurationException when a value breaks a rule
     */
    static ServiceSettings of(String queueName, String poisonQueueName, int batchSize, Integer newBatchThreshold,
                              BigDecimal maxPollingIntervalSeconds, BigDecimal redeliveryDelaySeconds,
                              int maxDeliveryCount) {
        checkQueueName("queueName", queueName);
        String poison = poisonQueueName;
        if (poison == null) {
            if (queueName.length() > MAX_QUEUE_NAME_WITH_DEFAULT_POISON) {
                throw new ConfigurationException("poisonQueueName is required: the default '" + queueName
                        + POISON_SUFFIX + "' exceeds 63 characters");
            }
            poison = queueName + POISON_SUFFIX;
        } else {
            checkQueueName("poisonQueueName", poison);
            if (poison.equals(queueName)) {
                throw new ConfigurationException("poisonQueueName must differ from queueName: '" + queueName + "'");
            }
        }
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new ConfigurationException("batchSize must be between 1 and 32, found " + batchSize);
        }
        int threshold = newBatchThreshold == null ? batchSize / 2 : newBatchThreshold;
        if (threshold < 0 || threshold > batchSize) {
            throw new ConfigurationException("newBatchThreshold must be between 0 and batchSize (" + batchSize
                    + "), found " + threshold);
        }
        if (maxPollingIntervalSeconds == null || maxPollingIntervalSeconds.signum() <= 0) {
            throw new ConfigurationException("maxPollingIntervalSeconds must be greater than zero, found "
                    + maxPollingIntervalSeconds);
        }
        if (redeliveryDelaySeconds == null || redeliveryDelaySeconds.signum() < 0
                || redeliveryDelaySeconds.compareTo(BigDecimal.valueOf(Delays.MAX_SECONDS)) > 0) {
            throw new ConfigurationException("redeliveryDelaySeconds must be between 0 and 604800, found "
                    + redeliveryDelaySeconds);
        }
        if (maxDeliveryCount < 1) {
            throw new ConfigurationException("maxDeliveryCount must be at least 1, found " + maxDeliveryCount);
        }
        long maxPollingMillis = Math.max(1, maxPollingIntervalSeconds.multiply(BigDecimal.valueOf(1000))
                .setScale(0, RoundingMode.HALF_UP).longValue());
        long redelivery = redeliveryDelaySeconds.setScale(0, RoundingMode.CEILING).longValueExact();
        return new ServiceSettings(queueName, poison, batchSize, threshold, maxPollingMillis, redelivery,
                maxDeliveryCount);
    }

    private static void checkQueueName(String field, String name) {
        if (name == null || !QUEUE_NAME.matcher(name).matches()) {
            throw new ConfigurationException("'" + name + "' is not a valid " + field + ": " + QUEUE_NAME_RULE);
        }
    }

    /**
     * Checks this service against the services already attached to its listener: no two may
     * consume one queue, and the poison queues may not form a cycle.
     *
     * @param attached the settings of the attached services
     * @throws ConfigurationException when the service duplicates a queue or closes a cycle
     */
    void checkAgainst(Collection<ServiceSettings> attached) {
        Map<String, String> poisonOf = new HashMap<>();
        for (ServiceSettings other : attached) {
            if (other.queueName.equals(queueName)) {
                throw new ConfigurationException("queue '" + queueName + "' is already consumed by another service "
                        + "of this listener");
            }
            poisonOf.put(other.queueName, other.poisonQueueName);
        }
        poisonOf.put(queueName, poisonQueueName);
        List<String> chain = new ArrayList<>();
        chain.add(queueName);
        String next = poisonQueueName;
        while (next != null && chain.size() <= poisonOf.size()) {
            chain.add(next);
            if (next.equals(queueName)) {
                throw new ConfigurationException("the poison queues of this listener's services would form a cycle: "
                        + String.join(" -> ", chain));
            }
            next = poisonOf.get(next);
        }
    }

    /** The most messages the service has in flight at once. */
    int concurrency() {
        return batchSize + newBatchThreshold;
    }
}
