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

import io.ballerina.lib.azure.storage.blob.util.ModuleUtils;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The listener's diagnostics, at the three levels it uses, each with key-value fields. The
 * runtime-backed implementation goes through the module's {@code logListener} function:
 * slf4j has no binding in any Ballerina distribution, so logging from Java directly would be
 * discarded silently.
 */
interface ListenerLog {

    String PREFIX = "azure.storage.blob listener: ";

    void warn(String message, Map<String, Object> fields);

    void error(String message, Map<String, Object> fields);

    void debug(String message, Map<String, Object> fields);

    default void warn(String message) {
        warn(message, Map.of());
    }

    default void error(String message) {
        error(message, Map.of());
    }

    default void debug(String message) {
        debug(message, Map.of());
    }

    /** Builds an ordered field map from alternating keys and values. */
    static Map<String, Object> fields(Object... keyValues) {
        Map<String, Object> fields = new LinkedHashMap<>();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            if (keyValues[i + 1] != null) {
                fields.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
            }
        }
        return fields;
    }

    /** The log of a running listener, through the Ballerina {@code log} module. */
    final class RuntimeLog implements ListenerLog {

        private static final String LOG_FUNCTION = "logListener";

        private final Runtime runtime;

        RuntimeLog(Runtime runtime) {
            this.runtime = runtime;
        }

        @Override
        public void warn(String message, Map<String, Object> fields) {
            emit("warn", message, fields);
        }

        @Override
        public void error(String message, Map<String, Object> fields) {
            emit("error", message, fields);
        }

        @Override
        public void debug(String message, Map<String, Object> fields) {
            emit("debug", message, fields);
        }

        private void emit(String level, String message, Map<String, Object> fields) {
            try {
                BMap<BString, Object> values = ValueCreator.createMapValue(
                        TypeCreator.createMapType(PredefinedTypes.TYPE_ANYDATA));
                for (Map.Entry<String, Object> field : fields.entrySet()) {
                    values.put(StringUtils.fromString(field.getKey()), toAnydata(field.getValue()));
                }
                runtime.callFunction(ModuleUtils.getModule(), LOG_FUNCTION, new StrandMetadata(true, null),
                        StringUtils.fromString(level), StringUtils.fromString(PREFIX + message), values);
            } catch (RuntimeException ignored) {
                // Reporting a diagnostic must never take down the path that raised it.
            }
        }

        private static Object toAnydata(Object value) {
            if (value instanceof Long || value instanceof Integer) {
                return ((Number) value).longValue();
            }
            if (value instanceof Boolean) {
                return value;
            }
            return StringUtils.fromString(String.valueOf(value));
        }
    }
}
