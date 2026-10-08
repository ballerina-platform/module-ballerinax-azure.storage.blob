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

import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Conversions between Java values and Ballerina values: {@code time:Utc} tuples,
 * {@code map<string>} metadata, and string maps in both directions.
 */
public final class ValueUtils {

    private ValueUtils() {
    }

    private static final TupleType UTC_TUPLE_TYPE =
            TypeCreator.createTupleType(List.of(PredefinedTypes.TYPE_INT, PredefinedTypes.TYPE_DECIMAL));

    /**
     * Converts an SDK timestamp to a Ballerina {@code time:Utc} tuple.
     *
     * @param time the timestamp
     * @return the readonly {@code [int, decimal]} tuple
     */
    public static BArray toUtc(OffsetDateTime time) {
        Instant instant = time.toInstant();
        BArray tuple = ValueCreator.createTupleValue(UTC_TUPLE_TYPE);
        tuple.add(0, instant.getEpochSecond());
        tuple.add(1, ValueCreator.createDecimalValue(BigDecimal.valueOf(instant.getNano())
                .divide(BigDecimal.valueOf(1_000_000_000), MathContext.DECIMAL128)));
        tuple.freezeDirect();
        return tuple;
    }

    /**
     * Converts a Ballerina {@code time:Utc} tuple to an SDK timestamp.
     *
     * @param utc the {@code [int, decimal]} tuple
     * @return the timestamp at UTC
     */
    public static OffsetDateTime fromUtc(BArray utc) {
        // A time:Utc value is always the [int, decimal] tuple.
        long seconds = utc.getInt(0);
        long nanos = ((BDecimal) utc.get(1)).decimalValue().multiply(BigDecimal.valueOf(1_000_000_000)).longValue();
        return Instant.ofEpochSecond(seconds, nanos).atOffset(ZoneOffset.UTC);
    }

    /**
     * Converts a Ballerina {@code map<string>} to a Java string map.
     *
     * @param map the Ballerina map, possibly {@code null}
     * @return the Java map, or {@code null} when the input is {@code null}
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> toStringMap(Object map) {
        if (map == null) {
            return null;
        }
        BMap<BString, BString> bMap = (BMap<BString, BString>) map;
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<BString, BString> entry : bMap.entrySet()) {
            result.put(entry.getKey().getValue(), entry.getValue().getValue());
        }
        return result;
    }

    /**
     * Converts a Java string map to a Ballerina {@code map<string>}.
     *
     * @param map the Java map
     * @return the Ballerina map
     */
    public static BMap<BString, Object> toBStringMap(Map<String, String> map) {
        BMap<BString, Object> result = ValueCreator.createMapValue(
                TypeCreator.createMapType(PredefinedTypes.TYPE_STRING));
        for (Map.Entry<String, String> entry : map.entrySet()) {
            result.put(StringUtils.fromString(entry.getKey()), StringUtils.fromString(entry.getValue()));
        }
        return result;
    }

    /** Reads an optional string field off a Ballerina record; {@code null} when absent. */
    public static String optString(BMap<BString, Object> record, BString field) {
        Object value = record.get(field);
        return value == null ? null : ((BString) value).getValue();
    }

    /** Reads an optional {@code map<string>} field off a Ballerina record; {@code null} when absent. */
    public static Map<String, String> optStringMap(BMap<BString, Object> record, BString field) {
        return toStringMap(record.get(field));
    }
}
