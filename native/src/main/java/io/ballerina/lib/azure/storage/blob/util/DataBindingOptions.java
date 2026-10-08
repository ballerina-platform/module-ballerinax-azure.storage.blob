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

import io.ballerina.runtime.api.Module;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

/**
 * Builds the option records of the data.jsondata, data.xmldata, and data.csv modules with the
 * connector's projection settings. Shared by the listener's content binding (which honors
 * {@code laxDataBinding}) and the client's typed reads (which bind strictly).
 */
public final class DataBindingOptions {

    private static final BString ALLOW_DATA_PROJECTION = StringUtils.fromString("allowDataProjection");
    private static final BString NIL_AS_OPTIONAL_FIELD = StringUtils.fromString("nilAsOptionalField");
    private static final BString ABSENT_AS_NILABLE_TYPE = StringUtils.fromString("absentAsNilableType");
    private static final String JSON_OPTIONS_RECORD = "Options";
    private static final String XML_SOURCE_OPTIONS_RECORD = "SourceOptions";
    private static final String CSV_PARSE_OPTIONS_RECORD = "ParseOptions";
    private static final Module XMLDATA_MODULE = new Module("ballerina", "data.xmldata", "1");

    private DataBindingOptions() {
    }

    /** Builds the data.jsondata parse options. */
    public static BMap<BString, Object> jsonParseOptions(boolean laxDataBinding) {
        BMap<BString, Object> options = ValueCreator.createRecordValue(
                io.ballerina.lib.data.ModuleUtils.getModule(), JSON_OPTIONS_RECORD);
        applyProjection(options, laxDataBinding);
        return options;
    }

    /** Builds the data.xmldata source options. */
    public static BMap<BString, Object> xmlSourceOptions(boolean laxDataBinding) {
        BMap<BString, Object> options = ValueCreator.createRecordValue(XMLDATA_MODULE, XML_SOURCE_OPTIONS_RECORD);
        options.put(ALLOW_DATA_PROJECTION, laxDataBinding);
        return options;
    }

    /**
     * Builds the data.csv parse options. Record targets keep the module's default of mapping
     * record fields through the file's header row.
     */
    public static BMap<BString, Object> csvParseOptions(boolean laxDataBinding) {
        BMap<BString, Object> options = ValueCreator.createRecordValue(
                io.ballerina.lib.data.csvdata.utils.ModuleUtils.getModule(), CSV_PARSE_OPTIONS_RECORD);
        applyProjection(options, laxDataBinding);
        return options;
    }

    private static void applyProjection(BMap<BString, Object> options, boolean laxDataBinding) {
        if (laxDataBinding) {
            @SuppressWarnings("unchecked")
            BMap<BString, Object> projection = (BMap<BString, Object>) options.getMapValue(ALLOW_DATA_PROJECTION);
            // getMapValue returns the live map, so mutating it is enough.
            projection.put(NIL_AS_OPTIONAL_FIELD, Boolean.TRUE);
            projection.put(ABSENT_AS_NILABLE_TYPE, Boolean.TRUE);
        } else {
            options.put(ALLOW_DATA_PROJECTION, Boolean.FALSE);
        }
    }
}
