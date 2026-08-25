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

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Module;

/**
 * Holds the Ballerina module reference, captured during module initialization, so the native
 * layer can create module-scoped typed errors.
 */
public final class ModuleUtils {

    private static Module module;

    private ModuleUtils() {
    }

    /**
     * Captures the current module reference during Ballerina module initialization.
     *
     * @param env the Ballerina runtime environment
     */
    public static void setModule(Environment env) {
        module = env.getCurrentModule();
    }

    /**
     * Returns the captured module reference.
     *
     * @return the module reference
     */
    public static Module getModule() {
        return module;
    }
}
