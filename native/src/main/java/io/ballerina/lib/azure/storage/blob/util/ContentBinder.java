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

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.IntersectionType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.TypeTags;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.utils.XmlUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.nio.charset.StandardCharsets;

/**
 * Binds JSON and XML content to a target type through the {@code ballerina/data.jsondata} and
 * {@code ballerina/data.xmldata} modules, shared by the client typed reads (strict) and, later, the
 * listener content handlers (lax-aware). A binding failure is thrown as the module's generic
 * {@code Error}, prefixed with the caller's context message. CSV binding runs on a Ballerina
 * strand instead (see {@code TypedReadOps#parseCsv} and {@code TypedReadOps#streamTarget}), because the data.csv
 * parser needs the runtime environment.
 */
public final class ContentBinder {

    private ContentBinder() {
    }

    /**
     * Binds JSON bytes to the target type.
     *
     * @param content        the content bytes
     * @param targetType     the target type
     * @param laxDataBinding whether relaxed data projection applies
     * @param bindContext    the failure-message prefix naming what the content failed to bind to
     * @return the bound value
     */
    public static Object bindJson(BArray content, Type targetType, boolean laxDataBinding, String bindContext) {
        BMap<BString, Object> options = DataBindingOptions.jsonParseOptions(laxDataBinding);
        Object result;
        try {
            result = io.ballerina.lib.data.jsondata.json.Native.parseBytes(content, options,
                    ValueCreator.createTypedescValue(targetType));
        } catch (BError e) {
            throw failure(bindContext, e);
        }
        if (result instanceof BError bError) {
            throw failure(bindContext, bError);
        }
        return result;
    }

    /**
     * Binds XML bytes to the target type. A bare {@code xml} target keeps the plain document
     * parse; a record target binds through data.xmldata.
     *
     * @param content        the content bytes
     * @param targetType     the target type
     * @param laxDataBinding whether relaxed data projection applies
     * @param bindContext    the failure-message prefix for a record-binding failure
     * @param parseContext   the failure-message prefix for a malformed bare-XML document
     * @return the bound value
     */
    public static Object bindXml(BArray content, Type targetType, boolean laxDataBinding,
                                 String bindContext, String parseContext) {
        if (TypeTags.isXMLTypeTag(TypeUtils.getImpliedType(targetType).getTag())) {
            Object document;
            try {
                document = XmlUtils.parse(new String(content.getBytes(), StandardCharsets.UTF_8));
            } catch (BError e) {
                throw failure(parseContext, e);
            }
            // An XML subtype target (xml:Element, xml<xml:Element>, ...) takes only a document
            // of that shape; the native return is not checked by the runtime, so it is here.
            try {
                return io.ballerina.runtime.api.utils.ValueUtils.convert(document, targetType);
            } catch (BError e) {
                throw BlobErrorCreator.clientError(bindContext + ": the document is not a value of the declared "
                        + "XML type " + targetType, e);
            }
        }
        Object result;
        try {
            // The xmldata parser takes the mutable record type itself, neither a reference nor
            // a readonly intersection (whose effective type it could not fill); the value is
            // made readonly afterwards when the target asks for it.
            Type referred = TypeUtils.getReferredType(targetType);
            Type recordType = mutableMember(referred);
            result = io.ballerina.lib.data.xmldata.xml.Native.parseBytes(content,
                    DataBindingOptions.xmlSourceOptions(laxDataBinding),
                    ValueCreator.createTypedescValue(recordType));
            if (!(result instanceof BError) && recordType != referred) {
                result = io.ballerina.runtime.api.utils.ValueUtils.convert(result, targetType);
            }
        } catch (BError e) {
            throw failure(bindContext, e);
        }
        if (result instanceof BError bError) {
            throw failure(bindContext, bError);
        }
        return result;
    }

    // The record member of a readonly intersection, or the type itself.
    private static Type mutableMember(Type type) {
        if (type instanceof IntersectionType intersection) {
            for (Type member : intersection.getConstituentTypes()) {
                Type candidate = TypeUtils.getReferredType(member);
                if (candidate.getTag() != TypeTags.READONLY_TAG) {
                    return candidate;
                }
            }
        }
        return type;
    }

    private static BError failure(String context, BError cause) {
        return BlobErrorCreator.clientError(context + ": " + cause.getErrorMessage(), cause);
    }
}
