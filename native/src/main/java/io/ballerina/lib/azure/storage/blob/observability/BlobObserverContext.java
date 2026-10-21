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

package io.ballerina.lib.azure.storage.blob.observability;

import io.ballerina.runtime.observability.ObserverContext;

import static java.util.Objects.requireNonNullElse;

/**
 * An observer context carrying the connector's identity tags: {@code module}, {@code type},
 * {@code remote.url} and {@code protocol}. The tag keys are the file-module observability
 * vocabulary shared with the sibling connectors.
 */
public class BlobObserverContext extends ObserverContext {

    static final String TAG_CONTEXT = "type";
    static final String TAG_MODULE = "module";
    static final String TAG_REMOTE_URL = "remote.url";
    static final String TAG_PROTOCOL = "protocol";
    static final String TAG_INSTANCE_URL = "host";
    static final String TAG_WATCHED_PATH = "watched.path";

    static final String TAG_ACTION_TYPE = "action.type";
    static final String TAG_FILE_STAGE = "file.stage";
    static final String TAG_EVENT_TYPE = "event.type";
    static final String TAG_OPERATION_TYPE = "operation.type";
    static final String TAG_HANDLER_NAME = "handler.name";
    static final String TAG_CLEANUP_ACTION = "cleanup.action";

    static final String TAG_OUTCOME = "outcome";
    static final String TAG_ERROR_TYPE = "error.type";

    static final String TAG_FILE_PATH = "file.path";
    static final String TAG_DESTINATION_PATH = "destination.path";
    static final String TAG_FILE_SIZE = "file.size";
    static final String TAG_FILE_MODIFIED_TIME = "file.modified_time";

    public BlobObserverContext(String context, String remoteUrl, String protocol) {
        addTag(TAG_CONTEXT, context);
        addTag(TAG_MODULE, BlobMetricsUtil.MODULE);
        addTag(TAG_REMOTE_URL, remoteUrl != null ? remoteUrl : BlobMetricsUtil.UNKNOWN);
        addTag(TAG_PROTOCOL, protocol != null ? protocol : BlobMetricsUtil.UNKNOWN);
        addTag(TAG_INSTANCE_URL, requireNonNullElse(BlobMetricsUtil.getInstanceUrl(), BlobMetricsUtil.NONE));
    }
}
