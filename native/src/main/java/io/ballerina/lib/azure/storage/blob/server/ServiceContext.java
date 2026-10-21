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

import com.azure.storage.queue.QueueClient;
import io.ballerina.lib.azure.storage.blob.util.ConfigurationException;
import io.ballerina.runtime.api.Module;
import io.ballerina.runtime.api.types.MethodType;
import io.ballerina.runtime.api.types.ObjectType;
import io.ballerina.runtime.api.types.Parameter;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.TypeTags;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * One attached service: its settings, its handlers with their routing patterns and parameter
 * shapes, its queue clients and poison queue, its poller, and the per-queue memory of the
 * never-ours warnings.
 */
final class ServiceContext {

    // One handler: its routing patterns, which optional parameters it declares, and its content type.
    record HandlerConfig(String methodName, Pattern namePattern, Pattern contentTypePattern, boolean takesEvent,
                         boolean takesCaller, Type contentType) {
    }

    // Which optional parameters the service's onError declares.
    record OnErrorShape(boolean takesEvent, boolean takesCaller) {
    }

    private static final String PACKAGE_ORG = "ballerinax";
    private static final String PACKAGE_NAME = "azure.storage.blob";
    private static final String SERVICE_CONFIG_ANNOTATION = ":ServiceConfig";
    private static final String FUNCTION_CONFIG_ANNOTATION = ":FunctionConfig";
    private static final BString NAME_PATTERN = StringUtils.fromString("namePattern");
    private static final BString CONTENT_TYPE_PATTERN = StringUtils.fromString("contentTypePattern");
    private static final String CALLER_TYPE_NAME = "Caller";
    private static final String BLOB_EVENT_TYPE_NAME = "BlobEvent";
    private static final Set<String> HANDLER_NAMES = Set.of(Listener.ON_BLOB, Listener.ON_BLOB_TEXT,
            Listener.ON_BLOB_JSON, Listener.ON_BLOB_XML, Listener.ON_BLOB_CSV, Listener.ON_BLOB_DELETE);

    final BObject service;
    final ObjectType type;
    final ServiceSettings settings;
    final Map<String, HandlerConfig> handlers;
    final OnErrorShape onError;
    final QueueClient queue;
    final QueueGateway retrying;
    final QueueGateway retryFree;
    final PoisonQueue poison;
    // Messages being handled for this service; the poller refills at the threshold.
    final AtomicInteger inFlight = new AtomicInteger();
    final Map<String, AtomicLong> neverOurs = new ConcurrentHashMap<>();
    volatile Poller poller;

    private ServiceContext(BObject service, ObjectType type, ServiceSettings settings,
                           Map<String, HandlerConfig> handlers, OnErrorShape onError, QueueClient queue,
                           QueueGateway retryFree, PoisonQueue poison) {
        this.service = service;
        this.type = type;
        this.settings = settings;
        this.handlers = handlers;
        this.onError = onError;
        this.queue = queue;
        this.retrying = new QueueGateway.SdkQueueGateway(queue);
        this.retryFree = retryFree;
        this.poison = poison;
    }

    String queueName() {
        return settings.queueName();
    }

    /** Whether a method may run on a concurrent strand: the service and the method are isolated. */
    boolean concurrentSafe(String methodName) {
        return type.isIsolated() && type.isIsolated(methodName);
    }

    /**
     * Reads the service's annotation and handlers and builds its clients.
     *
     * @param ctx     the listener
     * @param service the service object
     * @return the service context
     * @throws ConfigurationException when the annotation or a handler shape breaks a rule
     */
    static ServiceContext parse(ListenerContext ctx, BObject service) {
        ObjectType type = (ObjectType) TypeUtils.getReferredType(TypeUtils.getType(service));
        ServiceSettings settings = ServiceSettings.fromAnnotation(annotation(type.getAnnotations(),
                SERVICE_CONFIG_ANNOTATION));
        Map<String, HandlerConfig> handlers = new LinkedHashMap<>();
        OnErrorShape onError = null;
        for (MethodType method : type.getMethods()) {
            String name = method.getName();
            Parameter[] params = method.getParameters();
            if (Listener.ON_ERROR.equals(name)) {
                onError = onErrorShape(params);
                continue;
            }
            if (!HANDLER_NAMES.contains(name)) {
                continue;
            }
            Pattern namePattern = null;
            Pattern contentTypePattern = null;
            BMap<BString, Object> functionConfig = annotation(method.getAnnotations(), FUNCTION_CONFIG_ANNOTATION);
            if (functionConfig != null) {
                namePattern = compile(functionConfig.get(NAME_PATTERN));
                contentTypePattern = compile(functionConfig.get(CONTENT_TYPE_PATTERN));
            }
            boolean takesEvent = false;
            boolean takesCaller = false;
            int first = Listener.ON_BLOB_DELETE.equals(name) ? 0 : 1;
            for (int i = first; i < params.length; i++) {
                String typeName = moduleTypeName(params[i].type);
                if (CALLER_TYPE_NAME.equals(typeName)) {
                    takesCaller = true;
                } else if (BLOB_EVENT_TYPE_NAME.equals(typeName)) {
                    takesEvent = true;
                }
            }
            Type contentType = params.length >= 1 ? params[0].type : null;
            handlers.put(name, new HandlerConfig(name, namePattern, contentTypePattern,
                    takesEvent || Listener.ON_BLOB_DELETE.equals(name), takesCaller, contentType));
        }
        if (handlers.isEmpty()) {
            throw new ConfigurationException("a service declares at least one handler: onBlob, onBlobText, "
                    + "onBlobJson, onBlobXml, onBlobCsv, or onBlobDelete");
        }
        QueueClient queue = QueueClients.build(ctx, settings.queueName(), ctx.retrying);
        QueueGateway retryFree = new QueueGateway.SdkQueueGateway(QueueClients.build(ctx, settings.queueName(),
                ctx.retryFree));
        PoisonQueue poison = new PoisonQueue(settings.poisonQueueName(), new QueueGateway.SdkQueueGateway(
                QueueClients.build(ctx, settings.poisonQueueName(), ctx.retrying)));
        return new ServiceContext(service, type, settings, handlers, onError, queue, retryFree, poison);
    }

    // onError takes the error, then optionally the event and the Caller, each declared nilable.
    private static OnErrorShape onErrorShape(Parameter[] params) {
        boolean takesEvent = false;
        boolean takesCaller = false;
        for (int i = 1; i < params.length; i++) {
            String typeName = moduleTypeName(params[i].type);
            if (!nilable(params[i].type)) {
                throw new ConfigurationException("onError's '" + params[i].name + "' parameter must be nilable: "
                        + "the event and the Caller are '()' for a failure that carries none");
            }
            if (BLOB_EVENT_TYPE_NAME.equals(typeName) && !takesCaller) {
                takesEvent = true;
            } else if (CALLER_TYPE_NAME.equals(typeName) && !takesCaller) {
                takesCaller = true;
            } else {
                throw new ConfigurationException("onError takes (error, BlobEvent?, Caller?), in that order; found '"
                        + params[i].name + "' of type " + params[i].type);
            }
        }
        return new OnErrorShape(takesEvent, takesCaller);
    }

    // The name of the module type a parameter is declared as, through a nilable union.
    private static String moduleTypeName(Type type) {
        Type referred = TypeUtils.getReferredType(type);
        if (referred instanceof UnionType union) {
            for (Type member : union.getMemberTypes()) {
                Type memberType = TypeUtils.getReferredType(member);
                if (memberType.getTag() != TypeTags.NULL_TAG) {
                    referred = memberType;
                    break;
                }
            }
        }
        Module module = referred.getPackage();
        if (module == null || !PACKAGE_ORG.equals(module.getOrg()) || !PACKAGE_NAME.equals(module.getName())) {
            return null;
        }
        return referred.getName();
    }

    private static boolean nilable(Type type) {
        Type referred = TypeUtils.getReferredType(type);
        if (referred.getTag() == TypeTags.NULL_TAG) {
            return true;
        }
        if (referred instanceof UnionType union) {
            for (Type member : union.getMemberTypes()) {
                if (TypeUtils.getReferredType(member).getTag() == TypeTags.NULL_TAG) {
                    return true;
                }
            }
        }
        return false;
    }

    private static BMap<BString, Object> annotation(BMap<BString, Object> annotations, String suffix) {
        if (annotations == null) {
            return null;
        }
        for (BString key : annotations.getKeys()) {
            if (key.getValue().endsWith(suffix) && annotations.get(key) instanceof BMap) {
                @SuppressWarnings("unchecked")
                BMap<BString, Object> map = (BMap<BString, Object>) annotations.get(key);
                return map;
            }
        }
        return null;
    }

    private static Pattern compile(Object pattern) {
        if (pattern == null) {
            return null;
        }
        String value = ((BString) pattern).getValue();
        try {
            return Pattern.compile(value);
        } catch (PatternSyntaxException e) {
            throw new ConfigurationException("invalid regular expression in @blob:FunctionConfig: " + value);
        }
    }
}
