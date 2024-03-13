/*
 * Copyright 2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchsupport.action;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.UnparsedDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableMap;

public abstract class Action<RequestType extends Action.Request, ResponseType extends Action.Response> extends ActionType<ResponseType> {
    private final static Logger log = LogManager.getLogger(Action.class);

    private final MessageParser<RequestType> requestParser;
    private final MessageParser<ResponseType> responseParser;

    public Action(String name, MessageParser<RequestType> requestParser, MessageParser<ResponseType> responseParser) {
        super(name, (in) -> parse(in, responseParser));
        this.requestParser = requestParser;
        this.responseParser = responseParser;
    }

    RequestType parseRequest(UnparsedMessage message) throws ConfigValidationException {
        return requestParser.apply(message);
    }

    ResponseType parseResponse(UnparsedMessage message) throws ConfigValidationException {
        return responseParser.apply(message);
    }

    static UnparsedDocument<?> toUnparsedDoc(StreamInput in) throws IOException {
        byte messageType = in.readByte();

        if (messageType == MessageType.EMPTY) {
            return null;
        } else if (messageType == MessageType.SMILE) {
            byte[] smile = in.readByteArray();
            return UnparsedDocument.from(smile, Format.SMILE);
        } else if (messageType == MessageType.JSON_STRING) {
            String json = in.readString();
            return UnparsedDocument.from(json, Format.JSON);
        } else if (messageType == MessageType.YAML_STRING) {
            String yaml = in.readString();
            return UnparsedDocument.from(yaml, Format.YAML);
        } else {
            throw new IllegalArgumentException("Unknown messageType " + messageType);
        }
    }

    public static abstract class Request extends ActionRequest implements Document<Object> {
        private String ifMatch;
        private String ifNoneMatch;

        public Request() {

        }

        public Request(UnparsedMessage metaData) {
            DocNode metaDataDocNode = metaData.getMetaDataDocNode();
            ifMatch = metaDataDocNode.getAsString("if-match");
            ifNoneMatch = metaDataDocNode.getAsString("if-none-match");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {            
            out.writeByte((byte) 0);
            out.writeByte((byte) 0);
            
            Map<String, Object> metaData = getMetaData();

            if (metaData != null && !metaData.isEmpty()) {
                out.writeMap(metaData);
            } else {
                out.writeMap((Map<String, Object>) null);
            }
            
            Object basicObject = toBasicObject();

            if (basicObject != null) {
                out.writeByte(MessageType.SMILE);
                out.writeByteArray(DocWriter.smile().writeAsBytes(basicObject));
            } else {
                out.writeByte(MessageType.EMPTY);
            }
        }

        public String getIfMatch() {
            return ifMatch;
        }
        
        public Request ifMatch(String matchConcurrencyControlEntityTag) {
            this.ifMatch = matchConcurrencyControlEntityTag;
            return this;
        }
        
        public String getIfNoneMatch() {
            return ifNoneMatch;
        }
        
        public boolean mustNotExist() {
            return "*".equals(ifNoneMatch);
        }

        public Request ifNoneMatch(String ifNoneMatch) {
            this.ifNoneMatch = ifNoneMatch;
            return this;
        }
        
        protected Map<String, Object> getMetaData() {
            return ImmutableMap.ofNonNull("if-match", ifMatch, "if-none-match", ifNoneMatch);
        }
        
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }


    }

    public static abstract class Response extends ActionResponse implements Document<Object>, StatusToXContentObject {

        private int restStatus = 200;
        private String concurrencyControlEntityTag;

        public Response() {
        }

        public Response(UnparsedMessage metaData) {
            DocNode metaDataDocNode = metaData.getMetaDataDocNode();
            restStatus = ((Number) metaDataDocNode.get("status")).intValue();
            concurrencyControlEntityTag = metaDataDocNode.getAsString("etag");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte((byte) 0);
            out.writeByte((byte) 0);
            out.writeMap(ImmutableMap.ofNonNull("status", restStatus, "etag", concurrencyControlEntityTag));
            out.writeByte(MessageType.SMILE);
            out.writeByteArray(this.toSmile());
        }

        @Override
        public final RestStatus status() {
            RestStatus result = RestStatus.fromCode(restStatus);

            if (result == null) {
                if (restStatus >= 200 && restStatus < 300) {
                    result = RestStatus.OK;
                } else if (restStatus >= 400 && restStatus < 500) {
                    result = RestStatus.BAD_REQUEST;
                } else {
                    result = RestStatus.INTERNAL_SERVER_ERROR;
                }
            }

            return result;
        }

        public int getStatus() {
            return restStatus;
        }

        public Response status(int status) {
            this.restStatus = status;
            return this;
        }

        public String getConcurrencyControlEntityTag() {
            return concurrencyControlEntityTag;
        }

        public Response concurrencyControlEntityTag(String concurrencyControlEntityTag) {
            this.concurrencyControlEntityTag = concurrencyControlEntityTag;
            return this;
        }
        
        public Response eTag(String concurrencyControlEntityTag) {
            this.concurrencyControlEntityTag = concurrencyControlEntityTag;
            return this;
        }
        
        
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.value(this.toBasicObject());
            return builder;
        }

        public RestResponse toRestResponse() {
            return RestApi.toRestResponse(this);
        }
    }

    public static abstract class Handler<RequestType extends Request, ResponseType extends Response>
            extends HandledTransportAction<RequestType, ResponseType> {
        private final Executor executor;

        protected Handler(Action<RequestType, ResponseType> action, HandlerDependencies handlerDependencies) {
            super(action.name(), handlerDependencies.transportService, handlerDependencies.actionFilters, (in) -> parse(in, action.requestParser));
            this.executor = handlerDependencies.threadPool.generic();
        }

        @Override
        protected final void doExecute(Task task, RequestType request, ActionListener<ResponseType> listener) {
            try {
                doExecute(request).whenComplete((response, e) -> {
                    if (response != null) {
                        listener.onResponse(response);
                    } else if (e instanceof Exception) {
                        log.error("Error while executing {}", request, e);
                        listener.onFailure((Exception) e);
                    } else if (e != null) {
                        log.error("Error while executing {}", request, e);
                        listener.onFailure(new Exception(e));
                    } else {
                        Exception e2 = new Exception("doExecute() of " + this + " did not provide a result");
                        log.error("Error while executing {}", request, e2);
                        listener.onFailure(e2);
                    }
                });
            } catch (Exception e) {
                log.error("Error while executing {}", request, e);
                listener.onFailure(e);
            } catch (Throwable t) {
                log.error("Error while executing {}", request, t);
                listener.onFailure(new Exception(t));                
            }
        }

        protected abstract CompletableFuture<ResponseType> doExecute(RequestType request);

        protected static RuntimeException restStatusException(int status, String message) {
            throw new ElasticsearchStatusException(message,
                    RestStatus.fromCode(status) != null ? RestStatus.fromCode(status) : RestStatus.INTERNAL_SERVER_ERROR);
        }

        protected static RuntimeException notFound(String message) {
            throw new ElasticsearchStatusException(message, RestStatus.NOT_FOUND);
        }

        protected Executor getExecutor() {
            return executor;
        }
        
        protected <U> CompletableFuture<U> supplyAsync(Supplier<U> supplier) {
            return CompletableFuture.supplyAsync(supplier, executor);
        }
    }

    public static class HandlerDependencies {
        final ThreadPool threadPool;
        final TransportService transportService;
        final ActionFilters actionFilters;

        @Inject
        public HandlerDependencies(ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters) {
            this.threadPool = threadPool;
            this.transportService = transportService;
            this.actionFilters = actionFilters;
        }

    }

    private static <M> M parse(StreamInput in, MessageParser<M> parser) throws IOException {
        try {
            byte majorVersion = in.readByte();
            byte minorVersion = in.readByte();

            Map<String, Object> metaData = in.readMap();

            if (metaData == null) {
                metaData = Collections.emptyMap();
            }

            UnparsedDocument<?> unparsedDoc = toUnparsedDoc(in);

            UnparsedMessage message = new UnparsedMessage(unparsedDoc, DocNode.wrap(metaData), majorVersion, minorVersion);

            return parser.apply(message);
        } catch (ConfigValidationException e) {
            throw new IOException(e);
        }
    }

    @FunctionalInterface
    public static interface MessageParser<M> {
        M apply(UnparsedMessage messageMetaData) throws ConfigValidationException;
    }

    public static class UnparsedMessage {
        private final UnparsedDocument<?> unparsedDoc;
        private final DocNode metaDataDocNode;
        private final byte majorVersion;
        private final byte minorVersion;

        UnparsedMessage(UnparsedDocument<?> unparsedDoc, DocNode metaDataDocNode) {
            this.unparsedDoc = unparsedDoc;
            this.metaDataDocNode = metaDataDocNode;
            this.majorVersion = 0;
            this.minorVersion = 0;
        }

        UnparsedMessage(UnparsedDocument<?> unparsedDoc, DocNode metaDataDocNode, byte majorVersion, byte minorVersion) {
            this.unparsedDoc = unparsedDoc;
            this.metaDataDocNode = metaDataDocNode;
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
        }

        public UnparsedDocument<?> unparsedDoc() {
            return unparsedDoc;
        }

        public UnparsedDocument<?> requiredUnparsedDoc() throws ConfigValidationException {
            if (unparsedDoc != null) {
                return unparsedDoc;
            } else {
                throw new ConfigValidationException(new ValidationError(null, "Request body missing"));
            }
        }

        public DocNode requiredDocNode() throws ConfigValidationException {
            return requiredUnparsedDoc().parseAsDocNode();
        }

        public DocNode getMetaDataDocNode() {
            return metaDataDocNode;
        }

        public byte getMajorVersion() {
            return majorVersion;
        }

        public byte getMinorVersion() {
            return minorVersion;
        }
    }

    private static interface MessageType {
        final static byte EMPTY = 0;
        final static byte SMILE = 1;
        final static byte JSON_STRING = 2;
        final static byte YAML_STRING = 3;
    }

}
