/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.enterprise.femt.request.handler;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;


import java.util.Objects;
import java.util.Optional;

public class RequestHandlerFactory {

    private final Client nodeClient;
    private final ThreadContext threadContext;

    public RequestHandlerFactory(Client nodeClient, ThreadContext threadContext) {
        this.nodeClient = Objects.requireNonNull(nodeClient, "nodeClient is required");
        this.threadContext = Objects.requireNonNull(threadContext, "threadContext is required");
    }

    @SuppressWarnings("unchecked")
    public <T extends ActionRequest> Optional<RequestHandler<T>> requestHandlerFor(Object request) {
        RequestHandler<T> handler = null;
        if (request instanceof IndexRequest) {
            handler = (RequestHandler<T>) new IndexRequestHandler();
        } else if (request instanceof DeleteRequest) {
            handler = (RequestHandler<T>)  new DeleteRequestHandler();
        } else if (request instanceof SearchRequest) {
            handler = (RequestHandler<T>)  new SearchRequestHandler(nodeClient, threadContext);
        } else if (request instanceof GetRequest) {
            handler = (RequestHandler<T>)  new GetRequestHandler(nodeClient, threadContext);
        } else if (request instanceof MultiGetRequest) {
            handler = (RequestHandler<T>)  new MultiGetRequestHandler(nodeClient, threadContext);
        } else if (request instanceof ClusterSearchShardsRequest) {
            handler = (RequestHandler<T>)  new ClusterSearchShardsRequestHandler(threadContext);
        } else if (request instanceof BulkRequest) {
            handler = (RequestHandler<T>)  new BulkRequestHandler(nodeClient, threadContext);
        } else if (request instanceof UpdateRequest) {
            handler = (RequestHandler<T>)  new UpdateRequestHandler(nodeClient, threadContext);
        }
        return Optional.ofNullable(handler);
    }

}
