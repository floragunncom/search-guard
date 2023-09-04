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

import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.IndicesService;

import javax.annotation.Nullable;

public class RequestHandlerFactory {

    private final Logger log;
    private final Client nodeClient;
    private final ThreadContext threadContext;
    private final ClusterService clusterService;
    private final IndicesService indicesService;

    public RequestHandlerFactory(Logger log, Client nodeClient, ThreadContext threadContext, ClusterService clusterService, IndicesService indicesService) {
        this.log = log;
        this.nodeClient = nodeClient;
        this.threadContext = threadContext;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public <T extends ActionRequest> RequestHandler<T> requestHandlerFor(Object request) {
        RequestHandler<T> handler = null;
        if (request instanceof IndexRequest) {
            handler = (RequestHandler<T>) new IndexRequestHandler(log);
        } else if (request instanceof DeleteRequest) {
            handler = (RequestHandler<T>)  new DeleteRequestHandler(log);
        } else if (request instanceof SearchRequest) {
            handler = (RequestHandler<T>)  new SearchRequestHandler(log, nodeClient, threadContext);
        } else if (request instanceof GetRequest) {
            handler = (RequestHandler<T>)  new GetRequestHandler(log, nodeClient, threadContext, clusterService, indicesService);
        } else if (request instanceof MultiGetRequest) {
            handler = (RequestHandler<T>)  new MultiGetRequestHandler(log, nodeClient, threadContext);
        } else if (request instanceof ClusterSearchShardsRequest) {
            handler = (RequestHandler<T>)  new ClusterSearchShardsRequestHandler(log, threadContext);
        } else if (request instanceof BulkRequest) {
            handler = (RequestHandler<T>)  new BulkRequestHandler(log, nodeClient, threadContext);
        } else if (request instanceof UpdateRequest) {
            handler = (RequestHandler<T>)  new UpdateRequestHandler(log, nodeClient, threadContext);
        }
        return handler;
    }

}
