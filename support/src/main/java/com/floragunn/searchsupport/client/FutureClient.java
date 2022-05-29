/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchsupport.client;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;

public class FutureClient {

    private final Client client;

    FutureClient(Client client) {
        this.client = client;
    }

    public CompletableFuture<DocNode> get(String index, String id) {
        CompletableFuture<DocNode> result = new CompletableFuture<>();

        this.client.get(new GetRequest(index, id), new ActionListener<GetResponse>() {

            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists()) {
                    result.complete(DocNode.wrap(getResponse.getSource()));
                } else {
                    result.completeExceptionally(new DocumentNotFoundException(index, id));
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof org.elasticsearch.index.IndexNotFoundException) {
                    result.completeExceptionally(new IndexNotFoundException(index, e));
                } else {
                    result.completeExceptionally(e);
                }
            }
        });

        return result;
    }

    public CompletableFuture<Optional<DocNode>> getOptional(String index, String id) {
        CompletableFuture<Optional<DocNode>> result = new CompletableFuture<>();

        this.client.get(new GetRequest(index, id), new ActionListener<GetResponse>() {

            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists()) {
                    result.complete(Optional.of(DocNode.wrap(getResponse.getSource())));
                } else {
                    result.complete(Optional.empty());
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof org.elasticsearch.index.IndexNotFoundException) {
                    result.completeExceptionally(new IndexNotFoundException(index, e));
                } else {
                    result.completeExceptionally(e);
                }
            }
        });

        return result;
    }

    public CompletableFuture<ImmutableMap<String, DocNode>> get(String index, Collection<String> ids) {
        if (ids.isEmpty()) {
            return CompletableFuture.completedFuture(ImmutableMap.empty());
        }

        CompletableFuture<ImmutableMap<String, DocNode>> futureResult = new CompletableFuture<>();
        MultiGetRequest multiGetRequest = new MultiGetRequest();

        ids.forEach((id) -> {
            multiGetRequest.add(index, id);
        });

        client.multiGet(multiGetRequest, new ActionListener<MultiGetResponse>() {

            @Override
            public void onResponse(MultiGetResponse response) {
                try {
                    ImmutableMap.Builder<String, Exception> failures = new ImmutableMap.Builder<>();
                    ImmutableMap.Builder<String, DocNode> result = new ImmutableMap.Builder<>(ids.size());

                    for (MultiGetItemResponse itemResponse : response.getResponses()) {
                        if (itemResponse.getResponse() == null) {
                            if (itemResponse.getFailure() != null) {
                                if (itemResponse.getFailure().getFailure() instanceof org.elasticsearch.index.IndexNotFoundException) {
                                    failures.put(itemResponse.getId(), new IndexNotFoundException(index, itemResponse.getFailure().getFailure()));
                                } else {
                                    failures.put(itemResponse.getId(), itemResponse.getFailure().getFailure());
                                }
                            } else {
                                failures.put(itemResponse.getId(), new Exception("No information"));
                            }
                            continue;
                        }

                        if (itemResponse.getResponse().isExists()) {
                            result.put(itemResponse.getId(), DocNode.wrap(itemResponse.getResponse().getSource()));
                        } else {
                            failures.put(itemResponse.getId(), new DocumentNotFoundException(index, itemResponse.getId()));
                        }

                        if (failures.size() == 0 && result.size() != ids.size()) {
                            for (String id : ids) {
                                if (!result.contains(id)) {
                                    failures.put(id, new DocumentNotFoundException(index, id));
                                }
                            }
                        }

                        if (failures.size() != 0) {
                            futureResult.completeExceptionally(MultiException.get(index, failures.build()));
                        } else {
                            futureResult.complete(result.build());
                        }
                    }
                } catch (Exception e) {
                    futureResult.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                futureResult.completeExceptionally(e);
            }
        });

        return futureResult;
    }

    public CompletableFuture<ImmutableMap<String, DocNode>> getOptional(String index, Collection<String> ids) {
        if (ids.isEmpty()) {
            return CompletableFuture.completedFuture(ImmutableMap.empty());
        }

        CompletableFuture<ImmutableMap<String, DocNode>> futureResult = new CompletableFuture<>();
        MultiGetRequest multiGetRequest = new MultiGetRequest();

        ids.forEach((id) -> {
            multiGetRequest.add(index, id);
        });

        client.multiGet(multiGetRequest, new ActionListener<MultiGetResponse>() {

            @Override
            public void onResponse(MultiGetResponse response) {
                try {
                    ImmutableMap.Builder<String, Exception> failures = new ImmutableMap.Builder<>();
                    ImmutableMap.Builder<String, DocNode> result = new ImmutableMap.Builder<>(ids.size());

                    for (MultiGetItemResponse itemResponse : response.getResponses()) {
                        if (itemResponse.getResponse() == null) {
                            if (itemResponse.getFailure() != null) {
                                if (itemResponse.getFailure().getFailure() instanceof org.elasticsearch.index.IndexNotFoundException) {
                                    continue;
                                } else {
                                    failures.put(itemResponse.getId(), itemResponse.getFailure().getFailure());
                                }
                            } else {
                                failures.put(itemResponse.getId(), new Exception("No information"));
                            }
                            continue;
                        }

                        if (itemResponse.getResponse().isExists()) {
                            result.put(itemResponse.getId(), DocNode.wrap(itemResponse.getResponse().getSource()));
                        }
                    }

                    if (failures.size() != 0) {
                        futureResult.completeExceptionally(MultiException.get(index, failures.build()));
                    } else {
                        futureResult.complete(result.build());
                    }
                } catch (Exception e) {
                    futureResult.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                futureResult.completeExceptionally(e);
            }
        });

        return futureResult;
    }

    public CompletableFuture<IndexResult> index(String index, String id, Document<?> document) {
        CompletableFuture<IndexResult> result = new CompletableFuture<>();

        client.index(new IndexRequest(index).id(id).source(document.toJsonString(), XContentType.JSON), new ActionListener<IndexResponse>() {

            @Override
            public void onResponse(IndexResponse response) {
                result.complete(new IndexResult(response.getVersion(), response.getSeqNo(), response.getPrimaryTerm()));
            }

            @Override
            public void onFailure(Exception e) {
                result.completeExceptionally(e);
            }
        });

        return result;
    }

    public CompletableFuture<IndexResult> indexImmediately(String index, String id, Document<?> document) {
        CompletableFuture<IndexResult> result = new CompletableFuture<>();

        client.index(new IndexRequest(index).id(id).source(document.toJsonString(), XContentType.JSON).setRefreshPolicy(RefreshPolicy.IMMEDIATE),
                new ActionListener<IndexResponse>() {

                    @Override
                    public void onResponse(IndexResponse response) {
                        result.complete(new IndexResult(response.getVersion(), response.getSeqNo(), response.getPrimaryTerm()));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        result.completeExceptionally(e);
                    }
                });

        return result;
    }

    public CompletableFuture<Long> count(String index, org.elasticsearch.index.query.QueryBuilder query) {
        CompletableFuture<Long> result = new CompletableFuture<>();

        client.search(new SearchRequest(index).source(new SearchSourceBuilder().query(query).size(0)), new ActionListener<SearchResponse>() {

            @Override
            public void onResponse(SearchResponse response) {
                result.complete(response.getHits().getTotalHits().value);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof org.elasticsearch.index.IndexNotFoundException) {
                    result.completeExceptionally(new IndexNotFoundException(index, e));
                } else {
                    result.completeExceptionally(e);
                }
            }
        });

        return result;
    }

    public CompletableFuture<Long> countByTerms(String index, String termKey1, Object termValue1, Object... more) {
        QueryBuilder query;

        if (more == null || more.length == 0) {
            query = QueryBuilders.termQuery(termKey1, termValue1);
        } else {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            boolQuery.must(QueryBuilders.termQuery(termKey1, termValue1));

            for (int i = 0; i < more.length; i += 2) {
                boolQuery.must(QueryBuilders.termQuery(String.valueOf(more[i]), more[i + 1]));
            }

            query = boolQuery;
        }

        return count(index, query);
    }

    public static class IndexResult {

        private final long version;
        private final long seqNo;
        private final long primaryTerm;

        IndexResult(long version, long seqNo, long primaryTerm) {
            this.version = version;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
        }

        public long getVersion() {
            return version;
        }

        public long getSeqNo() {
            return seqNo;
        }

        public long getPrimaryTerm() {
            return primaryTerm;
        }
    }

    public static class MultiException extends Exception {

        private static final long serialVersionUID = -8537502834754975611L;

        private final ImmutableMap<String, Exception> subExceptions;

        MultiException(String index, ImmutableMap<String, Exception> subExceptions) {
            super(buildMessage(index, subExceptions));
            this.subExceptions = subExceptions;
        }

        private static String buildMessage(String index, ImmutableMap<String, Exception> subExceptions) {
            StringBuilder result = new StringBuilder("Several failures while accessing ").append(index).append(":\n");

            subExceptions.forEach((k, v) -> {
                result.append(k).append(": ").append(v).append("\n");
            });

            return result.toString();
        }

        public static Exception get(String index, Map<String, Exception> exceptions) {
            if (exceptions.size() == 1) {
                return exceptions.values().iterator().next();
            } else {
                ImmutableSet.Builder<String> docIdsNotFound = new ImmutableSet.Builder<String>();
                int indexNotFound = 0;
                IndexNotFoundException firstIndexNotFound = null;

                for (Exception e : exceptions.values()) {
                    if (e instanceof DocumentNotFoundException) {
                        docIdsNotFound.add(((DocumentNotFoundException) e).getId());
                    } else if (e instanceof IndexNotFoundException) {
                        indexNotFound++;
                        if (firstIndexNotFound == null) {
                            firstIndexNotFound = (IndexNotFoundException) e;
                        }
                    }

                    if (indexNotFound == exceptions.size()) {
                        return new IndexNotFoundException(firstIndexNotFound.getIndex(), firstIndexNotFound);
                    } else if (docIdsNotFound.size() == exceptions.size()) {
                        return new DocumentsNotFoundException(index, docIdsNotFound.build());
                    }
                }

                return new MultiException(index, ImmutableMap.of(exceptions));
            }
        }

        public ImmutableMap<String, Exception> getSubExceptions() {
            return subExceptions;
        }
    }

    public static class DocumentNotFoundException extends Exception {
        private static final long serialVersionUID = -7877818260197807986L;
        private final String index;
        private final String id;

        public DocumentNotFoundException(String index, String id) {
            super("Document not found: " + index + "/" + id);
            this.index = index;
            this.id = id;
        }

        public String getIndex() {
            return index;
        }

        public String getId() {
            return id;
        }
    }

    public static class DocumentsNotFoundException extends Exception {
        private static final long serialVersionUID = -7877818260197807986L;
        private final String index;
        private final ImmutableSet<String> ids;

        public DocumentsNotFoundException(String index, ImmutableSet<String> ids) {
            super("Documents not found: " + index + "/" + ids);
            this.index = index;
            this.ids = ids;
        }

        public String getIndex() {
            return index;
        }

        public ImmutableSet<String> getIds() {
            return ids;
        }
    }

    public static class IndexNotFoundException extends Exception {

        private static final long serialVersionUID = -9223030818809963294L;
        private final String index;

        public IndexNotFoundException(String index, Throwable cause) {
            super("Index not found: " + index, cause);
            this.index = index;
        }

        public String getIndex() {
            return index;
        }
    }

}
