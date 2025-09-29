/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auditlog.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.searchguard.auditlog.AuditLog.Origin;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.Category;
import com.floragunn.searchguard.user.UserInformation;

public final class RequestResolver {

    private static final Logger log = LogManager.getLogger(RequestResolver.class);

    public static List<AuditMessage> resolve(final Category category, final Origin origin, final String action, final String privilege,
            final UserInformation effectiveUser, final Boolean sgAdmin, final UserInformation initiatingUser, final TransportAddress remoteAddress,
            final TransportRequest request, final Map<String, String> headers, final Task task, final IndexNameExpressionResolver resolver,
            final ClusterState clusterState, final Settings settings, final boolean logRequestBody, final Pattern ignoredRequestBodies, final boolean resolveIndices,
            final boolean resolveBulk, final Pattern searchguardIndex, final boolean excludeSensitiveHeaders, final Throwable exception) {

        if (resolveBulk && request instanceof BulkShardRequest) {
            final BulkItemRequest[] innerRequests = ((BulkShardRequest) request).items();
            final List<AuditMessage> messages = new ArrayList<AuditMessage>(innerRequests.length);

            for (BulkItemRequest ar : innerRequests) {
                final DocWriteRequest<?> innerRequest = ar.request();
                final AuditMessage msg = resolveInner(category, effectiveUser, sgAdmin, initiatingUser, remoteAddress, action, privilege, origin,
                        innerRequest, headers, task, resolver, clusterState, settings, logRequestBody, ignoredRequestBodies, resolveIndices, searchguardIndex,
                        excludeSensitiveHeaders, exception);
                msg.addShardId(((BulkShardRequest) request).shardId());

                messages.add(msg);
            }

            return messages;
        }

        if (request instanceof BulkShardRequest) {

            if (category != Category.FAILED_LOGIN && category != Category.MISSING_PRIVILEGES && category != Category.SG_INDEX_ATTEMPT) {

                return Collections.emptyList();
            }
        }

        return Collections.singletonList(
                resolveInner(category, effectiveUser, sgAdmin, initiatingUser, remoteAddress, action, privilege, origin, request, headers, task,
                        resolver, clusterState, settings, logRequestBody, ignoredRequestBodies, resolveIndices, searchguardIndex, excludeSensitiveHeaders, exception));
    }

    private static AuditMessage resolveInner(final Category category, final UserInformation effectiveUser, final Boolean sgAdmin,
            final UserInformation initiatingUser, final TransportAddress remoteAddress, final String action, final String priv, final Origin origin,
            final Object request, final Map<String, String> headers, final Task task, final IndexNameExpressionResolver resolver,
            final ClusterState clusterState, final Settings settings, final boolean logRequestBody, final Pattern ignoredRequestBodies, final boolean resolveIndices,
            final Pattern searchguardIndex, final boolean excludeSensitiveHeaders, final Throwable exception) {

        final ClusterState localClusterState = clusterState;

        final AuditMessage msg = new AuditMessage(category, localClusterState, origin, Origin.TRANSPORT);
        msg.addInitiatingUser(initiatingUser);
        msg.addEffectiveUser(effectiveUser);
        msg.addRemoteAddress(remoteAddress);
        msg.addAction(action);

        if (request != null) {
            msg.addRequestType(request.getClass().getSimpleName());
        }

        if (sgAdmin != null) {
            msg.addIsAdminDn(sgAdmin);
        }

        msg.addException(exception);
        msg.addPrivilege(priv);
        msg.addTransportHeaders(headers, excludeSensitiveHeaders);

        if (task != null) {
            msg.addTaskId(task.getId());
            if (task.getParentTaskId() != null && task.getParentTaskId().isSet()) {
                msg.addTaskParentId(task.getParentTaskId().toString());
            }
        }

        boolean addSource = logRequestBody && !(ignoredRequestBodies.matches(action) || (request != null && ignoredRequestBodies.matches(request.getClass().getSimpleName())));

        //attempt to resolve indices/types/id/source 
        if (request instanceof MultiGetRequest.Item) {
            final MultiGetRequest.Item item = (MultiGetRequest.Item) request;
            final String[] indices = arrayOrEmpty(item.indices());
            final String id = item.id();
            msg.addId(id);
            addIndicesSourceSafe(msg, indices, resolver, localClusterState, null, null, settings, resolveIndices, addSource, false,
                    searchguardIndex);
        } else if (request instanceof CreateIndexRequest) {
            final CreateIndexRequest cir = (CreateIndexRequest) request;
            final String[] indices = arrayOrEmpty(cir.indices());
            addIndicesSourceSafe(msg, indices, resolver, localClusterState, null, null, settings, resolveIndices, addSource, false,
                    searchguardIndex);
        } else if (request instanceof DeleteIndexRequest) {
            final DeleteIndexRequest dir = (DeleteIndexRequest) request;
            final String[] indices = arrayOrEmpty(dir.indices());
            //dir id alle id's beim schreiben protokolloieren
            addIndicesSourceSafe(msg, indices, resolver, localClusterState, null, null, settings, resolveIndices, addSource, false,
                    searchguardIndex);
        } else if (request instanceof IndexRequest) {
            final IndexRequest ir = (IndexRequest) request;
            final String[] indices = arrayOrEmpty(ir.indices());
            final String id = ir.id();
            msg.addShardId(ir.shardId());
            msg.addId(id);
            addIndicesSourceSafe(msg, indices, resolver, localClusterState, ir.getContentType(), ir.source(), settings, resolveIndices,
                    addSource, true, searchguardIndex);
        } else if (request instanceof DeleteRequest) {
            final DeleteRequest dr = (DeleteRequest) request;
            final String[] indices = arrayOrEmpty(dr.indices());
            final String id = dr.id();
            msg.addShardId(dr.shardId());
            msg.addId(id);
            addIndicesSourceSafe(msg, indices, resolver, localClusterState, null, null, settings, resolveIndices, addSource, false,
                    searchguardIndex);
        } else if (request instanceof UpdateRequest) {
            final UpdateRequest ur = (UpdateRequest) request;
            final String[] indices = arrayOrEmpty(ur.indices());
            final String id = ur.id();
            msg.addId(id);
            addIndicesSourceSafe(msg, indices, resolver, localClusterState, null, null, settings, resolveIndices, addSource, false,
                    searchguardIndex);
            if (addSource) {

                if (ur.doc() != null) {
                    msg.addTupleToRequestBody(ur.doc() == null ? null : convertSource(ur.doc().getContentType(), ur.doc().source()));
                }

                if (ur.script() != null) {
                    msg.addMapToRequestBody(ur.script() == null ? null : Utils.convertJsonToxToStructuredMap(ur.script()));
                }
            }
        } else if (request instanceof GetRequest) {
            final GetRequest gr = (GetRequest) request;
            final String[] indices = arrayOrEmpty(gr.indices());
            final String id = gr.id();
            msg.addId(id);
            addIndicesSourceSafe(msg, indices, resolver, localClusterState, null, null, settings, resolveIndices, addSource, false,
                    searchguardIndex);
        } else if (request instanceof SearchRequest) {
            final SearchRequest sr = (SearchRequest) request;
            final String[] indices = arrayOrEmpty(sr.indices());

            Map<String, Object> sourceAsMap = sr.source() == null ? null : Utils.convertJsonToxToStructuredMap(sr.source());
            addIndicesSourceSafe(msg, indices, resolver, localClusterState, XContentType.JSON, sourceAsMap, settings, resolveIndices, addSource,
                    false, searchguardIndex);
        } else if (request instanceof ClusterUpdateSettingsRequest) {
            if (addSource) {
                final ClusterUpdateSettingsRequest cusr = (ClusterUpdateSettingsRequest) request;
                final Settings persistentSettings = cusr.persistentSettings();
                final Settings transientSettings = cusr.transientSettings();

                XContentBuilder builder = null;
                try {

                    builder = XContentFactory.jsonBuilder();
                    builder.startObject();
                    if (persistentSettings != null) {
                        builder.field("persistent_settings", Utils.convertJsonToxToStructuredMap(persistentSettings));
                    }
                    if (transientSettings != null) {
                        builder.field("transient_settings", Utils.convertJsonToxToStructuredMap(persistentSettings));
                    }
                    builder.endObject();
                    msg.addUnescapedJsonToRequestBody(builder == null ? null : Strings.toString(builder));
                } catch (IOException e) {
                    log.error(e);
                } finally {
                    if (builder != null) {
                        builder.close();
                    }
                }

            }
        } else if (request instanceof ReindexRequest) {
            final IndexRequest ir = ((ReindexRequest) request).getDestination();
            final String[] indices = arrayOrEmpty(ir.indices());
            final String id = ir.id();
            msg.addShardId(ir.shardId());
            msg.addId(id);
            addIndicesSourceSafe(msg, indices, resolver, localClusterState, ir.getContentType(), ir.source(), settings, resolveIndices,
                    addSource, true, searchguardIndex);
        } else if (request instanceof DeleteByQueryRequest) {
            final DeleteByQueryRequest ir = (DeleteByQueryRequest) request;
            final String[] indices = arrayOrEmpty(ir.indices());
            addIndicesSourceSafe(msg, indices, resolver, localClusterState, null, null, settings, resolveIndices, addSource, false,
                    searchguardIndex);
        } else if (request instanceof UpdateByQueryRequest) {
            final UpdateByQueryRequest ir = (UpdateByQueryRequest) request;
            final String[] indices = arrayOrEmpty(ir.indices());
            addIndicesSourceSafe(msg, indices, resolver, localClusterState, null, null, settings, resolveIndices, addSource, false,
                    searchguardIndex);
        } else if (request instanceof PutMappingRequest) {
            final PutMappingRequest pr = (PutMappingRequest) request;
            final Index ci = pr.getConcreteIndex();
            String[] indices = new String[0];
            msg.addIndices(indices);

            if (ci != null) {
                indices = new String[] { ci.getName() };
            }

            if (addSource) {
                msg.addUnescapedJsonToRequestBody(pr.source());
            }

            if (resolveIndices) {
                msg.addResolvedIndices(indices);
            }
        } else if (request instanceof IndicesRequest) { //less specific
            final IndicesRequest ir = (IndicesRequest) request;
            final String[] indices = arrayOrEmpty(ir.indices());
            addIndicesSourceSafe(msg, indices, resolver, localClusterState, null, null, settings, resolveIndices, addSource, false,
                    searchguardIndex);
        }

        return msg;
    }

    private static void addIndicesSourceSafe(final AuditMessage msg, final String[] indices, final IndexNameExpressionResolver resolver,
            final ClusterState clusterState, final XContentType xContentType, final Object source, final Settings settings, boolean resolveIndices,
            final boolean addSource, final boolean sourceIsSensitive, final Pattern searchguardIndex) {

        if (addSource) {
            resolveIndices = true;
        }

        final String[] _indices = indices == null ? new String[0] : indices;
        msg.addIndices(_indices);

        final Set<String> allIndices;

        if (resolveIndices && clusterState != null) {
            final String[] resolvedIndices = (resolver == null) ? new String[0]
                    : resolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), indices);
            msg.addResolvedIndices(resolvedIndices);
            allIndices = new HashSet<String>(Arrays.asList(resolvedIndices));
        } else {
            allIndices = new HashSet<String>(_indices.length);
            allIndices.addAll(Arrays.asList(_indices));
            if (allIndices.contains("_all")) {
                allIndices.add("*");
            }
        }

        if (addSource) {
            if (sourceIsSensitive && source != null) {
                if (!searchguardIndex.matches(allIndices)) {
                    if (source instanceof BytesReference) {
                        msg.addTupleToRequestBody(convertSource(xContentType, (BytesReference) source));
                    } else {
                        msg.addMapToRequestBody((Map) source);
                    }
                }
            } else if (source != null) {
                if (source instanceof BytesReference) {
                    msg.addTupleToRequestBody(convertSource(xContentType, (BytesReference) source));
                } else {
                    msg.addMapToRequestBody((Map) source);
                }
            }
        }
    }

    private static Tuple<XContentType, BytesReference> convertSource(XContentType type, BytesReference bytes) {
        if (type == null) {
            type = XContentType.JSON;
        }

        return new Tuple<XContentType, BytesReference>(type, bytes);
    }

    private static String[] arrayOrEmpty(String[] array) {
        if (array == null) {
            return new String[0];
        }

        if (array.length == 1 && array[0] == null) {
            return new String[0];
        }

        return array;
    }
}
