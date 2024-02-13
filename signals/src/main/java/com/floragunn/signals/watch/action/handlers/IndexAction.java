package com.floragunn.signals.watch.action.handlers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.execution.ActionExecutionException;
import com.floragunn.signals.execution.SimulationMode;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.support.InlineMustacheTemplate;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class IndexAction extends ActionHandler {
    private static final Logger log = LogManager.getLogger(IndexAction.class);

    public static final String TYPE = "index";

    private final String index;
    private InlineMustacheTemplate<String> docId;
    private Integer timeout;
    private RefreshPolicy refreshPolicy;

    public IndexAction(String index, RefreshPolicy refreshPolicy) {
        this.index = index;
        this.refreshPolicy = refreshPolicy;
    }

    @Override
    public ActionExecutionResult execute(WatchExecutionContext ctx) throws ActionExecutionException {

        NestedValueMap data = ctx.getContextData().getData();
        Object subDoc = data.get("_doc");

        if (subDoc instanceof Collection) {
            return indexMultiDoc(ctx, (Collection<?>) subDoc);
        } else if (subDoc instanceof Object[]) {
            return indexMultiDoc(ctx, Arrays.asList((Object[]) subDoc));
        } else {
            return indexSingleDoc(ctx, data);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private ActionExecutionResult indexSingleDoc(WatchExecutionContext ctx, NestedValueMap data) throws ActionExecutionException {

        try {

            IndexRequest indexRequest = createIndexRequest(ctx, data, this.refreshPolicy);

            if (ctx.getSimulationMode() == SimulationMode.FOR_REAL) {
                IndexResponse indexResponse = ctx.getClient().index(indexRequest).get();

                if (log.isDebugEnabled()) {
                    log.debug("Result of " + this + ":\n" + Strings.toString(indexResponse));
                }
            }

            return new ActionExecutionResult(indexRequest);
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new ActionExecutionException(this, e);
        }
    }

    private ActionExecutionResult indexMultiDoc(WatchExecutionContext ctx, Collection<?> documents) throws ActionExecutionException {
        try {
            BulkRequest bulkRequest = new BulkRequest();

            for (Object data : documents) {
                if (data instanceof NestedValueMap) {
                    bulkRequest.add(createIndexRequest(ctx, (NestedValueMap) data, null));
                } else if (data instanceof Map) {
                    bulkRequest.add(createIndexRequest(ctx, NestedValueMap.copy((Map<?, ?>) data), null));

                }
            }

            if (refreshPolicy != null) {
                bulkRequest.setRefreshPolicy(refreshPolicy);
            }

            if (ctx.getSimulationMode() == SimulationMode.FOR_REAL) {
                BulkResponse response = ctx.getClient().bulk(bulkRequest).get();

                if (log.isDebugEnabled()) {
                    log.debug("Result of " + this + ":\n" + Strings.toString(response));
                }

                if (response.hasFailures()) {
                    throw new ActionExecutionException(this, "BulkRequest contains failures: " + response.buildFailureMessage());
                }
            }

            return new ActionExecutionResult(bulkRequest);

        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new ActionExecutionException(this, e);
        }
    }

    private IndexRequest createIndexRequest(WatchExecutionContext ctx, NestedValueMap data, RefreshPolicy refreshPolicy) throws IOException {

        String index = this.index;

        if (data.get("_index") != null) {
            index = String.valueOf(data.get("_index"));
        }

        String docId = null;

        if (data.get("_id") != null) {
            docId = String.valueOf(data.get("_id"));
        }

        if (docId == null && this.docId != null) {
            docId = this.docId.render(ctx.getTemplateScriptParamsAsMap());
        }

        IndexRequest indexRequest = new IndexRequest(index);

        if (docId != null) {
            indexRequest.id(docId);
        }

        indexRequest.timeout(new TimeValue(timeout != null ? timeout : 60, TimeUnit.SECONDS));

        if (refreshPolicy != null) {
            indexRequest.setRefreshPolicy(refreshPolicy);
        }

        try (XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()) {
            indexRequest.source(jsonBuilder.prettyPrint().map(data.without("_index", "_id")));
        }

        return indexRequest;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        if (timeout != null) {
            builder.field("timeout", timeout);
        }

        if (refreshPolicy != null) {
            builder.field("refresh", refreshPolicy.getValue());
        }

        if (index != null) {
            builder.field("index", index);
        }

        if (docId != null) {
            builder.field("doc_id", docId);
        }

        return builder;
    }

    public static class Factory extends ActionHandler.Factory<IndexAction> {
        public Factory() {
            super(IndexAction.TYPE);
        }

        @Override
        protected IndexAction create(WatchInitializationService watchInitService, ValidatingDocNode vJsonNode, ValidationErrors validationErrors)
                throws ConfigValidationException {
            String index = vJsonNode.get("index").asString();
            RefreshPolicy refreshPolicy = vJsonNode.get("refresh").expected("true|false|wait_for").byString((s) -> RefreshPolicy.parse(s));

            IndexAction result = new IndexAction(index, refreshPolicy);

            if (vJsonNode.hasNonNull("doc_id")) {
                result.docId = vJsonNode.get("doc_id").byString((s) -> InlineMustacheTemplate.parse(watchInitService.getScriptService(), s));
            }

            if (vJsonNode.hasNonNull("timeout")) {
                result.timeout = vJsonNode.get("timeout").asInt();
            }

            return result;
        }
    }

    public InlineMustacheTemplate<String> getDocId() {
        return docId;
    }

    public void setDocId(InlineMustacheTemplate<String> docId) {
        this.docId = docId;
    }

    public void setDocId(String docId) {
        this.docId = InlineMustacheTemplate.constant(docId);
    }
}
