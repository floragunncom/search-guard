/*
 * Copyright 2017-2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.femt;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.floragunn.searchguard.authz.TenantManager;
import com.floragunn.searchguard.enterprise.femt.request.handler.RequestHandler;
import com.floragunn.searchguard.enterprise.femt.request.handler.RequestHandlerFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.PrivilegesEvaluationException;
import com.floragunn.searchguard.authz.SyncAuthorizationFilter;
import com.floragunn.searchguard.authz.actions.Action;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.ResolvedIndices;
import com.floragunn.searchguard.authz.actions.Actions;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;

public class MultiTenancyAuthorizationFilter implements SyncAuthorizationFilter {

    private static final String TEMP_MIGRATION_INDEX_NAME_POSTFIX = "_reindex_temp";

    public static final String SG_FILTER_LEVEL_FEMT_DONE = ConfigConstants.SG_CONFIG_PREFIX + "filter_level_femt_done";

    private final Action KIBANA_ALL_SAVED_OBJECTS_WRITE;
    private final Action KIBANA_ALL_SAVED_OBJECTS_READ;

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final String kibanaServerUsername;
    private final String kibanaIndexName;
    private final String kibanaIndexNamePrefix;
    private final Pattern versionedKibanaIndexPattern;
    private final boolean enabled;
    private final ThreadContext threadContext;
    private final Client nodeClient;
    private final ImmutableList<String> indexSubNames = ImmutableList.of("alerting_cases", "analytics", "security_solution", "ingest");
    private final RoleBasedTenantAuthorization tenantAuthorization;
    private final FrontendDataMigrationInterceptor frontendDataMigrationInterceptor;
    private final RequestHandlerFactory requestHandlerFactory;
    private final TenantManager tenantManager;

    public MultiTenancyAuthorizationFilter(FeMultiTenancyConfig config, RoleBasedTenantAuthorization tenantAuthorization, TenantManager tenantManager,
                                           Actions actions, ThreadContext threadContext, Client nodeClient) {
        this.enabled = config.isEnabled();
        this.kibanaServerUsername = config.getServerUsername();
        this.kibanaIndexName = config.getIndex();
        this.kibanaIndexNamePrefix = this.kibanaIndexName + "_";
        this.versionedKibanaIndexPattern = Pattern.compile(
                "(" + Pattern.quote(this.kibanaIndexName) + toPatternFragment(indexSubNames) + ")" + "(_[0-9]+\\.[0-9]+\\.[0-9]+(_[0-9]{3})?)?" + "(" + Pattern.quote(TEMP_MIGRATION_INDEX_NAME_POSTFIX) + ")?");

        this.KIBANA_ALL_SAVED_OBJECTS_WRITE = KibanaActionsProvider.getKibanaWriteAction(actions);
        this.KIBANA_ALL_SAVED_OBJECTS_READ = KibanaActionsProvider.getKibanaReadAction(actions);
        this.threadContext = threadContext;
        this.nodeClient = nodeClient;
        this.tenantAuthorization = tenantAuthorization;
        this.frontendDataMigrationInterceptor = new FrontendDataMigrationInterceptor(threadContext, nodeClient, config);
        this.requestHandlerFactory = new RequestHandlerFactory(this.nodeClient, this.threadContext);
        this.tenantManager = tenantManager;
        log.info("Filter which supports front-end multi tenancy created, enabled '{}'.", enabled);
    }

    @Override
    public SyncAuthorizationFilter.Result apply(PrivilegesEvaluationContext context, ActionListener<?> listener) {
        if (!enabled) {
            log.trace("MultiTenancyAuthorizationFilter is disabled");
            return SyncAuthorizationFilter.Result.OK;
        }

        if (threadContext.getHeader(SG_FILTER_LEVEL_FEMT_DONE) != null) {
            if (log.isDebugEnabled()) {
                log.debug("FEMT is already done for: " + threadContext.getHeader(SG_FILTER_LEVEL_FEMT_DONE));
            }

            return SyncAuthorizationFilter.Result.PASS_ON_FAST_LANE;
        }

        User user = context.getUser();
        ResolvedIndices requestedResolved = context.getRequestInfo().getResolvedIndices();

        if (log.isDebugEnabled()) {
            log.debug("apply(" + context.getAction() + ", " + user + ")\nrequestedResolved: " + requestedResolved + "\nrequestedTenant: "
                    + user.getRequestedTenant());
        }

        List<IndexInfo> kibanaIndicesInfo = checkForExclusivelyUsedKibanaIndexOrAlias((ActionRequest) context.getRequest(), requestedResolved);

        if (kibanaIndicesInfo.isEmpty()) {
            // This is not about the .kibana index: Nothing to do here, get out early!
            return SyncAuthorizationFilter.Result.OK;
        }

        if (log.isDebugEnabled()) {
            log.debug("IndexInfo: " + kibanaIndicesInfo);
        }

        Result initializationProcessingResult = frontendDataMigrationInterceptor.process(getOriginalIndicesNames(kibanaIndicesInfo), context, listener);

        if (Result.PASS_ON_FAST_LANE != initializationProcessingResult) {
            return initializationProcessingResult;
        }

        String requestedTenant = user.getRequestedTenant();

        log.trace("User's '{}' requested tenant is '{}'", user.getName(), requestedTenant);

        if (tenantManager.isTenantHeaderEmpty(requestedTenant)) {
            //requestedTenant = Tenant.GLOBAL_TENANT_ID;
            return SyncAuthorizationFilter.Result.OK;
        }
        try {
            if (user.getName().equals(kibanaServerUsername) || isTenantAllowed(context, (ActionRequest) context.getRequest(), context.getAction(), requestedTenant)) {
                return handle(context, requestedTenant, listener);
            } else {
                return SyncAuthorizationFilter.Result.DENIED;
            }
        } catch (PrivilegesEvaluationException e) {
            // TODO
            log.error(e, e);
            return SyncAuthorizationFilter.Result.DENIED;
        }
    }

    private SyncAuthorizationFilter.Result handle(PrivilegesEvaluationContext context, String requestedTenant, ActionListener<?> listener) {

        Object request = context.getRequest();

        Optional<RequestHandler<ActionRequest>> requestHandler = requestHandlerFactory.requestHandlerFor(request);

        String internalTenantName = tenantManager.toInternalTenantName(context.getUser());

        return requestHandler
                .map(handler -> handler.handle(context, internalTenantName, (ActionRequest) request, listener))
                .orElseGet(() -> {
                    if (log.isTraceEnabled()) {
                        log.trace("Not giving {} special treatment for FEMT", request);
                    }
                    return SyncAuthorizationFilter.Result.OK;
                });
    }

    private boolean isTenantAllowed(PrivilegesEvaluationContext context, ActionRequest request, Action action, String requestedTenant)
            throws PrivilegesEvaluationException {

        if (!tenantManager.isTenantHeaderValid(requestedTenant)) {
            log.warn("Invalid tenant: " + requestedTenant + "; user: " + context.getUser());

            return false;
        }

        if (tenantManager.isUserTenantHeader(requestedTenant)) {
            return true;
        }

        boolean hasReadPermission = tenantAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_READ, requestedTenant).isOk();
        boolean hasWritePermission = tenantAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_WRITE, requestedTenant).isOk();

        hasReadPermission |= hasWritePermission;

        if (!hasReadPermission) {
            log.warn("Tenant {} is not allowed for user {}", requestedTenant, context.getUser().getName());
            return false;
        }

        if (!hasWritePermission && action.name().startsWith("indices:data/write")) {
            log.warn("Tenant {} is not allowed to write (user: {})", requestedTenant, context.getUser().getName());
            return false;
        }

        return true;
    }

    private List<IndexInfo> checkForExclusivelyUsedKibanaIndexOrAlias(ActionRequest request, ResolvedIndices requestedResolved) {
        if (requestedResolved.isLocalAll()) {
            return Collections.emptyList();
        }
        Set<String> allQueryIndices = getIndices(request);
        List<IndexInfo> multiTenancyRelatedIndices = allQueryIndices//
            .stream() //
            .map(indexName -> checkForExclusivelyUsedKibanaIndexOrAlias(indexName)) //
            .filter(Objects::nonNull) //
            .collect(Collectors.toList());
        if((multiTenancyRelatedIndices.size() > 0) && (allQueryIndices.size() != multiTenancyRelatedIndices.size())) {
            String indicesNames = String.join(", ", allQueryIndices);
            log.debug("Request '{}' is related to multi-tenancy indices and some other indices '{}'", request.getClass(), indicesNames);
        }
        return multiTenancyRelatedIndices;
    }

    private IndexInfo checkForExclusivelyUsedKibanaIndexOrAlias(String aliasOrIndex) {

        if (aliasOrIndex.equals(kibanaIndexName)) {
            // Pre 7.12: Just .kibana
            return new IndexInfo(aliasOrIndex, kibanaIndexName, null);
        }

        if (aliasOrIndex.startsWith(kibanaIndexNamePrefix)) {
            Matcher matcher = versionedKibanaIndexPattern.matcher(aliasOrIndex);

            if (matcher.matches()) {
                // Post 7.12: .kibana_7.12.0
                // Prefix will be: .kibana_
                // Suffix will be: _7.12.0
                return new IndexInfo(aliasOrIndex, matcher.group(1), matcher.group(2));
            }

        }

        return null;
    }

    private Set<String> getOriginalIndicesNames(List<IndexInfo> indicesInfo) {
        return indicesInfo.stream()
                .map(info -> info.originalName)
                .collect(Collectors.toSet());
    }

    private Set<String> getIndices(ActionRequest request) {
        if (request instanceof PutMappingRequest) {
            PutMappingRequest putMappingRequest = (PutMappingRequest) request;

            return ImmutableSet.of(putMappingRequest.getConcreteIndex() != null ? putMappingRequest.getConcreteIndex().getName() : null,
                    putMappingRequest.indices());
        } else if (request instanceof IndicesRequest) {
            if (((IndicesRequest) request).indices() != null) {
                return ImmutableSet.of(Arrays.asList(((IndicesRequest) request).indices()));
            } else {
                return Collections.emptySet();
            }
        } else if (request instanceof BulkRequest) {
            return ((BulkRequest) request).getIndices();
        } else if (request instanceof MultiGetRequest) {
            return ((MultiGetRequest) request).getItems().stream().map(item -> item.index()).collect(Collectors.toSet());
        } else if (request instanceof MultiSearchRequest) {
            return ((MultiSearchRequest) request).requests().stream().flatMap(r -> Arrays.stream(r.indices())).collect(Collectors.toSet());
        } else if (request instanceof MultiTermVectorsRequest) {
            return ((MultiTermVectorsRequest) request).getRequests().stream().flatMap(r -> Arrays.stream(r.indices())).collect(Collectors.toSet());
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Not supported for multi tenancy: {} ", request);
            }

            return Collections.emptySet();
        }
    }

    private class IndexInfo {
        final String originalName;
        final String prefix;
        final String suffix;

        IndexInfo(String originalName, String prefix, String suffix) {
            this.originalName = originalName;
            this.prefix = prefix;
            this.suffix = suffix;
        }

        @Override
        public String toString() {
            return "IndexInfo [originalName=" + originalName + ", prefix=" + prefix + ", suffix=" + suffix + "]";
        }

    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getKibanaIndex() {
        return kibanaIndexName;
    }

    public String getKibanaServerUser() {
        return kibanaServerUsername;
    }

    public Map<String, Boolean> mapTenants(User user, ImmutableSet<String> roles) {
        if (user == null) {
            return ImmutableMap.empty();
        }

        ImmutableMap.Builder<String, Boolean> result = new ImmutableMap.Builder<>(roles.size());
        result.put(user.getName(), true);

        PrivilegesEvaluationContext context = new PrivilegesEvaluationContext(user, roles, null, null, false, null, null);

        for (String tenant : tenantManager.getAllKnownTenantNames()) {
            try {
                boolean hasReadPermission = tenantAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_READ, tenant).isOk();
                boolean hasWritePermission = tenantAuthorization.hasTenantPermission(context, KIBANA_ALL_SAVED_OBJECTS_WRITE, tenant).isOk();

                if (hasWritePermission) {
                    result.put(tenant, true);
                } else if (hasReadPermission) {
                    result.put(tenant, false);
                }
            } catch (PrivilegesEvaluationException e) {
                log.error("Error while evaluating privileges for " + user + " " + tenant, e);
            }
        }

        return result.build();
    }

    private String toPatternFragment(Collection<String> indexSuffixes) {
        StringBuilder result = new StringBuilder("(?:");

        for (String suffix : indexSuffixes) {
            // An empty first element is expected in the result
            result.append("|_");
            result.append(Pattern.quote(suffix));
        }

        result.append(")");

        return result.toString();
    }
}
