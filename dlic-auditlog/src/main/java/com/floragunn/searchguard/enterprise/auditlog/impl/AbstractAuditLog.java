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

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.patch.JsonPatch;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.enterprise.auditlog.AuditLogConfig;
import com.floragunn.searchguard.enterprise.auditlog.impl.AuditMessage.Category;
import com.floragunn.searchguard.support.Base64Helper;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchguard.user.UserInformation;
import com.floragunn.searchsupport.PrivilegedCode;
import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.engine.Engine.Delete;
import org.elasticsearch.index.engine.Engine.DeleteResult;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.engine.Engine.IndexResult;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

public abstract class AbstractAuditLog implements AuditLog {

    protected final Logger log = LogManager.getLogger(this.getClass());
    protected final ThreadPool threadPool;
    protected final IndexNameExpressionResolver resolver;

    protected volatile ClusterState clusterState = null;

    protected final Settings settings;
    protected final boolean restAuditingEnabled;
    protected final boolean transportAuditingEnabled;
    protected final boolean resolveBulkRequests;

    protected final boolean logRequestBody;
    protected final boolean resolveIndices;

    private Pattern ignoredRequestBodies;
    private Pattern ignoredAuditUsers;
    private Pattern ignoredComplianceUsersForRead;
    private Pattern ignoredComplianceUsersForWrite;
    private Pattern ignoreAuditRequests;
    private final List<String> disabledRestCategories;
    private final List<String> disabledTransportCategories;
    private final List<String> defaultDisabledCategories = Arrays.asList(Category.AUTHENTICATED.toString(), Category.GRANTED_PRIVILEGES.toString());
    private final List<String> defaultIgnoredUsers = Collections.singletonList("kibanaserver");
    private final boolean excludeSensitiveHeaders;
    private final boolean logEnvVars;
    private AuditLogConfig complianceConfig;

    private final Pattern searchguardIndexPattern;
    protected final ConfigurationRepository configurationRepository;
    private static final List<String> writeClasses = new ArrayList<>();

    {
        writeClasses.add(IndexRequest.class.getSimpleName());
        writeClasses.add(UpdateRequest.class.getSimpleName());
        writeClasses.add(BulkRequest.class.getSimpleName());
        writeClasses.add(BulkShardRequest.class.getSimpleName());
        writeClasses.add(DeleteRequest.class.getSimpleName());
    }

    protected AbstractAuditLog(Settings settings, final ThreadPool threadPool, final IndexNameExpressionResolver resolver,
            final ClusterService clusterService, ConfigurationRepository configurationRepository) {
        super();
        ValidationErrors validationErrors = new ValidationErrors();
        this.threadPool = threadPool;

        this.settings = settings;
        this.resolver = resolver;

        if (clusterService != null) {
            clusterService.addListener(new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    clusterState = event.state();
                }
            });
        }

        this.configurationRepository = configurationRepository;
        this.searchguardIndexPattern = configurationRepository != null ? configurationRepository.getConfiguredSearchguardIndices() : Pattern.blank();

        resolveBulkRequests = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_BULK_REQUESTS, false);

        restAuditingEnabled = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_REST, true);
        transportAuditingEnabled = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_AUDIT_ENABLE_TRANSPORT, true);
        logEnvVars = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_EXTERNAL_CONFIG_ENV_VARS_ENABLED, true);

        disabledRestCategories = new ArrayList<>(
                settings.getAsList(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_REST_CATEGORIES, defaultDisabledCategories).stream()
                        .map(c -> c.toUpperCase()).collect(Collectors.toList()));

        if (disabledRestCategories.size() == 1 && "NONE".equals(disabledRestCategories.get(0))) {
            disabledRestCategories.clear();
        }

        if (disabledRestCategories.size() > 0) {
            log.info("Configured categories on rest layer to ignore: {}", disabledRestCategories);
        }

        disabledTransportCategories = new ArrayList<>(
                settings.getAsList(ConfigConstants.SEARCHGUARD_AUDIT_CONFIG_DISABLED_TRANSPORT_CATEGORIES, defaultDisabledCategories).stream()
                        .map(c -> c.toUpperCase()).collect(Collectors.toList()));

        if (disabledTransportCategories.size() == 1 && "NONE".equals(disabledTransportCategories.get(0))) {
            disabledTransportCategories.clear();
        }

        if (disabledTransportCategories.size() > 0) {
            log.info("Configured categories on transport layer to ignore: {}", disabledTransportCategories);
        }

        logRequestBody = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_AUDIT_LOG_REQUEST_BODY, true);
        List<String> ignoredRequestBodies = settings.getAsList(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_REQUEST_BODIES, Collections.emptyList());
        try {
            this.ignoredRequestBodies = Pattern.create(ignoredRequestBodies);
        } catch (ConfigValidationException e) {
            validationErrors.add(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_REQUEST_BODIES, e);
            this.ignoredRequestBodies = Pattern.blank();
        }

        resolveIndices = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_AUDIT_RESOLVE_INDICES, true);

        List<String> ignoredAuditUsers = new ArrayList<>(settings.getAsList(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_USERS, defaultIgnoredUsers));

        if (ignoredAuditUsers.size() == 0 || (ignoredAuditUsers.size() == 1 && "NONE".equals(ignoredAuditUsers.get(0)))) {
            this.ignoredAuditUsers = Pattern.blank();
        } else {
            log.info("Configured Users to ignore: {}", ignoredAuditUsers);
            try {
                this.ignoredAuditUsers = Pattern.create(ignoredAuditUsers);
            } catch (ConfigValidationException e) {
                validationErrors.add(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_USERS, e);
                this.ignoredAuditUsers = Pattern.blank();
            }
        }

        List<String> ignoredComplianceUsersForRead = new ArrayList<>(
                settings.getAsList(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_IGNORE_USERS, defaultIgnoredUsers));

        if (ignoredComplianceUsersForRead.size() == 0
                || (ignoredComplianceUsersForRead.size() == 1 && "NONE".equals(ignoredComplianceUsersForRead.get(0)))) {
            this.ignoredComplianceUsersForRead = Pattern.blank();
        } else {
            log.info("Configured Users to ignore for read compliance events: {}", ignoredComplianceUsersForRead);
            try {
                this.ignoredComplianceUsersForRead = Pattern.create(ignoredComplianceUsersForRead);
            } catch (ConfigValidationException e) {
                validationErrors.add(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_READ_IGNORE_USERS, e);
                this.ignoredComplianceUsersForRead = Pattern.blank();
            }
        }

        List<String> ignoredComplianceUsersForWrite = new ArrayList<>(
                settings.getAsList(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_IGNORE_USERS, defaultIgnoredUsers));

        if (ignoredComplianceUsersForRead.size() == 0
                || (ignoredComplianceUsersForWrite.size() == 1 && "NONE".equals(ignoredComplianceUsersForWrite.get(0)))) {
            this.ignoredComplianceUsersForWrite = Pattern.blank();
        } else {
            log.info("Configured Users to ignore for write compliance events: {}", ignoredComplianceUsersForWrite);
            try {
                this.ignoredComplianceUsersForWrite = Pattern.create(ignoredComplianceUsersForWrite);
            } catch (ConfigValidationException e) {
                validationErrors.add(ConfigConstants.SEARCHGUARD_COMPLIANCE_HISTORY_WRITE_IGNORE_USERS, e);
                this.ignoredComplianceUsersForWrite = Pattern.blank();
            }
        }

        List<String> ignoreAuditRequests = settings.getAsList(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_REQUESTS, Collections.emptyList());
        try {
            this.ignoreAuditRequests = Pattern.create(ignoreAuditRequests);
        } catch (ConfigValidationException e) {
            validationErrors.add(ConfigConstants.SEARCHGUARD_AUDIT_IGNORE_REQUESTS, e);
            this.ignoreAuditRequests = Pattern.blank();
        }

        // check if some categories are invalid
        for (String event : disabledRestCategories) {
            try {
                AuditMessage.Category.valueOf(event.toUpperCase());
            } catch (Exception iae) {
                log.error("Unknown category {}, please check searchguard.audit.config.disabled_categories settings", event);
            }
        }

        // check if some categories are invalid
        for (String event : disabledTransportCategories) {
            try {
                AuditMessage.Category.valueOf(event.toUpperCase());
            } catch (Exception iae) {
                log.error("Unknown category {}, please check searchguard.audit.config.disabled_categories settings", event);
            }
        }

        this.excludeSensitiveHeaders = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_AUDIT_EXCLUDE_SENSITIVE_HEADERS, true);

        if (validationErrors.size() != 0) {
            log.error("The audit log configuration contains errors:\n" + validationErrors);
        }
    }

    public void setComplianceConfig(AuditLogConfig complianceConfig) {
        this.complianceConfig = complianceConfig;
    }

    @Override
    public void logFailedLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, TransportRequest request, Task task) {
        final String action = null;

        if (!checkTransportFilter(Category.FAILED_LOGIN, action, effectiveUser, request)) {
            return;
        }

        final TransportAddress remoteAddress = getRemoteAddress();
        final List<AuditMessage> msgs = RequestResolver.resolve(Category.FAILED_LOGIN, getOrigin(), action, null, effectiveUser, sgadmin,
                initiatingUser, remoteAddress, request, getThreadContextHeaders(), task, resolver, clusterState, settings, logRequestBody, ignoredRequestBodies,
                resolveIndices, resolveBulkRequests, searchguardIndexPattern, excludeSensitiveHeaders, null);

        for (AuditMessage msg : msgs) {
            save(msg);
        }
    }

    @Override
    public void logFailedLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request) {

        if (!checkRestFilter(Category.FAILED_LOGIN, effectiveUser, request)) {
            return;
        }

        AuditMessage msg = new AuditMessage(Category.FAILED_LOGIN, clusterState, getOrigin(), Origin.REST);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);

        if (request != null) {
            msg.addPath(request.path());
            msg.addRestHeaders(request.getHeaders(), excludeSensitiveHeaders);
            msg.addRestParams(request.params());
        }

        msg.addInitiatingUser(initiatingUser);
        msg.addEffectiveUser(effectiveUser);
        msg.addIsAdminDn(sgadmin);

        save(msg);
    }

    @Override
    public void logBlockedUser(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, TransportRequest request, Task task) {

        final String action = null;

        if (!checkTransportFilter(Category.BLOCKED_USER, action, effectiveUser, request)) {
            return;
        }

        final TransportAddress remoteAddress = getRemoteAddress();
        final List<AuditMessage> msgs = RequestResolver.resolve(Category.BLOCKED_USER, getOrigin(), action, null, effectiveUser, sgadmin,
                initiatingUser, remoteAddress, request, getThreadContextHeaders(), task, resolver, clusterState, settings, logRequestBody, ignoredRequestBodies,
                resolveIndices, resolveBulkRequests, searchguardIndexPattern, excludeSensitiveHeaders, null);

        for (AuditMessage msg : msgs) {
            save(msg);
        }
    }

    @Override
    public void logBlockedUser(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request) {
        if (!checkRestFilter(Category.BLOCKED_USER, effectiveUser, request)) {
            return;
        }

        AuditMessage msg = new AuditMessage(Category.BLOCKED_USER, clusterState, getOrigin(), Origin.REST);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);

        if (request != null) {
            msg.addPath(request.path());
            msg.addRestHeaders(request.getHeaders(), excludeSensitiveHeaders);
            msg.addRestParams(request.params());
        }

        msg.addInitiatingUser(initiatingUser);
        msg.addEffectiveUser(effectiveUser);
        msg.addIsAdminDn(sgadmin);

        save(msg);
    }

    @Override
    public void logSucceededLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, TransportRequest request,
            String action, Task task) {

        if (!checkTransportFilter(Category.AUTHENTICATED, action, effectiveUser, request)) {
            return;
        }

        final TransportAddress remoteAddress = getRemoteAddress();
        final List<AuditMessage> msgs = RequestResolver.resolve(Category.AUTHENTICATED, getOrigin(), action, null, effectiveUser, sgadmin,
                initiatingUser, remoteAddress, request, getThreadContextHeaders(), task, resolver, clusterState, settings, logRequestBody, ignoredRequestBodies,
                resolveIndices, resolveBulkRequests, searchguardIndexPattern, excludeSensitiveHeaders, null);

        for (AuditMessage msg : msgs) {
            save(msg);
        }
    }

    @Override
    public void logSucceededLogin(UserInformation effectiveUser, boolean sgadmin, UserInformation initiatingUser, RestRequest request) {

        if (!checkRestFilter(Category.AUTHENTICATED, effectiveUser, request)) {
            return;
        }

        AuditMessage msg = new AuditMessage(Category.AUTHENTICATED, clusterState, getOrigin(), Origin.REST);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);
        if (request != null) {
            msg.addPath(request.path());
            msg.addRestHeaders(request.getHeaders(), excludeSensitiveHeaders);
            msg.addRestParams(request.params());
        }
        msg.addInitiatingUser(initiatingUser);
        msg.addEffectiveUser(effectiveUser);
        msg.addIsAdminDn(sgadmin);

        if (request != null && logRequestBody && request.hasContentOrSourceParam() && !ignoredRequestBodies.matches(request.path())) {
            AuditRestRequestBodyReader.readRequestBody(request, body -> {
                msg.addTupleToRequestBody(new Tuple<>(request.getXContentType(), body));
                save(msg);
            });
        } else {
            save(msg);
        }
    }

    @Override
    public void logMissingPrivileges(String privilege, UserInformation effectiveUser, RestRequest request) {
        if (!checkRestFilter(Category.MISSING_PRIVILEGES, effectiveUser, request)) {
            return;
        }

        AuditMessage msg = new AuditMessage(Category.MISSING_PRIVILEGES, clusterState, getOrigin(), Origin.REST);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);

        if (request != null) {
            msg.addPath(request.path());
            msg.addRestHeaders(request.getHeaders(), excludeSensitiveHeaders);
            msg.addRestParams(request.params());
        }

        msg.addEffectiveUser(effectiveUser);

        save(msg);
    }

    @Override
    public void logMissingPrivileges(String privilege, TransportRequest request, Task task) {
        final String action = null;

        if (!checkTransportFilter(Category.MISSING_PRIVILEGES, privilege, getUser(), request)) {
            return;
        }

        final TransportAddress remoteAddress = getRemoteAddress();
        final List<AuditMessage> msgs = RequestResolver.resolve(Category.MISSING_PRIVILEGES, getOrigin(), action, privilege, getUser(), null, null,
                remoteAddress, request, getThreadContextHeaders(), task, resolver, clusterState, settings, logRequestBody, ignoredRequestBodies, resolveIndices,
                resolveBulkRequests, searchguardIndexPattern, excludeSensitiveHeaders, null);

        for (AuditMessage msg : msgs) {
            save(msg);
        }
    }

    @Override
    public void logGrantedPrivileges(String privilege, TransportRequest request, Task task) {
        final String action = null;

        if (!checkTransportFilter(Category.GRANTED_PRIVILEGES, privilege, getUser(), request)) {
            return;
        }

        final TransportAddress remoteAddress = getRemoteAddress();
        final List<AuditMessage> msgs = RequestResolver.resolve(Category.GRANTED_PRIVILEGES, getOrigin(), action, privilege, getUser(), null, null,
                remoteAddress, request, getThreadContextHeaders(), task, resolver, clusterState, settings, logRequestBody, ignoredRequestBodies, resolveIndices,
                resolveBulkRequests, searchguardIndexPattern, excludeSensitiveHeaders, null);

        for (AuditMessage msg : msgs) {
            save(msg);
        }
    }

    @Override
    public void logBadHeaders(TransportRequest request, String action, Task task) {

        if (!checkTransportFilter(Category.BAD_HEADERS, action, getUser(), request)) {
            return;
        }

        final TransportAddress remoteAddress = getRemoteAddress();
        final List<AuditMessage> msgs = RequestResolver.resolve(Category.BAD_HEADERS, getOrigin(), action, null, getUser(), null, null, remoteAddress,
                request, getThreadContextHeaders(), task, resolver, clusterState, settings, logRequestBody, ignoredRequestBodies, resolveIndices, resolveBulkRequests,
                searchguardIndexPattern, excludeSensitiveHeaders, null);

        for (AuditMessage msg : msgs) {
            save(msg);
        }
    }

    @Override
    public void logBadHeaders(RestRequest request) {

        if (!checkRestFilter(Category.BAD_HEADERS, getUser(), request)) {
            return;
        }

        AuditMessage msg = new AuditMessage(Category.BAD_HEADERS, clusterState, getOrigin(), Origin.REST);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);

        if (request != null) {
            msg.addPath(request.path());
            msg.addRestHeaders(request.getHeaders(), excludeSensitiveHeaders);
            msg.addRestParams(request.params());
        }

        msg.addEffectiveUser(getUser());

        save(msg);
    }

    @Override
    public void logBlockedIp(TransportRequest request, String action, TransportAddress remoteAddress, Task task) {
        if (!checkTransportFilter(Category.BLOCKED_IP, action, getUser(), request)) {
            return;
        }

        final List<AuditMessage> msgs = RequestResolver.resolve(Category.BLOCKED_IP, getOrigin(), action, null, getUser(), null, null, remoteAddress,
                request, getThreadContextHeaders(), task, resolver, clusterState, settings, logRequestBody, ignoredRequestBodies, resolveIndices, resolveBulkRequests,
                searchguardIndexPattern, excludeSensitiveHeaders, null);

        for (AuditMessage msg : msgs) {
            save(msg);
        }
    }

    @Override
    public void logBlockedIp(RestRequest request, InetSocketAddress remoteAddress) {
        if (!checkRestFilter(Category.BLOCKED_IP, getUser(), request)) {
            return;
        }

        AuditMessage msg = new AuditMessage(Category.BLOCKED_IP, clusterState, getOrigin(), Origin.REST);
        // getAddress() call is checked in BackendRegistry for null
        msg.addRemoteAddress(remoteAddress.getAddress().getHostAddress());

        if (request != null) {
            msg.addPath(request.path());
            msg.addRestHeaders(request.getHeaders(), excludeSensitiveHeaders);
            msg.addRestParams(request.params());
        }

        msg.addEffectiveUser(getUser());

        save(msg);
    }

    @Override
    public void logSgIndexAttempt(TransportRequest request, String action, Task task) {

        if (!checkTransportFilter(Category.SG_INDEX_ATTEMPT, action, getUser(), request)) {
            return;
        }

        final TransportAddress remoteAddress = getRemoteAddress();
        final List<AuditMessage> msgs = RequestResolver.resolve(Category.SG_INDEX_ATTEMPT, getOrigin(), action, null, getUser(), false, null,
                remoteAddress, request, getThreadContextHeaders(), task, resolver, clusterState, settings, logRequestBody, ignoredRequestBodies, resolveIndices,
                resolveBulkRequests, searchguardIndexPattern, excludeSensitiveHeaders, null);

        for (AuditMessage msg : msgs) {
            save(msg);
        }
    }

    @Override
    public void logImmutableIndexAttempt(TransportRequest request, String action, Task task) {

        if (!checkTransportFilter(Category.COMPLIANCE_IMMUTABLE_INDEX_ATTEMPT, action, getUser(), request)) {
            return;
        }

        final TransportAddress remoteAddress = getRemoteAddress();
        final List<AuditMessage> msgs = RequestResolver.resolve(Category.COMPLIANCE_IMMUTABLE_INDEX_ATTEMPT, getOrigin(), action, null, getUser(),
                false, null, remoteAddress, request, getThreadContextHeaders(), task, resolver, clusterState, settings, logRequestBody, ignoredRequestBodies,
                resolveIndices, resolveBulkRequests, searchguardIndexPattern, excludeSensitiveHeaders, null);

        for (AuditMessage msg : msgs) {
            save(msg);
        }
    }

    @Override
    public void logSSLException(TransportRequest request, Throwable t, String action, Task task) {

        if (!checkTransportFilter(Category.SSL_EXCEPTION, action, getUser(), request)) {
            return;
        }

        final TransportAddress remoteAddress = getRemoteAddress();

        final List<AuditMessage> msgs = RequestResolver.resolve(Category.SSL_EXCEPTION, Origin.TRANSPORT, action, null, getUser(), false, null,
                remoteAddress, request, getThreadContextHeaders(), task, resolver, clusterState, settings, logRequestBody, ignoredRequestBodies, resolveIndices,
                resolveBulkRequests, searchguardIndexPattern, excludeSensitiveHeaders, t);

        for (AuditMessage msg : msgs) {
            save(msg);
        }
    }

    @Override
    public void logSSLException(RestRequest request, Throwable t) {

        if (!checkRestFilter(Category.SSL_EXCEPTION, getUser(), request)) {
            return;
        }

        AuditMessage msg = new AuditMessage(Category.SSL_EXCEPTION, clusterState, Origin.REST, Origin.REST);

        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);

        if (request != null) {
            msg.addPath(request.path());
            msg.addRestHeaders(request.getHeaders(), excludeSensitiveHeaders);
            msg.addRestParams(request.params());
        }
        msg.addException(t);
        msg.addEffectiveUser(getUser());

        save(msg);
    }

    @Override
    public void logDocumentRead(String index, String id, ShardId shardId, Map<String, String> fieldNameValues) {

        if (complianceConfig == null || !complianceConfig.readHistoryEnabledForIndex(index)) {
            return;
        }

        final String initiatingRequestClass = threadPool.getThreadContext().getHeader(ConfigConstants.SG_INITIAL_ACTION_CLASS_HEADER);

        if (initiatingRequestClass != null && writeClasses.contains(initiatingRequestClass)) {
            return;
        }

        Category category = searchguardIndexPattern.matches(index) ? Category.COMPLIANCE_INTERNAL_CONFIG_READ : Category.COMPLIANCE_DOC_READ;

        UserInformation effectiveUser = getUser();
        if (!checkComplianceFilter(category, effectiveUser, getOrigin())) {
            return;
        }

        if (fieldNameValues != null && !fieldNameValues.isEmpty()) {
            AuditMessage msg = new AuditMessage(category, clusterState, getOrigin(), null);
            TransportAddress remoteAddress = getRemoteAddress();
            msg.addRemoteAddress(remoteAddress);
            msg.addEffectiveUser(effectiveUser);
            msg.addIndices(new String[] { index });
            msg.addResolvedIndices(new String[] { index });
            msg.addShardId(shardId);
            //msg.addIsAdminDn(sgAdmin);
            msg.addId(id);

            try {
                if (complianceConfig.logReadMetadataOnly()) {
                    try {
                        XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent);
                        builder.startObject();
                        builder.field("field_names", fieldNameValues.keySet());
                        builder.endObject();
                        builder.close();
                        msg.addUnescapedJsonToRequestBody(Strings.toString(builder));
                    } catch (IOException e) {
                        log.error(e.toString(), e);
                    }
                } else {
                    if (searchguardIndexPattern.matches(index) && !"tattr".equals(id) && !"*".equals(id)) {
                        try {
                            Map<String, String> map = fieldNameValues.entrySet().stream()
                                    .filter(e->e.getKey().equals(id))
                                    .collect(Collectors.toMap(entry -> "id",
                                            entry -> decodeBase64IfNecessary(entry.getValue())));
//### This is not working ###
//                            Map<String, String> map = fieldNameValues.entrySet().stream()
//                                    .filter(e -> e.getKey().equals(id))
//                                    .collect(Collectors.toMap(entry -> "id",
//                                            entry -> new String(BaseEncoding.base64().decode(entry.getValue()), StandardCharsets.UTF_8)));
//###
                            msg.addMapToRequestBody(Utils.convertJsonToxToStructuredMap(map.get("id")));
                        } catch (Exception e) {
                            log.error("Unexpected Exception {}", e, e);
                        }

                    } else {
                        msg.addMapToRequestBody(new HashMap<String, Object>(fieldNameValues));
                    }
                }
            } catch (Exception e) {
                log.error("Unable to generate request body for {} and {}", msg.toPrettyString(), fieldNameValues, e);
            }

            save(msg);
        }

    }

    private static String decodeBase64IfNecessary(String base64EncodedOrPlainJson) {

        if(base64EncodedOrPlainJson == null || base64EncodedOrPlainJson.isEmpty()) {
            return base64EncodedOrPlainJson;
        }

        if(base64EncodedOrPlainJson.stripLeading().startsWith("{")) {
            //is already plain json
            return base64EncodedOrPlainJson;
        }

        //assume base64 encoded
        //if not, fallback and return plain value
        try {
            return new String(BaseEncoding.base64().decode(base64EncodedOrPlainJson), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            //thrown if there is underlying decoding exception
            return base64EncodedOrPlainJson;
        }
    }

    @Override
    public void logDocumentWritten(ShardId shardId, GetResult originalResult, Index currentIndex, IndexResult result) {

        if (complianceConfig == null || !complianceConfig.writeHistoryEnabledForIndex(shardId.getIndexName())) {
            return;
        }

        Category category = searchguardIndexPattern.matches(shardId.getIndexName()) ? Category.COMPLIANCE_INTERNAL_CONFIG_WRITE
                : Category.COMPLIANCE_DOC_WRITE;

        UserInformation effectiveUser = getUser();

        if (!checkComplianceFilter(category, effectiveUser, getOrigin())) {
            return;
        }

        AuditMessage msg = new AuditMessage(category, clusterState, getOrigin(), null);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);
        msg.addEffectiveUser(effectiveUser);
        msg.addIndices(new String[] { shardId.getIndexName() });
        msg.addResolvedIndices(new String[] { shardId.getIndexName() });
        msg.addId(currentIndex.id());
        msg.addShardId(shardId);
        msg.addComplianceDocVersion(result.getVersion());
        msg.addComplianceOperation(result.isCreated() ? Operation.CREATE : Operation.UPDATE);

        if (complianceConfig.logDiffsForWrite() && originalResult != null && originalResult.isExists()
                && originalResult.internalSourceRef() != null) {
            try {
                String originalSource = null;
                String currentSource = null;
                if (searchguardIndexPattern.matches(shardId.getIndexName())) {
                    try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, originalResult.internalSourceRef(), XContentType.JSON)) {
                        Object base64 = parser.map().values().iterator().next();
                        if (base64 instanceof String) {
                            originalSource = (new String(BaseEncoding.base64().decode((String) base64)));
                        } else {
                            originalSource = XContentHelper.convertToJson(originalResult.internalSourceRef(), false, XContentType.JSON);
                        }
                    } catch (Exception e) {
                        log.error(e);
                    }

                    try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, currentIndex.source(), XContentType.JSON)) {
                        Object base64 = parser.map().values().iterator().next();
                        if (base64 instanceof String) {
                            currentSource = (new String(BaseEncoding.base64().decode((String) base64)));
                        } else {
                            currentSource = XContentHelper.convertToJson(currentIndex.source(), false, XContentType.JSON);
                        }
                    } catch (Exception e) {
                        log.error(e);
                    }
                } else {
                    originalSource = XContentHelper.convertToJson(originalResult.internalSourceRef(), false, XContentType.JSON);
                    currentSource = XContentHelper.convertToJson(currentIndex.source(), false, XContentType.JSON);
                }
                DocNode originalDocument = DocNode.parse(Format.JSON).from(originalSource);
                DocNode currentDocument = DocNode.parse(Format.JSON).from(currentSource);

                JsonPatch diff = JsonPatch.fromDiff(originalDocument, currentDocument);

                msg.addComplianceWriteDiffSource(diff.isEmpty() ? "" : diff.toJsonString());
            } catch (Exception e) {
                log.error("Unable to generate diff for {}", msg.toPrettyString(), e);
            }
        }

        if (!complianceConfig.logWriteMetadataOnly()) {
            if (searchguardIndexPattern.matches(shardId.getIndexName())) {
                //current source, normally not null or empty
                try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        currentIndex.source(), XContentType.JSON)) {
                    Object base64 = parser.map().values().iterator().next();
                    if (base64 instanceof String) {
                        msg.addUnescapedJsonToRequestBody(new String(BaseEncoding.base64().decode((String) base64)));
                    } else {
                        msg.addTupleToRequestBody(new Tuple<XContentType, BytesReference>(XContentType.JSON, currentIndex.source()));
                    }
                } catch (Exception e) {
                    log.error(e);
                }

                //if we want to have msg.ComplianceWritePreviousSource we need to do the same as above

            } else {

                //previous source, can be null if document is a new one
                //msg.ComplianceWritePreviousSource(new Tuple<XContentType, BytesReference>(XContentType.JSON, originalResult.internalSourceRef()));

                //current source, normally not null or empty
                msg.addTupleToRequestBody(new Tuple<XContentType, BytesReference>(XContentType.JSON, currentIndex.source()));
            }

        }

        save(msg);
    }

    @Override
    public void logDocumentDeleted(ShardId shardId, Delete delete, DeleteResult result) {

        UserInformation effectiveUser = getUser();

        if (!checkComplianceFilter(Category.COMPLIANCE_DOC_WRITE, effectiveUser, getOrigin())) {
            return;
        }

        AuditMessage msg = new AuditMessage(Category.COMPLIANCE_DOC_WRITE, clusterState, getOrigin(), null);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);
        msg.addEffectiveUser(effectiveUser);
        msg.addIndices(new String[] { shardId.getIndexName() });
        msg.addResolvedIndices(new String[] { shardId.getIndexName() });
        msg.addId(delete.id());
        msg.addShardId(shardId);
        msg.addComplianceDocVersion(result.getVersion());
        msg.addComplianceOperation(Operation.DELETE);
        save(msg);
    }

    @Override
    public void logExternalConfig(Settings settings, Environment environment) {

        if (!checkComplianceFilter(Category.COMPLIANCE_EXTERNAL_CONFIG, null, getOrigin())) {
            return;
        }

        final Map<String, Object> configAsMap = Utils.convertJsonToxToStructuredMap(settings);
        final Map<String, String> envAsMap;

        if (logEnvVars) {
            envAsMap = PrivilegedCode.execute(() -> System.getenv());
        } else {
            envAsMap = null;
        }

        final Properties propsAsMap = PrivilegedCode.execute(() -> System.getProperties());

        final String sha256 = DigestUtils.sha256Hex(configAsMap.toString() + (envAsMap != null ? envAsMap.toString() : "") + propsAsMap.toString());
        AuditMessage msg = new AuditMessage(Category.COMPLIANCE_EXTERNAL_CONFIG, clusterState, null, null);

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject();
            builder.startObject("external_configuration");
            builder.field("elasticsearch_yml", configAsMap);

            if (logEnvVars) {
                builder.field("os_environment", envAsMap);
            }

            builder.field("java_properties", propsAsMap);
            builder.field("sha256_checksum", sha256);
            builder.endObject();
            builder.endObject();
            builder.close();
            msg.addUnescapedJsonToRequestBody(Strings.toString(builder));
        } catch (Exception e) {
            log.error("Unable to build message", e);
        }

        Map<String, Path> paths = new HashMap<String, Path>();
        for (String key : settings.keySet()) {
            if (key.startsWith("searchguard") && (key.contains("filepath") || key.contains("file_path"))) {
                String value = settings.get(key);
                if (value != null && !value.isEmpty()) {
                    Path path = value.startsWith("/") ? Paths.get(value) : environment.configDir().resolve(value);
                    paths.put(key, path);
                }
            }
        }
        msg.addFileInfos(paths);

        save(msg);
    }

    @Override
    public void logIndexTemplatePutted(String templateName, ComposableIndexTemplate originalTemplate,
                                       ComposableIndexTemplate currentTemplate, String action, TransportRequest transportRequest) {
        if (currentTemplate == null) {
            log.error("Unable to log putted composable index template. Current index template is null.");
            return;
        }

        Category category = Category.INDEX_TEMPLATE_WRITE;

        UserInformation effectiveUser = getUser();

        if (!checkTransportFilter(category, action, effectiveUser, transportRequest)) {
            return;
        }

        AuditMessage msg = new AuditMessage(category, clusterState, getOrigin(), Origin.TRANSPORT);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);
        msg.addEffectiveUser(effectiveUser);
        msg.addIndexTemplates(new String[] { templateName });
        msg.addComplianceIndexTemplateVersion(currentTemplate.version());
        msg.addComplianceOperation(originalTemplate == null ? Operation.CREATE : Operation.UPDATE);

        try {
            msg.addTupleToRequestBody(new Tuple<>(XContentType.JSON, XContentHelper.toXContent(currentTemplate, XContentType.JSON, false)));
        } catch (Exception e) {
            log.error("Unable to parse current composable index template source", e);
        }

        save(msg);
    }

    @Override
    public void logIndexTemplatePutted(String templateName, IndexTemplateMetadata originalTemplate,
                                       IndexTemplateMetadata currentTemplate, String action, TransportRequest transportRequest) {
        if (currentTemplate == null) {
            log.error("Unable to log putted legacy index template. Current index template is null.");
            return;
        }

        Category category = Category.INDEX_TEMPLATE_WRITE;

        UserInformation effectiveUser = getUser();

        if (!checkTransportFilter(category, action, effectiveUser, transportRequest)) {
            return;
        }

        AuditMessage msg = new AuditMessage(category, clusterState, getOrigin(), Origin.TRANSPORT);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);
        msg.addEffectiveUser(effectiveUser);
        msg.addIndexTemplates(new String[] { templateName });
        msg.addComplianceIndexTemplateVersion(currentTemplate.getVersion() != null? Long.valueOf(currentTemplate.getVersion()) : null);
        msg.addComplianceOperation(originalTemplate == null ? Operation.CREATE : Operation.UPDATE);

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject();
            IndexTemplateMetadata.Builder.toXContent(currentTemplate, builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            msg.addTupleToRequestBody(new Tuple<>(XContentType.JSON, BytesReference.bytes(builder)));
        } catch (Exception e) {
            log.error("Unable to parse current legacy index template source", e);
        }

        save(msg);
    }

    @Override
    public void logIndexTemplateDeleted(List<String> templateNames, String action, TransportRequest transportRequest) {
        UserInformation effectiveUser = getUser();

        Category category = Category.INDEX_TEMPLATE_WRITE;

        if (!checkTransportFilter(category, action ,effectiveUser, transportRequest)) {
            return;
        }

        AuditMessage msg = new AuditMessage(category, clusterState, getOrigin(), Origin.TRANSPORT);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);
        msg.addEffectiveUser(effectiveUser);
        msg.addIndexTemplates(templateNames.toArray(new String[] {}));
        msg.addComplianceOperation(Operation.DELETE);

        save(msg);
    }

    @Override
    public void logIndexCreated(String unresolvedIndexName, String action, TransportRequest transportRequest) {
        Category category = Category.INDEX_WRITE;

        UserInformation effectiveUser = getUser();

        if (!checkTransportFilter(category, action, effectiveUser, transportRequest)) {
            return;
        }

        AuditMessage msg = new AuditMessage(category, clusterState, getOrigin(), Origin.TRANSPORT);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);
        msg.addEffectiveUser(effectiveUser);
        msg.addIndices(new String[] { unresolvedIndexName });
        msg.addComplianceOperation(Operation.CREATE);

        try {
            msg.addUnescapedJsonToRequestBody(serializeRequestContent(transportRequest));
        } catch (Exception e) {
            log.error("Unable to parse current index source", e);
        }

        save(msg);
    }

    @Override
    public void logIndicesDeleted(List<String> indexNames, String action, TransportRequest transportRequest) {
        UserInformation effectiveUser = getUser();

        Category category = Category.INDEX_WRITE;

        if (!checkTransportFilter(category, action, effectiveUser, transportRequest)) {
            return;
        }

        AuditMessage msg = new AuditMessage(category, clusterState, getOrigin(), Origin.TRANSPORT);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);
        msg.addEffectiveUser(effectiveUser);
        msg.addIndices(indexNames.toArray(new String[] {}));
        msg.addComplianceOperation(Operation.DELETE);

        save(msg);
    }

    @Override
    public void logIndexSettingsUpdated(List<String> indexNames, String action, TransportRequest transportRequest) {
        Category category = Category.INDEX_WRITE;

        UserInformation effectiveUser = getUser();

        if (!checkTransportFilter(category, action, effectiveUser, transportRequest)) {
            return;
        }

        AuditMessage msg = new AuditMessage(category, clusterState, getOrigin(), Origin.TRANSPORT);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);
        msg.addEffectiveUser(effectiveUser);
        msg.addIndices(indexNames.toArray(new String[] {}));
        msg.addComplianceOperation(Operation.UPDATE);

        try {
            msg.addUnescapedJsonToRequestBody(serializeRequestContent(transportRequest));
        } catch (Exception e) {
            log.error("Unable to parse current index settings source", e);
        }

        save(msg);
    }

    @Override
    public void logIndexMappingsUpdated(List<String> indexNames, String action, TransportRequest transportRequest) {

        Category category = Category.INDEX_WRITE;

        UserInformation effectiveUser = getUser();

        if (!checkTransportFilter(category, action, effectiveUser, transportRequest)) {
            return;
        }

        AuditMessage msg = new AuditMessage(category, clusterState, getOrigin(), Origin.TRANSPORT);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);
        msg.addEffectiveUser(effectiveUser);
        msg.addIndices(indexNames.toArray(new String[] {}));
        msg.addComplianceOperation(Operation.UPDATE);

        try {
            msg.addUnescapedJsonToRequestBody(serializeRequestContent(transportRequest));
        } catch (Exception e) {
            log.error("Unable to parse current index mappings source", e);
        }

        save(msg);
    }

    @Override
    public void logSucceededKibanaLogin(UserInformation effectiveUser) {
        Category category = Category.KIBANA_LOGIN;

        if (!checkRestFilter(category, effectiveUser, null)) {
            return;
        }

        AuditMessage msg = new AuditMessage(category, clusterState, getOrigin(), Origin.REST);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);

        msg.addEffectiveUser(effectiveUser);
        save(msg);
    }

    @Override
    public void logSucceededKibanaLogout(UserInformation effectiveUser) {
        Category category = Category.KIBANA_LOGOUT;

        if (!checkRestFilter(category, effectiveUser, null)) {
            return;
        }

        AuditMessage msg = new AuditMessage(category, clusterState, getOrigin(), Origin.REST);
        TransportAddress remoteAddress = getRemoteAddress();
        msg.addRemoteAddress(remoteAddress);

        msg.addEffectiveUser(effectiveUser);
        save(msg);
    }

    private Origin getOrigin() {
        String origin = threadPool.getThreadContext().getTransient(ConfigConstants.SG_ORIGIN);

        if (origin == null && threadPool.getThreadContext().getHeader(ConfigConstants.SG_ORIGIN_HEADER) != null) {
            origin = threadPool.getThreadContext().getHeader(ConfigConstants.SG_ORIGIN_HEADER);
        }

        return origin == null ? null : Origin.valueOf(origin);
    }

    private TransportAddress getRemoteAddress() {
        TransportAddress address = threadPool.getThreadContext().getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
        if (address == null && threadPool.getThreadContext().getHeader(ConfigConstants.SG_REMOTE_ADDRESS_HEADER) != null) {
            address = new TransportAddress((InetSocketAddress) Base64Helper
                    .deserializeObject(threadPool.getThreadContext().getHeader(ConfigConstants.SG_REMOTE_ADDRESS_HEADER)));
        }
        return address;
    }

    private UserInformation getUser() {
        User user = threadPool.getThreadContext().getTransient(ConfigConstants.SG_USER);
        if (user == null && threadPool.getThreadContext().getHeader(ConfigConstants.SG_USER_HEADER) != null) {
            user = (User) Base64Helper.deserializeObject(threadPool.getThreadContext().getHeader(ConfigConstants.SG_USER_HEADER));
        }
        return user;
    }

    private Map<String, String> getThreadContextHeaders() {
        return threadPool.getThreadContext().getHeaders();
    }

    private boolean checkTransportFilter(final Category category, final String action, final UserInformation effectiveUser,
            TransportRequest request) {

        if (log.isTraceEnabled()) {
            log.trace("Check category:{}, action:{}, effectiveUser:{}, request:{}", category, action, effectiveUser,
                    request == null ? null : request.getClass().getSimpleName());
        }

        if (!transportAuditingEnabled) {
            //ignore for certain categories
            if (category != Category.FAILED_LOGIN && category != Category.MISSING_PRIVILEGES && category != Category.SG_INDEX_ATTEMPT) {

                return false;
            }

        }

        //skip internals
        if (action != null && (action.startsWith("internal:") || action.startsWith("cluster:monitor") || action.startsWith("indices:monitor"))) {

            //if(log.isTraceEnabled()) {
            //    log.trace("Skipped audit log message due to category ({}) or action ({}) does not match", category, action);
            //}

            return false;
        }

        if (effectiveUser != null && ignoredAuditUsers.matches(effectiveUser.getName())) {

            if (log.isTraceEnabled()) {
                log.trace("Skipped audit log message because of user {} is ignored", effectiveUser);
            }

            return false;
        }

        if (request != null && (ignoreAuditRequests.matches(action) || ignoreAuditRequests.matches(request.getClass().getSimpleName()))) {

            if (log.isTraceEnabled()) {
                log.trace("Skipped audit log message because request {} is ignored", action + "#" + request.getClass().getSimpleName());
            }

            return false;
        }

        if (!disabledTransportCategories.contains(category.toString())) {
            return true;
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Skipped audit log message because category {} not enabled", category);
            }
            return false;
        }

        //skip cluster:monitor, index:monitor, internal:*
        //check transport audit enabled
        //check category enabled
        //check action
        //check ignoreAuditUsers

    }

    private boolean checkComplianceFilter(final Category category, final UserInformation effectiveUser, Origin origin) {
        if (log.isTraceEnabled()) {
            log.trace("Check for COMPLIANCE category:{}, effectiveUser:{}, origin: {}", category, effectiveUser, origin);
        }

        if (origin == Origin.LOCAL && effectiveUser == null && category != Category.COMPLIANCE_EXTERNAL_CONFIG) {
            if (log.isTraceEnabled()) {
                log.trace("Skipped compliance log message because of null user and local origin");
            }
            return false;
        }

        if (category == Category.COMPLIANCE_DOC_READ || category == Category.COMPLIANCE_INTERNAL_CONFIG_READ) {
            if (effectiveUser != null && effectiveUser.getName() != null && ignoredComplianceUsersForRead.matches(effectiveUser.getName())) {

                if (log.isTraceEnabled()) {
                    log.trace("Skipped compliance log message because of user {} is ignored", effectiveUser);
                }

                return false;
            }
        }

        if (category == Category.COMPLIANCE_DOC_WRITE || category == Category.COMPLIANCE_INTERNAL_CONFIG_WRITE) {
            if (effectiveUser != null && effectiveUser.getName() != null && ignoredComplianceUsersForWrite.matches(effectiveUser.getName())) {

                if (log.isTraceEnabled()) {
                    log.trace("Skipped compliance log message because of user {} is ignored", effectiveUser);
                }

                return false;
            }
        }

        return true;
    }

    private boolean checkRestFilter(final Category category, final UserInformation effectiveUser, RestRequest request) {
        if (log.isTraceEnabled()) {
            log.trace("Check for REST category:{}, effectiveUser:{}, request:{}", category, effectiveUser, request == null ? null : request.path());
        }

        if (!restAuditingEnabled) {
            //ignore for certain categories
            if (category != Category.FAILED_LOGIN && category != Category.MISSING_PRIVILEGES && category != Category.SG_INDEX_ATTEMPT) {

                return false;
            }

        }

        if (effectiveUser != null && ignoredAuditUsers.matches(effectiveUser.getName())) {

            if (log.isTraceEnabled()) {
                log.trace("Skipped audit log message because of user {} is ignored", effectiveUser);
            }

            return false;
        }

        if (request != null && ignoreAuditRequests.matches(request.path())) {

            if (log.isTraceEnabled()) {
                log.trace("Skipped audit log message because request {} is ignored", request.path());
            }

            return false;
        }

        if (!disabledRestCategories.contains(category.toString())) {
            return true;
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Skipped audit log message because category {} not enabled", category);
            }
            return false;
        }

        //check rest audit enabled
        //check category enabled
        //check action
        //check ignoreAuditUsers
    }

    protected String serializeRequestContent(TransportRequest transportRequest) {
        if (transportRequest instanceof CreateIndexRequest) {
            return serializeRequestContent((CreateIndexRequest) transportRequest);
        } else if (transportRequest instanceof UpdateSettingsRequest) {
                return serializeRequestContent((UpdateSettingsRequest) transportRequest);
        } else if (transportRequest instanceof PutMappingRequest) {
                return serializeRequestContent((PutMappingRequest) transportRequest);
        } else {
            throw new IllegalArgumentException(String.format("Unexpected request type: %s", transportRequest.getClass().getName()));
        }
    }

    private String serializeRequestContent(CreateIndexRequest request) {
        List<Map<String, Object>> aliases = request.aliases().stream().map(Utils::convertJsonToxToStructuredMap).collect(Collectors.toList());
        return DocNode.of(
                "index", request.index(),
                "settings", Utils.convertJsonToxToStructuredMap(request.settings()),
                "mappings", Utils.convertJsonToxToStructuredMap(request.mappings()),
                "aliases", aliases,
                "cause", request.cause(),
                "origin", request.origin()
        ).toJsonString();
    }

    private String serializeRequestContent(UpdateSettingsRequest request) {
        return DocNode.of(
            "indices", request.indices(),
            "settings", Utils.convertJsonToxToStructuredMap(request.settings()),
            "preserve_existing", request.isPreserveExisting(),
            "origin", request.origin()
        ).toJsonString();
    }

    private String serializeRequestContent(PutMappingRequest request) {
        return DocNode.of(
                "indices", request.indices(),
                "source", Utils.convertJsonToxToStructuredMap(request.source()),
                "write_index_only", request.writeIndexOnly(),
                "origin", request.origin()
        ).toJsonString();
    }

    protected abstract void save(final AuditMessage msg);
}
