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

package com.floragunn.searchguard.dlic.rest.api;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigUnavailableException;
import com.floragunn.searchguard.configuration.ConfigurationLoader;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.Hideable;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.configuration.StaticDefinable;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import com.floragunn.searchguard.configuration.validation.ConfigModificationValidators;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateNodeResponse;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator.ErrorType;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.user.User;
import com.google.common.collect.ImmutableMap;

public abstract class AbstractApiAction extends BaseRestHandler {

	protected final Logger log = LogManager.getLogger(this.getClass());

	protected final ConfigurationRepository cl;
	protected final ClusterService cs;
	final ThreadPool threadPool;
	private final RestApiPrivilegesEvaluator restApiPrivilegesEvaluator;
	protected final Boolean acceptInvalidLicense;
	protected final AuditLog auditLog;
	protected final Settings settings;
	protected final StaticSgConfig staticSgConfig;
    protected final ConfigurationLoader configLoader;
	protected final ConfigModificationValidators configModificationValidators;

    protected AbstractApiAction(final Settings settings, final Path configPath, final RestController controller, final Client client,
            final AdminDNs adminDNs, final ConfigurationRepository configRepository, StaticSgConfig staticSgConfig, final ClusterService cs,
            final PrincipalExtractor principalExtractor, AuthorizationService authorizationService,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, ThreadPool threadPool,
            AuditLog auditLog, ConfigModificationValidators configModificationValidators) {
		super();
		this.settings = settings;
		this.acceptInvalidLicense = settings.getAsBoolean(ConfigConstants.SEARCHGUARD_UNSUPPORTED_RESTAPI_ACCEPT_INVALID_LICENSE, Boolean.FALSE);

		this.cl = configRepository;
		this.cs = cs;
		this.threadPool = threadPool;
        this.restApiPrivilegesEvaluator = new RestApiPrivilegesEvaluator(settings, adminDNs, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, principalExtractor, configPath, threadPool);
		this.auditLog = auditLog;
		this.staticSgConfig = staticSgConfig;
        this.configLoader = new ConfigurationLoader(client, configRepository);
		this.configModificationValidators = configModificationValidators;
	}

	protected abstract AbstractConfigurationValidator getValidator(RestRequest request, BytesReference ref, Object... params);

	protected abstract String getResourceName();

	protected abstract CType<?> getConfigName();

	/**
	 * @param content RestRequest content with incremented reference count. It's important to use its value instead of
	 *                explicitly calling {@link RestRequest#content()} or {@link RestRequest#requiredContent()}.
	 *                Reading the RestRequest content after {@link org.elasticsearch.rest.RestHandler#handleRequest} will cause errors because
	 *                the content reference count will already be decremented. More details
	 *                <a href="https://github.com/elastic/elasticsearch/pull/116115">RestRequest content</a>
	 */
	protected void handleApiRequest(final RestChannel channel, final RestRequest request, final Client client, final BytesReference content)
			throws IOException {

		// validate additional settings, if any
		log.debug("Handling API request for request {}", System.identityHashCode(request));
		AbstractConfigurationValidator validator = getValidator(request, content);
		if (!validator.validate()) {
			request.params().clear();
			badRequestResponse(channel, validator);
			return;
		}
		switch (request.method()) {
		case DELETE:
			handleDelete(channel, request, client, validator.getContentAsNode());
			break;
		case POST:
			handlePost(channel, request, client, validator.getContentAsNode());
			break;
		case PUT:
			handlePut(channel, request, client, validator.getContentAsNode());
			break;
		case GET:
			handleGet(channel, request, client, validator.getContentAsNode());
			break;
		default:
			badRequestResponse(channel, request.method() + " not supported for " + this.getName());
			break;
		}

		//TODO strip source for JsonMappingException
		//if(jme.getLocation() == null || jme.getLocation().getSourceRef() == null) {
		//    throw jme;
		//} else throw new JsonMappingException(null, jme.getMessage());

	}

	protected void handleDelete(final RestChannel channel, final RestRequest request, final Client client, final DocNode content) throws IOException {
		final String name = request.param("name");

		if (name == null || name.length() == 0) {
			badRequestResponse(channel, "No " + getResourceName() + " specified.");
			return;
		}

		SgDynamicConfiguration<?> existingConfiguration;
		try {
			existingConfiguration = load(getConfigName(), false);
		} catch (ConfigUnavailableException e) {
			internalErrorResponse(channel, e.getMessage());
			return;
		}

		if (isHidden(existingConfiguration, name)) {
            notFound(channel, getResourceName() + " " + name + " not found.");
            return;
		}

		if (isReserved(existingConfiguration, name)) {
			forbidden(channel, "Resource '"+ name +"' is read-only.");
			return;
		}

        boolean existed = existingConfiguration.exists(name);
        existingConfiguration = existingConfiguration.without(name);

		if (existed) {
			saveAnUpdateConfigs(client, request, getConfigName(), existingConfiguration, new OnSucessActionListener<DocWriteResponse>(channel) {

                @Override
                public void onResponse(DocWriteResponse response) {
                    successResponse(channel, "'" + name + "' deleted.");
                }
            });

		} else {
			notFound(channel, getResourceName() + " " + name + " not found.");
		}
	}

	@SuppressWarnings("unchecked")
    protected void handlePut(String name, RestChannel channel, RestRequest request, Client client, DocNode content) throws IOException {

		SgDynamicConfiguration<Object> existingConfiguration;
		try {
			existingConfiguration = (SgDynamicConfiguration<Object>) load(getConfigName(), false);
		} catch (ConfigUnavailableException e) {
			internalErrorResponse(channel, e.getMessage());
			return;
		}

		if (isHidden(existingConfiguration, name)) {
            forbidden(channel, "Resource '"+ name +"' is not available.");
            return;
		}

		if (isReserved(existingConfiguration, name)) {
			forbidden(channel, "Resource '"+ name +"' is read-only.");
			return;
		}

		if (log.isTraceEnabled() && content != null) {
			log.trace(content.toString());
		}

		boolean existed = existingConfiguration.exists(name);
		Parser.ReturningValidationResult<?, ConfigurationRepository.Context> parser = getConfigName().getParser();

        ValidationResult<?> validatedConfig = parser.parse(content, cl.getParserContext());

        try {
			ValidationErrors validationErrors = validatedConfig.getValidationErrors();
			Object configEntry = validatedConfig.peek();
			validationErrors.add(configModificationValidators.validateConfigEntry(configEntry));

			//validationErrors.throwExceptionForPresentErrors() is not explicitly called, since validatedConfig.get() calls it anyway
            existingConfiguration = existingConfiguration.with(name, validatedConfig.get());
        } catch (ConfigValidationException e) {
            badRequestResponse(channel, e.toJsonString());
            return;
        }

		saveAnUpdateConfigs(client, request, getConfigName(), existingConfiguration, new OnSucessActionListener<DocWriteResponse>(channel) {

            @Override
            public void onResponse(DocWriteResponse response) {
                if (existed) {
                    successResponse(channel, "'" + name + "' updated.");
                } else {
                    createdResponse(channel, "'" + name + "' created.");
                }

            }
        });

	}

	protected void handlePut(final RestChannel channel, final RestRequest request, final Client client, final DocNode content) throws IOException {

        final String name = request.param("name");

        if (name == null || name.length() == 0) {
            badRequestResponse(channel, "No " + getResourceName() + " specified.");
            return;
        }

        handlePut(name, channel, request, client, content);
    }

	protected void handlePost(final RestChannel channel, final RestRequest request, final Client client, final DocNode content) throws IOException {
		notImplemented(channel, Method.POST);
	}

	@SuppressWarnings("resource")
    protected void handleGet(final RestChannel channel, RestRequest request, Client client, final DocNode content)
	        throws IOException{

		final String resourcename = request.param("name");

		try {
			SgDynamicConfiguration<?> configuration;

			// Drop user information to avoid logging audit log events. Audit log is done manually below.
			try (StoredContext ctx = threadPool.getThreadContext().stashContext()) {
			    configuration = configLoader.loadSync(getConfigName(), "API Request", cl.getParserContext());
			}

			logComplianceEvent(configuration);

			configuration = filter(configuration);

			// no specific resource requested, return complete config
			if (resourcename == null || resourcename.length() == 0) {
			    // The legacy REST API also returns static configuration. This is a bit weird if you cannot modify it by PUT or DELETE.
			    // Anyway, we keep this here to support for example the Search Guard config UI
			    configuration = staticSgConfig.addTo(configuration);

				successResponse(channel, configuration);
				return;
			}

			if (!configuration.exists(resourcename)) {
				notFound(channel, "Resource '" + resourcename + "' not found.");
				return;
			}

			configuration = configuration.only(resourcename);
			successResponse(channel, configuration);
		} catch (ConfigUnavailableException e) {
			log.error("Error while loading configuration", e);
			internalErrorResponse(channel, e.getMessage());
		}
	}

	protected final <T> SgDynamicConfiguration<T> load(final CType<T> config, boolean logComplianceEvent) throws ConfigUnavailableException {
        SgDynamicConfiguration<T> loaded;

        // Drop user information to avoid logging audit log events. Audit log events are logged manually below.
        try (StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            loaded = configLoader.loadSync(config, "API Request", cl.getParserContext());
        }

        if (logComplianceEvent) {
            logComplianceEvent(loaded);
        }

        loaded = staticSgConfig.addTo(loaded);
	    return loaded;
	}

	protected boolean ensureIndexExists() {
		return cl.isIndexInitialized();
	}

	protected SgDynamicConfiguration<?> filter(SgDynamicConfiguration<?> builder) {
	    return builder.withoutHidden();
	}

	abstract class OnSucessActionListener<Response> implements ActionListener<Response> {

	    private final RestChannel channel;

        public OnSucessActionListener(RestChannel channel) {
            super();
            this.channel = channel;
        }

        @Override
        public final void onFailure(Exception e) {
            internalErrorResponse(channel, "Error "+e.getMessage());
        }

	}

	protected void saveAnUpdateConfigs(final Client client, final RestRequest request, final CType<?> cType,
	        SgDynamicConfiguration<?> configuration, OnSucessActionListener<DocWriteResponse> actionListener) {
	    String searchGuardIndex = cl.getEffectiveSearchGuardIndex();

	    if (searchGuardIndex == null) {
	        throw new RuntimeException("The Search Guard index has not yet been created");
	    }

		final IndexRequest ir = new IndexRequest(searchGuardIndex);

		//final String type = "_doc";
		final String id = cType.toLCString();

		configuration = configuration.withoutStatic();

		try {
            client.index(ir.id(id)
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .setIfSeqNo(configuration.getSeqNo())
                    .setIfPrimaryTerm(configuration.getPrimaryTerm())
                    .source(id, XContentHelper.toXContent(configuration, XContentType.JSON, false)),
                    new ConfigUpdatingActionListener<DocWriteResponse>(client, actionListener));
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
	}

	private static class ConfigUpdatingActionListener<Response> implements ActionListener<Response>{

	    private final Client client;
	    private final ActionListener<Response> delegate;

        public ConfigUpdatingActionListener(Client client, ActionListener<Response> delegate) {
            super();
            this.client = client;
            this.delegate = delegate;
        }

        @Override
        public void onResponse(Response response) {

            final ConfigUpdateRequest cur = new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0]));

            client.execute(ConfigUpdateAction.INSTANCE, cur, new ActionListener<ConfigUpdateResponse>() {
                @Override
                public void onResponse(final ConfigUpdateResponse ur) {
                    if(ur.hasFailures()) {
                        delegate.onFailure(ur.failures().get(0));
                        return;
                    }
                    delegate.onResponse(response);
                }

                @Override
                public void onFailure(final Exception e) {
                    delegate.onFailure(e);
                }
            });

        }

        @Override
        public void onFailure(Exception e) {
            delegate.onFailure(e);
        }

	}

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        // consume all parameters first so we can return a correct HTTP status,
        // not 400
        consumeParameters(request);

        // dirty hack to avoid "request does not support having a body" error
        request.content();

		final ThreadContext threadContext = threadPool.getThreadContext();

        // check if SG index has been initialized
        if (!ensureIndexExists()) {
            return channel -> internalErrorResponse(channel, ErrorType.SG_NOT_INITIALIZED.getMessage());
        }

        // check if request is authorized
        String authError = restApiPrivilegesEvaluator.checkAccessPermissions(request, getEndpoint());

        if (authError != null) {
            log.error("No permission to access REST API: " + authError);
            final User user = (User) threadContext.getTransient(ConfigConstants.SG_USER);
            auditLog.logMissingPrivileges(authError, user, request);
            // for rest request
            request.params().clear();
            return channel -> forbidden(channel, "No permission to access REST API: " + authError);
        }

        final Object originalUser = threadContext.getTransient(ConfigConstants.SG_USER);
        final Object originalRemoteAddress = threadContext.getTransient(ConfigConstants.SG_REMOTE_ADDRESS);
        final Object originalOrigin = threadContext.getTransient(ConfigConstants.SG_ORIGIN);
		final Map<String, List<String>> originalResponseHeaders = threadContext.getResponseHeaders();

		final ReleasableBytesReference content = request.content();
		content.mustIncRef();
        return channel -> {

            threadPool.generic().submit(() -> {

                try (StoredContext ctx = threadContext.stashContext()) {

                    threadContext.putHeader(ConfigConstants.SG_CONF_REQUEST_HEADER, "true");
                    threadContext.putTransient(ConfigConstants.SG_USER, originalUser);
                    threadContext.putTransient(ConfigConstants.SG_REMOTE_ADDRESS, originalRemoteAddress);
                    threadContext.putTransient(ConfigConstants.SG_ORIGIN, originalOrigin);

                    originalResponseHeaders.entrySet().forEach(

                            h ->  h.getValue().forEach(v -> threadContext.addResponseHeader(h.getKey(), v))

                    );


                    handleApiRequest(channel, request, client, content);

                } catch (Exception e) {
                    log.error("Error while processing request " + request, e);
                    internalErrorResponse(channel, "Error while processing request: " + e);
				} finally {
					content.decRef();
				}
            });
        };
    }

	protected boolean checkConfigUpdateResponse(final ConfigUpdateResponse response) {

		final int nodeCount = cs.state().getNodes().getNodes().size();
		final int expectedConfigCount = 1;

		boolean success = response.getNodes().size() == nodeCount;
		if (!success) {
		    log.error(
					"Expected " + nodeCount + " nodes to return response, but got only " + response.getNodes().size());
		}

		for (final String nodeId : response.getNodesMap().keySet()) {
			final ConfigUpdateNodeResponse node = response.getNodesMap().get(nodeId);
			final boolean successNode = node.getUpdatedConfigTypes() != null
					&& node.getUpdatedConfigTypes().length == expectedConfigCount;

			if (!successNode) {
			    log.error("Expected " + expectedConfigCount + " config types for node " + nodeId + " but got only "
						+ Arrays.toString(node.getUpdatedConfigTypes()));
			}

			success = success && successNode;
		}

		return success;
	}

	protected static XContentBuilder convertToJson(RestChannel channel, ToXContent toxContent) {
		try {
            XContentBuilder builder = channel.newBuilder();
            toxContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder;
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
	}

	protected void response(RestChannel channel, RestStatus status, String message) {

		try {
			final XContentBuilder builder = channel.newBuilder();
			builder.startObject();
			builder.field("status", status.name());
			builder.field("message", message);
			builder.endObject();
			channel.sendResponse(new RestResponse(status, builder));
		} catch (IOException e) {
		    throw ExceptionsHelper.convertToElastic(e);
		}
	}

	protected void successResponse(RestChannel channel, SgDynamicConfiguration<?> response) {
        try {
            final XContentBuilder builder = channel.newBuilder();
            builder.value(response.toRedactedLegacyBasicObject());
            channel.sendResponse(new RestResponse(RestStatus.OK, builder));
        } catch (Exception e) {
            log.error(e.toString(), e);
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

	protected void badRequestResponse(RestChannel channel, AbstractConfigurationValidator validator) {
        channel.sendResponse(new RestResponse(RestStatus.BAD_REQUEST, validator.errorsAsXContent(channel)));
    }

	protected void successResponse(RestChannel channel, String message) {
		response(channel, RestStatus.OK, message);
	}

	protected void createdResponse(RestChannel channel, String message) {
		response(channel, RestStatus.CREATED, message);
	}

	protected void badRequestResponse(RestChannel channel, String message) {
		response(channel, RestStatus.BAD_REQUEST, message);
	}

	protected void notFound(RestChannel channel, String message) {
		response(channel, RestStatus.NOT_FOUND, message);
	}

	protected void forbidden(RestChannel channel, String message) {
		response(channel, RestStatus.FORBIDDEN, message);
	}

	protected void internalErrorResponse(RestChannel channel, String message) {
		response(channel, RestStatus.INTERNAL_SERVER_ERROR, message);
	}

	protected void unprocessable(RestChannel channel, String message) {
		response(channel, RestStatus.UNPROCESSABLE_ENTITY, message);
	}

	protected void notImplemented(RestChannel channel, Method method) {
		response(channel, RestStatus.NOT_IMPLEMENTED,
				"Method " + method.name() + " not supported for this action.");
	}

	protected final boolean isReserved(SgDynamicConfiguration<?> configuration, String resourceName) {
	    if(isStatic(configuration, resourceName)) { //static is also always reserved
	        return true;
	    }

	    final Object o = configuration.getCEntry(resourceName);
	    return o != null && o instanceof Hideable && ((Hideable) o).isReserved();
	}

    protected final boolean isHidden(SgDynamicConfiguration<?> configuration, String resourceName) {
        final Object o = configuration.getCEntry(resourceName);
        return o != null && o instanceof Hideable && ((Hideable) o).isHidden();
    }

    protected final boolean isStatic(SgDynamicConfiguration<?> configuration, String resourceName) {
        final Object o = configuration.getCEntry(resourceName);
        return o != null && o instanceof StaticDefinable && ((StaticDefinable) o).isStatic();
    }

	/**
	 * Consume all defined parameters for the request. Before we handle the
	 * request in subclasses where we actually need the parameter, some global
	 * checks are performed, e.g. check whether the SG index exists. Thus, the
	 * parameter(s) have not been consumed, and ES will always return a 400 with
	 * an internal error message.
	 *
	 * @param request
	 */
	protected void consumeParameters(final RestRequest request) {
		request.param("name");
	}

	@Override
	public String getName() {
		return getClass().getSimpleName();
	}

	protected abstract Endpoint getEndpoint();

    private <T> void logComplianceEvent(SgDynamicConfiguration<T> result) {
        Map<String, String> fields = ImmutableMap.of(result.getCType().toLCString(), Strings.toString(result));
        auditLog.logDocumentRead(cl.getEffectiveSearchGuardIndex(), result.getCType().toLCString(), null, fields);
    }

}
