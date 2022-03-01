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
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.bouncycastle.crypto.generators.OpenBSDBCrypt;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.ThreadPool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.internal_users_db.InternalUser;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.ConfigUnavailableException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.dlic.rest.validation.InternalUsersValidator;
import com.floragunn.searchguard.privileges.PrivilegesEvaluator;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.sgconf.StaticSgConfig;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;
import com.floragunn.searchguard.support.SgJsonNode;

public class InternalUsersApiAction extends PatchableResourceApiAction {

    @Inject
    public InternalUsersApiAction(final Settings settings, final Path configPath, final RestController controller, final Client client,
            final AdminDNs adminDNs, final ConfigurationRepository cl, StaticSgConfig staticSgConfig, final ClusterService cs,
            final PrincipalExtractor principalExtractor, final PrivilegesEvaluator evaluator,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, ThreadPool threadPool,
            AuditLog auditLog) {
        super(settings, configPath, controller, client, adminDNs, cl, staticSgConfig, cs, principalExtractor, evaluator,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog);
    }
    
    @Override
    public List<Route> routes() {
        return getStandardResourceRoutes("internalusers");
    }

    @Override
    protected Endpoint getEndpoint() {
        return Endpoint.INTERNALUSERS;
    }

    @Override
    protected void handlePut(RestChannel channel, final RestRequest request, final Client client, final JsonNode content) throws IOException {

        final String username = request.param("name");

        if (username == null || username.length() == 0) {
            badRequestResponse(channel, "No " + getResourceName() + " specified.");
            return;
        }

        // TODO it might be sensible to consolidate this with the overridden method in
        // order to minimize duplicated logic

        SgDynamicConfiguration<InternalUser> configuration;
		try {
			configuration = load(getConfigName(), false);
		} catch (ConfigUnavailableException e1) {
			internalErrorResponse(channel, e1.getMessage());
			return;
		}

        if (isHidden(configuration, username)) {
            forbidden(channel, "Resource '" + username + "' is not available.");
            return;
        }

        // check if resource is writeable
        if (isReserved(configuration, username)) {
            forbidden(channel, "Resource '" + username + "' is read-only.");
            return;
        }
        
        final ObjectNode contentAsNode = (ObjectNode) content;
        final SgJsonNode sgJsonNode = new SgJsonNode(contentAsNode);
        
        // if password is set, it takes precedence over hash
        final String plainTextPassword = sgJsonNode.get("password").asString();
        final String origHash = sgJsonNode.get("hash").asString();
        if (plainTextPassword != null && plainTextPassword.length() > 0) {
            contentAsNode.remove("password");
            contentAsNode.put("hash", hash(plainTextPassword.toCharArray()));
        } else if(origHash != null && origHash.length() > 0) {
            contentAsNode.remove("password");
        } else if(plainTextPassword != null && plainTextPassword.isEmpty() && origHash == null) {
            contentAsNode.remove("password");
        }
        
        // check if user exists
        SgDynamicConfiguration<InternalUser> internaluser;
		try {
			internaluser = load(CType.INTERNALUSERS, false);
		} catch (ConfigUnavailableException e1) {
			internalErrorResponse(channel, e1.getMessage());
			return;
		}

        final boolean userExisted = internaluser.exists(username);

        // when updating an existing user password hash can be blank, which means no
        // changes

        // sanity checks, hash is mandatory for newly created users
        if (!userExisted && sgJsonNode.get("hash").asString() == null) {
            badRequestResponse(channel, "Please specify either 'hash' or 'password' when creating a new internal user.");
            return;
        }

        // for existing users, hash is optional
        if (userExisted && sgJsonNode.get("hash").asString() == null) {
            // sanity check, this should usually not happen
            final String hash = internaluser.getCEntry(username).getPasswordHash();
            if (hash == null || hash.length() == 0) {
                internalErrorResponse(channel, 
                        "Existing user " + username + " has no password, and no new password or hash was specified.");
                return;
            }
            contentAsNode.put("hash", hash);
        }

        String newJson = DefaultObjectMapper.writeJsonTree(contentAsNode);
        
        internaluser.remove(username);

        // checks complete, create or update the user
        try {
            internaluser.putCEntry(username, InternalUser.parse(DocReader.json().readObject(newJson), cl.getParserContext()).get());
        } catch (ConfigValidationException e) {
            throw new RuntimeException(e);
        }

        saveAnUpdateConfigs(client, request, CType.INTERNALUSERS, internaluser, new OnSucessActionListener<IndexResponse>(channel) {
            
            @Override
            public void onResponse(IndexResponse response) {
                if (userExisted) {
                    successResponse(channel, "'" + username + "' updated.");
                } else {
                    createdResponse(channel, "'" + username + "' created.");
                }
                
            }
        });

        

    }
    
    @Override
    protected AbstractConfigurationValidator postProcessApplyPatchResult(RestChannel channel, RestRequest request, JsonNode existingResourceAsJsonNode,
            JsonNode updatedResourceAsJsonNode, String resourceName) {
    	AbstractConfigurationValidator retVal = null;
        JsonNode passwordNode = updatedResourceAsJsonNode.get("password");

        if (passwordNode != null) {
            String plainTextPassword = passwordNode.asText();
            try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
				builder.startObject();
				builder.field("password", plainTextPassword);
				builder.endObject();
				retVal = getValidator(request, BytesReference.bytes(builder), resourceName);
			} catch (IOException e) {
				log.error(e);
			}

            ((ObjectNode) updatedResourceAsJsonNode).remove("password");
            ((ObjectNode) updatedResourceAsJsonNode).set("hash", new TextNode(hash(plainTextPassword.toCharArray())));
            return retVal;
        }
        
        return null;
    }

    public static String hash(final char[] clearTextPassword) {
        final byte[] salt = new byte[16];
        new SecureRandom().nextBytes(salt);
        final String hash = OpenBSDBCrypt.generate((Objects.requireNonNull(clearTextPassword)), salt, 12);
        Arrays.fill(salt, (byte) 0);
        Arrays.fill(clearTextPassword, '\0');
        return hash;
    }

    @Override
    protected String getResourceName() {
        return "user";
    }

    @Override
    protected CType getConfigName() {
        return CType.INTERNALUSERS;
    }

    @Override
    protected AbstractConfigurationValidator getValidator(RestRequest request, BytesReference ref, Object... params) {
        return new InternalUsersValidator(request, ref, this.settings, params);
    }
}
