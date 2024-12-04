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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.floragunn.searchguard.configuration.validation.ConfigModificationValidators;
import org.bouncycastle.crypto.generators.OpenBSDBCrypt;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.auditlog.AuditLog;
import com.floragunn.searchguard.authc.internal_users_db.InternalUser;
import com.floragunn.searchguard.authz.AuthorizationService;
import com.floragunn.searchguard.configuration.AdminDNs;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigUnavailableException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.configuration.StaticSgConfig;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator;
import com.floragunn.searchguard.dlic.rest.validation.AbstractConfigurationValidator.ErrorType;
import com.floragunn.searchguard.dlic.rest.validation.InternalUsersValidator;
import com.floragunn.searchguard.privileges.SpecialPrivilegesEvaluationContextProviderRegistry;
import com.floragunn.searchguard.ssl.transport.PrincipalExtractor;

public class InternalUsersApiAction extends PatchableResourceApiAction {

    @Inject
    public InternalUsersApiAction(final Settings settings, final Path configPath, final RestController controller, final Client client,
            final AdminDNs adminDNs, final ConfigurationRepository cl, StaticSgConfig staticSgConfig, final ClusterService cs,
            final PrincipalExtractor principalExtractor, AuthorizationService authorizationService,
            SpecialPrivilegesEvaluationContextProviderRegistry specialPrivilegesEvaluationContextProviderRegistry, ThreadPool threadPool,
            AuditLog auditLog, ConfigModificationValidators configModificationValidators) {
        super(settings, configPath, controller, client, adminDNs, cl, staticSgConfig, cs, principalExtractor, authorizationService,
                specialPrivilegesEvaluationContextProviderRegistry, threadPool, auditLog, configModificationValidators);
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
    protected void handlePut(RestChannel channel, final RestRequest request, final Client client, DocNode content) throws IOException {

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
        
        // if password is set, it takes precedence over hash
        final String plainTextPassword = content.getAsString("password");
        final String origHash = content.getAsString("hash");
        if (plainTextPassword != null && plainTextPassword.length() > 0) {
            content = content.without("password").with(DocNode.of("hash", hash(plainTextPassword.toCharArray())));
        } else if(origHash != null && origHash.length() > 0) {
            content = content.without("password");
        } else if(plainTextPassword != null && plainTextPassword.isEmpty() && origHash == null) {
            content = content.without("password");
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
        if (!userExisted && content.get("hash") == null) {
            badRequestResponse(channel, "Please specify either 'hash' or 'password' when creating a new internal user.");
            return;
        }

        // for existing users, hash is optional
        if (userExisted && content.get("hash") == null) {
            // sanity check, this should usually not happen
            final String hash = internaluser.getCEntry(username).getPasswordHash();
            if (hash == null || hash.length() == 0) {
                internalErrorResponse(channel, 
                        "Existing user " + username + " has no password, and no new password or hash was specified.");
                return;
            }
            content = content.with(DocNode.of("hash", hash));
        }

        String newJson = content.toJsonString();
        
        // checks complete, create or update the user
        try {
            internaluser = internaluser.with(username, InternalUser.parse(DocReader.json().readObject(newJson), cl.getParserContext()).get());
        } catch (ConfigValidationException e) {
            throw new RuntimeException(e);
        }

        saveAnUpdateConfigs(client, request, CType.INTERNALUSERS, internaluser, new OnSucessActionListener<DocWriteResponse>(channel) {
            
            @Override
            public void onResponse(DocWriteResponse response) {
                if (userExisted) {
                    successResponse(channel, "'" + username + "' updated.");
                } else {
                    createdResponse(channel, "'" + username + "' created.");
                }
                
            }
        });

        

    }
    
    @Override
    protected DocNode postProcessApplyPatchResult(RestChannel channel, RestRequest request, DocNode existingResourceAsJsonNode,
            DocNode updatedResourceAsJsonNode, String resourceName) throws ConfigValidationException {
        String plainTextPassword = updatedResourceAsJsonNode.getAsString("password");

        if (plainTextPassword != null) {
            Map<String, Object> updatedResource = new LinkedHashMap<>(updatedResourceAsJsonNode.toMap());
            
            String userName = resourceName;
            
            ErrorType error = InternalUsersValidator.validatePassword(userName, plainTextPassword, settings);
            
            if (error != null) {
                throw new ConfigValidationException(new ValidationError("password", error.getMessage()));
            }            
            
            updatedResource.remove("password");
            updatedResource.put("hash", hash(plainTextPassword.toCharArray()));
            return DocNode.wrap(updatedResource);
        } else {
            return updatedResourceAsJsonNode;
        }
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
    protected CType<InternalUser> getConfigName() {
        return CType.INTERNALUSERS;
    }

    @Override
    protected AbstractConfigurationValidator getValidator(RestRequest request, BytesReference ref, Object... params) {
        return new InternalUsersValidator(request, ref, this.settings, params);
    }
}
