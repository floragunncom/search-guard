/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.session;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchsupport.json.YamlRewriter;
import com.floragunn.searchsupport.json.YamlRewriter.RewriteException;
import com.floragunn.searchsupport.json.YamlRewriter.RewriteResult;
import com.google.common.collect.ImmutableMap;

public class SessionConfigMigrator {

    public static void main(String[] args) {
        System.out.println("Welcome to the Search Guard config migration tool.\n\n"
                + "This tool converts legacy Search Guard configuration to configuration suitable for Search Guard 53. The tool also provides basic guidance for a seamless update process without outages.\n");

        File sgConfig = null;
        File kibanaConfig = null;

        for (String arg : args) {
            File file = new File(arg);

            if (file.getName().startsWith("sg_config") && file.getName().endsWith(".yml")) {
                sgConfig = file;
            } else if (arg.endsWith("kibana.yml")) {
                kibanaConfig = file;
            }
        }

        if (sgConfig == null) {
            System.out.flush();
            System.err.println("You must specify a path to a sg_config.yml on the command line");
            System.exit(1);
        }

        if (kibanaConfig == null) {
            System.out.flush();
            System.err.println("You must specify a path to a kibana.yml on the command line");
            System.exit(1);
        }

        if (!sgConfig.exists()) {
            System.out.flush();
            System.err.println("The file " + sgConfig + " does not exist");
            System.exit(1);
        }

        if (!kibanaConfig.exists()) {
            System.out.flush();
            System.err.println("The file " + kibanaConfig + " does not exist");
            System.exit(1);
        }

        try {
            SessionConfigMigrator sessionConfigMigrator = new SessionConfigMigrator(sgConfig, kibanaConfig);
            UpdateInstructions updateInstructions = sessionConfigMigrator.createUpdateInstructions();

            System.out.println(updateInstructions.getMainInstructions());

            if (sessionConfigMigrator.oldKibanaConfigValidationErrors.hasErrors() || sessionConfigMigrator.oldSgConfigValidationErrors.hasErrors()) {
                System.out.println(
                        "\nWARNING: We detected validation errors in the provided configuration files. We try to create the new configuration files anyway.\n"
                                + "However, you might want to review the validation errors and the generated files.\n");

                if (sessionConfigMigrator.oldKibanaConfigValidationErrors.hasErrors()) {
                    System.out.println("Errors in " + kibanaConfig + "\n" + sessionConfigMigrator.oldKibanaConfigValidationErrors + "\n");
                }

                if (sessionConfigMigrator.oldSgConfigValidationErrors.hasErrors()) {
                    System.out.println("Errors in " + sgConfig + "\n" + sessionConfigMigrator.oldSgConfigValidationErrors + "\n");
                }
            }

            System.out.println("The update process consists of these steps:\n");
            System.out.println(
                    "- Update the Search Guard plugin for Elasticsearch on all nodes of your cluster. In this step, you do not yet need to modify the configuration.\n");

            if (updateInstructions.sgFrontendConfig != null && !updateInstructions.sgFrontendConfig.isEmpty()) {
                System.out.println("- " + updateInstructions.sgFrontendConfigInstructions);

                if (updateInstructions.sgFrontendConfigInstructionsAdvanced != null) {
                    System.out.println(updateInstructions.sgFrontendConfigInstructionsAdvanced);
                }

                if (updateInstructions.sgFrontendConfigInstructionsReview != null) {
                    System.out.println(updateInstructions.sgFrontendConfigInstructionsReview);
                }

                System.out.println("\n------------------------------------------------------");

                System.out.println(DocWriter.yaml().writeAsString(updateInstructions.sgFrontendConfig));

                System.out.println("------------------------------------------------------\n");

            } else {
                System.out.println("- " + updateInstructions.sgFrontendConfigInstructions);
            }

            System.out.println("- Afterwards, you need to update the Search Guard plugin for Kibana. " + updateInstructions.kibanaConfigInstructions);

            if (updateInstructions.kibanaConfig != null) {
                System.out.println("\n------------------------------------------------------");
                System.out.println(updateInstructions.kibanaConfig);
                System.out.println("------------------------------------------------------\n");

            }

        } catch (Exception e) {
            // TODO improve
            e.printStackTrace();
            System.exit(1);
        }
    }

    private final ValidationErrors oldSgConfigValidationErrors = new ValidationErrors();
    private final ValidationErrors oldKibanaConfigValidationErrors = new ValidationErrors();
    private final ValidatingDocNode oldSgConfig;
    private final ValidatingDocNode oldKibanaConfig;
    private final YamlRewriter kibanaConfigRewriter;

    public SessionConfigMigrator(File legacySgConfig, File legacyKibanaConfig) throws JsonProcessingException, FileNotFoundException, IOException {
        this.oldSgConfig = new ValidatingDocNode(DocReader.yaml().readObject(legacySgConfig), oldSgConfigValidationErrors);
        this.oldKibanaConfig = new ValidatingDocNode(DocReader.yaml().readObject(legacyKibanaConfig), oldKibanaConfigValidationErrors);
        this.kibanaConfigRewriter = new YamlRewriter(legacyKibanaConfig);
    }

    /*
    public SessionConfigMigrator(Map<String, Object> legacySgConfig, String legacyKibanaConfig) {
        this.oldSgConfig = new ValidatingDocumentNode(legacySgConfig, oldSgConfigValidationErrors);
        this.oldKibanaConfig = new ValidatingDocumentNode(legacyKibanaConfig, oldKibanaConfigValidationErrors);
    }
    */

    public UpdateInstructions createUpdateInstructions() {
        KibanaAuthType kibanaAuthType = oldKibanaConfig.get("searchguard.auth.type").withDefault(KibanaAuthType.BASICAUTH)
                .asEnum(KibanaAuthType.class);

        switch (kibanaAuthType) {
        case BASICAUTH:
            return createSgFrontendConfigBasicAuth();
        case SAML:
            return createSgFrontendConfigSaml();
        case OPENID:
            // Note: The name "OPENID" stems from the old config. This is actually a wrong name. Correct would be "oidc" instead. The new config will consistently use oidc.
            return createSgFrontendConfigOidc();
        case JWT:
            return createSgFrontendConfigJwt();
        case KERBEROS:
        case PROXY:
        default:
            throw new RuntimeException("Not implemented: " + kibanaAuthType);

        }

    }

    public UpdateInstructions createSgFrontendConfigBasicAuth() {
        UpdateInstructions updateInstructions = new UpdateInstructions()
                .mainInstructions("You have configured the Search Guard Kibana plugin to use basic authentication (user name and password based).");

        Map<String, Object> newSgFrontendConfig = new LinkedHashMap<>();

        newSgFrontendConfig.put("authcz", Collections.singletonList(ImmutableMap.of("type", "basic")));

        URI loadbalancerUrl = oldKibanaConfig.get("searchguard.basicauth.loadbalancer_url").asURI();

        if (loadbalancerUrl != null) {
            newSgFrontendConfig.put("base_url", loadbalancerUrl.toString());
        }

        // TOOD login form config

        updateInstructions.sgFrontendConfig(newSgFrontendConfig);

        this.kibanaConfigRewriter.remove("searchguard.auth.type");
        this.kibanaConfigRewriter.remove("searchguard.basicauth.loadbalancer_url");

        try {
            RewriteResult rewriteResult = this.kibanaConfigRewriter.rewrite();

            if (rewriteResult.isChanged()) {
                updateInstructions.kibanaConfigInstructions(
                        "Before starting Kibana with the updated plugin, you need to update the file config/kibana.yml in your Kibana installation. An updated kibana.yml file has been put by this tool to ...");
                updateInstructions.kibanaConfig(rewriteResult.getYaml());
            } else {
                updateInstructions.kibanaConfigInstructions("You do not need to update the Kibana configuration.");
            }
        } catch (RewriteException e) {
            updateInstructions.kibanaConfigInstructions(
                    "Before starting Kibana with the updated plugin, you need to update the file config/kibana.yml in your Kibana installation.\nPlease perform the following updates: "
                            + e.getManualInstructions());
        }

        // TODO other auth methods defined in sg_config.yml, esp. SAML

        return updateInstructions;
    }

    public UpdateInstructions createSgFrontendConfigSaml() {
        Map<String, Object> newSgFrontendConfig = new LinkedHashMap<>();

        List<DocNode> samlAuthDomains = oldSgConfig.getDocumentNode()
                .findNodesByJsonPath("$.sg_config.dynamic.authc.*[?(@.http_authenticator.type == 'saml')]");

        String frontendBaseUrl = null;

        if (samlAuthDomains.isEmpty()) {
            return new UpdateInstructions().error(
                    "No auth domains of type 'saml' are defined in the provided sg_config.yml file, even though kibana.yml is configured to use SAML authentication. This is an invalid configuration. Please check if you have provided the correct configuration files.");
        }

        List<DocNode> activeSamlAuthDomains = samlAuthDomains.stream().filter((node) -> node.get("http_enabled") != Boolean.FALSE).collect(toList());

        if (activeSamlAuthDomains.isEmpty()) {
            return new UpdateInstructions().error(
                    "All auth domains of type 'saml' defined in sg_config.yml are disabled, even though kibana.yml is configured to use SAML authentication. This is an invalid configuration. Please check if you have provided the correct configuration files.");
        }

        UpdateInstructions updateInstructions = new UpdateInstructions()
                .mainInstructions("You have configured Search Guard to use SAML authentication.");

        if (activeSamlAuthDomains.size() > 1) {
            updateInstructions.sgFrontendConfigInstructionsAdvanced(
                    "sg_config.yml defines more than one auth domain of type 'saml'. This is a non-standard advanced cofiguration. The new Search Guard Kibana plugin will use this configuration to present a list of all available SAML auth domains when logging in. The user can then choose from one of the auth domains.");
            updateInstructions.sgFrontendConfigInstructionsReview(
                    "Please review the settings. If one of the SAML auth domains is not necessary, you should remove it.");
        }

        List<Map<String, Object>> newAuthDomains = new ArrayList<>();

        for (DocNode samlAuthDomain : activeSamlAuthDomains) {
            Map<String, Object> newAuthDomain = new LinkedHashMap<>();
            newAuthDomains.add(newAuthDomain);

            newAuthDomain.put("type", "saml");

            if (activeSamlAuthDomains.size() > 1) {
                newAuthDomain.put("label", samlAuthDomain.getKey());
            }

            ValidationErrors samlAuthDomainValidationErrors = new ValidationErrors();
            ValidatingDocNode vSamlAuthDomain = new ValidatingDocNode(newAuthDomain, samlAuthDomainValidationErrors);

            String kibanaUrl = vSamlAuthDomain.get("http_authenticator.config.kibana_url").required().asString();

            if (frontendBaseUrl == null) {
                frontendBaseUrl = kibanaUrl;
            } else if (kibanaUrl != null && !frontendBaseUrl.equals(kibanaUrl)) {
                // TODO
                System.err.println(
                        "You have two SAML auth domains for different Kibana URLs. If you are running several Kibana instances, you need ... ");
            }

            String idpMetadataUrl = vSamlAuthDomain.get("http_authenticator.config.idp.metadata_url").asString();
            String idpMetadataFile = vSamlAuthDomain.get("http_authenticator.config.idp.metadata_file").asString();

            if (idpMetadataFile == null && idpMetadataUrl == null) {
                samlAuthDomainValidationErrors.add(new MissingAttribute("http_authenticator.config.idp.metadata_url"));
            }

            String idpEntityId = vSamlAuthDomain.get("http_authenticator.config.idp.entity_id").required().asString();

            Map<String, Object> idp = new LinkedHashMap<>();

            if (idpMetadataUrl != null) {
                idp.put("metadata_url", idpMetadataUrl);
            }

            if (idpMetadataFile != null) {
                idp.put("metadata_file", idpMetadataFile);
            }

            idp.put("entity_id", idpEntityId);

            // TODO tls stuff

            newAuthDomain.put("idp", idp);

            String spEntityId = vSamlAuthDomain.get("http_authenticator.config.sp.entity_id").required().asString();
            String spSignatureAlgorithm = vSamlAuthDomain.get("http_authenticator.config.sp.signature_algorithm").asString();
            String spSignaturePrivateKeyPassword = vSamlAuthDomain.get("http_authenticator.config.sp.signature_private_key_password").asString();
            String spSignaturePrivateKeyFilepath = vSamlAuthDomain.get("http_authenticator.config.sp.signature_private_key_filepath").asString();
            String spSignaturePrivateKey = vSamlAuthDomain.get("http_authenticator.config.sp.signature_private_key").asString();
            Boolean useForceAuth = vSamlAuthDomain.get("http_authenticator.config.sp.forceAuthn").asBoolean();

            Map<String, Object> sp = new LinkedHashMap<>();

            sp.put("entity_id", spEntityId);

            if (spSignatureAlgorithm != null) {
                sp.put("signature_algorithm", spSignatureAlgorithm);
            }

            if (spSignaturePrivateKeyPassword != null) {
                sp.put("signature_private_key_password", spSignaturePrivateKeyPassword);
            }

            if (spSignaturePrivateKeyFilepath != null) {
                sp.put("signature_private_key_filepath", spSignaturePrivateKeyFilepath);
            }

            if (spSignaturePrivateKey != null) {
                sp.put("signature_private_key", spSignaturePrivateKey);
            }

            if (useForceAuth != null) {
                sp.put("forceAuthn", useForceAuth);
            }

            newAuthDomain.put("sp", sp);

            String subjectKey = vSamlAuthDomain.get("http_authenticator.config.subject_key").asString();

            if (subjectKey != null) {
                newAuthDomain.put("subject_key", subjectKey);
            }

            String subjectPattern = vSamlAuthDomain.get("http_authenticator.config.subject_pattern").asString();

            if (subjectPattern != null) {
                newAuthDomain.put("subject_pattern", subjectPattern);
            }

            String rolesKey = vSamlAuthDomain.get("http_authenticator.config.roles_key").required().asString();

            if (rolesKey != null) {
                newAuthDomain.put("roles_key", rolesKey);
            }

            String rolesSeparator = vSamlAuthDomain.get("http_authenticator.config.roles_seperator").asString();

            if (rolesSeparator != null) {
                newAuthDomain.put("roles_seperator", rolesKey);
            }

            Boolean checkIssuer = vSamlAuthDomain.get("http_authenticator.config.check_issuer").asBoolean();

            if (checkIssuer != null) {
                newAuthDomain.put("check_issuer", checkIssuer);
            }

            Object validator = vSamlAuthDomain.get("http_authenticator.config.validator").asAnything();

            if (validator instanceof Map) {
                newAuthDomain.put("validator", validator);
            }

            if (samlAuthDomainValidationErrors.hasErrors()) {
                oldSgConfigValidationErrors.add("sg_config.dynamic.authc." + samlAuthDomain.getKey(), samlAuthDomainValidationErrors);
            }

        }

        newSgFrontendConfig.put("base_url", frontendBaseUrl);
        newSgFrontendConfig.put("authcz", newAuthDomains);

        updateInstructions.sgFrontendConfig(newSgFrontendConfig);

        this.kibanaConfigRewriter.remove("searchguard.auth.type");
        this.kibanaConfigRewriter.remove("searchguard.basicauth.loadbalancer_url");

        try {
            RewriteResult rewriteResult = this.kibanaConfigRewriter.rewrite();

            if (rewriteResult.isChanged()) {
                updateInstructions.kibanaConfigInstructions(
                        "Before starting Kibana with the updated plugin, you need to update the file config/kibana.yml in your Kibana installation. An updated kibana.yml file has been put by this tool to ...");
                updateInstructions.kibanaConfig(rewriteResult.getYaml());
            } else {
                updateInstructions.kibanaConfigInstructions("You do not need to update the Kibana configuration.");
            }
        } catch (RewriteException e) {
            updateInstructions.kibanaConfigInstructions(
                    "Before starting Kibana with the updated plugin, you need to update the file config/kibana.yml in your Kibana installation.\nPlease perform the following updates: "
                            + e.getManualInstructions());
        }

        return updateInstructions;
    }

    public UpdateInstructions createSgFrontendConfigOidc() {
        Map<String, Object> newSgFrontendConfig = new LinkedHashMap<>();

        List<DocNode> oidcAuthDomains = oldSgConfig.getDocumentNode()
                .findNodesByJsonPath("$.sg_config.dynamic.authc.*[?(@.http_authenticator.type == 'openid')]");

        String frontendBaseUrl = oldKibanaConfig.get("searchguard.openid.base_redirect_url").asString();

        if (frontendBaseUrl == null) {
            frontendBaseUrl = getFrontendBaseUrlFromKibanaYaml();
        }

        if (oidcAuthDomains.isEmpty()) {
            return new UpdateInstructions().error(
                    "No auth domains of type 'openid' are defined in the provided sg_config.yml, even though kibana.yml is configured to use OIDC authentication. This is an invalid configuration. Please check if you have provided the correct configuration files.");
        }

        List<DocNode> activeOidcAuthDomains = oidcAuthDomains.stream().filter((node) -> node.get("http_enabled") != Boolean.FALSE).collect(toList());

        if (activeOidcAuthDomains.isEmpty()) {
            return new UpdateInstructions().error(
                    "All auth domains of type 'openid' defined in sg_config.yml are disabled, even though kibana.yml is configured to use OIDC authentication. This is an invalid configuration. Please check if you have provided the correct configuration files.");
        }

        UpdateInstructions updateInstructions = new UpdateInstructions()
                .mainInstructions("You have configured Search Guard to use OIDC authentication.");

        if (activeOidcAuthDomains.size() > 1) {
            // TODO multi instances
        }

        List<Map<String, Object>> newAuthDomains = new ArrayList<>();

        for (DocNode oidcAuthDomain : activeOidcAuthDomains) {
            Map<String, Object> newAuthDomain = new LinkedHashMap<>();
            newAuthDomains.add(newAuthDomain);

            newAuthDomain.put("type", "oidc");

            if (activeOidcAuthDomains.size() > 1) {
                newAuthDomain.put("label", oidcAuthDomain.getKey());
            }

            ValidationErrors authDomainValidationErrors = new ValidationErrors();
            ValidatingDocNode vOidcAuthDomain = new ValidatingDocNode(oidcAuthDomain, authDomainValidationErrors);

            String openIdConnectUrl = vOidcAuthDomain.get("http_authenticator.config.openid_connect_url").required().asString();
            String kibanaYmlOpenIdConnectUrl = oldKibanaConfig.get("searchguard.openid.connect_url").asString();

            if (openIdConnectUrl != null && kibanaYmlOpenIdConnectUrl != null && !openIdConnectUrl.equals(kibanaYmlOpenIdConnectUrl)) {
                authDomainValidationErrors.add(new ValidationError("http_authenticator.config.openid_connect_url",
                        "The openid_connect_url in sg_config.yml and kibana.yml must be equal. However, in the given configuration the URLs differ."));
            }

            String clientId = oldKibanaConfig.get("searchguard.openid.client_id").required().asString();
            String clientSecret = oldKibanaConfig.get("searchguard.openid.client_secret").required().asString();
            String scope = oldKibanaConfig.get("searchguard.openid.scope").asString();
            String logoutUrl = oldKibanaConfig.get("searchguard.openid.logout_url").asString();

            newAuthDomain.put("idp.openid_configuration_url", openIdConnectUrl);
            newAuthDomain.put("client_id", clientId);
            newAuthDomain.put("client_secret", clientSecret);

            if (scope != null) {
                newAuthDomain.put("scope", scope);
            }

            if (logoutUrl != null) {
                newAuthDomain.put("logout_url", logoutUrl);
            }

            String subjectKey = vOidcAuthDomain.get("http_authenticator.config.subject_key").asString();

            if (subjectKey != null) {
                newAuthDomain.put("subject_key", subjectKey);
            }

            String subjectPath = vOidcAuthDomain.get("http_authenticator.config.subject_path").asString();

            if (subjectPath != null) {
                newAuthDomain.put("subject_path", subjectPath);
            }

            String subjectPattern = vOidcAuthDomain.get("http_authenticator.config.subject_pattern").asString();

            if (subjectPattern != null) {
                newAuthDomain.put("subject_pattern", subjectPattern);
            }

            String rolesKey = vOidcAuthDomain.get("http_authenticator.config.roles_key").asString();

            if (rolesKey != null) {
                newAuthDomain.put("roles_key", rolesKey);
            }

            String rolesPath = vOidcAuthDomain.get("http_authenticator.config.roles_path").asString();

            if (rolesPath != null) {
                newAuthDomain.put("roles_path", rolesPath);
            }

            Object claimsToUserAttrs = vOidcAuthDomain.get("http_authenticator.config.map_claims_to_user_attrs").asAnything();

            if (claimsToUserAttrs != null) {
                newAuthDomain.put("map_claims_to_user_attrs", claimsToUserAttrs);
            }

            Object proxy = vOidcAuthDomain.get("http_authenticator.config.proxy").asAnything();

            if (proxy != null) {
                newAuthDomain.put("idp.proxy", proxy);
            }

            Map<String, Object> tls = vOidcAuthDomain.get("http_authenticator.config.openid_connect_idp").asMap();

            if (tls != null) {
                MigrationResult migrationResult = migrateTlsConfig(tls);

                if (migrationResult != null) {
                    newAuthDomain.put("idp.tls", tls);
                    oldSgConfigValidationErrors.add("http_authenticator.config.openid_connect_idp", migrationResult.getSourceValidationErrors());
                }
            }

            migrateAttribute("idp_request_timeout_ms", vOidcAuthDomain, newAuthDomain);
            migrateAttribute("idp_queued_thread_timeout_ms", vOidcAuthDomain, newAuthDomain);
            migrateAttribute("refresh_rate_limit_time_window_ms", vOidcAuthDomain, newAuthDomain);
            migrateAttribute("refresh_rate_limit_count", vOidcAuthDomain, newAuthDomain);
            migrateAttribute("cache_jwks_endpoint", vOidcAuthDomain, newAuthDomain);

            if (authDomainValidationErrors.hasErrors()) {
                oldSgConfigValidationErrors.add("sg_config.dynamic.authc." + oidcAuthDomain.getKey(), authDomainValidationErrors);
            }

        }

        newSgFrontendConfig.put("base_url", frontendBaseUrl);
        newSgFrontendConfig.put("authcz", newAuthDomains);

        updateInstructions.sgFrontendConfig(newSgFrontendConfig);

        this.kibanaConfigRewriter.remove("searchguard.auth.type");
        this.kibanaConfigRewriter.remove("searchguard.basicauth.loadbalancer_url");
        this.kibanaConfigRewriter.remove("searchguard.openid.connect_url");
        this.kibanaConfigRewriter.remove("searchguard.openid.client_id");
        this.kibanaConfigRewriter.remove("searchguard.openid.client_secret");
        this.kibanaConfigRewriter.remove("searchguard.openid.scope");
        this.kibanaConfigRewriter.remove("searchguard.openid.header");
        this.kibanaConfigRewriter.remove("searchguard.openid.base_redirect_url");
        this.kibanaConfigRewriter.remove("searchguard.openid.logout_url");

        try {
            RewriteResult rewriteResult = this.kibanaConfigRewriter.rewrite();

            if (rewriteResult.isChanged()) {
                updateInstructions.kibanaConfigInstructions(
                        "Before starting Kibana with the updated plugin, you need to update the file config/kibana.yml in your Kibana installation. An updated kibana.yml file has been put by this tool to ...");
                updateInstructions.kibanaConfig(rewriteResult.getYaml());
            } else {
                updateInstructions.kibanaConfigInstructions("You do not need to update the Kibana configuration.");
            }
        } catch (RewriteException e) {
            updateInstructions.kibanaConfigInstructions(
                    "Before starting Kibana with the updated plugin, you need to update the file config/kibana.yml in your Kibana installation.\nPlease perform the following updates: "
                            + e.getManualInstructions());
        }

        return updateInstructions;

    }

    public UpdateInstructions createSgFrontendConfigJwt() {

        // header is not used any more
        //String header = oldKibanaConfig.get("searchguard.jwt.header").asString();
        String urlParameter = oldKibanaConfig.get("searchguard.jwt.url_parameter").asString();
        String loginEndpoint = oldKibanaConfig.get("searchguard.jwt.login_endpoint").asString();

        UpdateInstructions updateInstructions = new UpdateInstructions();

        if (urlParameter != null) {
            updateInstructions.mainInstructions("You have configured Search Guard to use authentication using a JWT specified as URL parameter.");
        } else {
            updateInstructions.mainInstructions(
                    "You have configured Search Guard to use authentication using a JWT provided as an Authorization header. This is an advanced configuration, usually only found in combination with a proxy which adds the Authorization header to HTTP requests.");
            // TODO migrate to proxy auth
            return updateInstructions;
        }

        this.kibanaConfigRewriter.insertAfter("searchguard.auth.type", new YamlRewriter.Attribute("searchguard.auth.jwt.enabled", true));
        this.kibanaConfigRewriter.remove("searchguard.auth.type");
        this.kibanaConfigRewriter.remove("searchguard.jwt.header");
        this.kibanaConfigRewriter.remove("searchguard.jwt.login_endpoint");

        try {
            RewriteResult rewriteResult = this.kibanaConfigRewriter.rewrite();

            if (rewriteResult.isChanged()) {
                updateInstructions.kibanaConfigInstructions(
                        "Before starting Kibana with the updated plugin, you need to update the file config/kibana.yml in your Kibana installation. An updated kibana.yml file has been put by this tool to ...");
                updateInstructions.kibanaConfig(rewriteResult.getYaml());
            } else {
                updateInstructions.kibanaConfigInstructions("You do not need to update the Kibana configuration.");
            }
        } catch (RewriteException e) {
            updateInstructions.kibanaConfigInstructions(
                    "Before starting Kibana with the updated plugin, you need to update the file config/kibana.yml in your Kibana installation.\nPlease perform the following updates: "
                            + e.getManualInstructions());
        }

        if (loginEndpoint != null) {
            Map<String, Object> newSgFrontendConfig = new LinkedHashMap<>();

            newSgFrontendConfig.put("authcz", Collections.singletonList(ImmutableMap.of("type", "link", "url", loginEndpoint)));

            updateInstructions.sgFrontendConfig(newSgFrontendConfig);

            return updateInstructions;
        } else {
            updateInstructions.sgFrontendConfigInstructions(
                    "In the current configuration, the Search Guard Kibana plugin does not provide a login form. The only way to login is opening a Kibana URL with the URL parameter "
                            + urlParameter
                            + ". Thus, the sg_frontend_config.yml file generated by this tool will also define no authenticators. If you want to have more login methods, you can add these to sg_frontend_config.yml.");

            updateInstructions.sgFrontendConfig(Collections.emptyMap());

            return updateInstructions;
        }
    }

    private String getFrontendBaseUrlFromKibanaYaml() {
        boolean https = oldKibanaConfig.get("server.ssl.enabled").withDefault(false).asBoolean();
        String host = oldKibanaConfig.get("server.host").required().asString();
        int port = oldKibanaConfig.get("server.port").withDefault(-1).asInteger();
        String basePath = oldKibanaConfig.get("server.basepath").asString();

        if (port == 80 && !https) {
            port = -1;
        } else if (port == 443 && https) {
            port = -1;
        }

        try {
            return new URI(https ? "https" : "http", null, host, port, basePath, null, null).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void migrateAttribute(String name, ValidatingDocNode source, Map<String, Object> target) {
        Object value = source.get("http_authenticator.config." + name).asAnything();

        if (value != null) {
            target.put(name, value);
        }
    }

    private MigrationResult migrateTlsConfig(Map<String, Object> config) {
        if (config == null) {
            return null;
        }

        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(config, validationErrors);

        if (!vNode.get("enable_ssl").withDefault(false).asBoolean()) {
            return null;
        }

        Map<String, Object> result = new LinkedHashMap<>();

        if (vNode.hasNonNull("pemtrustedcas_content")) {
            List<String> pems = vNode.get("pemtrustedcas_content").asListOfStrings();
            result.put("trusted_cas", pems.size() == 1 ? pems.get(0) : pems);
        } else if (vNode.hasNonNull("pemtrustedcas_filepath")) {
            String path = vNode.get("pemtrustedcas_filepath").asString();
            result.put("trusted_cas", "${file:" + path + "}");
        }

        if (vNode.get("enable_ssl_client_auth").withDefault(false).asBoolean()) {
            Map<String, Object> newClientAuthConfig = new LinkedHashMap<>();

            if (vNode.hasNonNull("pemcert_content")) {
                List<String> pems = vNode.get("pemcert_content").asListOfStrings();
                newClientAuthConfig.put("certificate", pems.size() == 1 ? pems.get(0) : pems);
            } else if (vNode.hasNonNull("pemcert_filepath")) {
                String path = vNode.get("pemcert_filepath").asString();
                newClientAuthConfig.put("certificate", "${file:" + path + "}");
            }

            if (vNode.hasNonNull("pemkey_content")) {
                List<String> pems = vNode.get("pemkey_content").asListOfStrings();
                newClientAuthConfig.put("private_key", pems.size() == 1 ? pems.get(0) : pems);
            } else if (vNode.hasNonNull("pemkey_filepath")) {
                String path = vNode.get("pemkey_filepath").asString();
                newClientAuthConfig.put("private_key", "${file:" + path + "}");
            }

            if (vNode.hasNonNull("pemkey_password")) {
                String password = vNode.get("pemkey_password").asString();
                newClientAuthConfig.put("private_key_password", password);
            }

            if (newClientAuthConfig.size() != 0) {
                result.put("client_auth", newClientAuthConfig);
            }

        }

        if (vNode.hasNonNull("enabled_ssl_protocols")) {
            result.put("enabled_protocols", vNode.get("enabled_ssl_protocols").asListOfStrings());
        }

        if (vNode.hasNonNull("enabled_ssl_ciphers")) {
            result.put("enabled_ciphers", vNode.get("enabled_ssl_ciphers").asListOfStrings());
        }

        if (vNode.hasNonNull("trust_all")) {
            result.put("trust_all", vNode.get("enabled_ssl_ciphers").asBoolean());
        }

        if (vNode.hasNonNull("verify_hostnames")) {
            result.put("verify_hostnames", vNode.get("verify_hostnames").asBoolean());
        }

        return new MigrationResult(result, validationErrors);
    }

    public static enum KibanaAuthType {
        BASICAUTH, JWT, OPENID, PROXY, KERBEROS, SAML
    }

    static class UpdateInstructions {

        private String mainInstructions;
        private String error;

        private String esPluginUpdateInstructions = "";
        private String sgFrontendConfigInstructions = "After having updated the Search Guard Elasticsearch plugin, please upload the new sg_frontend_config.yml with sgadmin. Do not modify other configuration files.\n"
                + "The file sg_frontend_config.yml has been automatically generated from the settings in sg_config.yml and kibana.yml.";
        private String sgFrontendConfigInstructionsAdvanced;
        private String sgFrontendConfigInstructionsReview = "Please review the settings.";
        private String sgFrontendConfigInstructionsUpload = "";
        private Map<String, Object> sgFrontendConfig;

        private String kibanaPluginUpdateInstructions = "After the new sg_frontend_config.yml has been successfully uploaded to Search Guard, you can update the Search Guard Kibana plugin.";
        private String kibanaConfigInstructions;
        private String kibanaConfig;

        public Map<String, Object> getSgFrontendConfig() {
            return sgFrontendConfig;
        }

        public UpdateInstructions sgFrontendConfig(Map<String, Object> sgFrontendConfig) {
            this.sgFrontendConfig = sgFrontendConfig;
            return this;
        }

        public String getKibanaConfig() {
            return kibanaConfig;
        }

        public UpdateInstructions kibanaConfig(String kibanaConfig) {
            this.kibanaConfig = kibanaConfig;
            return this;
        }

        public String getMainInstructions() {
            return mainInstructions;
        }

        public UpdateInstructions mainInstructions(String mainInstructions) {
            this.mainInstructions = mainInstructions;
            return this;

        }

        public String getSgFrontendConfigInstructions() {
            return sgFrontendConfigInstructions;
        }

        public UpdateInstructions sgFrontendConfigInstructions(String sgFrontendConfigInstructions) {
            this.sgFrontendConfigInstructions = sgFrontendConfigInstructions;
            return this;

        }

        public String getKibanaConfigInstructions() {
            return kibanaConfigInstructions;
        }

        public UpdateInstructions kibanaConfigInstructions(String kibanaConfigInstructions) {
            this.kibanaConfigInstructions = kibanaConfigInstructions;
            return this;

        }

        public String getError() {
            return error;
        }

        public UpdateInstructions error(String error) {
            this.error = error;
            return this;

        }

        public String getEsPluginUpdateInstructions() {
            return esPluginUpdateInstructions;
        }

        public UpdateInstructions esPluginUpdateInstructions(String esPluginUpdateInstructions) {
            this.esPluginUpdateInstructions = esPluginUpdateInstructions;
            return this;
        }

        public String getKibanaPluginUpdateInstructions() {
            return kibanaPluginUpdateInstructions;
        }

        public UpdateInstructions kibanaPluginUpdateInstructions(String kibanaPluginUpdateInstructions) {
            this.kibanaPluginUpdateInstructions = kibanaPluginUpdateInstructions;
            return this;
        }

        public String getSgFrontendConfigInstructionsAdvanced() {
            return sgFrontendConfigInstructionsAdvanced;
        }

        public UpdateInstructions sgFrontendConfigInstructionsAdvanced(String sgFrontendConfigInstructionsAdvanced) {
            this.sgFrontendConfigInstructionsAdvanced = sgFrontendConfigInstructionsAdvanced;
            return this;
        }

        public String getSgFrontendConfigInstructionsReview() {
            return sgFrontendConfigInstructionsReview;
        }

        public UpdateInstructions sgFrontendConfigInstructionsReview(String sgFrontendConfigInstructionsReview) {
            this.sgFrontendConfigInstructionsReview = sgFrontendConfigInstructionsReview;
            return this;

        }

    }

    static class MigrationResult {
        private final Map<String, Object> config;

        private final ValidationErrors sourceValidationErrors;

        MigrationResult(Map<String, Object> config, ValidationErrors sourceValidationErrors) {
            this.config = config;
            this.sourceValidationErrors = sourceValidationErrors;
        }

        public Map<String, Object> getConfig() {
            return config;
        }

        public ValidationErrors getSourceValidationErrors() {
            return sourceValidationErrors;
        }

    }

}
