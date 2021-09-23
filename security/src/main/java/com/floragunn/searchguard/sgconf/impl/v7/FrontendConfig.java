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

package com.floragunn.searchguard.sgconf.impl.v7;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContent.Params;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.VariableResolvers;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.searchguard.auth.AuthenticationFrontend;
import com.floragunn.searchguard.auth.session.ApiAuthenticationFrontend;
import com.floragunn.searchguard.modules.NoSuchComponentException;
import com.floragunn.searchguard.modules.SearchGuardComponentRegistry;
import com.google.common.hash.Hashing;

public class FrontendConfig implements Document<FrontendConfig> {
    private static final Logger log = LogManager.getLogger(FrontendConfig.class);

    public static final Authcz DEFAULT_BASIC_AUTHCZ = new Authcz(null, "basic", "Login",
            "If you have forgotten your username or password, please ask your system administrator");
    public static final FrontendConfig BASIC = new FrontendConfig(Collections.singletonList(DEFAULT_BASIC_AUTHCZ));

    private List<Authcz> authcz;
    private Multitenancy multitenancy;
    private LoginPage loginPage;
    private boolean debug;
    private Map<String, Object> parsedJson;

    FrontendConfig() {
    }

    public FrontendConfig(List<Authcz> authcz) {
        this.authcz = Collections.unmodifiableList(authcz);
        this.loginPage = LoginPage.DEFAULT;
    }

    public List<Authcz> getAuthcz() {
        return authcz;
    }

    public Multitenancy getMultitenancy() {
        return multitenancy;
    }

    public static FrontendConfig parse(Map<String, Object> parsedJson,
            SearchGuardComponentRegistry<ApiAuthenticationFrontend> authenticationFrontendRegistry, VariableResolvers configVariableProviders)
            throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(parsedJson, validationErrors);

        FrontendConfig result = new FrontendConfig();
        result.parsedJson = parsedJson;

        AuthenticationFrontend.Context context = new AuthenticationFrontend.Context(null, null, configVariableProviders);

        result.authcz = vNode.get("authcz").asList((documentNode) -> Authcz.parse(documentNode, context, authenticationFrontendRegistry));
        result.multitenancy = vNode.get("multitenancy").withDefault(Multitenancy.DEFAULT).by(Multitenancy::parse);
        result.loginPage = vNode.get("login_page").withDefault(LoginPage.DEFAULT).by(LoginPage::parse);
        result.debug = vNode.get("debug").withDefault(false).asBoolean();

        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        return result;
    }

    public static class Authcz implements Document<Authcz> {
        private String type;
        private String label;
        private boolean enabled = true;
        private AuthenticationFrontend authenticationFrontend;
        private boolean unavailable = false;
        private String message;
        private String id;
        private Map<String, Object> parsedJson;

        public Authcz() {

        }

        public Authcz(String id, String type, String label) {
            this.id = id;
            this.type = type;
            this.label = label;
        }

        public Authcz(String id, String type, String label, String message) {
            this.id = id;
            this.type = type;
            this.label = label;
            this.message = message;
        }

        public Authcz(String id, String type, String label, AuthenticationFrontend authenticationFrontend) {
            this.id = id;
            this.type = type;
            this.label = label;
            this.authenticationFrontend = authenticationFrontend;
        }

        public static Authcz parse(DocNode documentNode, AuthenticationFrontend.Context context,
                SearchGuardComponentRegistry<ApiAuthenticationFrontend> authenticationFrontendRegistry) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(documentNode, validationErrors);

            Authcz result = new Authcz();
            result.parsedJson = documentNode.toMap();
            result.type = vNode.get("type").required().asString();
            result.label = vNode.get("label").withDefault(result.type).asString();
            result.enabled = vNode.get("enabled").withDefault(true).asBoolean();
            result.message = vNode.get("message").asString();

            if ("basic".equals(result.type)) {
                if (result.message == null) {
                    result.message = DEFAULT_BASIC_AUTHCZ.getMessage();
                }
            } else if (authenticationFrontendRegistry != null && result.enabled) {
                result.id = Hashing.sha256().hashString(documentNode.toMap().toString(), StandardCharsets.UTF_8).toString();

                try {
                    result.authenticationFrontend = authenticationFrontendRegistry.getInstance(result.type,
                            documentNode.without("type", "label", "enabled", "message"), context);
                } catch (ConfigValidationException e) {
                    log.warn("Invalid config for authentication frontend " + result, e);

                    validationErrors.add(null, e);
                    result.unavailable = true;
                    result.message = "Unavailable due to configuration error. Please contact your system administrator";
                } catch (NoSuchComponentException e) {
                    validationErrors.add(new InvalidAttributeValue("type", result.type, "basic|oidc|saml"));
                } catch (Exception e) {
                    log.error("Error while creating authentication frontend " + result, e);
                    result.unavailable = true;
                    result.message = "Unavailable due to configuration error. Please contact your system administrator";
                }
            }

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        public String getType() {
            return type;
        }

        public String getLabel() {
            return label;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public AuthenticationFrontend getAuthenticationFrontend() {
            return authenticationFrontend;
        }

        public boolean isUnavailable() {
            return unavailable;
        }

        public String getMessage() {
            return message;
        }

        public String getId() {
            return id;
        }

        @Override
        public Object toBasicObject() {
            return parsedJson;
        }

        @Override
        public String toString() {
            return "Authcz [type=" + type + ", authenticationFrontend=" + authenticationFrontend + ", id=" + id + "]";
        }
    }

    public static class Multitenancy {
        public static final Multitenancy DEFAULT = new Multitenancy();

        private boolean enabled = true;
        private String index = ".kibana";
        private String serverUsername = "kibanaserver";

        public static Multitenancy parse(DocNode documentNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(documentNode, validationErrors);

            Multitenancy result = new Multitenancy();
            result.enabled = vNode.get("enabled").withDefault(true).asBoolean();
            result.index = vNode.get("index").withDefault(".kibana").asString();
            result.serverUsername = vNode.get("server_username").withDefault("kibanaserver").asString();

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public String getIndex() {
            return index;
        }

        public String getServerUsername() {
            return serverUsername;
        }
    }

    public static class LoginPage implements Document<LoginPage> {
        public static final LoginPage DEFAULT = new LoginPage();

        private URI brandImage = URI.create("plugins/searchguard/assets/searchguard_logo.svg");
        private boolean showBrandImage = true;
        private String title = "Please log in";
        private String buttonStyle = "";

        public static LoginPage parse(DocNode documentNode) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(documentNode, validationErrors);

            LoginPage result = new LoginPage();
            result.brandImage = vNode.get("brand_image").withDefault(DEFAULT.brandImage).asURI();
            result.showBrandImage = vNode.get("show_brand_image").withDefault(true).asBoolean();
            result.title = vNode.get("title").asString();
            result.buttonStyle = vNode.get("button_style").asString();

            validationErrors.throwExceptionForPresentErrors();

            return result;
        }

        public URI getBrandImage() {
            return brandImage;
        }

        public void setBrandImage(URI brandImage) {
            this.brandImage = brandImage;
        }

        public boolean isShowBrandImage() {
            return showBrandImage;
        }

        public void setShowBrandImage(boolean showBrandImage) {
            this.showBrandImage = showBrandImage;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getButtonStyle() {
            return buttonStyle;
        }

        public void setButtonStyle(String buttonStyle) {
            this.buttonStyle = buttonStyle;
        }

        @Override
        public Object toBasicObject() {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("brand_image", brandImage != null ? brandImage.toASCIIString() : null);
            result.put("show_brand_image", showBrandImage);
            result.put("title", title);
            result.put("button_style", buttonStyle);
            return result;
        }
    }

    public LoginPage getLoginPage() {
        return loginPage;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    @Override
    public Object toBasicObject() {
        if (parsedJson != null) {
            return parsedJson;
        } else {
            Map<String, Object> result = new LinkedHashMap<>();

            if (loginPage != LoginPage.DEFAULT) {
                result.put("login_page", loginPage.toBasicObject());
            }

            result.put("authcz", authcz.stream().map(Authcz::toBasicObject).collect(Collectors.toList()));

            return result;
        }
    }

}
