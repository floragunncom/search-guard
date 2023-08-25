/*
 * Copyright 2021-2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.session;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser.Context;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.codova.documents.patch.PatchableDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.AuthenticationFrontend;
import com.floragunn.searchguard.authc.base.StandardAuthenticationDomain;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FrontendAuthcConfig implements PatchableDocument<FrontendAuthcConfig>, AutoCloseable {

    public static final FrontendAuthenticationDomain DEFAULT_BASIC_AUTHC = new FrontendAuthenticationDomain("basic", "Login",
            "If you have forgotten your username or password, please ask your system administrator");
    public static final FrontendAuthcConfig BASIC = new FrontendAuthcConfig(Collections.singletonList(DEFAULT_BASIC_AUTHC));

    private final Logger log = LogManager.getLogger(FrontendAuthcConfig.class);
    private ImmutableList<FrontendAuthenticationDomain> authDomains;
    private LoginPage loginPage;
    private boolean debug;
    private Map<String, Object> parsedJson;

    FrontendAuthcConfig() {
    }

    public FrontendAuthcConfig(List<FrontendAuthenticationDomain> authDomains) {
        this.authDomains = ImmutableList.of(authDomains);
        this.loginPage = LoginPage.DEFAULT;
    }

    public ImmutableList<FrontendAuthenticationDomain> getAuthDomains() {
        return authDomains;
    }

    public static ValidationResult<FrontendAuthcConfig> parse(Object parsedJson, ConfigurationRepository.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode;
        try {
            vNode = new ValidatingDocNode(DocNode.wrap(parsedJson).splitDottedAttributeNamesToTree(), validationErrors, context);
        } catch (UnexpectedDocumentStructureException e) {
            return new ValidationResult<FrontendAuthcConfig>(e.getValidationErrors());
        }

        MetricsLevel metricsLevel = vNode.get("metrics").withDefault(MetricsLevel.BASIC).asEnum(MetricsLevel.class);

        FrontendAuthcConfig result = new FrontendAuthcConfig();
        result.parsedJson = DocNode.wrap(parsedJson);
        result.authDomains = ImmutableList
                .of(vNode.get("auth_domains").asList((documentNode) -> FrontendAuthenticationDomain.parse(documentNode, context, metricsLevel)));
        checkForMultipleAuthDomainsWithAutoSelectEnabled(result.authDomains, validationErrors);

        result.loginPage = vNode.get("login_page").withDefault(LoginPage.DEFAULT).by(LoginPage::parse);
        result.debug = vNode.get("debug").withDefault(false).asBoolean();

        vNode.checkForUnusedAttributes();

        return new ValidationResult<FrontendAuthcConfig>(result, validationErrors);
    }

    private static void checkForMultipleAuthDomainsWithAutoSelectEnabled(ImmutableList<FrontendAuthenticationDomain> authDomains, ValidationErrors validationErrors) {
        long domainsWithAutoSelectEnabled = authDomains
                .stream()
                .filter(FrontendAuthenticationDomain::isAutoSelect)
                .count();
        if (domainsWithAutoSelectEnabled > 1) {
            validationErrors.add(new ValidationError("auth_domains", "Only one frontend authentication domain can have 'auto_select' enabled"));
        }
    }

    public static class FrontendAuthenticationDomain implements Document<FrontendAuthenticationDomain> {
        private String type;
        private String label;
        private boolean enabled = true;
        private AuthenticationDomain<ApiAuthenticationFrontend> authenticationDomain;
        private boolean unavailable = false;
        private String message;
        private Map<String, Object> parsedJson;
        private boolean captureUrlFragment;
        private boolean autoSelect = false;

        public FrontendAuthenticationDomain() {

        }

        public FrontendAuthenticationDomain(String type, String label) {
            this.type = type;
            this.label = label;
        }

        public FrontendAuthenticationDomain(String type, String label, String message) {
            this.type = type;
            this.label = label;
            this.message = message;
        }

        public FrontendAuthenticationDomain(String type, String label, AuthenticationDomain<ApiAuthenticationFrontend> authenticationDomain) {
            this.type = type;
            this.label = label;
            this.authenticationDomain = authenticationDomain;
        }

        public static FrontendAuthenticationDomain parse(DocNode documentNode, ConfigurationRepository.Context context, MetricsLevel metricsLevel)
                throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(documentNode, validationErrors);

            FrontendAuthenticationDomain result = new FrontendAuthenticationDomain();
            try {
                result.parsedJson = documentNode.toMap();
                result.type = vNode.get("type").required().asString();
                result.label = vNode.get("label").withDefault(result.type).asString();
                result.enabled = vNode.get("enabled").withDefault(true).asBoolean();
                result.message = vNode.get("message").asString();
                result.captureUrlFragment = vNode.get("capture_url_fragment").withDefault(false).asBoolean();
                result.autoSelect = vNode.get("auto_select").withDefault(false).asBoolean();

                if ("basic".equals(result.type)) {
                    if (result.message == null) {
                        result.message = DEFAULT_BASIC_AUTHC.getMessage();
                    }
                } else if (context != null && result.enabled) {
                    result.authenticationDomain = StandardAuthenticationDomain.parse(vNode, validationErrors, ApiAuthenticationFrontend.class, context,
                            metricsLevel);
                }
            } catch (Exception e) { //handle all unknown exceptions
                if (e instanceof ConfigValidationException) {
                    throw e;
                } else {
                    validationErrors.add(new ValidationError(
                            null, String.format("Failed to parse config due to exception: %s - %s", e.getClass().getName(), e.getMessage())).cause(e)
                    );
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

        public AuthenticationDomain<ApiAuthenticationFrontend> getAuthenticationDomain() {
            return authenticationDomain;
        }

        public AuthenticationFrontend getAuthenticationFrontend() {
            return authenticationDomain != null ? authenticationDomain.getFrontend() : null;
        }

        public boolean isUnavailable() {
            return unavailable;
        }

        public String getMessage() {
            return message;
        }

        public String getId() {
            return authenticationDomain != null ? authenticationDomain.getId() : null;
        }

        @Override
        public Object toBasicObject() {
            return parsedJson;
        }

        public boolean isCaptureUrlFragment() {
            return captureUrlFragment;
        }

        public boolean isAutoSelect() {
            return autoSelect;
        }

        @Override
        public String toString() {
            return "Authenticator [type=" + type + ", authenticationDomain=" + authenticationDomain + "]";
        }
    }

    public static class LoginPage implements Document<LoginPage> {
        public static final LoginPage DEFAULT = new LoginPage();

        private URI brandImage = URI.create("/plugins/searchguard/assets/searchguard_logo.svg");
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

            result.put("authenticators", authDomains.stream().map(FrontendAuthenticationDomain::toBasicObject).collect(Collectors.toList()));

            return result;
        }
    }

    @Override
    public FrontendAuthcConfig parseI(DocNode docNode, Context context) throws ConfigValidationException {
        return parse(docNode, (ConfigurationRepository.Context) context).get();
    }

    @Override
    public void close()  {
        for (FrontendAuthenticationDomain authenticator : this.authDomains) {
            try {
                if (authenticator.getAuthenticationDomain() instanceof AutoCloseable) {
                    ((AutoCloseable) authenticator.getAuthenticationDomain()).close();
                }
            } catch (Exception e) {
                log.warn("Error while closing auth domain {}", authenticator.getAuthenticationDomain(), e);
            }
        }

    }

}
