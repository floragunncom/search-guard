/*
 * Copyright 2022 floragunn GmbH
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

package com.floragunn.searchguard.authc.base;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.AuthenticationBackend.UserMapper;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.google.common.base.Splitter;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;

public class UserMapping implements UserMapper, AuthenticationDomain.CredentialsMapper {
    private static final Logger log = LogManager.getLogger(UserMapping.class);

    public AuthCredentials mapCredentials(AuthCredentials authCredentials) throws CredentialsException {
        if (log.isDebugEnabled()) {
            log.debug("Mapping user using attributes " + authCredentials.getAttributesForUserMapping() + " for " + authCredentials);
        }

        if (userName == null || userName.isEmpty()) {
            return authCredentials;
        }

        ImmutableMap<String, Object> debugDetails = ImmutableMap.of("user_mapping_attributes", authCredentials.getAttributesForUserMapping(),
                "user_mapping", source);

        ImmutableSet<String> newUserNames = MappingSpecification.apply(userName, authCredentials);

        if (newUserNames.size() == 0) {
            throw new CredentialsException(new AuthcResult.DebugInfo(null, false, "No user name found", debugDetails));
        } else if (newUserNames.size() != 1) {
            throw new CredentialsException(new AuthcResult.DebugInfo(null, false, "More than one candidate for the user name was found",
                    debugDetails.with("user_name_candidates", newUserNames)));
        }

        if (log.isDebugEnabled()) {
            log.debug("Mapped user name: " + newUserNames.only());
        }

        return authCredentials.userName(newUserNames.only());
    }

    @Override
    public User map(AuthCredentials authCredentials) throws CredentialsException {

        if (log.isDebugEnabled()) {
            log.debug("Mapping user using attributes " + authCredentials.getAttributesForUserMapping() + " for " + authCredentials);
        }

        AuthCredentials.Builder result = authCredentials.copy();
        ImmutableMap<String, Object> debugDetails = ImmutableMap.of("user_mapping_attributes", authCredentials.getAttributesForUserMapping(),
                "user_mapping", source);

        if (userNameFromBackend != null && !userNameFromBackend.isEmpty()) {
            ImmutableSet<String> newUserNames = MappingSpecification.apply(userNameFromBackend, authCredentials);
            
            if (newUserNames.size() == 0) {
                throw new CredentialsException(new AuthcResult.DebugInfo(null, false, "No user name found", debugDetails));
            } else if (newUserNames.size() != 1) {
                throw new CredentialsException(new AuthcResult.DebugInfo(null, false, "More than one candidate for the user name was found",
                        debugDetails.with("user_name_candidates", newUserNames)));
            }
            
            if (log.isDebugEnabled()) {
                log.debug("Mapped user name: " + newUserNames.only());
            }
            
            result.userName(newUserNames.only());
        }
        
        if (roles != null && !roles.isEmpty()) {
            ImmutableSet<String> backendRoles = MappingSpecification.apply(roles, authCredentials);
            result.backendRoles(backendRoles);

            if (log.isDebugEnabled()) {
                log.debug("Mapped roles: " + backendRoles);
            }
        }

        if (attrs != null && !attrs.isEmpty()) {
            ImmutableMap<String, Object> attributes = MapMappingSpecification.apply(attrs, authCredentials);

            if (log.isDebugEnabled()) {
                log.debug("Mapped attributes: " + attributes);
            }
            try {
                result.attributes(attributes);
            } catch (IllegalArgumentException e) {
                throw new CredentialsException(new AuthcResult.DebugInfo(null, false, e.getMessage(), debugDetails), e);
            }
        }

        return User.forUser(result.getUserName()).with(result.build()).build();
    }

    public static UserMapping parse(DocNode docNode, Parser.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        ImmutableList<MappingSpecification> userName = vNode.get("user_name").by(MappingSpecification::parseUserNameMapping);
        ImmutableList<MappingSpecification> userNameFromBackend = vNode.get("user_name").by(MappingSpecification::parseUserNameFromBackendMapping);

        ImmutableList<MappingSpecification> roles = vNode.get("roles").by(MappingSpecification::parseRoleMapping);
        ImmutableList<MapMappingSpecification> attributes = vNode.get("attrs").by(MapMappingSpecification::parse);
        
        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        return new UserMapping(docNode, userName, userNameFromBackend, roles, attributes);
    }

    private final DocNode source;
    private final ImmutableList<MappingSpecification> userName;
    private final ImmutableList<MappingSpecification> userNameFromBackend;
    private final ImmutableList<MappingSpecification> roles;
    private final ImmutableList<MapMappingSpecification> attrs;

    public UserMapping(DocNode source, ImmutableList<MappingSpecification> userName, ImmutableList<MappingSpecification> userNameFromBackend,
            ImmutableList<MappingSpecification> roles, ImmutableList<MapMappingSpecification> attrs) {
        this.source = source;
        this.userName = userName;
        this.userNameFromBackend = userNameFromBackend;
        this.roles = roles;
        this.attrs = attrs;
    }

    public static class Static extends MappingSpecification {
        private final ImmutableSet<String> valueAsSet;

        Static(String value, boolean convertToLowerCase) {
            super(convertToLowerCase);
            this.valueAsSet = ImmutableSet.of(value);
        }

        static Static parse(DocNode docNode, Parser.Context context, boolean convertToLowerCase) throws ConfigValidationException {
            return new Static(docNode.toString(), convertToLowerCase);
        }

        static Parser<Static, Parser.Context> parser(boolean convertToLowerCase) {
            return (docNode, context) -> parse(docNode, context, convertToLowerCase);
        }

        @Override
        ImmutableSet<String> applyWithoutConversion(AuthCredentials authCredentials) {
            return valueAsSet;
        }
    }

    public static class FromAttribute extends MappingSpecification {
        private final JsonPath attributePath;
        private final java.util.regex.Pattern pattern;
        private final Splitter splitter;
        private final static Configuration attributePathConfiguration = BasicJsonPathDefaultConfiguration.listDefaultConfiguration()
                .addOptions(Option.SUPPRESS_EXCEPTIONS);

        FromAttribute(JsonPath attributePath, java.util.regex.Pattern pattern, String split, boolean convertToLowerCase) {
            super(convertToLowerCase);
            this.pattern = pattern;
            this.attributePath = attributePath;
            this.splitter = split != null ? Splitter.on(split).trimResults() : null;
        }

        static FromAttribute parse(DocNode docNode, Parser.Context context, boolean convertToLowerCase) throws ConfigValidationException {
            if (docNode.isString()) {
                try {
                    return new FromAttribute(JsonPath.compile(docNode.toString()), null, null, convertToLowerCase);
                } catch (InvalidPathException e) {
                    throw new ConfigValidationException(new InvalidAttributeValue(null, docNode, "JSON Path").message(e.getMessage()).cause(e));
                }
            } else if (docNode.isMap()) {
                ValidationErrors validationErrors = new ValidationErrors();
                ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

                JsonPath path = vNode.get("json_path").required().asJsonPath();
                java.util.regex.Pattern pattern = vNode.get("pattern").asPattern();
                String split = vNode.get("split").asString();

                validationErrors.throwExceptionForPresentErrors();

                return new FromAttribute(path, pattern, split, convertToLowerCase);
            } else {
                throw new ConfigValidationException(new InvalidAttributeValue(null, docNode, "JSON Path"));
            }
        }
        
        static FromAttribute parseCommaSeparated(DocNode docNode, Parser.Context context, boolean convertToLowerCase)
                throws ConfigValidationException {
            if (docNode.isString()) {
                try {
                    return new FromAttribute(JsonPath.compile(docNode.toString()), null, ",", convertToLowerCase);
                } catch (InvalidPathException e) {
                    throw new ConfigValidationException(new InvalidAttributeValue(null, docNode, "JSON Path").message(e.getMessage()).cause(e));
                }
            } else {
                throw new ConfigValidationException(new InvalidAttributeValue(null, docNode, "JSON Path"));
            }
        }

        static Parser<FromAttribute, Parser.Context> parser(boolean convertToLowerCase) {
            return (docNode, context) -> parse(docNode, context, convertToLowerCase);
        }

        static Parser<FromAttribute, Parser.Context> commaSeparatedParser(boolean convertToLowerCase) {
            return (docNode, context) -> parseCommaSeparated(docNode, context, convertToLowerCase);
        }

        @Override
        ImmutableSet<String> applyWithoutConversion(AuthCredentials authCredentials) {
            try {
                List<Object> elements = JsonPath.using(attributePathConfiguration).parse(authCredentials.getAttributesForUserMapping())
                        .read(attributePath);

                if (splitter != null) {
                    return ImmutableSet.flattenDeep(elements, String::valueOf).mapFlat((e) -> this.splitAndApplyPattern(e));
                } else {
                    return ImmutableSet.flattenDeep(elements, (o) -> this.applyPattern(o));
                }
            } catch (PathNotFoundException e) {
                return ImmutableSet.empty();
            }
        }

        private Collection<String> splitAndApplyPattern(String string) {
            return splitter.splitToStream(string).map((e) -> this.applyPattern(e)).filter(Objects::nonNull).collect(Collectors.toList());
        }

        private String applyPattern(Object object) {
            String string = object.toString();

            if (pattern == null) {
                return string;
            }

            Matcher matcher = pattern.matcher(string);

            if (!matcher.matches()) {
                return null;
            }

            if (matcher.groupCount() == 1) {
                return matcher.group(1);
            } else if (matcher.groupCount() > 1) {
                StringBuilder subjectBuilder = new StringBuilder();

                for (int i = 1; i <= matcher.groupCount(); i++) {
                    if (matcher.group(i) != null) {
                        subjectBuilder.append(matcher.group(i));
                    }
                }

                if (subjectBuilder.length() != 0) {
                    return subjectBuilder.toString();
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }
    }

    public static abstract class MappingSpecification {
        private final boolean convertToLowerCase;

        MappingSpecification(boolean convertToLowerCase) {
            this.convertToLowerCase = convertToLowerCase;
        }

        static ImmutableList<MappingSpecification> parseUserNameMapping(DocNode docNode, Parser.Context context)
                throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            boolean convertToLowerCase = vNode.get("convert_to_lower_case").withDefault(false).asBoolean();
            List<FromAttribute> from = vNode.get("from").asList().withEmptyListAsDefault().ofObjectsParsedBy(FromAttribute.parser(convertToLowerCase));
            List<Static> staticValues = vNode.get("static").asList().withEmptyListAsDefault().ofObjectsParsedBy(Static.parser(convertToLowerCase));

            vNode.used("from_backend");
            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();

            return ImmutableList.concat(from, staticValues);
        }
        
        static ImmutableList<MappingSpecification> parseUserNameFromBackendMapping(DocNode docNode, Parser.Context context)
                throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            boolean convertToLowerCase = vNode.get("convert_to_lower_case").withDefault(false).asBoolean();
            List<FromAttribute> from = vNode.get("from_backend").asList().withEmptyListAsDefault()
                    .ofObjectsParsedBy(FromAttribute.parser(convertToLowerCase));

            validationErrors.throwExceptionForPresentErrors();

            return ImmutableList.of(from);
        }

        static ImmutableList<MappingSpecification> parseRoleMapping(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            boolean convertToLowerCase = vNode.get("convert_to_lower_case").withDefault(false).asBoolean();
            List<FromAttribute> from = vNode.get("from").asList().withEmptyListAsDefault().ofObjectsParsedBy(FromAttribute.parser(convertToLowerCase));
            List<FromAttribute> fromCsv = vNode.get("from_comma_separated_string").asList().withEmptyListAsDefault()
                    .ofObjectsParsedBy(FromAttribute.commaSeparatedParser(convertToLowerCase));
            List<Static> staticValues = vNode.get("static").asList().withEmptyListAsDefault().ofObjectsParsedBy(Static.parser(convertToLowerCase));

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();

            return ImmutableList.concat(from, fromCsv, staticValues);
        }

        final ImmutableSet<String> apply(AuthCredentials authCredentials) {
            ImmutableSet<String> result = applyWithoutConversion(authCredentials);
            return convertToLowerCase ? result.map(value -> value.toLowerCase(Locale.ROOT)) : result;
        }

        abstract ImmutableSet<String> applyWithoutConversion(AuthCredentials authCredentials);

        static ImmutableSet<String> apply(Collection<MappingSpecification> mappingSpecifications, AuthCredentials authCredentials) {
            ImmutableSet<String> result = ImmutableSet.empty();

            for (MappingSpecification mappingSpecification : mappingSpecifications) {
                result = result.with(mappingSpecification.apply(authCredentials));
            }

            return result;
        }

    }

    public static class StaticMap extends MapMappingSpecification {
        private final ImmutableMap<String, Object> map;

        StaticMap(Map<String, Object> map, boolean convertKeysToLowerCase) {
            super(convertKeysToLowerCase);
            this.map = ImmutableMap.of(map);
        }

        static StaticMap parseStatic(DocNode docNode, Parser.Context context, boolean convertKeysToLowerCase)
                throws ConfigValidationException {
            if (docNode.isMap()) {
                return new StaticMap(docNode.toMap(), convertKeysToLowerCase);
            } else {
                throw new ConfigValidationException(new InvalidAttributeValue(null, docNode, "A mapping from attribute names to values"));
            }
        }

        static Parser<StaticMap, Parser.Context> parser(boolean convertKeysToLowerCase) {
            return (docNode, context) -> parseStatic(docNode, context, convertKeysToLowerCase);
        }

        @Override
        void apply(AuthCredentials authCredentials, ImmutableMap.Builder<String, Object> result) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                result.with(convertKey(entry.getKey()), entry.getValue());
            }
        }
    }

    public static class FromAttributeMap extends MapMappingSpecification {
        private final Map<String, JsonPath> attributePathMap;
        private final static Configuration attributePathConfiguration = BasicJsonPathDefaultConfiguration.defaultConfiguration();

        FromAttributeMap(Map<String, JsonPath> attributePathMap, boolean convertKeysToLowerCase) {
            super(convertKeysToLowerCase);
            this.attributePathMap = attributePathMap;
        }

        static FromAttributeMap parseFrom(DocNode docNode, Parser.Context context, boolean convertKeysToLowerCase)
                throws ConfigValidationException {
            if (docNode.isMap()) {
                ValidationErrors validationErrors = new ValidationErrors();
                ImmutableMap.Builder<String, JsonPath> result = new ImmutableMap.Builder<>();

                for (Map.Entry<String, Object> entry : docNode.toMap().entrySet()) {
                    try {
                        result.put(entry.getKey(), JsonPath.compile(String.valueOf(entry.getValue())));
                    } catch (InvalidPathException e) {
                        validationErrors
                                .add(new InvalidAttributeValue(entry.getKey(), entry.getValue(), "JSON Path").message(e.getMessage()).cause(e));
                    }
                }

                validationErrors.throwExceptionForPresentErrors();

                return new FromAttributeMap(result.build(), convertKeysToLowerCase);
            } else {
                throw new ConfigValidationException(new InvalidAttributeValue(null, docNode, "A mapping from attribute names to JSON Path"));
            }
        }

        static Parser<FromAttributeMap, Parser.Context> parser(boolean convertKeysToLowerCase) {
            return (docNode, context) -> parseFrom(docNode, context, convertKeysToLowerCase);
        }

        @Override
        void apply(AuthCredentials authCredentials, ImmutableMap.Builder<String, Object> result) {

            for (Map.Entry<String, JsonPath> entry : attributePathMap.entrySet()) {
                try {
                    JsonPath jsonPath = entry.getValue();
                    Object value = JsonPath.using(attributePathConfiguration).parse(authCredentials.getAttributesForUserMapping()).read(jsonPath);

                    result.with(convertKey(entry.getKey()), value);
                } catch (PathNotFoundException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Attribute mapping path not found: " + entry, e);
                    }
                } catch (Exception e) {
                    log.error("Error while evaluating map attribute mapping " + entry, e);
                }
            }
        }
    }

    public static abstract class MapMappingSpecification {
        private final boolean convertKeysToLowerCase;

        MapMappingSpecification(boolean convertKeysToLowerCase) {
            this.convertKeysToLowerCase = convertKeysToLowerCase;
        }

        static ImmutableList<MapMappingSpecification> parse(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            boolean convertKeysToLowerCase = vNode.get("convert_keys_to_lower_case").withDefault(false).asBoolean();
            List<FromAttributeMap> from = vNode.get("from").asList().withEmptyListAsDefault()
                    .ofObjectsParsedBy(FromAttributeMap.parser(convertKeysToLowerCase));
            List<StaticMap> staticValues = vNode.get("static").asList().withEmptyListAsDefault()
                    .ofObjectsParsedBy(StaticMap.parser(convertKeysToLowerCase));

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();

            return ImmutableList.concat(from, staticValues);
        }

        static ImmutableMap<String, Object> apply(Collection<MapMappingSpecification> mappingSpecifications, AuthCredentials authCredentials) {
            ImmutableMap.Builder<String, Object> result = new ImmutableMap.Builder<>();

            for (MapMappingSpecification mappingSpecification : mappingSpecifications) {
                mappingSpecification.apply(authCredentials, result);
            }

            return result.build();
        }

        String convertKey(String key) {
            return convertKeysToLowerCase ? key.toLowerCase(Locale.ROOT) : key;
        }

        abstract void apply(AuthCredentials authCredentials, ImmutableMap.Builder<String, Object> result);

    }

    public List<MappingSpecification> getUserName() {
        return userName;
    }

    public List<MappingSpecification> getRoles() {
        return roles;
    }

    public List<MapMappingSpecification> getAttrs() {
        return attrs;
    }

}
