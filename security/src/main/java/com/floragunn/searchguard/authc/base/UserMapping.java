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
import com.floragunn.searchguard.authc.AuthenticationDomain;
import com.floragunn.searchguard.authc.CredentialsException;
import com.floragunn.searchguard.authc.AuthenticationBackend.UserMapper;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.ImmutableList;
import com.floragunn.searchsupport.util.ImmutableMap;
import com.floragunn.searchsupport.util.ImmutableSet;
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

        ImmutableMap<String, Object> debugDetails = ImmutableMap.of("user_mapping_source_attributes", authCredentials.getAttributesForUserMapping(),
                "user_mapping", source);

        ImmutableSet<String> newUserNames = MappingSpecification.apply(userName, authCredentials);

        if (newUserNames.size() == 0) {
            throw new CredentialsException(new AuthczResult.DebugInfo(null, false, "No user name found", debugDetails));
        } else if (newUserNames.size() != 1) {
            throw new CredentialsException(new AuthczResult.DebugInfo(null, false, "More than one candidate for the user name was found",
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
        ImmutableMap<String, Object> debugDetails = ImmutableMap.of("user_mapping_source_attributes", authCredentials.getAttributesForUserMapping(),
                "user_mapping", source);

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
                throw new CredentialsException(new AuthczResult.DebugInfo(null, false, e.getMessage(), debugDetails), e);
            }
        }

        return User.forUser(result.getUserName()).with(result.build()).build();
    }

    public static UserMapping parse(DocNode docNode, Parser.Context context) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        List<MappingSpecification> userName = vNode.get("user_name").by(MappingSpecification::parseUserNameMapping);
        List<MappingSpecification> roles = vNode.get("roles").by(MappingSpecification::parseRoleMapping);
        List<MapMappingSpecification> attributes = vNode.get("attrs").by(MapMappingSpecification::parse);
        
        vNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        return new UserMapping(docNode, userName, roles, attributes);
    }

    private final DocNode source;
    private final ImmutableList<MappingSpecification> userName;
    private final ImmutableList<MappingSpecification> roles;
    private final ImmutableList<MapMappingSpecification> attrs;

    public UserMapping(DocNode source, List<MappingSpecification> userName, List<MappingSpecification> roles, List<MapMappingSpecification> attrs) {
        this.source = source;
        this.userName = ImmutableList.of(userName);
        this.roles = ImmutableList.of(roles);
        this.attrs = ImmutableList.of(attrs);
    }

    public static class Static extends MappingSpecification {
        private final ImmutableSet<String> valueAsSet;

        Static(String value) {
            this.valueAsSet = ImmutableSet.of(value);
        }

        static Static parse(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            return new Static(docNode.toString());
        }

        @Override
        ImmutableSet<String> apply(AuthCredentials authCredentials) {
            return valueAsSet;
        }
    }

    public static class FromAttribute extends MappingSpecification {
        private final JsonPath attributePath;
        private final java.util.regex.Pattern pattern;
        private final Splitter splitter;
        private final static Configuration attributePathConfiguration = BasicJsonPathDefaultConfiguration.listDefaultConfiguration()
                .addOptions(Option.SUPPRESS_EXCEPTIONS);

        FromAttribute(JsonPath attributePath, java.util.regex.Pattern pattern, String split) {
            this.pattern = pattern;
            this.attributePath = attributePath;
            this.splitter = split != null ? Splitter.on(split).trimResults() : null;
        }

        static FromAttribute parse(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            if (docNode.isString()) {
                try {
                    return new FromAttribute(JsonPath.compile(docNode.toString()), null, null);
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

                return new FromAttribute(path, pattern, split);
            } else {
                throw new ConfigValidationException(new InvalidAttributeValue(null, docNode, "JSON Path"));
            }
        }
        
        static FromAttribute parseCommaSeparated(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            if (docNode.isString()) {
                try {
                    return new FromAttribute(JsonPath.compile(docNode.toString()), null, ",");
                } catch (InvalidPathException e) {
                    throw new ConfigValidationException(new InvalidAttributeValue(null, docNode, "JSON Path").message(e.getMessage()).cause(e));
                }
            } else {
                throw new ConfigValidationException(new InvalidAttributeValue(null, docNode, "JSON Path"));
            }
        }

        @Override
        ImmutableSet<String> apply(AuthCredentials authCredentials) {
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

        static List<MappingSpecification> parseUserNameMapping(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            List<FromAttribute> from = vNode.get("from").asList().withEmptyListAsDefault().ofObjectsParsedBy(FromAttribute::parse);
            List<Static> staticValues = vNode.get("static").asList().withEmptyListAsDefault().ofObjectsParsedBy(Static::parse);

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();

            return ImmutableList.concat(from, staticValues);
        }

        static List<MappingSpecification> parseRoleMapping(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            List<FromAttribute> from = vNode.get("from").asList().withEmptyListAsDefault().ofObjectsParsedBy(FromAttribute::parse);
            List<FromAttribute> fromCsv = vNode.get("from_comma_separated_string").asList().withEmptyListAsDefault().ofObjectsParsedBy(FromAttribute::parseCommaSeparated);
            List<Static> staticValues = vNode.get("static").asList().withEmptyListAsDefault().ofObjectsParsedBy(Static::parse);

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();

            return ImmutableList.concat(from, fromCsv, staticValues);
        }

        abstract ImmutableSet<String> apply(AuthCredentials authCredentials);

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

        StaticMap(Map<String, Object> map) {
            this.map = ImmutableMap.of(map);
        }

        static StaticMap parseStatic(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            if (docNode.isMap()) {
                return new StaticMap(docNode.toMap());
            } else {
                throw new ConfigValidationException(new InvalidAttributeValue(null, docNode, "A mapping from attribute names to values"));
            }
        }

        @Override
        void apply(AuthCredentials authCredentials, ImmutableMap.Builder<String, Object> result) {
            result.putAll(map);
        }
    }

    public static class FromAttributeMap extends MapMappingSpecification {
        private final Map<String, JsonPath> attributePathMap;
        private final static Configuration attributePathConfiguration = BasicJsonPathDefaultConfiguration.defaultConfiguration();

        FromAttributeMap(Map<String, JsonPath> attributePathMap) {
            this.attributePathMap = attributePathMap;
        }

        static FromAttributeMap parseFrom(DocNode docNode, Parser.Context context) throws ConfigValidationException {
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

                return new FromAttributeMap(result.build());
            } else {
                throw new ConfigValidationException(new InvalidAttributeValue(null, docNode, "A mapping from attribute names to JSON Path"));
            }
        }

        @Override
        void apply(AuthCredentials authCredentials, ImmutableMap.Builder<String, Object> result) {

            for (Map.Entry<String, JsonPath> entry : attributePathMap.entrySet()) {
                try {
                    JsonPath jsonPath = entry.getValue();
                    Object value = JsonPath.using(attributePathConfiguration).parse(authCredentials.getAttributesForUserMapping()).read(jsonPath);

                    result.with(entry.getKey(), value);
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
        static List<MapMappingSpecification> parse(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

            List<FromAttributeMap> from = vNode.get("from").asList().withEmptyListAsDefault().ofObjectsParsedBy(FromAttributeMap::parseFrom);
            List<StaticMap> staticValues = vNode.get("static").asList().withEmptyListAsDefault().ofObjectsParsedBy(StaticMap::parseStatic);

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