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

package com.floragunn.codova.validation;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.floragunn.codova.config.temporal.DurationFormat;
import com.floragunn.codova.config.temporal.TemporalAmountFormat;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.codova.validation.errors.UnsupportedAttribute;
import com.floragunn.codova.validation.errors.ValidationError;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;

public class ValidatingDocNode {
    private ValidationErrors validationErrors;
    private DocNode documentNode;
    private Set<String> unconsumedAttributes;
    private Set<String> consumedAttributes = new HashSet<>();
    private Map<String, ValidatingFunction<String, ?>> variableProviders = new HashMap<>();

    public ValidatingDocNode(Map<String, Object> document, ValidationErrors validationErrors) {
        this(DocNode.wrap(document), validationErrors);
    }

    public ValidatingDocNode(DocNode documentNode, ValidationErrors validationErrors) {
        this.documentNode = documentNode;
        this.validationErrors = validationErrors;
        this.unconsumedAttributes = new HashSet<>(documentNode.keySet());
    }

    public ValidatingDocNode(ValidatingDocNode vJsonNode, ValidationErrors validationErrors) {
        this.documentNode = vJsonNode.documentNode;
        this.validationErrors = validationErrors;
        this.unconsumedAttributes = vJsonNode.unconsumedAttributes;
        this.consumedAttributes = vJsonNode.consumedAttributes;
    }

    public ValidatingDocNode expandVariables(String name, ValidatingFunction<String, ?> variableProvider) {
        this.variableProviders.put(name, variableProvider);
        return this;
    }

    public ValidatingDocNode expandVariables(Map<String, ValidatingFunction<String, ?>> variableProviders) {
        this.variableProviders.putAll(variableProviders);
        return this;
    }

    public ValidatingDocNode expandVariables(ConfigVariableProviders variableProviders) {
        if (variableProviders != null) {
            this.variableProviders.putAll(variableProviders.toMap());
        }
        return this;
    }

    public void used(String... attributes) {
        for (String attribute : attributes) {
            used(attribute);
        }
    }

    public void used(Set<String> attributes) {
        if (attributes != null) {
            for (String attribute : attributes) {
                used(attribute);
            }
        }
    }

    private void used(String attribute) {
        this.unconsumedAttributes.remove(attribute);
        this.consumedAttributes.add(attribute);

        int dot = attribute.lastIndexOf('.');

        if (dot != -1) {
            String parentAttribute = attribute.substring(0, dot);

            used(parentAttribute);
        }
        
        String attributeWithDot = attribute + ".";
        
        for (String unconsumed : new HashSet<>(unconsumedAttributes)) {
            if (unconsumed.startsWith(attributeWithDot)) {
                usedNonRecursive(unconsumed);
            }
        }
    }

    private void usedNonRecursive(String attribute) {
        this.unconsumedAttributes.remove(attribute);
        this.consumedAttributes.add(attribute);
    }

    public Attribute get(String attribute) {
        if (this.documentNode.hasNonNull(attribute)) {
            // YAML documents which have attributes specified like "a.b.c" won't be mapped to an object tree by BasicJsonReader.
            // This is actually okay, because this is error tolerant to invalid tree structures which can be defined in a YAML file.
            // Thus, we treat such attributes as special case here.
            used(attribute);
            return new Attribute(attribute, attribute, documentNode);
        }

        int dot = attribute.indexOf('.');

        if (dot == -1) {
            used(attribute);
            String attributeWithDot = attribute + ".";

            for (String docAttribute : this.documentNode.keySet()) {
                if (docAttribute.startsWith(attributeWithDot)) {
                    used(docAttribute);
                }
            }

            return new Attribute(attribute, attribute, documentNode);
        } else {
            String[] parts = attribute.split("\\.");

            DocNode currentDocumentNode = this.documentNode;
            StringBuilder path = new StringBuilder();

            for (int i = 0; i < parts.length - 1 && currentDocumentNode != null; i++) {
                if (i != 0) {
                    path.append('.');
                }

                path.append(parts[i]);
                currentDocumentNode = currentDocumentNode.getAsNode(parts[i]);
                usedNonRecursive(path.toString());
            }

            String subAttribute = parts[parts.length - 1];

            if (currentDocumentNode != null) {
                String subAttributeWithDot = subAttribute + ".";

                for (String docAttribute : currentDocumentNode.keySet()) {
                    if (docAttribute.equals(subAttribute) || docAttribute.startsWith(subAttributeWithDot)) {
                        used(path + "." + docAttribute);
                    }
                }

                return new Attribute(subAttribute, attribute, currentDocumentNode);
            } else {
                return new Attribute(subAttribute, attribute, DocNode.EMPTY);
            }
        }
    }

    public boolean hasNonNull(String attribute) {
        if (this.documentNode.get(attribute) != null) {
            return true;
        }

        int dot = attribute.indexOf('.');

        if (dot != -1) {
            String[] parts = attribute.split("\\.");

            DocNode currentDocumentNode = this.documentNode;

            for (int i = 0; i < parts.length - 1 && currentDocumentNode != null; i++) {
                currentDocumentNode = currentDocumentNode.getAsNode(parts[i]);
            }

            if (currentDocumentNode != null) {
                return true;
            }
        }

        return false;
    }

    public void checkForUnusedAttributes() {
        for (String attribute : this.unconsumedAttributes) {
            validationErrors.add(
                    new UnsupportedAttribute(attribute, documentNode.get(attribute) != null ? documentNode.get(attribute).toString() : null, null));
        }
    }

    public DocNode getDocumentNode() {
        return documentNode;
    }

    public abstract class AbstractAttribute<T> {
        protected final String name;
        protected final String fullAttributePath;
        protected final DocNode documentNode;
        protected String expandedVariable;

        protected AbstractAttribute(String name, String fullAttributePath, DocNode documentNode) {
            this.name = name;
            this.fullAttributePath = fullAttributePath;
            this.documentNode = documentNode;
        }

        protected String expected;

        @SuppressWarnings("unchecked")
        public T expected(String expected) {
            this.expected = expected;
            return (T) this;
        }

        protected String getAttributePathForValidationError() {
            if (expandedVariable == null) {
                return fullAttributePath;
            } else {
                return fullAttributePath + "." + expandedVariable;
            }
        }

        protected Object expandVariable(Object value) {
            if (value == null) {
                return null;
            }

            if (variableProviders.isEmpty()) {
                return value;
            }

            if (value instanceof List) {
                return expandVariables((List<?>) value);
            }

            if (!(value instanceof String)) {
                return value;
            }

            String string = (String) value;

            if (string.startsWith("${") && string.endsWith("}")) {
                String name = string.substring(2, string.length() - 1);
                ValidatingFunction<String, ?> variableProvider;

                int colon = name.indexOf(':');

                if (colon != -1) {
                    variableProvider = variableProviders.get(name.substring(0, colon));
                    name = name.substring(colon + 1);
                } else {
                    variableProvider = variableProviders.get("default");
                }

                if (variableProvider == null) {
                    validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), value, "Invalid variable definition"));
                    return value;
                }

                try {
                    Object newValue = variableProvider.apply(name);
                    if (newValue == null) {
                        validationErrors.add(
                                new InvalidAttributeValue(getAttributePathForValidationError(), value, "The variable " + value + " does not exist"));
                        return null;
                    }

                    this.expandedVariable = string;

                    return newValue;

                } catch (ConfigValidationException e) {
                    validationErrors.add(getAttributePathForValidationError(), e);
                    return null;
                } catch (Exception e) {
                    validationErrors
                            .add(new InvalidAttributeValue(getAttributePathForValidationError(), value, "Error while retrieving variable: " + e));
                    return null;
                }

            } else {
                return value;
            }
        }

        protected DocNode expandVariable(DocNode docNode) {
            if (docNode == null) {
                return null;
            }

            Object value = docNode.get(null);

            Object newValue = expandVariable(value);

            if (newValue == value) {
                return docNode;
            } else {
                return DocNode.wrap(newValue);
            }
        }

        protected List<?> expandVariables(List<?> values) {
            if (values == null || values.isEmpty() || variableProviders.isEmpty()) {
                return values;
            }

            List<Object> result = new ArrayList<>(values.size());

            for (Object value : values) {
                Object expandedValue = expandVariable(value);

                if (expandedValue != null) {
                    result.add(expandedValue);
                }
            }

            return result;
        }

        protected List<String> expandVariablesForStrings(List<String> values) {
            if (values == null || values.isEmpty() || variableProviders.isEmpty()) {
                return values;
            }

            List<String> result = new ArrayList<>(values.size());

            for (String value : values) {
                Object expandedValue = expandVariable(value);

                if (expandedValue != null) {
                    result.add(String.valueOf(expandedValue));
                }
            }

            return result;
        }
    }

    public class Attribute extends AbstractAttribute<Attribute> {

        Attribute(String name, String fullAttributePath, DocNode documentNode) {
            super(name, fullAttributePath, documentNode);
        }

        public Attribute required() {
            Object value = documentNode.get(name);

            if (value == null) {
                validationErrors.add(new MissingAttribute(getAttributePathForValidationError(), documentNode));
            }

            return this;
        }

        public StringAttribute withDefault(String defaultValue) {
            return new StringAttribute(name, fullAttributePath, documentNode).withDefault(defaultValue).expected(expected);
        }

        public StringAttribute validatedBy(Predicate<String> validationPredicate) {
            return new StringAttribute(name, fullAttributePath, documentNode).expected(expected).validatedBy(validationPredicate);
        }

        public NumberAttribute withDefault(Number defaultValue) {
            return new NumberAttribute(name, fullAttributePath, documentNode).withDefault(defaultValue).expected(expected);
        }

        public BooleanAttribute withDefault(boolean defaultValue) {
            return new BooleanAttribute(name, defaultValue, fullAttributePath, documentNode);
        }

        public URIAttribute withDefault(URI defaultValue) {
            return new URIAttribute(name, defaultValue, fullAttributePath, documentNode);
        }

        public DurationAttribute withDefault(Duration defaultValue) {
            return new DurationAttribute(name, defaultValue, fullAttributePath, documentNode);
        }

        public <T> TypedAttribute<T> withDefault(T defaultValue) {
            return new TypedAttribute<T>(name, fullAttributePath, documentNode).withDefault(defaultValue);
        }

        public <T> TypedAttribute<T> validatedBy(Function<T, ValidationErrors> validationFunction) {
            return new TypedAttribute<T>(name, fullAttributePath, documentNode).validatedBy(validationFunction);
        }

        public <E extends Enum<E>> EnumAttribute<E> withDefault(E defaultValue) {
            return new EnumAttribute<E>(name, fullAttributePath, documentNode).withDefault(defaultValue);
        }

        public ListAttribute asList() {
            return new ListAttribute(name, fullAttributePath, documentNode);
        }

        public StringListAttribute withListDefault(String... defaultValueStrings) {
            return new StringListAttribute(name, fullAttributePath, documentNode).withDefault(defaultValueStrings);
        }

        public String asString() {
            Object value = expandVariable(documentNode.get(name));

            if (value instanceof String) {
                return (String) value;
            } else if (value != null) {
                return String.valueOf(value);
            } else {
                return null;
            }
        }

        public Object asAnything() {
            return expandVariable(documentNode.get(name));
        }

        public List<String> asListOfStrings() {
            return expandVariablesForStrings(documentNode.getAsListOfStrings(name));
        }

        public Number asNumber() {
            Object object = expandVariable(documentNode.get(name));

            if (object == null) {
                return null;
            } else if (object instanceof Number) {
                return (Number) object;
            } else {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, "A numeric value"));
                return null;
            }
        }

        public int asInt() {
            Number number = asNumber();

            if (number != null) {
                return number.intValue();
            } else {
                return 0;
            }
        }

        public Integer asInteger() {
            Number number = asNumber();

            if (number instanceof Integer) {
                return (Integer) number;
            } else if (number != null) {
                return number.intValue();
            } else {
                return null;
            }
        }

        public Boolean asBoolean() {
            Object object = expandVariable(documentNode.get(name));

            if (object == null) {
                return null;
            } else if (object instanceof Boolean) {
                return (Boolean) object;
            } else {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, "true or false"));
                return null;
            }
        }

        public URI asURI() {
            Object object = expandVariable(documentNode.get(name));

            if (object instanceof String) {
                try {
                    return new URI((String) object);
                } catch (URISyntaxException e) {
                    validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, "A URI"));
                    return null;
                }
            } else if (object != null) {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, "A URI"));
                return null;
            } else {
                return null;
            }
        }

        public URI asAbsoluteURI() {
            URI uri = asURI();

            if (uri != null && !uri.isAbsolute()) {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), uri, "URI").message("Must be an absolute URI"));
            }

            return uri;
        }

        /**
         * Returns a normalized URL which is suitable as base URL. Espeically, this method guarantees that the path always ends with a slash.
         */
        public URI asBaseURL() {
            URI uri = asURI();

            if (uri != null) {
                try {
                    if (!uri.isAbsolute()) {
                        validationErrors.add(
                                new InvalidAttributeValue(getAttributePathForValidationError(), uri, "Base URL").message("Must be an absolute URL"));
                    }

                    if (uri.getRawQuery() != null) {
                        validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), uri, "Base URL")
                                .message("Cannot use query parameters for base URLs"));
                    }

                    if (uri.getRawFragment() != null) {
                        validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), uri, "Base URL")
                                .message("Cannot use fragments for base URLs"));
                    }

                    String path = uri.getRawPath();

                    if (path == null) {
                        path = "/";
                    } else if (!path.endsWith("/")) {
                        path += "/";
                    }

                    return new URI(uri.getScheme(), uri.getRawUserInfo(), uri.getHost(), uri.getPort(), path, null, null);
                } catch (URISyntaxException e) {
                    validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), uri, "Base URL"));
                    return null;
                }
            } else {
                return null;
            }
        }

        public <E extends Enum<E>> E asEnum(Class<E> enumClass) {
            Object object = expandVariable(documentNode.get(name));

            if (object instanceof String) {
                String value = (String) object;
                for (E e : enumClass.getEnumConstants()) {
                    if (value.equalsIgnoreCase(e.name())) {
                        return e;
                    }
                }
            }

            validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, enumClass));

            return null;
        }

        public Pattern asPattern() {
            Object object = expandVariable(documentNode.get(name));

            if (object instanceof String) {
                try {
                    return Pattern.compile((String) object);
                } catch (PatternSyntaxException e) {
                    validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, "Regular expression"));
                    return null;
                }
            } else if (object != null) {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, "Regular expression"));
                return null;
            } else {
                return null;
            }
        }

        public Map<String, Object> asMap() {
            DocNode value = documentNode.getAsNode(name);

            if (value == null) {
                return null;
            }

            if (!value.isMap()) {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), value, "JSON object"));

            }

            return value.toMap();
        }

        public TemporalAmount asTemporalAmount() {
            Object object = expandVariable(documentNode.get(name));

            if (object instanceof String) {
                try {
                    return TemporalAmountFormat.INSTANCE.parse((String) object);
                } catch (ConfigValidationException e) {
                    validationErrors.add(getAttributePathForValidationError(), e);
                    return null;
                }
            } else if (object != null) {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object,
                        "<Years>y? <Months>M? <Weeks>w? <Days>d?  |  <Days>d? <Hours>h? <Minutes>m? <Seconds>s? <Milliseconds>ms?"));
                return null;
            } else {
                return null;
            }
        }

        public Duration asDuration() {
            Object object = expandVariable(documentNode.get(name));

            if (object instanceof String) {
                try {
                    return DurationFormat.INSTANCE.parse((String) object);
                } catch (ConfigValidationException e) {
                    validationErrors.add(getAttributePathForValidationError(), e);
                    return null;
                }
            } else if (object != null) {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object,
                        "<Weeks>w? <Days>d? <Hours>h? <Minutes>m? <Seconds>s? <Milliseconds>ms?"));
                return null;
            } else {
                return null;
            }
        }

        public JsonPath asJsonPath() {
            Object object = expandVariable(documentNode.get(name));

            if (object instanceof String) {
                try {
                    return JsonPath.compile((String) object);
                } catch (InvalidPathException e) {
                    validationErrors.add(new ValidationError(getAttributePathForValidationError(), e.getMessage()));
                    return null;
                }
            } else if (object != null) {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, "JSON Path"));
                return null;
            } else {
                return null;
            }
        }

        public <T> T byString(ValidatingFunction<String, T> parser) {
            Object object = expandVariable(documentNode.get(name));

            if (object != null) {
                try {
                    String string;

                    if (object instanceof String) {
                        string = (String) object;
                    } else if (object instanceof Boolean || object instanceof Number || object instanceof Character) {
                        string = object.toString();
                    } else {
                        string = DocWriter.json().writeAsString(object);
                    }

                    return parser.apply(string);
                } catch (ConfigValidationException e) {
                    validationErrors.add(getAttributePathForValidationError(), e);
                    return null;
                } catch (Exception e) {
                    validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, expected).cause(e));
                    return null;
                }
            } else {
                return null;
            }
        }

        public <T> T by(ValidatingFunction<DocNode, T> parser) {
            DocNode value = expandVariable(documentNode.getAsNode(name));

            if (value != null) {
                try {
                    return parser.apply(value);
                } catch (ConfigValidationException e) {
                    validationErrors.add(getAttributePathForValidationError(), e);
                    return null;
                } catch (Exception e) {
                    validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), value, expected).cause(e));
                    return null;
                }
            } else {
                return null;
            }
        }

        public <T> List<T> viaStringsAsList(ValidatingFunction<String, T> parser) {
            try {
                return documentNode.getAsList(name, parser, expected);
            } catch (ConfigValidationException e) {
                validationErrors.add(getAttributePathForValidationError(), e);
                return Collections.emptyList();
            }
        }

        public <T> List<T> asList(ValidatingFunction<DocNode, T> parser) {
            try {
                return documentNode.getAsListFromNodes(name, parser, expected);
            } catch (ConfigValidationException e) {
                validationErrors.add(getAttributePathForValidationError(), e);
                return Collections.emptyList();
            }
        }
    }

    public class StringAttribute extends AbstractAttribute<StringAttribute> {
        private String defaultValue;

        StringAttribute(String name, String fullAttributePath, DocNode documentNode) {
            super(name, fullAttributePath, documentNode);

        }

        public StringAttribute withDefault(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public StringAttribute validatedBy(Predicate<String> validationPredicate) {
            Object value = expandVariable(documentNode.getAsString(name));

            if (value != null && !validationPredicate.test(String.valueOf(value))) {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), value, expected));
            }

            return this;
        }

        public String asString() {
            Object value = expandVariable(documentNode.get(name));

            if (value instanceof String) {
                return (String) value;
            } else if (value != null) {
                return String.valueOf(value);
            } else {
                return defaultValue;
            }
        }

    }

    public class NumberAttribute extends AbstractAttribute<NumberAttribute> {
        private Number defaultValue;
        private boolean allowNumericStrings;

        NumberAttribute(String name, String fullAttributePath, DocNode documentNode) {
            super(name, fullAttributePath, documentNode);

        }

        public NumberAttribute withDefault(Number defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public NumberAttribute allowingNumericStrings() {
            this.allowNumericStrings = true;
            return this;
        }

        public Number asNumber() {
            Object object = expandVariable(documentNode.get(name));

            if (object == null) {
                return defaultValue;
            } else if (object instanceof Number) {
                return (Number) object;
            } else if (allowNumericStrings && object instanceof String) {
                return parseString((String) object);
            } else {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, "A numeric value"));
                return defaultValue;
            }
        }

        public int asInt() {
            Number number = asNumber();

            if (number != null) {
                return number.intValue();
            } else {
                return 0;
            }
        }

        public Integer asInteger() {
            Number number = asNumber();

            if (number instanceof Integer) {
                return (Integer) number;
            } else if (number != null) {
                return number.intValue();
            } else {
                return null;
            }
        }

        public long asLong() {
            Number number = asNumber();

            if (number != null) {
                return number.longValue();
            } else {
                return 0;
            }
        }

        public float asFloat() {
            Number number = asNumber();

            if (number != null) {
                return number.floatValue();
            } else {
                return 0;
            }
        }

        public double asDouble() {
            Number number = asNumber();

            if (number != null) {
                return number.doubleValue();
            } else {
                return 0;
            }
        }

        private Number parseString(String string) {
            Number result = Longs.tryParse(string);

            if (result != null) {
                return result;
            }

            result = Doubles.tryParse(string);

            if (result != null) {
                return result;
            }

            validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), string, "A numeric value"));
            return defaultValue;
        }
    }

    public class BooleanAttribute extends AbstractAttribute<NumberAttribute> {
        private final boolean defaultValue;

        BooleanAttribute(String name, boolean defaultValue, String fullAttributePath, DocNode documentNode) {
            super(name, fullAttributePath, documentNode);
            this.defaultValue = defaultValue;
        }

        public boolean asBoolean() {
            Object object = expandVariable(documentNode.get(name));

            if (object == null) {
                return defaultValue;
            } else if (object instanceof Boolean) {
                return (Boolean) object;
            } else {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, "true or false"));
                return defaultValue;
            }
        }
    }

    public class URIAttribute extends AbstractAttribute<URIAttribute> {
        private URI defaultValue;

        URIAttribute(String name, URI defaultValue, String fullAttributePath, DocNode documentNode) {
            super(name, fullAttributePath, documentNode);
            this.defaultValue = defaultValue;
        }

        public URIAttribute withDefault(URI defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public URI asURI() {
            Object object = expandVariable(documentNode.getAsString(name));

            if (object instanceof String) {
                try {
                    return new URI((String) object);
                } catch (URISyntaxException e) {
                    validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, "A URI"));
                    return defaultValue;
                }
            } else if (object != null) {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, "A URI"));
                return defaultValue;
            } else {
                return defaultValue;
            }
        }

        public URI asAbsoluteURI() {
            URI uri = asURI();

            if (uri != defaultValue) {
                if (!uri.isAbsolute()) {
                    validationErrors
                            .add(new InvalidAttributeValue(getAttributePathForValidationError(), uri, "URI").message("Must be an absolute URI"));
                }
            }

            return uri;
        }
    }

    public class DurationAttribute extends AbstractAttribute<DurationAttribute> {
        private Duration defaultValue;

        DurationAttribute(String name, Duration defaultValue, String fullAttributePath, DocNode documentNode) {
            super(name, fullAttributePath, documentNode);
            this.defaultValue = defaultValue;
        }

        public DurationAttribute withDefault(Duration defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Duration asDuration() {
            Object object = expandVariable(documentNode.get(name));

            if (object instanceof String) {
                try {
                    return DurationFormat.INSTANCE.parse((String) object);
                } catch (ConfigValidationException e) {
                    validationErrors.add(getAttributePathForValidationError(), e);
                    return defaultValue;
                }
            } else if (object != null) {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object,
                        "<Weeks>w? <Days>d? <Hours>h? <Minutes>m? <Seconds>s? <Milliseconds>ms?"));
                return defaultValue;
            } else {
                return defaultValue;
            }
        }

    }

    public class TypedAttribute<T> extends AbstractAttribute<StringAttribute> {
        private T defaultValue;
        private Function<T, ValidationErrors> validationFunction;

        TypedAttribute(String name, String fullAttributePath, DocNode documentNode) {
            super(name, fullAttributePath, documentNode);
        }

        public TypedAttribute<T> withDefault(T defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public TypedAttribute<T> validatedBy(Function<T, ValidationErrors> validationFunction) {
            this.validationFunction = validationFunction;
            return this;

        }

        public T as(ValidatingFunction<String, T> parser) {
            Object object = expandVariable(documentNode.getAsString(name));

            if (object != null) {
                try {
                    T result = parser.apply(String.valueOf(object));

                    if (this.validationFunction != null) {
                        ValidationErrors resultValidationErrors = this.validationFunction.apply(result);

                        if (resultValidationErrors != null) {
                            validationErrors.add(name, resultValidationErrors);
                        }
                    }

                    return result;

                } catch (ConfigValidationException e) {
                    validationErrors.add(name, e);
                    return defaultValue;
                } catch (Exception e) {
                    validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, expected));
                    return defaultValue;
                }
            } else {
                return defaultValue;
            }
        }

        public T parse(ValidatingFunction<DocNode, T> parser) {
            DocNode value = documentNode.getAsNode(name);

            if (value != null) {
                try {
                    T result = parser.apply(value);

                    if (this.validationFunction != null) {
                        ValidationErrors resultValidationErrors = this.validationFunction.apply(result);

                        if (resultValidationErrors != null) {
                            validationErrors.add(getAttributePathForValidationError(), resultValidationErrors);
                        }
                    }

                    return result;
                } catch (ConfigValidationException e) {
                    validationErrors.add(getAttributePathForValidationError(), e);
                    return defaultValue;
                } catch (Exception e) {
                    validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), value, expected).cause(e));
                    return defaultValue;
                }
            } else {
                return defaultValue;
            }
        }

    }

    public class EnumAttribute<E extends Enum<E>> extends AbstractAttribute<EnumAttribute<E>> {
        private E defaultValue;

        EnumAttribute(String name, String fullAttributePath, DocNode documentNode) {
            super(name, fullAttributePath, documentNode);
        }

        public EnumAttribute<E> withDefault(E defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public E asEnum(Class<E> enumClass) {
            Object object = expandVariable(documentNode.get(name));

            if (object == null) {
                return defaultValue;
            }

            if (object instanceof String) {
                String value = (String) object;
                for (E e : enumClass.getEnumConstants()) {
                    if (value.equalsIgnoreCase(e.name())) {
                        return e;
                    }
                }
            }

            validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), object, enumClass));

            return defaultValue;
        }

    }

    public class ListAttribute extends AbstractAttribute<ListAttribute> {
        ListAttribute(String name, String fullAttributePath, DocNode documentNode) {
            super(name, fullAttributePath, documentNode);
        }

        public ListAttribute minElements(int minElements) {
            List<String> list = documentNode.getAsListOfStrings(name);

            if (list != null && list.size() < minElements) {
                validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError(), list,
                        "The list must contain at least " + minElements + " elements"));
            }

            return this;
        }

        public StringListAttribute withDefault(String... defaultValue) {
            return new StringListAttribute(name, fullAttributePath, documentNode).withDefault(Arrays.asList(defaultValue));
        }

        public StringListAttribute validatedBy(Predicate<String> validationPredicate) {
            return new StringListAttribute(name, fullAttributePath, documentNode).validatedBy(validationPredicate);
        }

        public List<String> ofStrings() {
            return new StringListAttribute(name, fullAttributePath, documentNode).ofStrings();
        }
    }

    public class StringListAttribute extends AbstractAttribute<StringListAttribute> {
        private List<String> defaultValue;

        StringListAttribute(String name, String fullAttributePath, DocNode documentNode) {
            super(name, fullAttributePath, documentNode);

        }

        public StringListAttribute withDefault(List<String> defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public StringListAttribute withDefault(String... defaultValue) {
            return this.withDefault(Arrays.asList(defaultValue));
        }

        public StringListAttribute validatedBy(Predicate<String> validationPredicate) {
            List<String> list = ofStrings();

            if (list != null) {
                for (int i = 0; i < list.size(); i++) {
                    String string = list.get(i);
                    if (!validationPredicate.test(string)) {
                        validationErrors.add(new InvalidAttributeValue(getAttributePathForValidationError() + "." + i, string, expected));
                    }

                }
            }

            return this;
        }

        public List<String> ofStrings() {
            if (documentNode.hasNonNull(name)) {
                return expandVariablesForStrings(documentNode.getAsListOfStrings(name));
            } else {
                return defaultValue;
            }
        }
    }

}
