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

package com.floragunn.searchguard.configuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.crypto.generators.OpenBSDBCrypt;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.documents.RedactableDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.OrderedImmutableMap;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentState.State;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.google.common.base.Charsets;

public class SgDynamicConfiguration<T> implements ToXContent, Document<Object>, RedactableDocument, ComponentStateProvider, AutoCloseable {

    private static final Logger log = LogManager.getLogger(SgDynamicConfiguration.class);

    private final CType<T> ctype;
    private final ComponentState componentState;

    private final OrderedImmutableMap<String, T> centries;
    private long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
    private String uninterpolatedJson;
    private long docVersion = -1;
    private final ValidationErrors validationErrors;

    public static <T> SgDynamicConfiguration<T> empty(CType<T> type) {
        return new SgDynamicConfiguration<T>(type, OrderedImmutableMap.empty());
    }

    public static <T> SgDynamicConfiguration<T> of(CType<T> type, String k1, T v1) {
        return new SgDynamicConfiguration<T>(type, OrderedImmutableMap.of(k1, v1));
    }

    public static <T> SgDynamicConfiguration<T> of(CType<T> type, String k1, T v1, String k2, T v2) {
        return new SgDynamicConfiguration<T>(type, OrderedImmutableMap.of(k1, v1, k2, v2));
    }

    public static <T> SgDynamicConfiguration<T> of(CType<T> type, Map<String, T> entries) {
        return new SgDynamicConfiguration<T>(type, OrderedImmutableMap.of(entries));
    }

    public static <T> ValidationResult<SgDynamicConfiguration<T>> fromJson(String uninterpolatedJson, CType<T> ctype, long docVersion, long seqNo,
            long primaryTerm, ConfigurationRepository.Context parserContext) throws DocumentParseException, ConfigValidationException {
        String jsonString;

        if (ctype.isReplaceLegacyEnvVars()) {
            jsonString = replaceEnvVars(uninterpolatedJson, ctype);
        } else {
            jsonString = uninterpolatedJson;
        }

        return fromDocNode(DocNode.wrap(DocReader.json().splitAttributesAtDotsStartingAtDepth(1).read(jsonString)), uninterpolatedJson, ctype,
                docVersion, seqNo, primaryTerm, parserContext);
    }

    public static <T> ValidationResult<SgDynamicConfiguration<T>> fromMap(Map<String, ?> map, CType<T> ctype,
            ConfigurationRepository.Context parserContext) throws ConfigValidationException {
        return fromDocNode(DocNode.wrap(map), null, ctype, -1, -1, -1, parserContext);
    }

    public static <T> ValidationResult<SgDynamicConfiguration<T>> fromDocNode(DocNode docNode, String uninterpolatedJson, CType<T> ctype,
            long docVersion, long seqNo, long primaryTerm, ConfigurationRepository.Context parserContext) {

        Parser.ReturningValidationResult<T, ConfigurationRepository.Context> parser = ctype.getParser();

        if (parser == null) {
            throw new IllegalArgumentException("Unsupported ctype " + ctype);
        }

        OrderedImmutableMap.Builder<String, T> entries = new OrderedImmutableMap.Builder<>(docNode.size());

        ValidationErrors validationErrors = new ValidationErrors();

        for (Map.Entry<String, Object> entry : docNode.entrySet()) {

            String id = entry.getKey();

            if (id.startsWith("_sg")) {
                continue;
            }

            try {
                ValidationResult<T> parsedEntry = parser.parse(DocNode.wrap(entry.getValue()), parserContext);

                if (parsedEntry.hasResult()) {
                    entries.put(id, parsedEntry.peek());
                }

                validationErrors.add(ctype.getArity() == CType.Arity.SINGLE ? null : id, parsedEntry.getValidationErrors());
            } catch (Exception e) {
                log.error("Unexpected exception while parsing " + entry, e);
                validationErrors.add(new ValidationError(ctype.getArity() == CType.Arity.SINGLE ? null : id, e.getMessage()).cause(e));
            }
        }

        return new ValidationResult<SgDynamicConfiguration<T>>(
                new SgDynamicConfiguration<>(ctype, entries.build(), seqNo, primaryTerm, docVersion, uninterpolatedJson, validationErrors),
                validationErrors);
    }

    private SgDynamicConfiguration(CType<T> ctype, OrderedImmutableMap<String, T> entries) {
        this.ctype = ctype;
        this.centries = entries;
        this.componentState = new ComponentState(0, "config", ctype.getName());
        this.validationErrors = new ValidationErrors();
    }

    private SgDynamicConfiguration(CType<T> ctype, OrderedImmutableMap<String, T> entries, long seqNo, long primaryTerm, long docVersion,
            String uninterpolatedJson, ValidationErrors validationErrors) {
        super();
        this.centries = entries;
        this.ctype = ctype;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.uninterpolatedJson = uninterpolatedJson;
        this.docVersion = docVersion;
        this.validationErrors = validationErrors;

        this.componentState = new ComponentState(0, "config", ctype.getName());
        this.componentState.setConfigVersion(docVersion);

        if (validationErrors.hasErrors()) {
            log.error("Errors in configuration " + ctype + "\n" + validationErrors.toDebugString());
            this.componentState.setState(State.PARTIALLY_INITIALIZED, "has_errors");
            this.componentState.addDetail(validationErrors.toBasicObject());
        } else {
            this.componentState.initialized();
        }
    }

    public String getETag() {
        return primaryTerm + "." + seqNo;
    }

    public OrderedImmutableMap<String, T> getCEntries() {
        return centries;
    }

    public SgDynamicConfiguration<T> with(String key, T entry) {
        return new SgDynamicConfiguration<>(ctype, centries.with(key, entry), seqNo, primaryTerm, docVersion, null, validationErrors);
    }

    public SgDynamicConfiguration<T> with(Map<String, T> map) {
        return new SgDynamicConfiguration<>(ctype, centries.with(OrderedImmutableMap.of(map)), seqNo, primaryTerm, docVersion, uninterpolatedJson,
                validationErrors);
    }

    public SgDynamicConfiguration<T> without(String key) {
        return new SgDynamicConfiguration<>(ctype, centries.without(key), seqNo, primaryTerm, docVersion, null, validationErrors);
    }

    public SgDynamicConfiguration<T> withoutStatic() {
        OrderedImmutableMap.Builder<String, T> entries = new OrderedImmutableMap.Builder<>(centries.size());

        for (Entry<String, T> entry : new HashMap<String, T>(centries).entrySet()) {
            if (!(entry.getValue() instanceof StaticDefinable) || !((StaticDefinable) entry.getValue()).isStatic()) {
                entries.put(entry.getKey(), entry.getValue());
            }
        }

        return new SgDynamicConfiguration<>(ctype, entries.build(), seqNo, primaryTerm, docVersion, uninterpolatedJson, validationErrors);
    }

    public SgDynamicConfiguration<T> withoutHidden() {
        OrderedImmutableMap.Builder<String, T> entries = new OrderedImmutableMap.Builder<>(centries.size());

        for (Entry<String, T> entry : new HashMap<String, T>(centries).entrySet()) {
            if (!(entry.getValue() instanceof Hideable && ((Hideable) entry.getValue()).isHidden())) {
                entries.put(entry.getKey(), entry.getValue());
            }
        }

        return new SgDynamicConfiguration<>(ctype, entries.build(), seqNo, primaryTerm, docVersion, uninterpolatedJson, validationErrors);
    }

    public SgDynamicConfiguration<T> only(String key) {
        T entry = centries.get(key);

        if (entry == null) {
            return empty(ctype);
        } else {
            return new SgDynamicConfiguration<>(ctype, OrderedImmutableMap.of(key, entry), seqNo, primaryTerm, docVersion, null, validationErrors);
        }
    }

    public T getCEntry(String key) {
        return centries.get(key);
    }

    public boolean exists(String key) {
        return centries.containsKey(key);
    }

    @Override
    public String toString() {
        if (primaryTerm == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            return ctype + "@[none]";
        } else {
            return ctype + "@" + primaryTerm + "." + seqNo + "/n:" + centries.size();
        }
    }

    @Override
    public Object toBasicObject() {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>(centries.size() + 1);

        for (Map.Entry<String, T> entry : centries.entrySet()) {
            result.put(entry.getKey(), ((Document<?>) entry.getValue()).toBasicObject());
        }

        return result;
    }

    @Override
    public Object toRedactedBasicObject() {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>(centries.size() + 1);

        for (Map.Entry<String, T> entry : centries.entrySet()) {
            if (entry.getValue() instanceof RedactableDocument) {
                result.put(entry.getKey(), ((RedactableDocument) entry.getValue()).toRedactedBasicObject());
            } else {
                result.put(entry.getKey(), ((Document<?>) entry.getValue()).toBasicObject());
            }
        }

        return result;
    }

    /**
     * This is for compatibility with the old REST API where clients (such as the Kibana config UI) depend on all attributes being present, even if they are empty.
     */
    public Object toRedactedLegacyBasicObject() {
        LinkedHashMap<String, Object> result = new LinkedHashMap<>(centries.size() + 1);

        for (Map.Entry<String, T> entry : centries.entrySet()) {
            if (entry.getValue() instanceof HasLegacyFormat) {
                result.put(entry.getKey(), ((HasLegacyFormat) entry.getValue()).toRedactedLegacyBasicObject());
            } else if (entry.getValue() instanceof RedactableDocument) {
                result.put(entry.getKey(), ((RedactableDocument) entry.getValue()).toRedactedBasicObject());
            } else {
                result.put(entry.getKey(), ((Document<?>) entry.getValue()).toDeepBasicObject());
            }
        }

        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        if (uninterpolatedJson != null) {
            builder.rawValue(new ByteArrayInputStream(uninterpolatedJson.getBytes(Charsets.UTF_8)), XContentType.JSON);
        } else if (Document.class.isAssignableFrom(this.ctype.getType())) {
            return builder.value(toBasicObject());
        } else {
            builder.startObject();

            for (Map.Entry<String, T> entry : centries.entrySet()) {
                String key = entry.getKey();
                T value = entry.getValue();

                builder.field(key, ((Document<?>) value).toBasicObject());
            }

            builder.endObject();
        }

        return builder;
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    public boolean documentExists() {
        return seqNo >= 0 && primaryTerm >= 0;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public CType<T> getCType() {
        return ctype;
    }

    public Class<T> getImplementingClass() {
        if (ctype == null) {
            return null;
        }

        return ctype.getType();
    }

    public long getDocVersion() {
        return docVersion;
    }

    public String getUninterpolatedJson() {
        return uninterpolatedJson;
    }

    public static interface HasLegacyFormat {
        Object toRedactedLegacyBasicObject();
    }

    public ValidationErrors getValidationErrors() {
        return validationErrors;
    }

    @Override
    public ComponentState getComponentState() {
        return componentState;
    }

    @Override
    public void close() {
        for (T entry : this.centries.values()) {
            if (entry instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) entry).close();
                } catch (Exception e) {
                    log.error("Error while closing {}", entry, e);
                }
            }
        }
    }

    private static final Pattern ENV_PATTERN = Pattern.compile("\\$\\{env\\.([\\w]+)((\\:\\-)?[\\w]*)\\}");
    private static final Pattern ENVBC_PATTERN = Pattern.compile("\\$\\{envbc\\.([\\w]+)((\\:\\-)?[\\w]*)\\}");
    private static final Pattern ENVBASE64_PATTERN = Pattern.compile("\\$\\{envbase64\\.([\\w]+)((\\:\\-)?[\\w]*)\\}");

    private static String replaceEnvVars(String in, CType<?> ctype) {
        if (in == null || in.isEmpty()) {
            return in;
        }

        return replaceEnvVarsBC(replaceEnvVarsNonBC(replaceEnvVarsBase64(in, ctype), ctype), ctype);
    }

    private static String replaceEnvVarsNonBC(String in, CType<?> ctype) {
        //${env.MY_ENV_VAR}
        //${env.MY_ENV_VAR:-default}
        Matcher matcher = ENV_PATTERN.matcher(in);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            final String replacement = resolveEnvVar(matcher.group(1), matcher.group(2), false);
            if (replacement != null) {
                matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            }
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static String replaceEnvVarsBC(String in, CType<?> ctype) {
        //${envbc.MY_ENV_VAR}
        //${envbc.MY_ENV_VAR:-default}
        Matcher matcher = ENVBC_PATTERN.matcher(in);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            final String replacement = resolveEnvVar(matcher.group(1), matcher.group(2), true);
            if (replacement != null) {
                matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            }
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static String replaceEnvVarsBase64(String in, CType<?> ctype) {
        //${envbc.MY_ENV_VAR}
        //${envbc.MY_ENV_VAR:-default}
        Matcher matcher = ENVBASE64_PATTERN.matcher(in);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            final String replacement = resolveEnvVar(matcher.group(1), matcher.group(2), false);
            if (replacement != null) {
                matcher.appendReplacement(sb,
                        (Matcher.quoteReplacement(new String(Base64.getDecoder().decode(replacement), StandardCharsets.UTF_8))));
            }
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    //${env.MY_ENV_VAR}
    //${env.MY_ENV_VAR:-default}
    private static String resolveEnvVar(String envVarName, String mode, boolean bc) {
        final String envVarValue = System.getenv(envVarName);
        if (envVarValue == null || envVarValue.isEmpty()) {
            if (mode != null && mode.startsWith(":-") && mode.length() > 2) {
                return bc ? hash(mode.substring(2).toCharArray()) : mode.substring(2);
            } else {
                return null;
            }
        } else {
            return bc ? hash(envVarValue.toCharArray()) : envVarValue;
        }
    }

    private static String hash(final char[] clearTextPassword) {
        final byte[] salt = new byte[16];
        new SecureRandom().nextBytes(salt);
        final String hash = OpenBSDBCrypt.generate((Objects.requireNonNull(clearTextPassword)), salt, 12);
        Arrays.fill(salt, (byte) 0);
        Arrays.fill(clearTextPassword, '\0');
        return hash;
    }

}