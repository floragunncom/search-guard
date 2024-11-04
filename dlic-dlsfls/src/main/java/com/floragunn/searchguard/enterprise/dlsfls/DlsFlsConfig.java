/*
 * Copyright 2022 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.dlsfls;

import java.util.Arrays;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.documents.Parser.Context;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.codova.documents.patch.PatchableDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.google.common.io.BaseEncoding;

public class DlsFlsConfig implements PatchableDocument<DlsFlsConfig> {
    public static CType<DlsFlsConfig> TYPE = new CType<DlsFlsConfig>("authz_dlsfls", "Document Level Security and Field Level Security", 10011,
            DlsFlsConfig.class, DlsFlsConfig::parse, CType.Storage.OPTIONAL, CType.Arity.SINGLE);

    public static final DlsFlsConfig DEFAULT = new DlsFlsConfig(null, FieldMasking.DEFAULT, false, MetricsLevel.BASIC, false, Mode.ADAPTIVE, false);

    private final DocNode source;
    private final FieldMasking fieldMasking;
    private final boolean debugEnabled;
    private final MetricsLevel metricsLevel;
    private final boolean nowAllowedInQueries;
    private final Mode dlsMode;
    private final boolean forceMinDocCountToOne;

    DlsFlsConfig(DocNode source, FieldMasking fieldMasking, boolean debugEnabled, MetricsLevel metricsLevel,
            boolean nowAllowedInQueries, Mode dlsMode, boolean forceMinDocCountToOne) {
        this.source = source;

        this.fieldMasking = fieldMasking;
        this.debugEnabled = debugEnabled;
        this.metricsLevel = metricsLevel;
        this.nowAllowedInQueries = nowAllowedInQueries;
        this.dlsMode = dlsMode;
        this.forceMinDocCountToOne = forceMinDocCountToOne;
    }

    public static ValidationResult<DlsFlsConfig> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode;
        try {
            vNode = new ValidatingDocNode(docNode.splitDottedAttributeNamesToTree(), validationErrors);
        } catch (UnexpectedDocumentStructureException e) {
            return new ValidationResult<DlsFlsConfig>(e.getValidationErrors());
        }

        FieldMasking fieldMasking = vNode.get("field_anonymization").withDefault(FieldMasking.DEFAULT).by(FieldMasking::parse);
        boolean debugEnabled = vNode.get("debug").withDefault(false).asBoolean();
        MetricsLevel metricsLevel = vNode.get("metrics").withDefault(MetricsLevel.BASIC).asEnum(MetricsLevel.class);
        // use_impl is not used after legacy DLSFLS implementation removal. The value is just consumed to avoid validation errors if
        // the value is present in the users' configuration.
        vNode.get("use_impl").withDefault("flx").asString();
        boolean nowAllowedInQueries = vNode.get("dls.allow_now").withDefault(false).asBoolean();
        Mode dlsMode = vNode.get("dls.mode").withDefault(Mode.ADAPTIVE).asEnum(Mode.class);
        boolean forceMinDocCountToOne = vNode.get("dls.force_min_doc_count_to_1").withDefault(false).asBoolean();

        vNode.checkForUnusedAttributes();

        if (!validationErrors.hasErrors()) {
            return new ValidationResult<DlsFlsConfig>(
                    new DlsFlsConfig(docNode, fieldMasking, debugEnabled, metricsLevel, nowAllowedInQueries, dlsMode, forceMinDocCountToOne));
        } else {
            return new ValidationResult<DlsFlsConfig>(validationErrors);
        }
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    @Override
    public DlsFlsConfig parseI(DocNode docNode, Context context) throws ConfigValidationException {
        return parse(docNode, (ConfigurationRepository.Context) context).get();
    }

    public boolean isDebugEnabled() {
        return debugEnabled;
    }

    public MetricsLevel getMetricsLevel() {
        return metricsLevel;
    }

    public boolean isNowAllowedInQueries() {
        return nowAllowedInQueries;
    }

    public FieldMasking getFieldMasking() {
        return fieldMasking;
    }

    public boolean isForceMinDocCountToOne() {
        return forceMinDocCountToOne;
    }

    public static class FieldMasking implements Document<FieldMasking> {
        static final String DEFAULT_SALT = "7A4EB67D40536EB6B107AF3202EA6275";
        static final String DEFAULT_PERSONALISATION = "searchguard-flx1";
        public static final FieldMasking DEFAULT = new FieldMasking(null, bytesFromHex(DEFAULT_SALT), DEFAULT_PERSONALISATION.getBytes(), null);

        private final DocNode source;
        private final byte[] salt;
        private final byte[] personalization;
        private final String prefix;

        public FieldMasking(DocNode source, byte[] salt, byte[] personalization, String prefix) {
            this.source = source;
            this.salt = salt;
            this.personalization = personalization;
            this.prefix = prefix;
        }

        public static FieldMasking parse(DocNode docNode, Parser.Context context) throws ConfigValidationException {
            ValidationErrors validationErrors = new ValidationErrors();
            ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors);

            byte[] salt = vNode.get("salt").withDefault(DEFAULT.salt).byString((s) -> bytesFromHex(s));
            byte[] personalization = vNode.get("personalisation").withDefault(DEFAULT.personalization).byString((s) -> s.getBytes());
            String prefix = vNode.get("prefix").asString();

            if (salt.length != 16) {
                validationErrors.add(new ValidationError("salt", "Must define exactly 16 bytes using a 32 character hexadecimal string"));
            }

            if (personalization.length != 16) {
                // Pad/shrink to 16 bytes if necessary
                personalization = Arrays.copyOf(personalization, 16);
            }

            vNode.checkForUnusedAttributes();
            validationErrors.throwExceptionForPresentErrors();

            return new FieldMasking(docNode, salt, personalization, prefix);
        }

        private static byte[] bytesFromHex(String hexString) {
            return BaseEncoding.base16().decode(hexString);
        }

        @Override
        public Object toBasicObject() {
            return source;
        }

        public DocNode getSource() {
            return source;
        }

        public byte[] getSalt() {
            return salt;
        }

        public byte[] getPersonalization() {
            return personalization;
        }

        public String getPrefix() {
            return prefix;
        }
    }

    public static enum Mode {
        ADAPTIVE, LUCENE_LEVEL, FILTER_LEVEL;
    }

    public Mode getDlsMode() {
        return dlsMode;
    }

}
