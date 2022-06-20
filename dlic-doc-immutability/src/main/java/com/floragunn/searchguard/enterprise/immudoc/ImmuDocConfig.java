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

package com.floragunn.searchguard.enterprise.immudoc;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.documents.Parser.Context;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.codova.documents.patch.PatchableDocument;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.configuration.CType;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;

public class ImmuDocConfig implements PatchableDocument<ImmuDocConfig> {
    public static CType<ImmuDocConfig> TYPE = new CType<ImmuDocConfig>("immutability", "Document and index immutability", 10012, ImmuDocConfig.class,
            ImmuDocConfig::parse, CType.Storage.OPTIONAL, CType.Arity.SINGLE);

    public static final ImmuDocConfig DEFAULT = new ImmuDocConfig(null, Pattern.blank(), MetricsLevel.NONE);

    private final DocNode source;
    private final Pattern immutableIndicesPattern;
    private final MetricsLevel metricsLevel;

    ImmuDocConfig(DocNode source, Pattern immutableIndicesPattern, MetricsLevel metricsLevel) {
        this.source = source;

        this.immutableIndicesPattern = immutableIndicesPattern;
        this.metricsLevel = metricsLevel;
    }

    public static ValidationResult<ImmuDocConfig> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode;
        try {
            vNode = new ValidatingDocNode(docNode.splitDottedAttributeNamesToTree(), validationErrors);
        } catch (UnexpectedDocumentStructureException e) {
            return new ValidationResult<ImmuDocConfig>(e.getValidationErrors());
        }

        Pattern immutableIndicesPattern = vNode.get("indices.append_only").by(Pattern::parse);

        MetricsLevel metricsLevel = vNode.get("metrics").withDefault(MetricsLevel.BASIC).asEnum(MetricsLevel.class);
        vNode.checkForUnusedAttributes();

        if (!validationErrors.hasErrors()) {
            return new ValidationResult<ImmuDocConfig>(new ImmuDocConfig(docNode, immutableIndicesPattern, metricsLevel));
        } else {
            return new ValidationResult<ImmuDocConfig>(validationErrors);
        }
    }

    public Pattern getImmutableIndicesPattern() {
        return immutableIndicesPattern;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    @Override
    public ImmuDocConfig parseI(DocNode docNode, Context context) throws ConfigValidationException {
        return parse(docNode, (ConfigurationRepository.Context) context).get();
    }

    public MetricsLevel getMetricsLevel() {
        return metricsLevel;
    }

    public ImmuDocConfig withImmutableDocPattern(Pattern immutableDocPattern) {
        return new ImmuDocConfig(source, Pattern.join(ImmutableList.of(this.immutableIndicesPattern, immutableDocPattern)), metricsLevel);
    }
}
