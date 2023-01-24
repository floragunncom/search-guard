/*
  * Copyright 2016-2022 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.auth.ldap;

import com.floragunn.codova.config.templates.AttributeSource;
import com.floragunn.codova.config.templates.ExpressionEvaluationException;
import com.floragunn.codova.config.templates.Template;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.unboundid.ldap.sdk.Filter;
import com.unboundid.ldap.sdk.LDAPException;

public class SearchFilter {

    public static final SearchFilter DEFAULT = new SearchFilter("sAMAccountName", null, "user.name");
    public static final SearchFilter DEFAULT_GROUP_SEARCH = new SearchFilter("member", null, "dn");

    private final String byAttribute;
    private final Template<String> raw;
    private final String byAttributeValueSource;

    SearchFilter(String byAttribute, Template<String> raw, String byAttributeValueSource) {
        this.byAttribute = byAttribute;
        this.raw = raw != null ? raw.stringEscapeFunction(Filter::encodeValue) : null;
        this.byAttributeValueSource = byAttributeValueSource;
    }

    Filter toFilter(AttributeSource attributeSource) throws LDAPException, ExpressionEvaluationException {
        Object userName = attributeSource.getAttributeValue(byAttributeValueSource);

        if (byAttribute != null && raw != null) {
            return Filter.createANDFilter(Filter.createEqualityFilter(byAttribute, String.valueOf(userName)), createRawFilter(attributeSource));
        } else if (byAttribute != null) {
            return Filter.createEqualityFilter(byAttribute, String.valueOf(userName));
        } else {
            return createRawFilter(attributeSource);
        }
    }

    private Filter createRawFilter(AttributeSource attributeSource) throws LDAPException, ExpressionEvaluationException {
        return Filter.create(raw.render(attributeSource));
    }

    public static SearchFilter parseForGroupSearch(DocNode docNode, Parser.Context context) throws ConfigValidationException {
        return parse(docNode, context, "dn");
    }

    public static SearchFilter parseForUserSearch(DocNode docNode, Parser.Context context) throws ConfigValidationException {
        return parse(docNode, context, "user.name");
    }

    public static SearchFilter parse(DocNode docNode, Parser.Context context, String byAttributeValueSource) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        String byAttribute = vNode.get("by_attribute").asString();
        Template<String> raw = vNode.get("raw").asTemplate();

        if (byAttribute == null && raw == null) {
            validationErrors.add(new MissingAttribute("by_attribute"));
        }

        validationErrors.throwExceptionForPresentErrors();

        return new SearchFilter(byAttribute, raw, byAttributeValueSource);
    }
}
