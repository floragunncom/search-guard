/*
 * Copyright 2015-2022 floragunn GmbH
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

package com.floragunn.searchguard.license;

import java.util.Map;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.ValidationResult;
import com.floragunn.codova.validation.errors.ValidationError;

public final class SearchGuardLicenseKey implements Document<SearchGuardLicenseKey> {

    private final SearchGuardLicense license;
    private final DocNode source;

    SearchGuardLicenseKey(SearchGuardLicense license, DocNode source) {
        this.license = license;
        this.source = source;
    }

    public SearchGuardLicense getLicense() {
        return license;
    }

    @Override
    public Object toBasicObject() {
        return source;
    }

    @Override
    public String toString() {
        return license.toString();
    }

    private static ValidationResult<SearchGuardLicenseKey> parseLicenseString(String licenseString, DocNode source) {
        String jsonString;

        try {
            jsonString = LicenseHelper.validateLicense(licenseString);
        } catch (ConfigValidationException e) {
            return new ValidationResult<SearchGuardLicenseKey>(e.getValidationErrors());
        } catch (Exception e) {
            return new ValidationResult<SearchGuardLicenseKey>(new ValidationError(null, e.getMessage()).cause(e));
        }

        Map<String, Object> parsedJson;
        try {
            parsedJson = DocReader.json().readObject(jsonString);
        } catch (ConfigValidationException e) {
            return new ValidationResult<SearchGuardLicenseKey>(e.getValidationErrors());
        }

        SearchGuardLicenseKey result = new SearchGuardLicenseKey(new SearchGuardLicense(parsedJson), source);

        return new ValidationResult<SearchGuardLicenseKey>(result, result.getLicense().staticValidate());
    }

    public static ValidationResult<SearchGuardLicenseKey> parse(DocNode docNode, Parser.Context context) {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vNode = new ValidatingDocNode(docNode, validationErrors, context);

        if (docNode.isString()) {
            return parseLicenseString(docNode.toString(), docNode);
        }

        ValidationResult<SearchGuardLicenseKey> result = vNode.get("key")
                .by((node) -> SearchGuardLicenseKey.parseLicenseString(node.toString(), docNode));

        if (result != null) {
            validationErrors.add("key", result);
        }

        vNode.checkForUnusedAttributes();

        if (result != null && result.hasResult()) {
            return new ValidationResult<SearchGuardLicenseKey>(result.peek(), validationErrors);
        } else {
            return new ValidationResult<SearchGuardLicenseKey>(validationErrors);
        }

    }

}
