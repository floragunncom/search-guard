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
package com.floragunn.searchguard.enterprise.dlsfls.lucene;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.Format;
import com.floragunn.searchguard.enterprise.dlsfls.DlsFlsConfig;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldAuthorization.FlsRule;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldMasking.FieldMaskingRule;
import com.floragunn.searchguard.enterprise.dlsfls.lucene.FlsStoredFieldVisitor.DocumentFilter;
import org.junit.Assert;
import org.junit.Test;

public class DocumentFilterTest {
    @Test
    public void identity() throws Exception {
        DocNode document = DocNode.of("array", DocNode.array("a", "b", "c", 1, 2, 3), "object", DocNode.of("a", 1, "b", DocNode.of("c", 2)),
                "boolean", true, "boolean2", false, "null", null, "float", 0.1);

        byte[] filteredDocumentBytes = DocumentFilter.filter(Format.JSON, document.toBytes(Format.JSON), FlsRule.ALLOW_ALL,
                FieldMaskingRule.ALLOW_ALL);

        DocNode filteredDocument = DocNode.parse(Format.JSON).from(filteredDocumentBytes);

        Assert.assertEquals(document.toDeepBasicObject(), filteredDocument.toDeepBasicObject());
    }

    @Test
    public void skipSimpleAttribute() throws Exception {
        FlsRule flsRule = FlsRule.of("*", "~x");

        DocNode document = DocNode.of("array", DocNode.array("a", "b", "c", 1, 2, 3), "object", DocNode.of("a", 1), "x", "y");

        byte[] filteredDocumentBytes = DocumentFilter.filter(Format.JSON, document.toBytes(Format.JSON), flsRule, FieldMaskingRule.ALLOW_ALL);

        DocNode filteredDocument = DocNode.parse(Format.JSON).from(filteredDocumentBytes);

        Assert.assertEquals(document.without("x").toDeepBasicObject(), filteredDocument.toDeepBasicObject());
    }

    @Test
    public void skipObjectAttribute() throws Exception {
        FlsRule flsRule = FlsRule.of("*", "~object");

        DocNode document = DocNode.of("array", DocNode.array("a", "b", "c", 1, 2, 3), "object", DocNode.of("a", 1), "x", "y");

        byte[] filteredDocumentBytes = DocumentFilter.filter(Format.JSON, document.toBytes(Format.JSON), flsRule, FieldMaskingRule.ALLOW_ALL);

        DocNode filteredDocument = DocNode.parse(Format.JSON).from(filteredDocumentBytes);

        Assert.assertEquals(document.without("object").toDeepBasicObject(), filteredDocument.toDeepBasicObject());
    }

    @Test
    public void skipArrayAttribute() throws Exception {
        FlsRule flsRule = FlsRule.of("*", "~array");

        DocNode document = DocNode.of("array", DocNode.array("a", "b", "c", 1, 2, 3), "object", DocNode.of("a", 1), "x", "y");

        byte[] filteredDocumentBytes = DocumentFilter.filter(Format.JSON, document.toBytes(Format.JSON), flsRule, FieldMaskingRule.ALLOW_ALL);

        DocNode filteredDocument = DocNode.parse(Format.JSON).from(filteredDocumentBytes);

        Assert.assertEquals(document.without("array").toDeepBasicObject(), filteredDocument.toDeepBasicObject());
    }

    @Test
    public void skipNestedAttribute() throws Exception {
        FlsRule flsRule = FlsRule.of("*", "~object.a");

        DocNode document = DocNode.of("array", DocNode.array("a", "b", "c", 1, 2, 3), "object", DocNode.of("a", 1, "b", 2), "x", "y");

        byte[] filteredDocumentBytes = DocumentFilter.filter(Format.JSON, document.toBytes(Format.JSON), flsRule, FieldMaskingRule.ALLOW_ALL);

        DocNode filteredDocument = DocNode.parse(Format.JSON).from(filteredDocumentBytes);

        Assert.assertEquals(DocNode.of("array", DocNode.array("a", "b", "c", 1, 2, 3), "object", DocNode.of("b", 2), "x", "y").toDeepBasicObject(),
                filteredDocument.toDeepBasicObject());
    }

    @Test
    public void hashSimpleAttribute() throws Exception {
        FieldMaskingRule fieldMaskingRule = FieldMaskingRule.of(DlsFlsConfig.FieldMasking.DEFAULT, "x");
        DocNode document = DocNode.of("array", DocNode.array("a", "b", "c", 1, 2, 3), "object", DocNode.of("a", 1), "x", "y");

        byte[] filteredDocumentBytes = DocumentFilter.filter(Format.JSON, document.toBytes(Format.JSON), FlsRule.ALLOW_ALL, fieldMaskingRule);

        DocNode filteredDocument = DocNode.parse(Format.JSON).from(filteredDocumentBytes);

        Assert.assertEquals(document.with("x", "0f9768c7af6190a3707258090b7966d429ae72b29ce19afeacb7c26b59b5448f").toDeepBasicObject(),
                filteredDocument.toDeepBasicObject());
    }
}
