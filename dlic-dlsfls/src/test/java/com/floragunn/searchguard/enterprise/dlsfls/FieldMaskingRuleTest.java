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

import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.enterprise.dlsfls.DlsFlsConfig.FieldMasking;
import com.floragunn.searchguard.enterprise.dlsfls.RoleBasedFieldMasking.FieldMaskingRule;

public class FieldMaskingRuleTest {

    @Test
    public void field_defaultHash_bytes() throws Exception {
        FieldMaskingRule.Field field = new FieldMaskingRule.Field(new Role.Index.FieldMaskingExpression("field"), DlsFlsConfig.FieldMasking.DEFAULT);

        Assert.assertArrayEquals("45b7d14f1b22aedaf7ff3895b78b86511f5355e45a67f5206af56762fe8f5d30".getBytes(), field.apply("foobar".getBytes()));
    }

    @Test
    public void field_defaultHash_bytesref() throws Exception {
        FieldMaskingRule.Field field = new FieldMaskingRule.Field(new Role.Index.FieldMaskingExpression("field"), DlsFlsConfig.FieldMasking.DEFAULT);

        Assert.assertEquals(new BytesRef("45b7d14f1b22aedaf7ff3895b78b86511f5355e45a67f5206af56762fe8f5d30".getBytes()),
                field.apply(new BytesRef("foobar".getBytes())));
    }

    @Test
    public void field_defaultHash_prefix() throws Exception {
        FieldMaskingRule.Field field = new FieldMaskingRule.Field(new Role.Index.FieldMaskingExpression("field"),
                FieldMasking.parse(DocNode.of("prefix", "my_prefix_"), null));

        Assert.assertEquals("my_prefix_45b7d14f1b22aedaf7ff3895b78b86511f5355e45a67f5206af56762fe8f5d30", field.apply("foobar"));
    }

    @Test
    public void field_customHash() throws Exception {
        FieldMaskingRule.Field field = new FieldMaskingRule.Field(new Role.Index.FieldMaskingExpression("field::SHA-512"),
                DlsFlsConfig.FieldMasking.DEFAULT);

        Assert.assertEquals(
                "0a50261ebd1a390fed2bf326f2673c145582a6342d523204973d0219337f81616a8069b012587cf5635f6925f1b56c360230c19b273500ee013e030601bf2425",
                field.apply("foobar"));
    }

    @Test
    public void field_customHash_bytes() throws Exception {
        FieldMaskingRule.Field field = new FieldMaskingRule.Field(new Role.Index.FieldMaskingExpression("field::SHA-512"),
                DlsFlsConfig.FieldMasking.DEFAULT);

        Assert.assertArrayEquals(
                "0a50261ebd1a390fed2bf326f2673c145582a6342d523204973d0219337f81616a8069b012587cf5635f6925f1b56c360230c19b273500ee013e030601bf2425"
                        .getBytes(),
                field.apply("foobar".getBytes()));
    }

    @Test
    public void field_customHash_bytesref() throws Exception {
        FieldMaskingRule.Field field = new FieldMaskingRule.Field(new Role.Index.FieldMaskingExpression("field::SHA-512"),
                DlsFlsConfig.FieldMasking.DEFAULT);

        Assert.assertEquals(new BytesRef(
                "0a50261ebd1a390fed2bf326f2673c145582a6342d523204973d0219337f81616a8069b012587cf5635f6925f1b56c360230c19b273500ee013e030601bf2425"
                        .getBytes()),
                field.apply(new BytesRef("foobar".getBytes())));
    }

    @Test
    public void field_customHash_prefix() throws Exception {
        FieldMaskingRule.Field field = new FieldMaskingRule.Field(new Role.Index.FieldMaskingExpression("field::SHA-512"),
                FieldMasking.parse(DocNode.of("prefix", "my_prefix_"), null));

        Assert.assertEquals(
                "my_prefix_0a50261ebd1a390fed2bf326f2673c145582a6342d523204973d0219337f81616a8069b012587cf5635f6925f1b56c360230c19b273500ee013e030601bf2425",
                field.apply("foobar"));
    }

    @Test
    public void field_customHash_prefixRegexpWithoutAlg() throws Exception {
        FieldMaskingRule.Field field = new FieldMaskingRule.Field(new Role.Index.FieldMaskingExpression("field::/ba.?/::***confidential***"),
                FieldMasking.parse(DocNode.of("prefix", "my_prefix_"), null));

        Assert.assertEquals("my_prefix_foo***confidential***", field.apply("foobar"));
    }

    @Test
    public void field_replace() throws Exception {
        FieldMaskingRule.Field field = new FieldMaskingRule.Field(
                new Role.Index.FieldMaskingExpression("ip_source::/[0-9]{1,3}$/::XXX::/^[0-9]{1,3}/::***"), DlsFlsConfig.FieldMasking.DEFAULT);

        Assert.assertEquals("***.0.0.XXX", field.apply("127.0.0.1"));
    }
}
