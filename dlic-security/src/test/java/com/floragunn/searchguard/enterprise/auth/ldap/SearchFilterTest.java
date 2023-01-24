/*
  * Copyright 2022 by floragunn GmbH - All rights reserved
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
import com.floragunn.codova.documents.DocNode;
import com.unboundid.ldap.sdk.Filter;
import org.junit.Assert;
import org.junit.Test;

public class SearchFilterTest {

    @Test
    public void escaping() throws Exception {
        SearchFilter searchFilter = SearchFilter.parse(DocNode.of("raw", "(uid=${value})"), null, "");
        Filter filter = searchFilter.toFilter(AttributeSource.of("value", "*)(cn=*))"));

        Assert.assertEquals(filter.toString(), Filter.FILTER_TYPE_EQUALITY, filter.getFilterType());
        Assert.assertEquals("*)(cn=*))", filter.getAssertionValue());
        Assert.assertEquals("(uid=\\2a\\29\\28cn=\\2a\\29\\29)", filter.toString());
    }
}
