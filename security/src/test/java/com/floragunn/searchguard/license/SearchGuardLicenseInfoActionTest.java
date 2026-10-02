/*
 * Copyright 2026 floragunn GmbH
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocWriter;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.fluent.collections.ImmutableSet;

public class SearchGuardLicenseInfoActionTest {

    private static final Map<String, Set<String>> LICENSES_REQUIRED = ImmutableMap.of("enterprise", ImmutableSet.of("dlsfls"));

    @Test
    public void toBasicObject_withoutLicense() throws Exception {
        SearchGuardLicenseInfoAction.Response response = new SearchGuardLicenseInfoAction.Response(null, LICENSES_REQUIRED);

        Object basicObject = response.toBasicObject();
        assertThat(basicObject, instanceOf(Map.class));

        Map<?, ?> result = (Map<?, ?>) basicObject;
        assertThat(result, hasKey("license"));
        assertThat(result.get("license"), nullValue());
        assertThat(result.get("license_required"), equalTo(Boolean.FALSE));
        assertThat(result.get("message"), instanceOf(String.class));
        assertThat(result.get("licenses_required"), equalTo(LICENSES_REQUIRED));

        // Must be serializable without errors, this is what the REST layer does
        DocNode serialized = DocNode.parse(com.floragunn.codova.documents.Format.JSON).from(DocWriter.json().writeAsString(response));
        assertThat(serialized.toMap(), hasKey("license"));
        assertThat(serialized.get("license"), nullValue());
        assertThat(serialized.get("license_required"), equalTo(Boolean.FALSE));
    }

    @Test
    public void toBasicObject_withLicense() throws Exception {
        SearchGuardLicense license = SearchGuardLicense.createTrialLicense("2026-01-01", null);
        SearchGuardLicenseInfoAction.Response response = new SearchGuardLicenseInfoAction.Response(license, LICENSES_REQUIRED);

        Map<?, ?> result = (Map<?, ?>) response.toBasicObject();
        assertThat(result.get("license"), instanceOf(Map.class));
        assertThat(((Map<?, ?>) result.get("license")).get("type"), equalTo("TRIAL"));
        assertThat(result.get("license_required"), equalTo(Boolean.TRUE));
        assertThat(result, not(hasKey("message")));
        assertThat(result.get("licenses_required"), equalTo(LICENSES_REQUIRED));
    }
}
