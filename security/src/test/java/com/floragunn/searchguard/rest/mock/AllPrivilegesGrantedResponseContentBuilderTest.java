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

package com.floragunn.searchguard.rest.mock;

import com.floragunn.codova.documents.DocNode;
import org.junit.Test;


import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;

public class AllPrivilegesGrantedResponseContentBuilderTest {

    @Test
    public void shouldBuildCorrectResponse_emptyBody() {
        String user = "john";
        Map<String, Object> responseMap = AllPrivilegesGrantedResponseContentBuilder.buildAllPrivilegesGrantedResponseContent(user, DocNode.EMPTY);

        assertThat(responseMap, aMapWithSize(5));
        assertThat(responseMap, hasEntry(equalTo("username"), equalTo(user)));
        assertThat(responseMap, hasEntry(equalTo("has_all_requested"), equalTo(true)));
        assertThat(responseMap, hasEntry(equalTo("cluster"), instanceOf(Map.class)));
        assertThat((Map<?, ?>) responseMap.get("cluster"), anEmptyMap());
        assertThat(responseMap, hasEntry(equalTo("index"), instanceOf(Map.class)));
        assertThat((Map<?, ?>) responseMap.get("index"), anEmptyMap());
        assertThat(responseMap, hasEntry(equalTo("application"), instanceOf(Map.class)));
        assertThat((Map<?, ?>) responseMap.get("application"), anEmptyMap());
    }

    @Test
    public void shouldMarkClusterPrivilegesAsGranted() {
        List<String> permissions = List.of("a", "b", "c", "d");
        DocNode requestBody = DocNode.of("cluster", permissions);
        Map<String, Boolean> markedPerms = AllPrivilegesGrantedResponseContentBuilder.markAllClusterPrivilegesAsGranted(requestBody);

        assertThat(markedPerms, aMapWithSize(permissions.size()));
        permissions.forEach(perm -> assertThat(markedPerms, hasEntry(equalTo(perm), equalTo(true))));
    }

    @Test
    public void shouldMarkIndexPrivilegesAsGranted() {
        DocNode requestBody = DocNode.of("index", DocNode.array(
                DocNode.of("names", List.of("index-1", "index-2"), "privileges", List.of("a", "a", "b")),
                DocNode.of("names", List.of("index-1"), "privileges", List.of("d"))
        ));
        Map<String, Map<String, Boolean>> markedPerms = AllPrivilegesGrantedResponseContentBuilder.markAllIndexPrivilegesAsGranted(requestBody);

        assertThat(markedPerms, aMapWithSize(2)); // index-1, index-2
        assertThat(markedPerms, hasKey(equalTo("index-1")));
        assertThat(markedPerms, hasKey(equalTo("index-2")));

        assertThat(markedPerms.get("index-1"), aMapWithSize(3)); // a, b, d
        assertThat(markedPerms.get("index-1"), hasEntry(equalTo("a"), equalTo(true)));
        assertThat(markedPerms.get("index-1"), hasEntry(equalTo("b"), equalTo(true)));
        assertThat(markedPerms.get("index-1"), hasEntry(equalTo("d"), equalTo(true)));

        assertThat(markedPerms.get("index-2"), aMapWithSize(2)); // a, b
        assertThat(markedPerms.get("index-2"), hasEntry(equalTo("a"), equalTo(true)));
        assertThat(markedPerms.get("index-2"), hasEntry(equalTo("b"), equalTo(true)));
    }

    @Test
    public void shouldMarkApplicationPrivilegesAsGranted() {
        DocNode requestBody = DocNode.of("application", DocNode.array(
                DocNode.of("application", "app1","resources", List.of("resource1", "resource2"), "privileges", List.of("a", "a", "b")),
                DocNode.of("application", "app2","resources", List.of("resource1", "resource2"), "privileges", List.of("a", "b")),
                DocNode.of("application", "app1","resources", List.of("resource4"), "privileges", List.of("d"))
        ));
        Map<String, Map<String, Map<String, Boolean>>> markedPerms = AllPrivilegesGrantedResponseContentBuilder.markAllApplicationPrivilegesAsGranted(requestBody);

        assertThat(markedPerms, aMapWithSize(2)); // app1, app2
        assertThat(markedPerms, hasKey(equalTo("app1")));
        assertThat(markedPerms, hasKey(equalTo("app2")));

        assertThat(markedPerms.get("app1"), aMapWithSize(3)); // resource1, resource2, resource4
        assertThat(markedPerms.get("app1"), hasKey("resource1"));
        assertThat(markedPerms.get("app1"), hasKey("resource2"));
        assertThat(markedPerms.get("app1"), hasKey("resource4"));

        assertThat(markedPerms.get("app1").get("resource1"), aMapWithSize(2)); // a, b
        assertThat(markedPerms.get("app1").get("resource1"), hasEntry(equalTo("a"), equalTo(true)));
        assertThat(markedPerms.get("app1").get("resource1"), hasEntry(equalTo("b"), equalTo(true)));

        assertThat(markedPerms.get("app1").get("resource2"), aMapWithSize(2)); // a, b
        assertThat(markedPerms.get("app1").get("resource2"), hasEntry(equalTo("a"), equalTo(true)));
        assertThat(markedPerms.get("app1").get("resource2"), hasEntry(equalTo("b"), equalTo(true)));

        assertThat(markedPerms.get("app1").get("resource4"), aMapWithSize(1)); // d
        assertThat(markedPerms.get("app1").get("resource4"), hasEntry(equalTo("d"), equalTo(true)));

        assertThat(markedPerms.get("app2"), aMapWithSize(2)); // resource1, resource2
        assertThat(markedPerms.get("app2"), hasKey("resource1"));
        assertThat(markedPerms.get("app2"), hasKey("resource2"));

        assertThat(markedPerms.get("app2").get("resource1"), aMapWithSize(2)); // a, b
        assertThat(markedPerms.get("app2").get("resource1"), hasEntry(equalTo("a"), equalTo(true)));
        assertThat(markedPerms.get("app2").get("resource1"), hasEntry(equalTo("b"), equalTo(true)));

        assertThat(markedPerms.get("app2").get("resource2"), aMapWithSize(2)); // a, b
        assertThat(markedPerms.get("app2").get("resource2"), hasEntry(equalTo("a"), equalTo(true)));
        assertThat(markedPerms.get("app2").get("resource2"), hasEntry(equalTo("b"), equalTo(true)));
    }
}
