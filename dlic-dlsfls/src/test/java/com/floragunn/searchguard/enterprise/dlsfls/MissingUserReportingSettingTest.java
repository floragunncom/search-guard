/*
 * Copyright 2026 by floragunn GmbH - All rights reserved
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

import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.searchsupport.diag.MissingUserReporting;

/**
 * The reporting about missing users is what makes an Elasticsearch code path which drops the thread context transients
 * visible; without it, DLS, FLS, field masking and compliance read auditing are skipped silently. It is off by default
 * because internal reads produce it during normal operation. This test makes sure that it can actually be switched on
 * while a cluster is running, which is only the case if the setting is registered and declared dynamic.
 */
public class MissingUserReportingSettingTest {

    static final String SETTING = MissingUserReporting.ENABLED.getKey();

    static final TestSgConfig.Authc AUTHC = new TestSgConfig.Authc(new TestSgConfig.Authc.Domain("basic/internal_users_db"));
    static final TestSgConfig.DlsFls DLSFLS = new TestSgConfig.DlsFls();

    @ClassRule
    public static LocalCluster.Embedded cluster = new LocalCluster.Builder().sslEnabled().enterpriseModulesEnabled().authc(AUTHC).dlsFls(DLSFLS)
            .resources("dlsfls").embedded().build();

    /**
     * Internal reads legitimately run without a user, so the reporting would be noise for a normally operating
     * cluster. This is not asserted through _cluster/settings because SearchGuardPlugin#getSettingsFilter hides
     * searchguard.* from that API.
     */
    @Test
    public void isDisabledByDefault() {
        Assert.assertFalse(MissingUserReporting.ENABLED.get(Settings.EMPTY));
    }

    @Test
    public void canBeSwitchedOnAndOffWhileTheClusterIsRunning() throws Exception {
        try (GenericRestClient client = cluster.getAdminCertRestClient()) {
            HttpResponse response = client.putJson("_cluster/settings", "{\"transient\": {\"" + SETTING + "\": true}}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
            Assert.assertEquals(response.getBody(), "true",
                    response.getBodyAsDocNode().getAsNode("transient").getAsNode("searchguard").getAsNode("diagnosis")
                            .getAsNode("report_missing_user").get("enabled"));

            response = client.putJson("_cluster/settings", "{\"transient\": {\"" + SETTING + "\": null}}");

            Assert.assertEquals(response.getBody(), HttpStatus.SC_OK, response.getStatusCode());
        }
    }
}
