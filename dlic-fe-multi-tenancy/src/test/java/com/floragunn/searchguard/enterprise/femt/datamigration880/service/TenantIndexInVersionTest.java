/*
 * Copyright 2023 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.enterprise.femt.datamigration880.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith(Parameterized.class)
public class TenantIndexInVersionTest {

    private final String indexName;

    private final String version;

    private final boolean matches;

    public TenantIndexInVersionTest(String indexName, String version, boolean matches) {
        this.indexName = indexName;
        this.version = version;
        this.matches = matches;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
            { ".kibana_8.7.0_001", "8.7.0", true},
            { ".kibana_8.7.23_001", "8.7.24", false},
            { ".kibana_8.17.24_001", "8.17.24", true},
            { ".kibana_33.17.24_001", "33.17.24", true},
            {".kibana_191795427_performancereviews_8.7.0_001", "8.7.0", true},
            {".kibana_-738948632_performancereviews_8.9.0_002", "8.9.0", true},
            {".kibana_-634608247_abcdef22_888.77.100_101", "888.77.10", false},
            {".kibana_580139487_admtenant_8.7.2_011", "8.7.", false},
            {".kibana_-1139640511_admin1_8.7.0_001", "8.8.0", false},
            {".kibana_-152937574_admintenant_8.7.0_001", "Oops!", false},
            {".kibana_-523190050_businessintelligence_8.7.0_001", "8.7.0", true},
            {".kibana_-1242674146_commandtenant_8.7.0_001", "8.7.0", true},
            {".kibana_1554582075_dept01_8.7.0_001", "8.7", false},
            {".kibana_1554582075_dept01_8.6.0_001", "8.6*", false},
            {".kibana_1554582076_dept02_8.7.0_001", "8.7.0", true},
            {".kibana_1554582076_dept02_5.7.0_001", "5.70.0", false},
            {".kibana_1554582077_dept03_8.7.0_001", "8.7.0", true},
            {".kibana_1554582077_dept03_80.7.0_001", "80.7.0", true},
            {".kibana_1554582078_dept04_8.7.0_001", "870", false},
            {".kibana_1554582078_dept04_8.7000.0_001", "8.7000.0", true},
            {".kibana_1554582079_dept05_8.7.0_001", ".kibana_1554582079_dept05_8.7.0_001", false},
            {".kibana_-1419750584_enterprisetenant_8.7.0_001", "8.7.0", true},
            {".kibana_-1419750584_enterprisetenant_8.6.2_001", "8.6.2", true},
            {".kibana_-853258278_finance_8.7.0_001", "8.7.0", true},
            {".kibana_-853258278_finance_8.4.3_001", "8.4.3_001", false},
            {".kibana_-1992298040_financemanagement_8.7.0_001", "8.7.0", true},
            {".kibana_-1992298040_financemanagement_8.7.101_001", "8.7.101", true},
            {".kibana_1592542611_humanresources_8.7.0_001", "8.7.0", true},
            {".kibana_1592542611_humanresources_2.5.5_001", "5.5.5", false},
            {".kibana_1482524924_kibana712aliascreationtest_8.7.0_001", "8.7.0", true},
            {".kibana_1482524924_kibana712aliascreationtest_1.1.2_001", "1.1.2", true},
            {".kibana_-815674808_kibana712aliastest_8.7.0_001", "8.7.0", true},
            {".kibana_-815674808_kibana712aliastest_88.77.44_001", "88.77.44", true},
            {".kibana_-2014056171_kltentro_8.7.0_001", "8.7.0", true},
            {".kibana_-2014056171_kltentro_8.7.0_001", "8.7.0", true},
            {".kibana_-2014056163_kltentrw_8.7.0_001", "8.7.0", true},
            {".kibana_-1799980989_management_8.7.0_001", "8.7.0", true},
            {".kibana_1593390681_performancedata_8.7.0_001", "8.7.0", true},
            {".kibana_-1386441184_praxisro_8.7.0_001", "8.7.0", true},
            {".kibana_-1386441176_praxisrw_8.7.0_001", "8.7.0", true},
            {".kibana_-1754201467_testtenantro_8.7.0_001", "8.7.0", true},
            {".kibana_-1754201459_testtenantrw_8.7.0_001", "8.7.0", true},
            {".kibana_739988528_ukasz_8.7.0_001", "8.7.0", true},
            {".kibana_3292183_kirk_8.7.0_001", "8.7.0", true},
            {".kibana_3292183_kirk_890.7890.01234567890_001", "890.7890.01234567890", true},
            {".kibana_-1091682490_lukasz_8.7.0_001", "8.7.0", true},
            {".kibana_-1091714203_luksz_8.7.0_001", "8.7.0", true},
        });
    }

    @Test
    public void shouldCompareVersions() {
        TenantIndex tenantIndex = new TenantIndex(indexName, "my tenant");

        boolean sameVersion = tenantIndex.isInVersion(version);

        assertThat(sameVersion, equalTo(matches));
    }
}