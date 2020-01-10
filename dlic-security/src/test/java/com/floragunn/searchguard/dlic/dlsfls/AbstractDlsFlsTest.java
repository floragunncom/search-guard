/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.dlic.dlsfls;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.test.DynamicSgConfig;
import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchguard.test.helper.rest.RestHelper;

public abstract class AbstractDlsFlsTest extends SingleClusterTest {

    protected RestHelper rh = null;
    
    @Override
    protected String getResourceFolder() {
        return "dlsfls";
    }
    
    protected final void setup() throws Exception {
        setup(Settings.EMPTY);
    }
    
    protected final void setup(Settings override) throws Exception {
        setup(override, new DynamicSgConfig());
    }
    
    protected final void setup(DynamicSgConfig dynamicSgConfig) throws Exception {
        setup(Settings.EMPTY, dynamicSgConfig);
    }
    
    protected final void setup(Settings override, DynamicSgConfig dynamicSgConfig) throws Exception {
        Settings settings = Settings.builder().put(ConfigConstants.SEARCHGUARD_AUDIT_TYPE_DEFAULT, "debug").put(override).build();
        setup(Settings.EMPTY, dynamicSgConfig, settings, true);
        
        try(TransportClient tc = getInternalTransportClient(this.clusterInfo, Settings.EMPTY)) {
            populateData(tc);
        }
        
        rh = nonSslRestHelper();
    }
    
    abstract void populateData(TransportClient tc);
}