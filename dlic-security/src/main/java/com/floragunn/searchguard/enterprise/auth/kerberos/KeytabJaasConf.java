/*
 * Based on https://github.com/apache/directory-kerby/blob/trunk/kerby-kerb/kerb-simplekdc/src/main/java/org/apache/kerby/kerberos/kerb/client/JaasKrbUtil.java
 * from Apache 2 licensed Apache Kerby
 * 
 * Copyright 2015-2019 The Apache Software Foundation
 * 
 * This product includes software developed at
 * The Apache Software Foundation (http://www.apache.org/).
 * 
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *  
 *    http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License. 
 *  
 */
package com.floragunn.searchguard.enterprise.auth.kerberos;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

public class KeytabJaasConf extends Configuration {
    private final String principal;
    private final Path keytabPath;
    private final boolean initiator;
    private final boolean debug;

    public KeytabJaasConf(String principal, Path keytab, boolean initiator, boolean debug) {
        this.principal = principal;
        this.keytabPath = keytab;
        this.initiator = initiator;
        this.debug = debug;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(final String name) {
        final Map<String, String> options = new HashMap<String, String>();
        options.put("keyTab", keytabPath.toAbsolutePath().toString());
        options.put("principal", principal);
        options.put("useKeyTab", "true");
        options.put("storeKey", "true");
        options.put("doNotPrompt", "true");
        options.put("renewTGT", "false");
        options.put("refreshKrb5Config", "true");
        options.put("isInitiator", String.valueOf(initiator));
        options.put("debug", String.valueOf(debug));

        return new AppConfigurationEntry[] {
                new AppConfigurationEntry(getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
    }

    private static String getKrb5LoginModuleName() {
        return System.getProperty("java.vendor").contains("IBM") ? "com.ibm.security.auth.module.Krb5LoginModule"
                : "com.sun.security.auth.module.Krb5LoginModule";
    }

    @Override
    public String toString() {
        return "KeytabJaasConf [principal=" + principal + ", keytabPath=" + keytabPath + ", initiator=" + initiator + ", debug=" + debug + "]";
    }
}