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

package com.floragunn.dlic.auth.ldap.srv;


import com.floragunn.dlic.auth.ldap.LdapBackendTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EmbeddedLDAPServer {
    private final static Logger log = LogManager.getLogger(LdapBackendTest.class);
    private final static int retryTries = 5;
    private final static int backOffMs = 300;

    LdapServer s = new LdapServer();

    public int applyLdif(final String... ldifFile) throws Exception {
        for (int i = 0; i <= retryTries; i++) {
            try {
                return s.start(ldifFile);
            } catch (Exception e) {
                log.error("Failed to start LDAP server after trying {} times with a {}ms back off.", i + 1, backOffMs);
                Thread.sleep(backOffMs);
            }
        }
        log.error("Failed to start LDAP server after trying {} times with a {}ms back off.", retryTries + 1, backOffMs);
        return -1;
    }

    public void start() throws Exception {

    }

    public void stop() throws Exception {
        s.stop();
    }

    public int getLdapPort() {
        return s.getLdapPort();
    }

    public int getLdapsPort() {
        return s.getLdapsPort();
    }
}