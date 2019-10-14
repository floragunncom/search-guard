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


public class EmbeddedLDAPServer {
    
    LdapServer s = new LdapServer();
    
    public int applyLdif(final String... ldifFile) throws Exception {
        return s.start(ldifFile);
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