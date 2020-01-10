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

package com.floragunn.dlic.auth.http.kerberos.util;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

public final class KrbConstants {

 static {
     Oid spnegoTmp = null;
     Oid krbTmp = null;
     try {
         spnegoTmp = new Oid("1.3.6.1.5.5.2");
         krbTmp = new Oid("1.2.840.113554.1.2.2");
     } catch (final GSSException e) {

     }
     SPNEGO = spnegoTmp;
     KRB5MECH = krbTmp;
 }

 public static final Oid SPNEGO;
 public static final Oid KRB5MECH;
 public static final String KRB5_CONF_PROP = "java.security.krb5.conf";
 public static final String JAAS_LOGIN_CONF_PROP = "java.security.auth.login.config";
 public static final String USE_SUBJECT_CREDS_ONLY_PROP = "javax.security.auth.useSubjectCredsOnly";
 public static final String NEGOTIATE = "Negotiate";
 public static final String WWW_AUTHENTICATE = "WWW-Authenticate";

 private KrbConstants() {
 }

}
