/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.floragunn.searchguard.support;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.bc.BcPEMDecryptorProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

public final class PemKeyReader {
    
    protected static final Logger log = LogManager.getLogger(PemKeyReader.class);
    static final String JKS = "JKS";
    static final String PKCS12 = "PKCS12";

    private static void safeClose(InputStream in) {
        try {
            in.close();
        } catch (IOException e) {
            //ignore
        }
    }
    
    public static PrivateKey toPrivateKey(File keyFile, String keyPassword) throws IOException, OperatorCreationException, PKCSException {
        if (keyFile == null) {
            return null;
        }

        InputStream in = new FileInputStream(keyFile);

        try {
        	return getPrivateKeyFromByteBuffer(in, keyPassword);
        } finally {
            safeClose(in);
        }      
    }
    
    public static PrivateKey toPrivateKey(InputStream in, String keyPassword) throws IOException, OperatorCreationException, PKCSException {
        if (in == null) {
            return null;
        }
        return getPrivateKeyFromByteBuffer(in, keyPassword);
    }

    //return null if there is no private key found in InputStream
    private static PrivateKey getPrivateKeyFromByteBuffer(InputStream in, String keyPassword) throws IOException, OperatorCreationException, PKCSException {

    	final JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
        
	    try(final PEMParser pemParser = new PEMParser(new InputStreamReader(in, StandardCharsets.US_ASCII))) {
	    	
	    	final Object object = pemParser.readObject();

	    	if(object == null) {
	        	return null;
	    	} else if(object instanceof PEMKeyPair) {
	    		return converter.getKeyPair((PEMKeyPair) object).getPrivate();	
	    	} else if (object instanceof PEMEncryptedKeyPair) {
	    		PEMDecryptorProvider pdp = new BcPEMDecryptorProvider(keyPassword==null?null:keyPassword.toCharArray());
	    		PEMKeyPair kp = ((PEMEncryptedKeyPair) object).decryptKeyPair(pdp);
	    		return converter.getKeyPair(kp).getPrivate();	
	    	} else if (object instanceof PrivateKeyInfo) {
	    		return converter.getPrivateKey((PrivateKeyInfo) object);
	    	} else if (object instanceof PKCS8EncryptedPrivateKeyInfo) {
	    		InputDecryptorProvider pdp = new JceOpenSSLPKCS8DecryptorProviderBuilder()
	    				.build(keyPassword==null?null:keyPassword.toCharArray());
	    		return converter.getPrivateKey(((PKCS8EncryptedPrivateKeyInfo) object).decryptPrivateKeyInfo(pdp));
	    	} else {
	    		throw new PKCSException("Unable to decrypt private key (Type: "+object.getClass()+" )");
	    	}
        }
        
    }
    
    public static X509Certificate loadCertificateFromFile(String file) throws Exception {
        if(file == null) {
            return null;
        }
        
        CertificateFactory fact = CertificateFactory.getInstance("X.509");
        try(FileInputStream is = new FileInputStream(file)) {
            return (X509Certificate) fact.generateCertificate(is);
        }
    }
    
    public static X509Certificate loadCertificateFromStream(InputStream in) throws Exception {
        if(in == null) {
            return null;
        }
        
        CertificateFactory fact = CertificateFactory.getInstance("X.509");
        return (X509Certificate) fact.generateCertificate(in);
    }
    
    public static KeyStore loadKeyStore(String storePath, String keyStorePassword, String type) throws Exception {
      if(storePath == null) {
          return null;
      }

      if(type == null || !type.toUpperCase().equals(JKS) || !type.toUpperCase().equals(PKCS12)) {
          type = JKS;
      }
      
      final KeyStore store = KeyStore.getInstance(type.toUpperCase());
      store.load(new FileInputStream(storePath), keyStorePassword==null?null:keyStorePassword.toCharArray());
      return store;
    }

    public static PrivateKey loadKeyFromFile(String password, String keyFile) throws Exception {
        
        if(keyFile == null) {
            return null;
        }
        
        return PemKeyReader.toPrivateKey(new File(keyFile), password);
    }
    
    public static PrivateKey loadKeyFromStream(String password, InputStream in) throws Exception {
        
        if(in == null) {
            return null;
        }
        
        return PemKeyReader.toPrivateKey(in, password);
    }
    
    public static void checkPath(String keystoreFilePath, String fileNameLogOnly) {
        
        if (keystoreFilePath == null || keystoreFilePath.length() == 0) {
            throw new ElasticsearchException("Empty file path for "+fileNameLogOnly);
        }
        
        if (Files.isDirectory(Paths.get(keystoreFilePath), LinkOption.NOFOLLOW_LINKS)) {
            throw new ElasticsearchException("Is a directory: " + keystoreFilePath+" Expected a file for "+fileNameLogOnly);
        }

        if(!Files.isReadable(Paths.get(keystoreFilePath))) {
            throw new ElasticsearchException("Unable to read " + keystoreFilePath + " ("+Paths.get(keystoreFilePath)+"). Please make sure this files exists and is readable regarding to permissions. Property: "+fileNameLogOnly);
        }
    }
    
    public static X509Certificate[] loadCertificatesFromFile(String file) throws Exception {
        if(file == null) {
            return null;
        }
        
        CertificateFactory fact = CertificateFactory.getInstance("X.509");
        try(FileInputStream is = new FileInputStream(file)) {
            Collection<? extends Certificate> certs = fact.generateCertificates(is);
            X509Certificate[] x509Certs = new X509Certificate[certs.size()];
            int i=0;
            for(Certificate cert: certs) {
                x509Certs[i++] = (X509Certificate) cert;
            }
            return x509Certs;
        }
        
    }
    
    public static X509Certificate[] loadCertificatesFromStream(InputStream in) throws CertificateException {
        if(in == null) {
            return null;
        }
        
        CertificateFactory fact = CertificateFactory.getInstance("X.509");
        Collection<? extends Certificate> certs = fact.generateCertificates(in);
        X509Certificate[] x509Certs = new X509Certificate[certs.size()];
        int i=0;
        for(Certificate cert: certs) {
            x509Certs[i++] = (X509Certificate) cert;
        }
        return x509Certs;
        
    }

    
    public static InputStream resolveStream(String propName, Settings settings) {
        final String content = settings.get(propName, null);
        
        if(content == null) {
            return null;
        }

        return new ByteArrayInputStream(content.getBytes(StandardCharsets.US_ASCII));
    }
    
    public static String resolve(String propName, Settings settings, Path configPath, boolean mustBeValid) {        
        final String originalPath = settings.get(propName, null);
        return resolve(originalPath, propName, settings, configPath, mustBeValid);
    }

    public static String resolve(String originalPath, String propName, Settings settings, Path configPath, boolean mustBeValid) {
        log.debug("Path is is {}", originalPath);
        String path = originalPath;
        final Environment env = new Environment(settings, configPath);
        
        if(env != null && originalPath != null && originalPath.length() > 0) {
            path = env.configDir().resolve(originalPath).toAbsolutePath().toString();
            log.debug("Resolved {} to {} against {}", originalPath, path, env.configDir().toAbsolutePath().toString());
        }
        
        if(mustBeValid) {
            checkPath(path, propName);
        }
        
        if("".equals(path)) {
            path = null;
        }
        
        return path;	
    }
    
    public static KeyStore toTruststore(final String trustCertificatesAliasPrefix, final X509Certificate[] trustCertificates) throws Exception {
        
        if(trustCertificates == null) {
            return null;
        }
        
        KeyStore ks = KeyStore.getInstance(JKS);
        ks.load(null);
        
        if(trustCertificates != null && trustCertificates.length > 0) {
            for (int i = 0; i < trustCertificates.length; i++) {
                X509Certificate x509Certificate = trustCertificates[i];
                ks.setCertificateEntry(trustCertificatesAliasPrefix+"_"+i, x509Certificate);
            }
        }
        return ks;
    }
    
    public static KeyStore toKeystore(final String authenticationCertificateAlias, final char[] password, final X509Certificate authenticationCertificate[], final PrivateKey authenticationKey) throws Exception {

        if(authenticationCertificateAlias != null && authenticationCertificate != null && authenticationKey != null) {          
            KeyStore ks = KeyStore.getInstance(JKS);
            ks.load(null, null);
            ks.setKeyEntry(authenticationCertificateAlias, authenticationKey, password, authenticationCertificate);
            return ks;
        } else {
            return null;
        }

    }
    
    public static char[] randomChars(int len) {
        final SecureRandom r = new SecureRandom();
        final char[] ret = new char[len];
        for(int i=0; i<len;i++) {
            ret[i] = (char)(r.nextInt(26) + 'a');
        }
        return ret;
    }

    private PemKeyReader() { }
}

