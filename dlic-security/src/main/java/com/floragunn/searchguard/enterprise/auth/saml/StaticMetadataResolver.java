/*
 * Copyright 2021 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.auth.saml;

import java.io.ByteArrayInputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import net.shibboleth.shared.component.ComponentInitializationException;
import net.shibboleth.shared.resolver.ResolverException;
import net.shibboleth.shared.xml.impl.BasicParserPool;
import org.elasticsearch.SpecialPermission;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.core.xml.io.UnmarshallingException;
import org.opensaml.saml.metadata.resolver.filter.FilterException;
import org.opensaml.saml.metadata.resolver.impl.AbstractBatchMetadataResolver;

import com.floragunn.codova.validation.ConfigValidationException;
import com.google.common.base.Charsets;



public class StaticMetadataResolver extends AbstractBatchMetadataResolver {

    private final XMLObject metadata;

    public StaticMetadataResolver(String metadataXml) throws ConfigValidationException {
        try {
            BasicParserPool basicParserPool = new BasicParserPool();
            try {
                basicParserPool.initialize();
            } catch (ComponentInitializationException e) {
                throw new RuntimeException(e);
            }
            setParserPool(basicParserPool);
            setId("static_metadata");

            this.metadata = unmarshallMetadata(new ByteArrayInputStream(metadataXml.getBytes(Charsets.UTF_8)));

            if (!isValid(metadata)) {
                throw new ResolverException("SAML metadata has expired");
            }
        } catch (ResolverException e) {
            throw new ConfigValidationException(new com.floragunn.codova.validation.errors.ValidationError(null, e.getMessage()).cause(e));
        } catch (UnmarshallingException e) {
            throw new ConfigValidationException(new com.floragunn.codova.validation.errors.ValidationError(null, "Not a valid XML structure").cause(e));
        } 
    }

    public void initializePrivileged() throws ComponentInitializationException {
        SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws ComponentInitializationException {
                    initialize();
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof ComponentInitializationException) {
                throw (ComponentInitializationException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    @Override
    protected void initMetadataResolver() throws ComponentInitializationException {
        super.initMetadataResolver();

        try {
            BatchEntityBackingStore backingStore = preProcessNewMetadata(metadata);
            setBackingStore(backingStore);

        } catch (FilterException e) {
            throw new ComponentInitializationException(e);
        }

    }

}
