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

import org.opensearch.SpecialPermission;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.core.xml.io.UnmarshallingException;
import org.opensaml.saml.metadata.resolver.filter.FilterException;
import org.opensaml.saml.metadata.resolver.impl.AbstractBatchMetadataResolver;

import com.google.common.base.Charsets;

import net.shibboleth.utilities.java.support.component.ComponentInitializationException;
import net.shibboleth.utilities.java.support.resolver.ResolverException;
import net.shibboleth.utilities.java.support.xml.BasicParserPool;

public class StaticMetadataResolver extends AbstractBatchMetadataResolver {

    private final XMLObject metadata;

    public StaticMetadataResolver(String metadataXml) throws UnmarshallingException, ResolverException, FilterException {

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
