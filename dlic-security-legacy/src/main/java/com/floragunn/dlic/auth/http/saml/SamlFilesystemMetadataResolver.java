/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
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

package com.floragunn.dlic.auth.http.saml;

import java.io.File;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import net.shibboleth.shared.component.ComponentInitializationException;
import net.shibboleth.shared.resolver.ResolverException;
import net.shibboleth.shared.xml.impl.BasicParserPool;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.opensaml.saml.metadata.resolver.impl.FilesystemMetadataResolver;


public class SamlFilesystemMetadataResolver extends FilesystemMetadataResolver {
    private static int componentIdCounter = 0;

    public SamlFilesystemMetadataResolver(Settings esSettings, Path configPath) throws ResolverException {
        super(getMetadataFile(esSettings, configPath));
        setId(SamlFilesystemMetadataResolver.class.getName() + "_" + (++componentIdCounter));
        setRequireValidMetadata(true);
        BasicParserPool basicParserPool = new BasicParserPool();
        try {
            basicParserPool.initialize();
        } catch (ComponentInitializationException e) {
            throw new RuntimeException(e);
        }
        setParserPool(basicParserPool);
    }

    @Override
    protected byte[] fetchMetadata() throws ResolverException {
        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<byte[]>() {
                @Override
                public byte[] run() throws ResolverException {
                    return SamlFilesystemMetadataResolver.super.fetchMetadata();
                }
            });
        } catch (PrivilegedActionException e) {

            if (e.getCause() instanceof ResolverException) {
                throw (ResolverException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private static File getMetadataFile(Settings settings, Path configPath) {

        String originalPath = settings.get("idp.metadata_file", null);
        
        if (originalPath == null || originalPath.length() == 0) {
            return null;
        }
        
        Environment env = new Environment(settings, configPath);

        return env.configFile().resolve(originalPath).toAbsolutePath().toFile();
    }
}
