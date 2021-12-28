/*
 * Copyright 2018 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.dlsfls;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import org.elasticsearch.SpecialPermission;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Filtering {
    private static final ObjectMapper internalMapper = new ObjectMapper();

    public static Map<String, Object> byteArrayToMutableJsonMap(byte[] jsonBytes) throws IOException {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<Map<String, Object>>() {
                @Override
                public Map<String, Object> run() throws Exception {
                    return internalMapper.readValue(jsonBytes, new TypeReference<Map<String, Object>>() {
                    });
                }
            });
        } catch (final PrivilegedActionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    public static byte[] jsonMapToByteArray(Map<String, Object> jsonAsMap) throws IOException {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<byte[]>() {
                @Override
                public byte[] run() throws Exception {
                    return internalMapper.writeValueAsBytes(jsonAsMap);
                }
            });
        } catch (final PrivilegedActionException e) {
            if (e.getCause() instanceof JsonProcessingException) {
                throw (JsonProcessingException) e.getCause();
            } else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

}
