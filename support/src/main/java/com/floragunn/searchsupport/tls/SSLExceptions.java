/*
 * Copyright 2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchsupport.tls;

import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;

import javax.net.ssl.SSLHandshakeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SSLExceptions {
    private final static Logger log = LogManager.getLogger(SSLExceptions.class);

    public static String toHumanReadableError(SSLHandshakeException sslHandshakeException) {
        for (Throwable e = sslHandshakeException; e != null; e = (e != e.getCause()) ? e.getCause() : null) {
            if (e.getClass().getName().equals("sun.security.provider.certpath.SunCertPathBuilderException")) {
                String result = handleSunCertPathBuilderException(e);

                if (result != null) {
                    return result;
                }
            }
        }

        return sslHandshakeException.toString();
    }

    public static String handleSunCertPathBuilderException(Throwable sunCertPathBuilderException) {
        try {
            Object adjacencyList = call(sunCertPathBuilderException, "getAdjacencyList");
            
            System.out.println("**** " + adjacencyList);
            
            if (adjacencyList == null) {
                return null;
            }
            
            Iterator<?> buildStepIterator = (Iterator<?>) call(adjacencyList, "iterator");
            
            StringBuilder result = new StringBuilder("Could not verify certificate of remote peer of connection using the current CA certificates");
            
            while (buildStepIterator.hasNext()) {
                Object buildStep = buildStepIterator.next();
                
                result.append("\n").append(buildStep.toString());
            }

            return result.toString();
        } catch (Throwable t) {
            log.warn("Error while handling " + sunCertPathBuilderException, t);
            return null;
        }
    }

    private static Object call(Object object, String methodName) {
        return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            if (object == null) {
                return null;
            }

            try {
                Method method = object.getClass().getMethod(methodName);
                return method.invoke(object);
            } catch (Exception e) {
                throw new RuntimeException("Error while accessing " + methodName + " in " + object, e);
            }
        });
    }
}
