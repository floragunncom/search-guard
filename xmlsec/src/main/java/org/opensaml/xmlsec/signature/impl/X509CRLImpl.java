/*
 * Based on https://git.shibboleth.net/view/?p=java-opensaml.git;a=blob;f=opensaml-xmlsec-impl/src/main/java/org/opensaml/xmlsec/signature/impl/X509CRLImpl.java;h=0a1442d9972b53eeba9c5e722193c332abde302a;hb=HEAD
 *
 * Licensed to the University Corporation for Advanced Internet Development,
 * Inc. (UCAID) under one or more contributor license agreements.  See the
 * NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The UCAID licenses this file to You under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opensaml.xmlsec.signature.impl;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.opensaml.core.xml.AbstractXMLObject;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.xmlsec.signature.X509CRL;

/** Concrete implementation of {@link X509CRL}. */
public class X509CRLImpl extends AbstractXMLObject implements X509CRL {

    private String elementValue;

    /**
     * Constructor.
     * 
     * @param namespaceURI the namespace the element is in
     * @param elementLocalName the local name of the XML element this Object represents
     * @param namespacePrefix the prefix for the given namespace
     */
    protected X509CRLImpl(final String namespaceURI, final String elementLocalName, final String namespacePrefix) {
        super(namespaceURI, elementLocalName, namespacePrefix);
    }

    /** {@inheritDoc} */
    public String getValue() {
        return elementValue;
    }

    /** {@inheritDoc} */
    public void setValue(final String newValue) {
        // Dump our cached DOM if the new value really is new
        final String currentCRL = elementValue;
        final String newCRL = prepareForAssignment(currentCRL, newValue);

        // This is a new value, remove the old one, add the new one
        if (!Objects.equals(currentCRL, newCRL)) {

            elementValue = newCRL.intern();
        }
    }

    /** {@inheritDoc} */
    @Override
    public List<XMLObject> getOrderedChildren() {
        return Collections.emptyList();
    }

}