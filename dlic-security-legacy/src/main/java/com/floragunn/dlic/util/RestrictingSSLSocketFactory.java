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

package com.floragunn.dlic.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;


public class RestrictingSSLSocketFactory extends SSLSocketFactory {

    private final SSLSocketFactory delegate;
    private final String[] enabledProtocols;
    private final String[] enabledCipherSuites;

    public RestrictingSSLSocketFactory(final SSLSocketFactory delegate, final String[] enabledProtocols, final String[] enabledCipherSuites) {
        this.delegate = delegate;
        this.enabledProtocols = enabledProtocols;
        this.enabledCipherSuites = enabledCipherSuites;
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return enabledCipherSuites == null ? delegate.getDefaultCipherSuites() : enabledCipherSuites;
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return enabledCipherSuites == null ? delegate.getSupportedCipherSuites() : enabledCipherSuites;
    }
    
    @Override
    public Socket createSocket() throws IOException {
        return enforce(delegate.createSocket());
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
        return enforce(delegate.createSocket(s, host, port, autoClose));
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
        return enforce(delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException, UnknownHostException {
        return enforce(delegate.createSocket(host, port, localHost, localPort));
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        return enforce(delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
        return enforce(delegate.createSocket(address, port, localAddress, localPort));
    }

    private Socket enforce(Socket socket) {
        if(socket != null && (socket instanceof SSLSocket)) {
            
            if(enabledProtocols != null)
            ((SSLSocket)socket).setEnabledProtocols(enabledProtocols);
            
            if(enabledCipherSuites != null)
            ((SSLSocket)socket).setEnabledCipherSuites(enabledCipherSuites);
        }
        return socket;
    }
}