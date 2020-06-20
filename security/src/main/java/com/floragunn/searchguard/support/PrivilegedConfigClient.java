package com.floragunn.searchguard.support;

import org.elasticsearch.client.Client;

import com.floragunn.searchguard.internalauthtoken.InternalAuthTokenProvider;
import com.floragunn.searchsupport.client.ContextHeaderDecoratorClient;

public class PrivilegedConfigClient extends ContextHeaderDecoratorClient {

    public PrivilegedConfigClient(Client in) {
        super(in, ConfigConstants.SG_CONF_REQUEST_HEADER, "true", InternalAuthTokenProvider.TOKEN_HEADER, null,
                InternalAuthTokenProvider.AUDIENCE_HEADER, null);
    }

    public static PrivilegedConfigClient adapt(Client client) {
        if (client instanceof PrivilegedConfigClient) {
            return (PrivilegedConfigClient) client;
        } else {
            return new PrivilegedConfigClient(client);
        }
    }
}