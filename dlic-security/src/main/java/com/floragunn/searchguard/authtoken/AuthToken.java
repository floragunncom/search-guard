package com.floragunn.searchguard.authtoken;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.sgconf.history.ConfigVersionSet;
import com.floragunn.searchguard.user.User;

public class AuthToken implements ToXContentObject {
    private final String name;
    private final String id;
    private final User user;
    private final ConfigVersionSet configVersions;
    private final RequestedPrivileges requestedPrivilges;
   // private final SgRoles sgRoles;

    AuthToken(String id, String name, User user, RequestedPrivileges requestedPrivilges, ConfigVersionSet configVersions) {
        this.id = id;
        this.name = name;
        this.user = user;
        this.requestedPrivilges = requestedPrivilges;
        this.configVersions = configVersions;
    }

    public User getUser() {
        return user;
    }

    public SgRoles getSgRoles() {
        return sgRoles;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("user", user.getName());
        builder.field("requested", requestedPrivilges);
        builder.field("base", (ToXContent) configVersions);
        
        builder.endObject();
        return builder;
    }

    public String getId() {
        return id;
    }

    public ConfigVersionSet getConfigVersions() {
        return configVersions;
    }

    public RequestedPrivileges getRequestedPrivilges() {
        return requestedPrivilges;
    }

}
