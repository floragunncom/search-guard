package com.floragunn.searchguard.sgconf.impl.v7;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.floragunn.searchguard.DefaultObjectMapper;
import com.floragunn.searchguard.auth.internal.InternalAuthenticationBackend;

public class ConfigV7 {

    public Dynamic dynamic;

    public ConfigV7() {
        super();
    }

    @Override
    public String toString() {
        return "Config [dynamic=" + dynamic + "]";
    }

    public static class Dynamic {

        public String filtered_alias_mode = "warn";
        public boolean disable_rest_auth;
        public boolean disable_intertransport_auth;
        public boolean respect_request_indices_options;
        public String license;
        public Kibana kibana = new Kibana();
        public Http http = new Http();
        public Authc authc = new Authc();
        public Authz authz = new Authz();
        public AuthFailureListeners auth_failure_listeners = new AuthFailureListeners();
        public boolean do_not_fail_on_forbidden = true;
        public boolean multi_rolespan_enabled = true;
        public String hosts_resolver_mode = "ip-only";
        public String transport_userrname_attribute;
        public boolean do_not_fail_on_forbidden_empty;
        public String field_anonymization_salt2;
        public HashMap<String, Object> auth_token_provider = new HashMap<>();
        public HashMap<String, Object> sessions = new HashMap<>();
        @JsonInclude(Include.NON_NULL)
        public Boolean debug;

        @Override
        public String toString() {
            return "Dynamic [filtered_alias_mode=" + filtered_alias_mode + ", kibana=" + kibana + ", http=" + http + ", authc=" + authc + ", authz="
                    + authz + ", salt2= " + field_anonymization_salt2 + "]";
        }
    }

    public static class Kibana {

        public boolean multitenancy_enabled = true;
        public String server_username = "kibanaserver";
        public String index = ".kibana";
        public boolean rbac_enabled;
        @Override
        public String toString() {
            return "Kibana [multitenancy_enabled=" + multitenancy_enabled + ", server_username=" + server_username + ", index=" + index
                    + ", rbac_enabled=" + rbac_enabled + "]";
        }
        
        
        
        
        
    }
    
    public static class Http {
        public boolean anonymous_auth_enabled = false;
        public Xff xff = new Xff();
        @Override
        public String toString() {
            return "Http [anonymous_auth_enabled=" + anonymous_auth_enabled + ", xff=" + xff + "]";
        }
        
        
    }
   
    public static class AuthFailureListeners {
        @JsonIgnore
        private final Map<String, AuthFailureListener> listeners = new HashMap<>();

        @JsonAnySetter
        void setListeners(String key, AuthFailureListener value) {
            listeners.put(key, value);
        }

        @JsonAnyGetter
        public Map<String, AuthFailureListener> getListeners() {
            return listeners;
        }

        
    }
    
    public static class AuthFailureListener {
        public String type;
        public String authentication_backend;
        public int allowed_tries = 10;
        public int time_window_seconds = 60 * 60;
        public int block_expiry_seconds = 60 * 10;
        public int max_blocked_clients = 100_000;
        public int max_tracked_clients = 100_000;
        
        
        
        public AuthFailureListener() {
            super();
        }

             @JsonIgnore
        public String asJson() {
            try {
                return DefaultObjectMapper.writeValueAsString(this, false);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public static class Xff {
        public boolean enabled = false;
        public String internalProxies = Pattern.compile(
                "10\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}|" +
                        "192\\.168\\.\\d{1,3}\\.\\d{1,3}|" +
                        "169\\.254\\.\\d{1,3}\\.\\d{1,3}|" +
                        "127\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}|" +
                        "172\\.1[6-9]{1}\\.\\d{1,3}\\.\\d{1,3}|" +
                        "172\\.2[0-9]{1}\\.\\d{1,3}\\.\\d{1,3}|" +
                        "172\\.3[0-1]{1}\\.\\d{1,3}\\.\\d{1,3}").toString();
        public String remoteIpHeader="X-Forwarded-For";
        @Override
        public String toString() {
            return "Xff [enabled=" + enabled + ", internalProxies=" + internalProxies + ", remoteIpHeader=" + remoteIpHeader+"]";
        }
        
        
    }
    
    public static class Authc {
        
        @JsonIgnore
        private final Map<String, AuthcDomain> domains = new HashMap<>();

        @JsonAnySetter
        void setDomains(String key, AuthcDomain value) {
            domains.put(key, value);
        }

        @JsonAnyGetter
        public Map<String, AuthcDomain> getDomains() {
            return domains;
        }

        @Override
        public String toString() {
            return "Authc [domains=" + domains + "]";
        }
        
        
        
    }
    
    public static class AuthcDomain {

        public boolean http_enabled= true;
        public boolean transport_enabled= true;
        @JsonInclude(Include.NON_NULL)
        public Boolean session_enabled = null;
        //public boolean enabled= true;
        public int order = 0;
        public HttpAuthenticator http_authenticator = new HttpAuthenticator();
        public AuthcBackend authentication_backend = new AuthcBackend();
        public String description;
        public List<String> skip_users = new ArrayList<>();
        public List<String> enabled_only_for_ips;

        public AuthcDomain() {
            super();
        }

        @Override
        public String toString() {
            return "AuthcDomain [http_enabled=" + http_enabled + ", transport_enabled=" + transport_enabled + ", order=" + order
                    + ", http_authenticator=" + http_authenticator + ", authentication_backend=" + authentication_backend + ", description="
                    + description +  ", skip_users=" + skip_users + "]";
        }
        
        
    }

    public static class HttpAuthenticator {
        public boolean challenge = true;
        public String type;
        public Map<String, Object> config = Collections.emptyMap();
        
        public HttpAuthenticator() {
            super();
        }
        
        @JsonIgnore
        public String configAsJson() {
            try {
                return DefaultObjectMapper.writeValueAsString(config, false);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }


        @Override
        public String toString() {
            return "HttpAuthenticator [challenge=" + challenge + ", type=" + type + ", config=" + config + "]";
        }
        
        
    }
    
    public static class AuthzBackend {
        public String type = "noop";
        public Map<String, Object> config = Collections.emptyMap();
        
        
        
        public AuthzBackend() {
            super();
        }

        @JsonIgnore
        public String configAsJson() {
            try {
                return DefaultObjectMapper.writeValueAsString(config, false);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }



        @Override
        public String toString() {
            return "AuthzBackend [type=" + type + ", config=" + config + "]";
        }
        
        
    }
    
    public static class AuthcBackend {
        public String type = InternalAuthenticationBackend.class.getName();
        public Map<String, Object> config = Collections.emptyMap();
        
        public AuthcBackend() {
            super();
        }

        @JsonIgnore
        public String configAsJson() {
            try {
                return DefaultObjectMapper.writeValueAsString(config, false);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }



        @Override
        public String toString() {
            return "AuthcBackend [type=" + type + ", config=" + config + "]";
        }
        
        
    }
    
    public static class Authz {
        @JsonIgnore
        private final Map<String, AuthzDomain> domains = new HashMap<>();

        @JsonAnySetter
        void setDomains(String key, AuthzDomain value) {
            domains.put(key, value);
        }

        @JsonAnyGetter
        public Map<String, AuthzDomain> getDomains() {
            return domains;
        }

        @Override
        public String toString() {
            return "Authz [domains=" + domains + "]";
        }
        
        
    }
    
    public static class AuthzDomain {
        public boolean http_enabled = true;
        public boolean transport_enabled = true;
        public AuthzBackend authorization_backend = new AuthzBackend();
        public String description;
        public List<String> skipped_users = new ArrayList<>();

        public AuthzDomain() {
            super();
        }
        
        @Override
        public String toString() {
            return "AuthzDomain [http_enabled=" + http_enabled + ", transport_enabled=" + transport_enabled
                    + ", authorization_backend=" + authorization_backend + ", description=" + description
                    + ", skipped_users=" + skipped_users + "]";
        }
        
        
    }
   
}