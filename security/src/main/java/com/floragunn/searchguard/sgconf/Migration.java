package com.floragunn.searchguard.sgconf;

import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;

import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.Meta;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v6.ActionGroupsV6;
import com.floragunn.searchguard.sgconf.impl.v6.ConfigV6;
import com.floragunn.searchguard.sgconf.impl.v6.InternalUserV6;
import com.floragunn.searchguard.sgconf.impl.v6.RoleMappingsV6;
import com.floragunn.searchguard.sgconf.impl.v6.RoleV6;
import com.floragunn.searchguard.sgconf.impl.v7.ActionGroupsV7;
import com.floragunn.searchguard.sgconf.impl.v7.ConfigV7;
import com.floragunn.searchguard.sgconf.impl.v7.InternalUserV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleMappingsV7;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;
import com.floragunn.searchguard.sgconf.impl.v7.TenantV7;

public class Migration {
    
    public static Tuple<SgDynamicConfiguration<RoleV7>,SgDynamicConfiguration<TenantV7>>  migrateRoles(SgDynamicConfiguration<RoleV6> r6cs, SgDynamicConfiguration<RoleMappingsV6> rms6) throws MigrationException {
        
        final SgDynamicConfiguration<RoleV7> r7 = SgDynamicConfiguration.empty();
        r7.setCType(r6cs.getCType());
        r7.set_sg_meta(new Meta());
        r7.get_sg_meta().setConfig_version(2);
        r7.get_sg_meta().setType("roles");
        
        final SgDynamicConfiguration<TenantV7> t7 = SgDynamicConfiguration.empty();
        t7.setCType(CType.TENANTS);
        t7.set_sg_meta(new Meta());
        t7.get_sg_meta().setConfig_version(2);
        t7.get_sg_meta().setType("tenants");

        Set<String> dedupTenants = new HashSet<>();
        
        for(final Entry<String, RoleV6> r6e: r6cs.getCEntries().entrySet()) {
            final String roleName  = r6e.getKey();
            final RoleV6 r6 = r6e.getValue();
            
            if(r6 == null) {
                RoleV7 noPermRole = new RoleV7();
                noPermRole.setDescription("Migrated from v6, was empty");
                r7.putCEntry(roleName, noPermRole);
                continue;
            }

            r7.putCEntry(roleName, new RoleV7(r6));
            
            for(Entry<String, String> tenant: r6.getTenants().entrySet()) {
                dedupTenants.add(tenant.getKey());
            }
        }
        
        if(rms6 != null) {
            for(final Entry<String, RoleMappingsV6> r6m: rms6.getCEntries().entrySet()) {
                final String roleName  = r6m.getKey();
                //final RoleMappingsV6 r6 = r6m.getValue();
                
                if(!r7.exists(roleName)) {
                    //rolemapping but role does not exists
                    RoleV7 noPermRole = new RoleV7();
                    noPermRole.setDescription("Migrated from v6, was in rolemappings but no role existed");
                    r7.putCEntry(roleName, noPermRole);
                }
                
            }
        }
        
        for(String tenantName: dedupTenants) {
            TenantV7 entry = new TenantV7();
            entry.setDescription("Migrated from v6");
            t7.putCEntry(tenantName, entry);
        }
        
        return new Tuple<SgDynamicConfiguration<RoleV7>, SgDynamicConfiguration<TenantV7>>(r7, t7);
        
    }
    
    public static SgDynamicConfiguration<ConfigV7> migrateConfig(SgDynamicConfiguration<ConfigV6> r6cs) throws MigrationException {
        final SgDynamicConfiguration<ConfigV7> c7 = SgDynamicConfiguration.empty();
        c7.setCType(r6cs.getCType());
        c7.set_sg_meta(new Meta());
        c7.get_sg_meta().setConfig_version(2);
        c7.get_sg_meta().setType("config");
        
        if(r6cs.getCEntries().size() != 1) {
            throw new MigrationException("Unable to migrate config because expected size was 1 but actual size is "+r6cs.getCEntries().size());
        }
        
        if(r6cs.getCEntries().get("searchguard") == null) {
            throw new MigrationException("Unable to migrate config because 'searchguard' key not found");
        }
        
        for(final Entry<String, ConfigV6> r6c: r6cs.getCEntries().entrySet()) {
            c7.putCEntry("sg_config", new ConfigV7(r6c.getValue()));
        }
        return c7;
    }
    
    public static SgDynamicConfiguration<InternalUserV7>  migrateInternalUsers(SgDynamicConfiguration<InternalUserV6> r6is) throws MigrationException {
        final SgDynamicConfiguration<InternalUserV7> i7 = SgDynamicConfiguration.empty();
        i7.setCType(r6is.getCType());
        i7.set_sg_meta(new Meta());
        i7.get_sg_meta().setConfig_version(2);
        i7.get_sg_meta().setType("internalusers");
        
        for(final Entry<String, InternalUserV6> r6i: r6is.getCEntries().entrySet()) {
            final  String username = !Strings.isNullOrEmpty(r6i.getValue().getUsername())?r6i.getValue().getUsername():r6i.getKey();
            i7.putCEntry(username, new InternalUserV7(r6i.getValue()));
        }
        
        return i7;
    }
    
    public static SgDynamicConfiguration<ActionGroupsV7>  migrateActionGroups(SgDynamicConfiguration<?> r6as) throws MigrationException {
        
        final SgDynamicConfiguration<ActionGroupsV7> a7 = SgDynamicConfiguration.empty();
        a7.setCType(r6as.getCType());
        a7.set_sg_meta(new Meta());
        a7.get_sg_meta().setConfig_version(2);
        a7.get_sg_meta().setType("actiongroups");
        
        if(r6as.getImplementingClass().isAssignableFrom(List.class)) {
            for(final Entry<String, ?> r6a: r6as.getCEntries().entrySet()) {
                a7.putCEntry(r6a.getKey(), new ActionGroupsV7(r6a.getKey(), (List<String>) r6a.getValue()));
            }
        } else {
            for(final Entry<String, ?> r6a: r6as.getCEntries().entrySet()) {
                a7.putCEntry(r6a.getKey(), new ActionGroupsV7(r6a.getKey(), (ActionGroupsV6)r6a.getValue()));
            }
        }

        return a7;
    }
    
    public static SgDynamicConfiguration<RoleMappingsV7>  migrateRoleMappings(SgDynamicConfiguration<RoleMappingsV6> r6rms) throws MigrationException {
        final SgDynamicConfiguration<RoleMappingsV7> rms7 = SgDynamicConfiguration.empty();
        rms7.setCType(r6rms.getCType());
        rms7.set_sg_meta(new Meta());
        rms7.get_sg_meta().setConfig_version(2);
        rms7.get_sg_meta().setType("rolesmapping");
        
        for(final Entry<String, RoleMappingsV6> r6m: r6rms.getCEntries().entrySet()) {
            rms7.putCEntry(r6m.getKey(), new RoleMappingsV7(r6m.getValue()));
        }
        
        return rms7;
    }

}
