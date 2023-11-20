package com.floragunn.searchguard.authz.int_tests;

import static com.floragunn.searchguard.test.IndexApiMatchers.containsExactly;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedTo;
import static com.floragunn.searchguard.test.IndexApiMatchers.limitedToNone;
import static com.floragunn.searchguard.test.IndexApiMatchers.unlimited;
import static com.floragunn.searchguard.test.IndexApiMatchers.unlimitedIncludingSearchGuardIndices;
import static com.floragunn.searchguard.test.RestMatchers.isBadRequest;
import static com.floragunn.searchguard.test.RestMatchers.isCreated;
import static com.floragunn.searchguard.test.RestMatchers.isForbidden;
import static com.floragunn.searchguard.test.RestMatchers.isOk;
import static com.floragunn.searchguard.test.RestMatchers.json;
import static com.floragunn.searchguard.test.RestMatchers.nodeAt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestAlias;
import com.floragunn.searchguard.test.TestComponentTemplate;
import com.floragunn.searchguard.test.TestDataStream;
import com.floragunn.searchguard.test.TestIndex;
import com.floragunn.searchguard.test.TestIndexTemplate;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.test.TestSgConfig.Role;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;

import javax.annotation.concurrent.NotThreadSafe;

@RunWith(Parameterized.class)
@NotThreadSafe
public class DataStreamAuthorizationReadWriteIntTests {
    static TestDataStream ds_ar1 = TestDataStream.name("ds_ar1").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_ar2 = TestDataStream.name("ds_ar2").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_aw1 = TestDataStream.name("ds_aw1").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_aw2 = TestDataStream.name("ds_aw2").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_br1 = TestDataStream.name("ds_br1").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_br2 = TestDataStream.name("ds_br2").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_bw1 = TestDataStream.name("ds_bw1").documentCount(22).rolloverAfter(10).build();
    static TestDataStream ds_bw2 = TestDataStream.name("ds_bw2").documentCount(22).rolloverAfter(10).build();
    static TestIndex index_cr1 = TestIndex.name("index_cr1").documentCount(10).build();
    static TestIndex index_cw1 = TestIndex.name("index_cw1").documentCount(10).build();
    static TestDataStream ds_hidden = TestDataStream.name("ds_hidden").documentCount(10).rolloverAfter(3).seed(8).attr("prefix", "h").build();

    static TestAlias alias_ab1r = new TestAlias("alias_ab1r", ds_ar1, ds_ar2, ds_aw1, ds_aw2, ds_br1, ds_bw1);
    static TestAlias alias_ab1w = new TestAlias("alias_ab1w", ds_aw1, ds_aw2, ds_bw1).writeIndex(ds_aw1);
    static TestAlias alias_ab1w_nowriteindex = new TestAlias("alias_ab1w_nowriteindex", ds_aw1, ds_aw2, ds_bw1);

    static TestAlias alias_c1 = new TestAlias("alias_c1", index_cr1);

    static TestIndex ds_bwx1 = TestIndex.name("ds_bwx1").documentCount(0).build(); // not initially created
    static TestIndex ds_bwx2 = TestIndex.name("ds_bwx2").documentCount(0).build(); // not initially created

    static TestAlias alias_bwx = new TestAlias("alias_bwx"); // not initially created

    static TestSgConfig.User LIMITED_USER_A = new TestSgConfig.User("limited_user_A")//
            .description("ds_a*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_a*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_aw*"))//
            .indexMatcher("read", limitedTo(ds_ar1, ds_ar2, ds_aw1, ds_aw2))//
            .indexMatcher("write", limitedTo(ds_aw1, ds_aw2))//
            .indexMatcher("create_data_stream", limitedToNone())//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B = new TestSgConfig.User("limited_user_B")//
            .description("ds_b*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_b*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_bw*"))//
            .indexMatcher("read", limitedTo(ds_br1, ds_br2, ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("write", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("create_data_stream", limitedToNone())//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B_READ_ONLY_A = new TestSgConfig.User("limited_user_B_read_only_A")//
            .description("ds_b*; read only on ds_a*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_a*", "ds_b*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_bw*"))//
            .indexMatcher("read", limitedTo(ds_ar1, ds_ar2, ds_aw1, ds_aw2, ds_br1, ds_br2, ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("write", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("create_data_stream", limitedToNone())//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    /**
     * This is an artificial user - in the sense that in real life it would likely not exist this way. 
     * It has privileges to write on ds_b*, but privileges for indices:admin/mapping/auto_put on all data streams.
     * The reason is that some indexing operations are two phase - first auto put, then indexing. To be able to test both
     * phases, we need which user which always allows the first phase to pass.
     */
    static TestSgConfig.User LIMITED_USER_B_AUTO_PUT_ON_ALL = new TestSgConfig.User("limited_user_B_auto_put_on_all")//
            .description("ds_b* with full auto put")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_b*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_bw*")//
                            .dataStreamPermissions("indices:admin/mapping/auto_put").on("*"))//
            .indexMatcher("read", limitedTo(ds_br1, ds_br2, ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("write", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("create_data_stream", limitedToNone())//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B_CREATE_DS = new TestSgConfig.User("limited_user_B_create_ds")//
            .description("ds_b* with create ds privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_b*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_bw*")//
                            .dataStreamPermissions("SGS_CREATE_DATA_STREAM").on("ds_bw*"))//
            .indexMatcher("read", limitedTo(ds_br1, ds_br2, ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("write", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("create_data_stream", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B_MANAGE_DS = new TestSgConfig.User("limited_user_B_manage_ds")//
            .description("ds_b* with manage privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_b*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_bw*")//
                            .dataStreamPermissions("SGS_MANAGE").on("ds_bw*"))//
            .indexMatcher("read", limitedTo(ds_br1, ds_br2, ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("write", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("create_data_stream", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("manage_data_stream", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("manage_alias", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("get_alias", limitedTo());

    static TestSgConfig.User LIMITED_USER_B_MANAGE_INDEX_ALIAS = new TestSgConfig.User("limited_user_B_manage_index_alias")//
            .description("ds_b*, alias_bwx* with manage privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_b*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_bw*")//
                            .dataStreamPermissions("SGS_MANAGE").on("ds_bw*")//
                            .aliasPermissions("SGS_MANAGE_ALIASES").on("alias_bwx*"))//
            .indexMatcher("read", limitedTo(ds_br1, ds_br2, ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("write", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("create_data_stream", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("manage_data_stream", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2, alias_bwx))//
            .indexMatcher("manage_alias", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2, alias_bwx))//
            .indexMatcher("get_alias", limitedTo(alias_bwx));

    static TestSgConfig.User LIMITED_USER_B_CREATE_INDEX_MANAGE_ALIAS = new TestSgConfig.User("limited_user_B_create_index")//
            .description("ds_b* with create ds privs and manage alias privs, alias_bwx* with manage alias privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_b*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_bw*")//
                            .dataStreamPermissions("SGS_CREATE_DATA_STREAM", "SGS_MANAGE_ALIASES").on("ds_bw*")//
                            .aliasPermissions("SGS_MANAGE_ALIASES").on("alias_bwx*"))//
            .indexMatcher("read", limitedTo(ds_br1, ds_br2, ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("write", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("create_data_stream", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2, alias_bwx))//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_B_HIDDEN_MANAGE_INDEX_ALIAS = new TestSgConfig.User("limited_user_B_HIDDEN_anage_index_alias")//
            .description("ds_b*, ds_hidden*, alias_bwx* with manage privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_b*", "ds_hidden*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_bw*", "ds_hidden*")//
                            .dataStreamPermissions("SGS_MANAGE").on("ds_bw*", "ds_hidden*")//
                            .aliasPermissions("SGS_MANAGE_ALIASES").on("alias_bwx*"))//
            .indexMatcher("read", limitedTo(ds_br1, ds_br2, ds_bw1, ds_bw2, ds_bwx1, ds_bwx2, ds_hidden))//
            .indexMatcher("write", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2, ds_hidden))//
            .indexMatcher("create_data_stream", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2, ds_hidden))//
            .indexMatcher("manage_data_stream", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2, alias_bwx, ds_hidden))//
            .indexMatcher("manage_alias", limitedTo(ds_bw1, ds_bw2, ds_bwx1, ds_bwx2, alias_bwx, ds_hidden))//
            .indexMatcher("get_alias", limitedTo(alias_bwx));

    static TestSgConfig.User LIMITED_USER_AB_MANAGE_INDEX = new TestSgConfig.User("limited_user_AB_manage_index")//
            .description("ds_a*, ds_b* with manage index privs")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on("ds_a*", "ds_b*")//
                            .dataStreamPermissions("SGS_WRITE").on("ds_aw*", "ds_bw*")//
                            .dataStreamPermissions("SGS_MANAGE").on("ds_aw*", "ds_bw*"))//
            .indexMatcher("read", limitedTo(ds_ar1, ds_ar2, ds_aw1, ds_aw2, ds_br1, ds_br2, ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("write", limitedTo(ds_aw1, ds_aw2, ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("create_data_stream", limitedTo(ds_aw1, ds_aw2, ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("manage_data_stream", limitedTo(ds_aw1, ds_aw2, ds_bw1, ds_bw2, ds_bwx1, ds_bwx2))//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_C = new TestSgConfig.User("limited_user_C")//
            .description("index_c*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh").on("index_c*")//
                            .indexPermissions("SGS_WRITE").on("index_cw*"))//
            .indexMatcher("read", limitedTo(index_cr1, index_cw1))//
            .indexMatcher("write", limitedTo(index_cw1))//
            .indexMatcher("create_data_stream", limitedToNone())//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_AB1_ALIAS = new TestSgConfig.User("limited_user_alias_AB1")//
            .description("alias_ab1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .aliasPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/aliases/get").on("alias_ab1r")//
                            .aliasPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/aliases/get", "SGS_WRITE", "indices:admin/refresh*")
                            .on("alias_ab1w*"))//
            .indexMatcher("read", limitedTo(ds_ar1, ds_ar2, ds_aw1, ds_aw2, ds_br1, ds_bw1, alias_ab1r, alias_ab1w_nowriteindex))//
            .indexMatcher("write", limitedTo(ds_aw1, ds_aw2, ds_bw1, alias_ab1w_nowriteindex))//
            .indexMatcher("create_data_stream", limitedTo(ds_aw1, ds_aw2, ds_bw1))//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedTo(ds_ar1, ds_ar2, ds_aw1, ds_aw2, ds_br1, ds_bw1, alias_ab1r));

    static TestSgConfig.User LIMITED_USER_AB1_ALIAS_READ_ONLY = new TestSgConfig.User("limited_user_alias_AB1_read_only")//
            .description("read/only on alias_ab1w, but with write privs in write index ds_aw1")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_WRITE", "indices:admin/refresh").on("ds_aw1")//
                            .aliasPermissions("SGS_READ").on("alias_ab1w"))//
            .indexMatcher("read", limitedTo(ds_aw1, ds_aw2, ds_bw1))//
            .indexMatcher("write", limitedTo(ds_aw1)) // alias_ab1w is included because ds_aw1 is the write index of alias_ab1w
            .indexMatcher("create_data_stream", limitedToNone())//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone());

    static TestSgConfig.User LIMITED_READ_ONLY_ALL = new TestSgConfig.User("limited_read_only_all")//
            .description("read/only on *")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ").on("*"))//
            .indexMatcher("read", unlimited())//
            .indexMatcher("write", limitedToNone())//
            .indexMatcher("create_data_stream", limitedToNone())//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_READ_ONLY_A = new TestSgConfig.User("limited_read_only_A")//
            .description("read/only on ds_a*")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_READ").on("ds_a*"))//
            .indexMatcher("read", limitedTo(ds_ar1, ds_ar2, ds_aw1, ds_aw2))//
            .indexMatcher("write", limitedToNone())//
            .indexMatcher("create_data_stream", limitedToNone())//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User LIMITED_USER_NONE = new TestSgConfig.User("limited_user_none")//
            .description("no privileges for existing indices")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .dataStreamPermissions("SGS_CRUD", "SGS_INDICES_MONITOR").on("ds_does_not_exist_*"))//
            .indexMatcher("read", limitedToNone())//
            .indexMatcher("write", limitedToNone())//
            .indexMatcher("create_data_stream", limitedToNone())//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User INVALID_USER_INDEX_PERMISSIONS_FOR_DATA_STREAM = new TestSgConfig.User("invalid_user_index_permissions_for_data_stream")//
            .description("invalid: index permissions for data stream")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_CRUD", "SGS_INDICES_MONITOR", "SGS_CREATE_DATA_STREAM").on("ds_*"))//
            .indexMatcher("read", limitedToNone())//
            .indexMatcher("write", limitedToNone())//
            .indexMatcher("create_data_stream", limitedToNone())//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    /**
     * Even if we give WRITE privileges on backing indices, bulk insert operations will not be supported, as DNFOF functionality is not available for the bulk operation.
     * However, it would be necessary, as we get into the OK_WHEN_RESOLVED status. Delete by query will be supported, though.
     */
    static TestSgConfig.User LIMITED_USER_PERMISSIONS_ON_BACKING_INDICES = new TestSgConfig.User("limited_user_permissions_on_backing_indices")//
            .description("ds_a* on backing indices")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("SGS_READ", "SGS_INDICES_MONITOR", "indices:admin/refresh*").on(".ds-ds_a*")//
                            .indexPermissions("SGS_WRITE").on(".ds-ds_aw*"))//
            .indexMatcher("read", limitedTo(ds_ar1, ds_ar2, ds_aw1, ds_aw2))//
            .indexMatcher("write", limitedTo(ds_aw1, ds_aw2))//
            .indexMatcher("create_data_stream", limitedToNone())//
            .indexMatcher("manage_data_stream", limitedToNone())//
            .indexMatcher("manage_alias", limitedToNone())//
            .indexMatcher("get_alias", limitedToNone());

    static TestSgConfig.User UNLIMITED_USER = new TestSgConfig.User("unlimited_user")//
            .description("unlimited")//
            .roles(//
                    new Role("r1")//
                            .clusterPermissions("SGS_CLUSTER_COMPOSITE_OPS", "SGS_CLUSTER_MONITOR")//
                            .indexPermissions("*").on("*")//
                            .aliasPermissions("*").on("*")//
                            .dataStreamPermissions("*").on("*")

            )//
            .indexMatcher("read", unlimited())//
            .indexMatcher("write", unlimited())//
            .indexMatcher("create_data_stream", unlimited())//
            .indexMatcher("manage_data_stream", unlimited())//
            .indexMatcher("manage_alias", unlimited())//
            .indexMatcher("get_alias", unlimited());

    /**
     * The SUPER_UNLIMITED_USER authenticates with an admin cert, which will cause all access control code to be skipped.
     * This serves as a base for comparison with the default behavior.
     */
    static TestSgConfig.User SUPER_UNLIMITED_USER = new TestSgConfig.User("super_unlimited_user")//
            .description("super unlimited (admin cert)")//
            .adminCertUser()//
            .indexMatcher("read", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("write", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("create_data_stream", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("manage_data_stream", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("manage_alias", unlimitedIncludingSearchGuardIndices())//
            .indexMatcher("get_alias", unlimitedIncludingSearchGuardIndices());

    static List<TestSgConfig.User> USERS = ImmutableList.of(LIMITED_USER_A, LIMITED_USER_B, LIMITED_USER_B_READ_ONLY_A,
            LIMITED_USER_B_AUTO_PUT_ON_ALL, LIMITED_USER_B_CREATE_DS, LIMITED_USER_B_MANAGE_DS, LIMITED_USER_B_MANAGE_INDEX_ALIAS,
            LIMITED_USER_B_HIDDEN_MANAGE_INDEX_ALIAS, LIMITED_USER_AB_MANAGE_INDEX, LIMITED_USER_C, LIMITED_USER_AB1_ALIAS,
            LIMITED_USER_AB1_ALIAS_READ_ONLY, LIMITED_READ_ONLY_ALL, LIMITED_READ_ONLY_A, LIMITED_USER_NONE,
            INVALID_USER_INDEX_PERMISSIONS_FOR_DATA_STREAM, LIMITED_USER_PERMISSIONS_ON_BACKING_INDICES, UNLIMITED_USER, SUPER_UNLIMITED_USER);

    @ClassRule
    public static LocalCluster cluster = new LocalCluster.Builder().singleNode().sslEnabled().users(USERS)//
            .indexTemplates(new TestIndexTemplate("ds_test", "ds_*").dataStream().composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
            .indexTemplates(new TestIndexTemplate("ds_hidden", "ds_hidden*").priority(10).dataStream("hidden", true)
                    .composedOf(TestComponentTemplate.DATA_STREAM_MINIMAL))//
            .indices(index_cr1, index_cw1)//
            .aliases(alias_ab1w, alias_ab1r, alias_ab1w_nowriteindex, alias_c1)//
            .dataStreams(ds_ar1, ds_ar2, ds_aw1, ds_aw2, ds_br1, ds_br2, ds_bw1, ds_bw2, ds_hidden)//
            .authzDebug(true)//
            .useExternalProcessCluster().build();

    final TestSgConfig.User user;

    @Test
    public void createDocument() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.postJson("/ds_bw1/_doc/", DocNode.of("a", 1, "@timestamp", Instant.now().toString()));
            assertThat(httpResponse, containsExactly(ds_bw1).at("_index").but(user.indexMatcher("write")).whenEmpty(403));
        }
    }

    @Test
    public void deleteByQuery_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            try (GenericRestClient adminRestClient = cluster.getAdminCertRestClient()) {
                // Init test data
                DocNode testDoc = DocNode.of("test", "deleteByQuery_indexPattern", "@timestamp", Instant.now().toString());

                HttpResponse httpResponse = adminRestClient.putJson("/ds_bw1/_create/put_delete_delete_by_query_b1?refresh=true",
                        testDoc.with("delete_by_query_test_delete", "yes"));
                assertThat(httpResponse, isCreated());
                httpResponse = adminRestClient.putJson("/ds_bw1/_create/put_delete_delete_by_query_b2?refresh=true",
                        testDoc.with("delete_by_query_test_delete", "no"));
                assertThat(httpResponse, isCreated());
                httpResponse = adminRestClient.putJson("/ds_aw1/_create/put_delete_delete_by_query_a1?refresh=true",
                        testDoc.with("delete_by_query_test_delete", "yes"));
                assertThat(httpResponse, isCreated());
                httpResponse = adminRestClient.putJson("/ds_aw1/_create/put_delete_delete_by_query_a2?refresh=true",
                        testDoc.with("delete_by_query_test_delete", "no"));
                assertThat(httpResponse, isCreated());
            }

            HttpResponse httpResponse = restClient.postJson("/ds_aw*,ds_bw*/_delete_by_query?refresh=true&wait_for_completion=true",
                    DocNode.of("query.term.delete_by_query_test_delete", "yes"));

            if (containsExactly(ds_aw1, ds_aw2, ds_bw1, ds_bw2).at("_index").but(user.indexMatcher("write")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                //user can remove some or all docs found by search request
                assertThat(httpResponse, isOk());
                int expectedDeleteCount = containsExactly(ds_aw1, ds_bw1).at("_index").but(user.indexMatcher("write")).size();
                assertThat(httpResponse, json(nodeAt("deleted", equalTo(expectedDeleteCount))));
            }
        } finally {
            deleteTestDocs("deleteByQuery_indexPattern", "ds_aw*,ds_bw*");
        }
    }

    @Test
    public void putDocument_bulk() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user)) {
            HttpResponse httpResponse = restClient.putNdJson("/_bulk?refresh=true", //
                    DocNode.of("create._index", "ds_aw1", "create._id", "d1"),
                    DocNode.of("a", 1, "test", "putDocument_bulk", "@timestamp", Instant.now().toString()), //
                    DocNode.of("create._index", "ds_bw1", "create._id", "d1"),
                    DocNode.of("b", 1, "test", "putDocument_bulk", "@timestamp", Instant.now().toString()));

            if (user == LIMITED_USER_PERMISSIONS_ON_BACKING_INDICES) {
                // special case for this user: As there is no DNFOF functionality available for bulk[s] operations,
                // these will also fail, even if we have write privileges on the backing indices. This is because
                // privilege evaluation will yield OK_WHEN_RESOLVED which will need DNFOF functionality.
                assertThat(httpResponse,
                        containsExactly().at("items[*].create[?(@.result == 'created')]._index").but(user.indexMatcher("write")).whenEmpty(200));
            } else {
                assertThat(httpResponse, containsExactly(ds_aw1, ds_bw1).at("items[*].create[?(@.result == 'created')]._index")
                        .but(user.indexMatcher("write")).whenEmpty(200));
            }

        } finally {
            deleteTestDocs("putDocument_bulk", "ds_aw*,ds_bw*");
        }
    }

    @Test
    public void createDataStream() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.put("/_data_stream/ds_bwx1");

            if (containsExactly(ds_bwx1).but(user.indexMatcher("create_data_stream")).isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, isOk());
            }
        }
    }

    @Test
    public void putDataStream() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            HttpResponse httpResponse = restClient.putJson("/ds_bwx1/", DocNode.EMPTY);

            if (user == UNLIMITED_USER || user == SUPER_UNLIMITED_USER || user == INVALID_USER_INDEX_PERMISSIONS_FOR_DATA_STREAM) {
                // This will fail because we try to create an index under a name of a data stream
                assertThat(httpResponse, isBadRequest());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void deleteDataStream() throws Exception {
        try (GenericRestClient adminRestClient = cluster.getAdminCertRestClient().trackResources();
                GenericRestClient restClient = cluster.getRestClient(user)) {

            // Init test data
            {
                HttpResponse httpResponse = adminRestClient.put("/_data_stream/ds_bwx1");
                assertThat(httpResponse, isOk());
            }

            HttpResponse httpResponse = restClient.delete("/_data_stream/ds_bwx1");

            if (user.indexMatcher("manage_data_stream").isEmpty()) {
                assertThat(httpResponse, isForbidden());
            } else {
                assertThat(httpResponse, isOk());
            }
        }
    }

    @Test
    public void aliases_createAlias() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed("/*/_alias/alias_bwx");

            HttpResponse httpResponse = restClient.postJson("/_aliases",
                    DocNode.of("actions", DocNode.array(DocNode.of("add.index", "ds_bw1", "add.alias", "alias_bwx"))));
            if (containsExactly(ds_bw1, alias_bwx).isCoveredBy(user.indexMatcher("manage_alias"))) {
                assertThat(httpResponse, isOk());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void aliases_createAlias_indexPattern() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user).trackResources(cluster.getAdminCertRestClient())) {
            restClient.deleteWhenClosed("/*/_alias/alias_bwx");

            HttpResponse httpResponse = restClient.postJson("/_aliases",
                    DocNode.of("actions", DocNode.array(DocNode.of("add.indices", DocNode.array("ds_bw*"), "add.alias", "alias_bwx"))));
            if (containsExactly(ds_bw1, ds_bw2, alias_bwx).isCoveredBy(user.indexMatcher("manage_alias"))) {
                assertThat(httpResponse, isOk());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Test
    public void aliases_removeIndex() throws Exception {
        try (GenericRestClient restClient = cluster.getRestClient(user);
                GenericRestClient adminRestClient = cluster.getAdminCertRestClient().trackResources()) {
            adminRestClient.deleteWhenClosed("/*/_alias/alias_bwx");

            // Initialization
            {
                HttpResponse httpResponse = adminRestClient.put("/_data_stream/ds_bwx1");
                assertThat(httpResponse, isOk());
                httpResponse = adminRestClient.put("/_data_stream/ds_bwx2");
                assertThat(httpResponse, isOk());
                httpResponse = adminRestClient.postJson("/_aliases", DocNode.of("actions",
                        DocNode.array(DocNode.of("add.indices", DocNode.array("ds_bwx1", "ds_bwx2"), "add.alias", "alias_bwx"))));
                assertThat(httpResponse, isOk());
            }

            HttpResponse httpResponse = restClient.postJson("/_aliases",
                    DocNode.of("actions", DocNode.array(DocNode.of("remove_index.index", "ds_bwx1"))));

            if (containsExactly(ds_bwx2).isCoveredBy(user.indexMatcher("manage_data_stream"))) {
                // Not supported by ES for data streams
                assertThat(httpResponse, isBadRequest());
            } else {
                assertThat(httpResponse, isForbidden());
            }
        }
    }

    @Parameters(name = "{1}")
    public static Collection<Object[]> params() {
        List<Object[]> result = new ArrayList<>();

        for (TestSgConfig.User user : USERS) {
            result.add(new Object[] { user, user.getDescription() });
        }

        return result;
    }

    public DataStreamAuthorizationReadWriteIntTests(TestSgConfig.User user, String description) throws Exception {
        this.user = user;
    }

    private void deleteTestDocs(String testName, String indices) {
        try (GenericRestClient adminRestClient = cluster.getAdminCertRestClient()) {
            adminRestClient.postJson("/" + indices + "/_delete_by_query?refresh=true&wait_for_completion=true",
                    DocNode.of("query.term", ImmutableMap.of("test.keyword", testName)));
        } catch (Exception e) {
            throw new RuntimeException("Error while cleaning up test docs", e);
        }
    }

}
