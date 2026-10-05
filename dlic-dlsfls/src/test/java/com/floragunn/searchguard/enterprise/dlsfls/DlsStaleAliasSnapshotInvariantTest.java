/*
 * Copyright 2026 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.enterprise.dlsfls;

import static com.floragunn.searchsupport.meta.Meta.Mock.indices;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Test;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.PrivilegesEvaluationContext;
import com.floragunn.searchguard.authz.actions.ResolvedIndices;
import com.floragunn.searchguard.authz.config.Role;
import com.floragunn.searchguard.configuration.ConfigurationRepository;
import com.floragunn.searchguard.configuration.SgDynamicConfiguration;
import com.floragunn.searchguard.test.TestSgConfig;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.cstate.metrics.Meter;
import com.floragunn.searchsupport.cstate.metrics.MetricsLevel;
import com.floragunn.searchsupport.meta.Meta;

/**
 * Reproducer for the "wrong hits.total with size=0 on an alias search" issue.
 *
 * The coordinator-side DlsFlsValve decides via hasRestrictions(context, resolvedIndices) whether the shard request cache
 * must be disabled for a request. The shard-side DlsFlsSearchOperationListener decides independently via
 * getRestriction(context, index) whether to inject a DLS query. If the valve says "no restrictions" (cache stays on)
 * while the shard side says "restricted", the restricted per-shard result is written into the user-agnostic shard
 * request cache and served to every other user afterwards.
 *
 * This test encodes the invariant both decisions must satisfy: if a request on alias A is reported as unrestricted,
 * then every member index of A must be unrestricted as well. Two situations violate it today:
 *
 * <ul>
 * <li>writeIndexAlias: the alias is defined with is_write_index=true on one member (exactly like in the issue report). The
 * metadata model groups alias members by the Elasticsearch AliasMetadata object, whose equality includes the write index
 * flag. The alias thus falls apart into two alias objects of the same name; the name-based alias set keeps only one of them,
 * and the stateful rules never see the write index as a member of the alias. No staleness is needed for this.</li>
 * <li>staleSnapshot: the "stateful rules" snapshot knows the member indices but not yet the alias (the snapshot is rebuilt
 * asynchronously on cluster state changes, so this is the state between an alias change and the rebuild).</li>
 * </ul>
 *
 * The writeIndexAlias and staleSnapshot tests are expected to FAIL on the current code base. The controls pass.
 */
public class DlsStaleAliasSnapshotInvariantTest {

    static final NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
            ImmutableList.of(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField(TermQueryBuilder.NAME),
                    (CheckedFunction<XContentParser, TermQueryBuilder, IOException>) (p) -> TermQueryBuilder.fromXContent(p))));
    static final ConfigurationRepository.Context parserContext = new ConfigurationRepository.Context(null, null, null, xContentRegistry, null);

    static final String[] MEMBER_INDICES = new String[] { "old-000001", "old-000002", "old-000003" };

    /** Stale snapshot: the member indices exist, the alias is not known yet */
    static final Meta META_WITHOUT_ALIAS = indices(MEMBER_INDICES);

    /** Fresh cluster metadata: alias "new" spans all member indices */
    static final Meta META_WITH_ALIAS = indices(MEMBER_INDICES).alias("new").of(MEMBER_INDICES);

    /** Cluster metadata built from real Elasticsearch metadata: alias "new" spans all member indices, old-000003 is the write index */
    static final Meta META_WITH_WRITE_INDEX_ALIAS = Meta.from(esMetadata(true));

    /** Same, but without a write index (control) */
    static final Meta META_WITH_ALIAS_FROM_ES = Meta.from(esMetadata(false));

    static final TestSgConfig.Role ALIAS_ROLE = new TestSgConfig.Role("alias_role").aliasPermissions("*").on("new");
    static final TestSgConfig.Role DLS_ROLE = new TestSgConfig.Role("dls_role").indexPermissions("*").dls(DocNode.of("term.level.value", "info"))
            .on("old-*");

    @Test
    public void aliasOnlyUser_writeIndexAlias_valveAndShardAgree() throws Exception {
        RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig(ALIAS_ROLE, DLS_ROLE), META_WITH_WRITE_INDEX_ALIAS,
                MetricsLevel.NONE);

        assertValveAndShardDecisionsAgree(subject, META_WITH_WRITE_INDEX_ALIAS, context("alias_role"));
    }

    @Test
    public void aliasPlusDlsUser_writeIndexAlias_valveAndShardAgree() throws Exception {
        RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig(ALIAS_ROLE, DLS_ROLE), META_WITH_WRITE_INDEX_ALIAS,
                MetricsLevel.NONE);

        assertValveAndShardDecisionsAgree(subject, META_WITH_WRITE_INDEX_ALIAS, context("alias_role", "dls_role"));
    }

    /**
     * Control: the same metadata built from Elasticsearch metadata, but without a write index. Passes today.
     */
    @Test
    public void aliasWithoutWriteIndexFromEsMetadata_consistent() throws Exception {
        RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig(ALIAS_ROLE, DLS_ROLE), META_WITH_ALIAS_FROM_ES,
                MetricsLevel.NONE);

        assertValveAndShardDecisionsAgree(subject, META_WITH_ALIAS_FROM_ES, context("alias_role"));
        assertValveAndShardDecisionsAgree(subject, META_WITH_ALIAS_FROM_ES, context("alias_role", "dls_role"));
    }

    @Test
    public void aliasOnlyUser_staleSnapshot_valveAndShardAgree() throws Exception {
        RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig(ALIAS_ROLE, DLS_ROLE), META_WITHOUT_ALIAS,
                MetricsLevel.NONE);

        assertValveAndShardDecisionsAgree(subject, META_WITH_ALIAS, context("alias_role"));
    }

    @Test
    public void aliasPlusDlsUser_staleSnapshot_valveAndShardAgree() throws Exception {
        RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig(ALIAS_ROLE, DLS_ROLE), META_WITHOUT_ALIAS,
                MetricsLevel.NONE);

        assertValveAndShardDecisionsAgree(subject, META_WITH_ALIAS, context("alias_role", "dls_role"));
    }

    /**
     * Control: with a snapshot that knows the alias, both decisions agree. Passes today.
     */
    @Test
    public void freshSnapshot_consistent() throws Exception {
        RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig(ALIAS_ROLE, DLS_ROLE), META_WITH_ALIAS,
                MetricsLevel.NONE);

        assertValveAndShardDecisionsAgree(subject, META_WITH_ALIAS, context("alias_role"));
        assertValveAndShardDecisionsAgree(subject, META_WITH_ALIAS, context("alias_role", "dls_role"));
    }

    /**
     * Control: without any stateful rules (static evaluation only), both decisions agree. Passes today.
     */
    @Test
    public void noStatefulRules_consistent() throws Exception {
        RoleBasedDocumentAuthorization subject = new RoleBasedDocumentAuthorization(roleConfig(ALIAS_ROLE, DLS_ROLE), null, MetricsLevel.NONE);

        assertValveAndShardDecisionsAgree(subject, META_WITH_ALIAS, context("alias_role"));
        assertValveAndShardDecisionsAgree(subject, META_WITH_ALIAS, context("alias_role", "dls_role"));
    }

    /**
     * @param clusterMetadata the metadata the request is resolved against, i.e. what the current cluster state says
     */
    private static void assertValveAndShardDecisionsAgree(RoleBasedDocumentAuthorization subject, Meta clusterMetadata,
            PrivilegesEvaluationContext context) throws Exception {
        // What the coordinator-side DlsFlsValve asks for a search on the alias (resolved against the fresh cluster metadata)
        ResolvedIndices resolvedAlias = ResolvedIndices.of(clusterMetadata, "new");
        boolean hasRestrictions = subject.hasRestrictions(context, resolvedAlias, Meter.NO_OP);

        assertFalse("Valve: user " + context.getMappedRoles() + " is expected to be unrestricted on alias new; resolved: " + resolvedAlias,
                hasRestrictions);

        // What the shard-side DlsFlsSearchOperationListener asks for each member index (again from the fresh cluster metadata)
        for (String indexName : MEMBER_INDICES) {
            Meta.Index index = (Meta.Index) clusterMetadata.getIndexOrLike(indexName);
            DlsRestriction restriction = subject.getRestriction(context, index, Meter.NO_OP);

            assertTrue("Shard: valve reported no restrictions on alias new for user " + context.getMappedRoles()
                    + ", but the member index " + indexName + " is restricted: " + restriction + "; alias as seen by the metadata model: "
                    + clusterMetadata.getIndexOrLike("new"), restriction.isUnrestricted());
        }
    }

    /**
     * Builds Elasticsearch metadata like the curl sequence of the issue report does: three indices, each created with the alias
     * "new"; the last one optionally as write index.
     */
    private static Metadata esMetadata(boolean withWriteIndex) {
        Metadata.Builder metadata = Metadata.builder();

        for (int i = 0; i < MEMBER_INDICES.length; i++) {
            boolean writeIndex = withWriteIndex && i == MEMBER_INDICES.length - 1;

            metadata.put(IndexMetadata.builder(MEMBER_INDICES[i])
                    .settings(Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current().id()))
                    .numberOfShards(1).numberOfReplicas(0).putAlias(AliasMetadata.builder("new").writeIndex(writeIndex)));
        }

        return metadata.build();
    }

    private static PrivilegesEvaluationContext context(String... mappedRoles) {
        User user = new User.Builder().name("test_user").build();
        return new PrivilegesEvaluationContext(user, false, ImmutableSet.ofArray(mappedRoles), null, null, true, null, null);
    }

    private static SgDynamicConfiguration<Role> roleConfig(TestSgConfig.Role... roles) throws ConfigValidationException {
        return TestSgConfig.Role.toActualRole(parserContext, roles);
    }
}
