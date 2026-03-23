/*
 * Copyright 2015-2024 floragunn GmbH
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

package com.floragunn.searchguard.authz.actions;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableSet;
import com.floragunn.searchguard.authz.SystemIndexAccess;
import com.floragunn.searchguard.authz.actions.ActionRequestIntrospector.IndicesRequestInfo;
import com.floragunn.searchsupport.action.IndicesOptionsSupport;
import com.floragunn.searchsupport.meta.Meta;
import com.floragunn.searchsupport.queries.DateMathExpressionResolver;
import com.floragunn.searchsupport.queries.WildcardExpressionResolver;

public class ResolvedIndices {

    final static ResolvedIndices EMPTY = new ResolvedIndices(false, ResolvedIndices.Local.EMPTY, ImmutableSet.empty(), ImmutableSet.empty());

    private final static Logger log = LogManager.getLogger(ResolvedIndices.class);
    private final boolean localAll;
    private ImmutableSet<IndicesRequestInfo> deferredRequests;
    private ResolvedIndices.Local localShallow;
    protected final ImmutableSet<String> remoteIndices;

    ResolvedIndices(boolean localAll, ResolvedIndices.Local localShallow, ImmutableSet<String> remoteIndices,
            ImmutableSet<IndicesRequestInfo> deferredRequests) {
        this.localAll = localAll;
        this.localShallow = localShallow;
        this.remoteIndices = remoteIndices;
        this.deferredRequests = deferredRequests;
    }

    public boolean isLocalAll() {
        return localAll;
    }

    public boolean isLocalIndicesEmpty() {
        return !localAll && getLocal().isEmpty();
    }

    public boolean containsOnlyRemoteIndices() {
        return !this.localAll && this.deferredRequests.isEmpty() && this.localShallow.isEmpty() && !this.remoteIndices.isEmpty();
    }

    ResolvedIndices with(ResolvedIndices other) {
        if (other == null || other == EMPTY) {
            return this;
        }

        return new ResolvedIndices(this.localAll || other.localAll, this.localShallow.with(other.localShallow),
                this.remoteIndices.with(other.remoteIndices), this.deferredRequests.with(other.deferredRequests));
    }

    ResolvedIndices with(Collection<ResolvedIndices> others) {
        if (others == null || others.isEmpty()) {
            return this;
        }

        ResolvedIndices result = this;

        for (ResolvedIndices other : others) {
            result = result.with(other);
        }

        return result;
    }

    private void resolveDeferredRequests() {
        ResolvedIndices.Local localShallow = this.localShallow;

        for (IndicesRequestInfo info : this.deferredRequests) {
            localShallow = localShallow.with(Local.resolve(info, info.indexMetadata));
        }

        this.deferredRequests = ImmutableSet.empty();

        this.localShallow = localShallow;
    }

    public ResolvedIndices.Local getLocal() {
        if (!deferredRequests.isEmpty()) {
            resolveDeferredRequests();
        }

        return localShallow;
    }

    public ImmutableSet<String> getRemoteIndices() {
        return remoteIndices;
    }

    public int getNumberOfLocalAndRemoteIndices() {
        return getLocal().getUnion().size() + getRemoteIndices().size();
    }

    @Override
    public String toString() {
        if (localAll) {
            if (remoteIndices.isEmpty() && !deferredRequests.isEmpty()) {
                return "local: _all";
            }

            StringBuilder result = new StringBuilder("local: _all");

            if (deferredRequests.isEmpty()) {
                result.append(" [").append(localShallow.size()).append("]");
            }

            if (!remoteIndices.isEmpty()) {
                result.append("; remote: ").append(remoteIndices.toShortString());
            }

            return result.toString();
        } else {
            return "local: " + (localShallow != null ? localShallow : "null") + "; remote: "
                    + (remoteIndices != null ? remoteIndices.toShortString() : "null");
        }
    }

    /**
     * Only for testing!
     */
    public static ResolvedIndices of(Meta indexMetadata, String... indices) {
        return new ResolvedIndices(false, Local.resolve(indexMetadata, indices), ImmutableSet.empty(), ImmutableSet.empty());
    }

    /**
     * Only for testing!
     */
    public ResolvedIndices local(ResolvedIndices.Local local) {
        return new ResolvedIndices(false, local, remoteIndices, ImmutableSet.empty());
    }

    public static ResolvedIndices empty() {
        return EMPTY;
    }

    public static class Local {
        private final ImmutableSet<Meta.Index> pureIndices;
        private final ImmutableSet<Meta.Alias> aliases;
        private final ImmutableSet<Meta.DataStream> dataStreams;
        private final ImmutableSet<Meta.NonExistent> nonExistingIndices;
        private final ImmutableSet<String> unionOfAliasesAndDataStreams;
        private final ImmutableSet<Meta.IndexLikeObject> union;
        private String asString;
        private ImmutableSet<Meta.IndexLikeObject> deepUnion;
        private Boolean containsAliasOrDataStreamMembers;

        Local(ImmutableSet<Meta.Index> pureIndices, ImmutableSet<Meta.Alias> aliases, ImmutableSet<Meta.DataStream> dataStreams,
                ImmutableSet<Meta.NonExistent> nonExistingIndices) {
            this.pureIndices = pureIndices;
            this.aliases = aliases;
            this.dataStreams = dataStreams;
            this.nonExistingIndices = nonExistingIndices;
            this.unionOfAliasesAndDataStreams = aliases.map(Meta.Alias::name).with(dataStreams.map(Meta.DataStream::name));
            this.union = ImmutableSet.<Meta.IndexLikeObject>of(pureIndices).with(aliases).with(dataStreams).with(this.nonExistingIndices);
        }

        private Local(ImmutableSet<Meta.Index> pureIndices, ImmutableSet<Meta.Alias> aliases, ImmutableSet<Meta.DataStream> dataStreams,
                ImmutableSet<Meta.NonExistent> nonExistingIndices, ImmutableSet<String> unionOfAliasesAndDataStreams,
                ImmutableSet<Meta.IndexLikeObject> union) {
            this.pureIndices = pureIndices;
            this.aliases = aliases;
            this.dataStreams = dataStreams;
            this.nonExistingIndices = nonExistingIndices;
            this.unionOfAliasesAndDataStreams = unionOfAliasesAndDataStreams;
            this.union = union;
        }

        boolean isEmpty() {
            return union.isEmpty();
        }

        ResolvedIndices.Local with(ResolvedIndices.Local other) {
            if (this.union.equals(other.union)) {
                return this;
            }

            if (this.unionOfAliasesAndDataStreams.equals(other.unionOfAliasesAndDataStreams)) {
                return new Local(this.pureIndices.with(other.pureIndices), this.aliases, this.dataStreams,
                        this.nonExistingIndices.with(other.nonExistingIndices), this.unionOfAliasesAndDataStreams, this.union.with(other.union));
            } else {
                // Remove entries from pureIndices which are contained in the other object's aliases or data streams (and vice versa)
                // This ensures the contract that pureIndices only contains indices which are not already indirectly contained in aliases or dataStreams

                ImmutableSet.Builder<Meta.Index> mergedPureIndices = new ImmutableSet.Builder<>(this.pureIndices.size() + other.pureIndices.size());

                for (Meta.Index index : this.pureIndices) {
                    if (index.parentDataStreamName() != null && other.dataStreams.contains(index.parentDataStream())) {
                        continue;
                    }

                    if (other.aliases.containsAny(index.parentAliases())) {
                        continue;
                    }

                    mergedPureIndices.add(index);
                }

                for (Meta.Index index : other.pureIndices) {
                    if (index.parentDataStreamName() != null && this.dataStreams.contains(index.parentDataStream())) {
                        continue;
                    }

                    if (this.aliases.containsAny(index.parentAliases())) {
                        continue;
                    }

                    mergedPureIndices.add(index);
                }

                return new Local(mergedPureIndices.build(), this.aliases.with(other.aliases), this.dataStreams.with(other.dataStreams),
                        this.nonExistingIndices.with(other.nonExistingIndices));
            }
        }

        public int size() {
            return this.union.size();
        }

        public ImmutableSet<Meta.Index> getPureIndices() {
            return pureIndices;
        }

        public ImmutableSet<Meta.Alias> getAliases() {
            return aliases;
        }

        public ImmutableSet<Meta.DataStream> getDataStreams() {
            return dataStreams;
        }

        public ImmutableSet<Meta.IndexCollection> getAliasesAndDataStreams() {
            return ImmutableSet.<Meta.IndexCollection>of(aliases).with(dataStreams);
        }

        public ImmutableSet<Meta.NonExistent> getNonExistingIndices() {
            return nonExistingIndices;
        }

        public ImmutableSet<Meta.IndexLikeObject> getUnion() {
            return union;
        }

        public ImmutableSet<Meta.IndexLikeObject> getDeepUnion() {
            ImmutableSet<Meta.IndexLikeObject> result = this.deepUnion;

            if (result == null) {
                result = this.union.with(Meta.IndexLikeObject.resolveDeep(aliases, Meta.Alias.ResolutionMode.NORMAL))
                        .with(Meta.IndexLikeObject.resolveDeep(dataStreams, Meta.Alias.ResolutionMode.NORMAL));
                this.deepUnion = result;
            }

            return result;
        }
        
        public boolean hasAliasOrDataStreamMembers() {
            Boolean result = this.containsAliasOrDataStreamMembers;

            if (result == null) {
                for (Meta.Index pureIndex : this.pureIndices) {
                    if (!pureIndex.parentAliasNames().isEmpty() || pureIndex.parentDataStreamName() != null) {
                        result = true;
                        break;
                    }
                }

                for (Meta.DataStream dataStream : this.dataStreams) {
                    if (!dataStream.parentAliasNames().isEmpty()) {
                        result = true;
                        break;
                    }
                }

                if (result == null) {
                    result = false;
                }

                this.containsAliasOrDataStreamMembers = result;
            }

            return result;
        }

        boolean hasAliasesOrDataStreams() {
            return !aliases.isEmpty() || !dataStreams.isEmpty();
        }

        public boolean hasAliasesOnly() {
            return !aliases.isEmpty() && this.dataStreams.isEmpty() && this.pureIndices.isEmpty();
        }

        public boolean hasNonExistingObjects() {
            return !this.nonExistingIndices.isEmpty() || this.pureIndices.forAnyApplies(i -> !i.exists())
                    || this.aliases.forAnyApplies(a -> !a.exists()) || this.dataStreams.forAnyApplies(d -> !d.exists());
        }

        @Override
        public String toString() {
            String result = this.asString;

            if (result != null) {
                return result;
            }

            StringBuilder resultBuilder = new StringBuilder("{");

            if (!this.pureIndices.isEmpty()) {
                resultBuilder.append("indices: ").append(this.pureIndices);
            }

            if (!this.aliases.isEmpty()) {
                if (!this.pureIndices.isEmpty()) {
                    resultBuilder.append("; ");
                }

                resultBuilder.append("aliases: ").append(this.aliases);
            }

            if (!this.dataStreams.isEmpty()) {
                if (!this.pureIndices.isEmpty() || !this.aliases.isEmpty()) {
                    resultBuilder.append("; ");
                }

                resultBuilder.append("dataStreams: ").append(this.dataStreams);
            }

            if (!this.nonExistingIndices.isEmpty()) {
                if (!this.pureIndices.isEmpty() || !this.aliases.isEmpty() || !this.dataStreams.isEmpty()) {
                    resultBuilder.append("; ");
                }

                resultBuilder.append("nonExistingIndices: ").append(this.nonExistingIndices);
            }

            resultBuilder.append("}");

            result = resultBuilder.toString();
            this.asString = result;

            return result;
        }

        static ResolvedIndices.Local resolve(IndicesRequestInfo request, Meta indexMetadata) {
            try {
                if (request.scope == IndicesRequestInfo.Scope.DATA_STREAM) {
                    if (request.isAll) {
                        return new Local(ImmutableSet.empty(), ImmutableSet.empty(), //
                                (request.indicesOptions().expandWildcardsHidden() //
                                        ? indexMetadata.dataStreams()//
                                        : indexMetadata.nonHiddenDataStreams()).matching(dataStream -> dataStream.isFailureStoreRelated() == request.allIndicesFailureStore), //
                                ImmutableSet.empty());
                    } else if (request.expandWildcards) {
                        return resolveWithPatterns(request, indexMetadata);
                    } else if (request.createIndexRequest) {
                        return new Local(ImmutableSet.empty(), ImmutableSet.empty(),
                                ImmutableSet.of(resolveDateMathExpressions(request.localIndices).map(ref -> Meta.DataStream.nonExistent(ref.metaName()))),
                                ImmutableSet.empty());
                    } else {
                        return resolveDataStreamsWithoutPatterns(request, indexMetadata);
                    }
                } else if (request.scope == IndicesRequestInfo.Scope.ALIAS) {
                    if (request.isAll) {
                        return new Local(ImmutableSet.empty(), indexMetadata.aliases().matching(alias -> alias.isFailureStoreRelated() == request.allIndicesFailureStore), ImmutableSet.empty(), ImmutableSet.empty());
                    } else if (request.expandWildcards) {
                        return resolveWithPatterns(request, indexMetadata);
                    } else if (request.createIndexRequest) {
                        return new Local(ImmutableSet.empty(),
                                ImmutableSet.of(resolveDateMathExpressions(request.localIndices)
                                        .map(ref -> Meta.Alias.nonExistent(ref.metaName()))), ImmutableSet.empty(),
                                ImmutableSet.empty());
                    } else {
                        return resolveAliasesWithoutPatterns(request, indexMetadata);
                    }
                } else {
                    // other scope

                    if (request.isAll) {
                        return resolveIsAll(request, indexMetadata);
                    } else if (request.expandWildcards) {
                        return resolveWithPatterns(request, indexMetadata);
                    } else {
                        // No wildcards, no write request, no create index request
                        return resolveWithoutPatterns(request, indexMetadata);
                    }
                }
            } catch (RuntimeException e) {
                log.error("Error while resolving " + request, e);
                throw e;
            }
        }

        /**
         * Only for testing!
         */
        public static ResolvedIndices.Local resolve(Meta indexMetadata, String... indices) {
            return resolveWithoutPatterns(new IndicesRequestInfo(null, ImmutableList.ofArray(indices), IndicesOptionsSupport.EXACT,
                    IndicesRequestInfo.Scope.ANY, SystemIndexAccess.DISALLOWED, indexMetadata), indexMetadata);
        }

        static ResolvedIndices.Local resolveWithPatterns(IndicesRequestInfo request, Meta indexMetadata) {
            Metadata metadata = indexMetadata.esMetadata();
            IndicesRequestInfo.Scope scope = request.scope;
            boolean includeDataStreams = request.includeDataStreams && scope.includeDataStreams;
            boolean includeIndices = scope.includeIndices;
            boolean includeAliases = (scope.includeAliases && !request.indicesOptions().ignoreAliases()) || scope == IndicesRequestInfo.Scope.ALIAS; // An explict ALIAS scope overrides ignoreAliases

            SortedMap<String, IndexAbstraction> indicesLookup = metadata.getProject().getIndicesLookup();

            ImmutableSet.Builder<Meta.Index> indices = new ImmutableSet.Builder<>();
            ImmutableSet.Builder<Meta.NonExistent> nonExistingIndices = new ImmutableSet.Builder<>();
            ImmutableSet.Builder<Meta.Alias> aliases = new ImmutableSet.Builder<>();
            ImmutableSet.Builder<Meta.DataStream> dataStreams = new ImmutableSet.Builder<>();
            Set<String> excludeNames = new HashSet<>();
            // Tracks names added to excludeNames by a *concrete* (non-wildcard) exclusion token, e.g. "-ds_a1".
            // Used to distinguish concrete exclusions from wildcard exclusions (e.g. "-ds_a*") so that an explicit
            // positive token can survive a wildcard exclusion that happens to cover the same name.
            //
            // Example: "ds_a1,*,-ds_a*"
            //   - The backwards pass processes "-ds_a*" first; wildcard expansion adds "ds_a1" (and its backing
            //     indices) to excludeNames, but NOT to concreteExcludeNames.
            //   - The forward pass then encounters the explicit "ds_a1" token.  Because "ds_a1" is absent from
            //     concreteExcludeNames, the token is NOT skipped — matching ES behaviour where an explicit name
            //     takes precedence over a subsequent wildcard exclusion.
            //
            // Contrast with "*,-ds_a1": the concrete exclusion "-ds_a1" puts "ds_a1" into concreteExcludeNames,
            // so the explicit token (if any) is still skipped, i.e. concrete exclusions continue to win.
            Set<String> concreteExcludeNames = new HashSet<>();
            // Tracks names that are the direct target of an exclusion token — either a concrete token (e.g. "-ds_a1")
            // or a name resolved from a wildcard exclusion (e.g. "-ds_a*" resolving to "ds_a1").
            //
            // This is a superset of concreteExcludeNames. It does NOT include names that were added to excludeNames
            // as member/child propagation by resolveNegationUpAndDown (e.g., when "-alias_ab" adds the alias's member
            // indices "index_a1" and "index_b1" to excludeNames).
            //
            // Used in the partial-exclusion branch to determine whether an alias/collection member should be filtered.
            // Without this distinction, the expression "*,-index_a1,-alias_ab" (where alias_ab = {index_a1, index_b1})
            // incorrectly loses index_b1:
            //   1. -alias_ab adds index_b1 to excludeNames (member propagation)
            //   2. -index_a1 adds alias_ab to partiallyExcludedObjects (parent marking)
            //   3. Forward pass: * matches alias_ab → partial-exclusion branch → index_b1 in excludeNames → skipped
            //
            // By checking directlyExcludedNames instead, index_b1 is correctly retained: it was never the direct
            // target of any exclusion token.
            Set<String> directlyExcludedNames = new HashSet<>();
            Set<String> partiallyExcludedObjects = new HashSet<>();

            // Note: We are going backwards through the list of indices in order to get negated patterns before the patterns they apply to
            for (int i = request.localIndices.size() - 1; i >= 0; i--) {
                IndexExpression index = request.localIndices.get(i);

                if (index.isExclusion()) {
                    index = index.dropExclusion();
                    index = index.mapBaseName(DateMathExpressionResolver::resolveExpression);

                    if (index.containsStarWildcard()) {
                        Map<String, IndexAbstraction> matchedAbstractions = WildcardExpressionResolver.matches(metadata, indicesLookup, index.baseName(),
                                request.indicesOptions(), includeDataStreams);

                        for (Map.Entry<String, IndexAbstraction> matchedEntry : matchedAbstractions.entrySet()) {
                            String resolvedIndex = matchedEntry.getKey();
                            IndexExpression resolvedIndexExpression = index.withIndexName(resolvedIndex);
                            directlyExcludedNames.add(resolvedIndexExpression.metaName());
                            resolveNegationUpAndDown(resolvedIndexExpression.metaName(), excludeNames, partiallyExcludedObjects, request, indexMetadata);
                            // When a wildcard negation directly matches a DataStream or Alias, also add it to
                            // partiallyExcludedObjects. This ensures the partial-exclusion branch is taken during
                            // positive wildcard expansion, where excludeNames is not checked for non-ConcreteIndex
                            // entries (because isNegationOnlyEffectiveForIndicesAndForOtherNonWildcardObjects() is
                            // true for ANY-scope requests like search). All backing indices are already in
                            // excludeNames via resolveNegationUpAndDown, so the partial-exclusion branch adds none
                            // of them — effectively fully excluding the collection.
                            if (matchedEntry.getValue().getType() == IndexAbstraction.Type.DATA_STREAM
                                    || matchedEntry.getValue().getType() == IndexAbstraction.Type.ALIAS) {
                                partiallyExcludedObjects.add(matchedEntry.getKey());
                            }
                        }
                    } else {
                        // Concrete exclusion (e.g. "-ds_a1"): record in concreteExcludeNames so that the forward
                        // pass can distinguish it from a wildcard exclusion that merely happens to match the same name.
                        concreteExcludeNames.add(index.metaName());
                        directlyExcludedNames.add(index.metaName());
                        resolveNegationUpAndDown(index.metaName(), excludeNames, partiallyExcludedObjects, request, indexMetadata);
                    }
                } else {
                    index = index.mapBaseName(DateMathExpressionResolver::resolveExpression);

                    if (index.containsStarWildcard()) {

                        Map<String, IndexAbstraction> matchedAbstractions = WildcardExpressionResolver.matches(metadata, indicesLookup, index.baseName(),
                                request.indicesOptions(), includeDataStreams);

                        for (Map.Entry<String, IndexAbstraction> entry : matchedAbstractions.entrySet()) {
                            String matchedAbstractionWithComponent = index.withIndexName(entry.getKey()).metaName();
                            if (request.isNegationOnlyEffectiveForIndicesAndForOtherNonWildcardObjects()) {
                                if (entry.getValue() instanceof IndexAbstraction.ConcreteIndex && excludeNames.contains(matchedAbstractionWithComponent)) {
                                    continue;
                                }
                            } else {
                                if (excludeNames.contains(matchedAbstractionWithComponent)) {
                                    continue;
                                }
                            }

                            if (!partiallyExcludedObjects.contains(matchedAbstractionWithComponent) || scope == IndicesRequestInfo.Scope.ALIAS
                                    || scope == IndicesRequestInfo.Scope.DATA_STREAM || scope == IndicesRequestInfo.Scope.INDICES_DATA_STREAMS) {
                                // This is the happy case, just include the object
                                IndexAbstraction indexAbstraction = entry.getValue();

                                if (indexAbstraction instanceof DataStream) {
                                    if (includeDataStreams) {
                                        Meta.DataStream dataStream = (Meta.DataStream) indexMetadata.getIndexOrLike(matchedAbstractionWithComponent);
                                        if (dataStream != null) {
                                            dataStreams.add(dataStream);
                                        } else {
                                            dataStreams.add(Meta.DataStream.nonExistent(matchedAbstractionWithComponent));
                                        }
                                    }
                                } else if (indexAbstraction instanceof IndexAbstraction.Alias) {
                                    if (includeAliases) {
                                        Meta.Alias alias = (Meta.Alias) indexMetadata.getIndexOrLike(matchedAbstractionWithComponent);
                                        if (alias != null) {
                                            aliases.add(alias);
                                        } else {
                                            // e.g., a regular index alias with ::failures component selector - no failure store counterpart exists
                                            aliases.add(Meta.Alias.nonExistent(matchedAbstractionWithComponent));
                                        }
                                    }
                                } else {
                                    if (includeIndices) {
                                        Meta.Index indexMeta = (Meta.Index) indexMetadata.getIndexOrLike(matchedAbstractionWithComponent);

                                        if (indexMeta == null) {
                                            continue;
                                        }
                                        if (!indexMeta.isSystem() || request.systemIndexAccess.isAllowed(indexMeta.nameForIndexPatternMatching())) {
                                            indices.add(indexMeta);
                                        }
                                    }
                                }
                            } else {
                                // Oh dear, one of the negated elements is a member of this. Now we have to resolve this to perform the exclusion

                                Meta.IndexLikeObject indexLike = indexMetadata.getIndexOrLike(matchedAbstractionWithComponent);

                                if (indexLike == null) {
                                    // Component selector refers to a non-existent variant (e.g., ::failures on a regular index alias)
                                    continue;
                                } else if (indexLike instanceof Meta.IndexCollection) {
                                    // Use directlyExcludedNames (not excludeNames) to filter members.
                                    // excludeNames includes names propagated by resolveNegationUpAndDown when a
                                    // parent collection was excluded (e.g. -alias_ab adds index_b1 to excludeNames).
                                    // Those propagated names should not cause members to be filtered here — only
                                    // names that were the direct target of an exclusion token should be filtered.
                                    //
                                    // Special case: if the collection is a DataStream and it was directly excluded,
                                    // skip it entirely. Unlike alias members (which are independent entities that
                                    // can be matched by wildcard on their own), backing indices are internal to
                                    // the data stream and should all be excluded when the data stream itself is.
                                    if (indexLike instanceof Meta.DataStream && directlyExcludedNames.contains(matchedAbstractionWithComponent)) {
                                        continue;
                                    }
                                    for (Meta.IndexLikeObject member : ((Meta.IndexCollection) indexLike).members()) {
                                        if (!directlyExcludedNames.contains(member.name())) {
                                            if (member instanceof Meta.Index) {
                                                if (includeIndices) {
                                                    Meta.Index indexMeta = (Meta.Index) member;
                                                    if (!indexMeta.isSystem() || request.systemIndexAccess.isAllowed(indexMeta.nameForIndexPatternMatching())) {
                                                        indices.add(indexMeta);
                                                    }
                                                }
                                            } else if (member instanceof Meta.DataStream) {
                                                if (includeDataStreams) {
                                                    Meta.DataStream dataStream = (Meta.DataStream) member;
                                                    if (dataStream.members().stream().anyMatch(dsMember -> directlyExcludedNames.contains(dsMember.name()))) {
                                                        for (Meta.IndexLikeObject dsMember : dataStream.members()) {
                                                            if (!directlyExcludedNames.contains(dsMember.name())) {
                                                                indices.add((Meta.Index) dsMember);
                                                            }
                                                        }
                                                    } else {
                                                        dataStreams.add(dataStream);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    indices.add((Meta.Index) indexLike);
                                }

                            }
                        }
                    } else {
                        // An explicit positive token (e.g. "ds_a1" in "ds_a1,*,-ds_a*") is only skipped when a
                        // *concrete* exclusion for the same name exists (e.g. "-ds_a1").  A wildcard exclusion
                        // like "-ds_a*" adds "ds_a1" to excludeNames but NOT to concreteExcludeNames, so the
                        // explicit token survives — matching ES behaviour where explicit names take precedence
                        // over wildcard exclusions.  See search_explicitDataStream_withWildcardExclusion_expandAll.
                        if (concreteExcludeNames.contains(index.metaName())) {
                            continue;
                        }

                        Meta.IndexLikeObject indexLikeObject = indexMetadata.getIndexOrLike(index.metaName());

                        if (indexLikeObject == null) {
                            if (scope == IndicesRequestInfo.Scope.DATA_STREAM) {
                                dataStreams.add(Meta.DataStream.nonExistent(index.metaName()));
                            } else if (scope == IndicesRequestInfo.Scope.ALIAS) {
                                aliases.add(Meta.Alias.nonExistent(index.metaName()));
                            } else {
                                nonExistingIndices.add(Meta.NonExistent.of(index.metaName()));
                            }
                        } else if (indexLikeObject instanceof Meta.Alias) {
                            if (includeAliases) {
                                aliases.add((Meta.Alias) indexLikeObject);
                            }
                        } else if (indexLikeObject instanceof Meta.DataStream) {
                            if (includeDataStreams) {
                                dataStreams.add((Meta.DataStream) indexLikeObject);
                            }
                        } else {
                            if (includeIndices) {
                                Meta.Index indexMeta = (Meta.Index) indexLikeObject;

                                if (!indexMeta.isSystem() || request.systemIndexAccess.isAllowed(indexMeta.nameForIndexPatternMatching())) {
                                    indices.add((Meta.Index) indexLikeObject);
                                }
                            }
                        }
                    }
                }
            }

            ImmutableSet<Meta.Index> pureIndices = indices.build();

            if ((aliases.size() != 0 || dataStreams.size() != 0) && scope != IndicesRequestInfo.Scope.ANY_DISTINCT) {
                // If there are aliases or dataStreams, remove the indices that are part of these aliases or dataStreams

                pureIndices = pureIndices.matching((index) -> {
                    if (index.parentDataStreamName() != null && dataStreams.contains(index.parentDataStream())) {
                        return false;
                    } else if (aliases.containsAny(index.parentAliases())) {
                        return false;
                    } else {
                        return true;
                    }
                });
            }

            return new Local(pureIndices, aliases.build(), dataStreams.build(), nonExistingIndices.build());
        }

        static ResolvedIndices.Local resolveIsAll(IndicesRequestInfo request, Meta indexMetadata) {
            IndicesRequestInfo.Scope scope = request.scope;

            boolean includeHidden = request.indicesOptions().expandWildcardsHidden();
            boolean excludeSystem = request.systemIndexAccess.isNotAllowed();
            boolean includeDataStreams = request.includeDataStreams && scope.includeDataStreams;
            boolean includeIndices = scope.includeIndices;
            boolean includeAliases = (scope.includeAliases && !request.indicesOptions().ignoreAliases()) || scope == IndicesRequestInfo.Scope.ALIAS;

            ImmutableSet<Meta.Index> pureIndices;
            ImmutableSet<Meta.Alias> aliases;
            ImmutableSet<Meta.DataStream> dataStreams;
            ImmutableSet<Meta.NonExistent> nonExistingIndices = ImmutableSet.empty();

            if (includeDataStreams && includeAliases && scope != IndicesRequestInfo.Scope.ANY_DISTINCT) {
                if (!includeIndices) {
                    pureIndices = ImmutableSet.empty();
                } else if (!includeHidden) {
                    pureIndices = indexMetadata.nonHiddenIndicesWithoutParents();
                } else if (excludeSystem) {
                    pureIndices = indexMetadata.nonSystemIndicesWithoutParents();
                } else {
                    pureIndices = indexMetadata.indicesWithoutParents();
                }

                aliases = includeHidden ? indexMetadata.aliases() : indexMetadata.nonHiddenAliases();
                dataStreams = includeHidden ? indexMetadata.dataStreams() : indexMetadata.nonHiddenDataStreams();
            } else {
                if (!includeIndices) {
                    pureIndices = ImmutableSet.empty();
                } else if (!includeHidden) {
                    pureIndices = indexMetadata.nonHiddenIndices();
                } else if (excludeSystem) {
                    pureIndices = indexMetadata.nonSystemIndices();
                } else {
                    pureIndices = indexMetadata.indices();
                }

                aliases = includeAliases ? (includeHidden ? indexMetadata.aliases() : indexMetadata.nonHiddenAliases()) : ImmutableSet.empty();
                dataStreams = includeDataStreams ? (includeHidden ? indexMetadata.dataStreams() : indexMetadata.nonHiddenDataStreams())
                        : ImmutableSet.empty();

                if (scope != IndicesRequestInfo.Scope.ANY_DISTINCT) {
                    if (!dataStreams.isEmpty()) {
                        pureIndices = pureIndices.matching(e -> e.parentDataStreamName() == null);
                    }

                    if (!aliases.isEmpty()) {
                        pureIndices = pureIndices.matching(e -> e.parentAliasNames().isEmpty());
                    }
                }
            }

            Predicate<Boolean> excludeStatePredicate = WildcardExpressionResolver.excludeStatePredicate(request.indicesOptions());
            if (excludeStatePredicate != null) {
                pureIndices = pureIndices.matching(e -> !excludeStatePredicate.test(e.isOpen()));
            }

            pureIndices = pureIndices.matching(index -> !index.isSystem() || request.systemIndexAccess.isAllowed(index.nameForIndexPatternMatching()));


            pureIndices = pureIndices.matching(index -> shouldIncludeIndexLikeForAllLocal(index, includeHidden, request.allIndicesFailureStore, scope));
            aliases = aliases.matching(alias -> shouldIncludeIndexLikeForAllLocal(alias, includeHidden, request.allIndicesFailureStore, scope));
            dataStreams = dataStreams.matching(dataStream -> shouldIncludeIndexLikeForAllLocal(dataStream, includeHidden, request.allIndicesFailureStore, scope));


            return new Local(pureIndices, aliases, dataStreams, nonExistingIndices);
        }

        private static boolean shouldIncludeIndexLikeForAllLocal(Meta.IndexLikeObject indexLike, boolean includeHidden,
                boolean allIndicesFailureStoreSelector, IndicesRequestInfo.Scope scope) {
            if (allIndicesFailureStoreSelector) {
                // only indices related to a failure store should be returned
                // expressions like "*::failures" or "_all::failures"
                return indexLike.isFailureStoreRelated();
            } else if (scope == IndicesRequestInfo.Scope.INDICES_DATA_STREAMS) {
                // INDICES_DATA_STREAMS scope is used for admin/alias requests that don't support component selectors.
                // For example, GET _alias/alias_c1 resolves its index part (implicit _all) with this scope.
                // If we included failure-store-related objects like ds_b3::failures here, SG would pass them
                // to ES when replacing indices for limited users, causing:
                //   unsupported_selector_exception: "Index component selectors are not supported in this context
                //   but found selector in expression [ds_b3::failures]"
                return !indexLike.isFailureStoreRelated();
            } else {
                // data selector "*::data" or "_all::data"
                // or no component selector "*" or "_all"
                if (includeHidden) {
                    // expand_wildcards=all: include both data and failure store components
                    return true;
                } else {
                    // include only index-like objects related to data component
                    return !indexLike.isFailureStoreRelated();
                }
            }
        }

        static ResolvedIndices.Local resolveWithoutPatterns(IndicesRequestInfo request, Meta indexMetadata) {
            IndicesRequestInfo.Scope scope = request.scope;

            ImmutableSet.Builder<Meta.Index> indices = new ImmutableSet.Builder<>();
            ImmutableSet.Builder<Meta.NonExistent> nonExistingIndices = new ImmutableSet.Builder<>();
            ImmutableSet.Builder<Meta.Alias> aliases = new ImmutableSet.Builder<>();
            ImmutableSet.Builder<Meta.DataStream> dataStreams = new ImmutableSet.Builder<>();

            for (IndexExpression indexExpression : request.localIndices) {
                IndexExpression resolvedIndexExpression = indexExpression.mapBaseName(DateMathExpressionResolver::resolveExpression);

                if (resolvedIndexExpression.containsWildcard()) {
                    continue;
                }

                Meta.IndexLikeObject indexLike = indexMetadata.getIndexOrLike(resolvedIndexExpression.metaName());

                if (scope == IndicesRequestInfo.Scope.INDEX) {
                    if (indexLike instanceof Meta.Index) {
                        indices.add((Meta.Index) indexLike);
                    } else {
                        indices.add(Meta.Index.nonExistent(resolvedIndexExpression.metaName()));
                    }
                } else if (scope == IndicesRequestInfo.Scope.ALIAS) {
                    if (indexLike instanceof Meta.Alias) {
                        aliases.add((Meta.Alias) indexLike);
                    } else {
                        aliases.add(Meta.Alias.nonExistent(resolvedIndexExpression.metaName()));
                    }
                } else if (scope == IndicesRequestInfo.Scope.DATA_STREAM) {
                    if (indexLike instanceof Meta.DataStream) {
                        dataStreams.add((Meta.DataStream) indexLike);
                    } else {
                        dataStreams.add(Meta.DataStream.nonExistent(resolvedIndexExpression.metaName()));
                    }
                } else {
                    if (indexLike == null) {
                        nonExistingIndices.add(Meta.NonExistent.of(resolvedIndexExpression.metaName()));
                    } else if (indexLike instanceof Meta.Alias) {
                        if (scope.includeAliases) {
                            aliases.add((Meta.Alias) indexLike);
                        } else {
                            nonExistingIndices.add(Meta.NonExistent.of(resolvedIndexExpression.metaName()));
                        }
                    } else if (indexLike instanceof Meta.DataStream) {
                        if (scope.includeDataStreams) {
                            dataStreams.add((Meta.DataStream) indexLike);
                        } else {
                            nonExistingIndices.add(Meta.NonExistent.of(resolvedIndexExpression.metaName()));
                        }
                    } else {
                        if (scope.includeIndices) {
                            indices.add((Meta.Index) indexLike);
                        } else {
                            nonExistingIndices.add(Meta.NonExistent.of(resolvedIndexExpression.metaName()));
                        }
                    }
                }

            }

            ImmutableSet<Meta.Index> pureIndices = indices.build();

            if ((aliases.size() != 0 || dataStreams.size() != 0) && scope != IndicesRequestInfo.Scope.ANY_DISTINCT) {
                // If there are aliases or dataStreams, remove the indices that are part of these aliases or dataStreams

                pureIndices = pureIndices.matching(index -> {
                    if (index.parentDataStreamName() != null && dataStreams.contains(index.parentDataStream())) {
                        return false;
                    } else if (aliases.containsAny(index.parentAliases())) {
                        return false;
                    } else {
                        return true;
                    }
                });
            }

            return new Local(pureIndices, aliases.build(), dataStreams.build(), nonExistingIndices.build());
        }

        static ResolvedIndices.Local resolveDataStreamsWithoutPatterns(IndicesRequestInfo request, Meta indexMetadata) {
            ImmutableSet.Builder<Meta.DataStream> dataStreams = new ImmutableSet.Builder<>();

            for (IndexExpression indexExpression : request.localIndices) {
                String resolved = DateMathExpressionResolver.resolveExpression(indexExpression.baseName());

                if (ActionRequestIntrospector.containsWildcard(resolved)) {
                    continue;
                }

                IndexExpression resolvedWithComponent = indexExpression.withIndexName(resolved);
                Meta.IndexLikeObject indexLike = indexMetadata.getIndexOrLike(resolvedWithComponent.metaName());

                if (indexLike == null) {
                    dataStreams.add(Meta.DataStream.nonExistent(resolvedWithComponent.metaName()));
                } else if (indexLike instanceof Meta.DataStream) {
                    dataStreams.add((Meta.DataStream) indexLike);
                }
            }

            return new Local(ImmutableSet.empty(), ImmutableSet.empty(), dataStreams.build(), ImmutableSet.empty());
        }

        static ResolvedIndices.Local resolveAliasesWithoutPatterns(IndicesRequestInfo request, Meta indexMetadata) {
            ImmutableSet.Builder<Meta.Alias> aliases = new ImmutableSet.Builder<>();

            for (IndexExpression indexExpression : request.localIndices) {
                IndexExpression resolved = indexExpression.mapBaseName(DateMathExpressionResolver::resolveExpression);

                if (resolved.containsWildcard()) {
                    continue;
                }

                Meta.IndexLikeObject indexLike = indexMetadata.getIndexOrLike(resolved.metaName());

                if (indexLike == null) {
                    aliases.add(Meta.Alias.nonExistent(resolved.metaName()));
                } else if (indexLike instanceof Meta.Alias) {
                    aliases.add((Meta.Alias) indexLike);
                }
            }

            return new Local(ImmutableSet.empty(), aliases.build(), ImmutableSet.empty(), ImmutableSet.empty());
        }

        static final ResolvedIndices.Local EMPTY = new Local(ImmutableSet.empty(), ImmutableSet.empty(), ImmutableSet.empty(), ImmutableSet.empty());

        private static ImmutableSet<IndexExpression> resolveDateMathExpressions(Collection<IndexExpression> indices) {
            ImmutableSet<IndexExpression> result = ImmutableSet.empty();

            for (IndexExpression indexExpression : indices) {
                String resolved = DateMathExpressionResolver.resolveExpression(indexExpression.baseName());
                result = result.with(indexExpression.withIndexName(resolved));
            }

            return result;
        }

        @Override
        public int hashCode() {
            return union.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            } else if (obj instanceof ResolvedIndices.Local) {
                return this.union.equals(((ResolvedIndices.Local) obj).union);
            } else {
                return false;
            }
        }

        private static void resolveNegationUpAndDown(String index, Set<String> excludeNames, Set<String> partiallyExcludedObjects,
                IndicesRequestInfo request, Meta indexMetadata) {
            Meta.IndexLikeObject indexLikeObject = indexMetadata.getIndexOrLike(index);

            excludeNames.add(index);

            if (indexLikeObject != null) {
                if (indexLikeObject.parentDataStreamName() != null) {
                    partiallyExcludedObjects.add(indexLikeObject.parentDataStreamName());
                    // Also mark ancestor aliases of the parent DataStream as partially excluded.
                    //
                    // Concrete example — request "index_c1,*,-.ds-ds_a1*" with expand_wildcards=all,
                    // where alias_ab1 = {ds_a1, ds_a2, ds_a3, ds_b1}:
                    //
                    //   -.ds-ds_a1* → WildcardExpressionResolver returns the ten concrete backing
                    //   indices .ds-ds_a1-<date>-000001 … 000010 (no DataStream entries, because
                    //   ".ds-ds_a1" is a key prefix that only covers backing index names).
                    //
                    //   For each backing index this method is called, setting:
                    //     excludeNames          = { .ds-ds_a1-<date>-000001, …, 000010 }
                    //     partiallyExcludedObjects = { ds_a1 }   ← added two lines above
                    //
                    //   The ancestor alias alias_ab1 is NOT yet in excludeNames or
                    //   partiallyExcludedObjects, so during the positive * expansion it would be
                    //   picked up by the happy-case branch and added to the resolved union as a whole.
                    //   This causes two problems:
                    //
                    //   Problem 1 — alias leaks ds_a1 through the resolved union:
                    //     alias_ab1 is in the resolved union; the privilege evaluator resolves
                    //     incomplete aliases into their member DataStreams (alias_ab1 → ds_a1, ds_a2,
                    //     ds_a3, ds_b1). A user with ds_a* permissions can access ds_a1, so it is
                    //     added to the available-indices set used to rewrite the request.
                    //     ES then receives ds_a1 and expands it to all its backing indices — exactly
                    //     what the negation was supposed to prevent.
                    //
                    //   Problem 2 — partial-exclusion branch is never triggered for alias_ab1:
                    //     isNegationOnlyEffectiveForIndicesAndForOtherNonWildcardObjects() returns
                    //     true for search (ANY scope), so the positive loop only checks excludeNames
                    //     for ConcreteIndex entries. alias_ab1 is an Alias, not a ConcreteIndex, so
                    //     it is not skipped — the partial-exclusion branch that would strip ds_a1 is
                    //     never reached.
                    //
                    // By adding alias_ab1 to partiallyExcludedObjects here, both problems are solved:
                    //   partiallyExcludedObjects = { ds_a1, alias_ab1 }
                    //
                    // - alias_ab1 takes the partial-exclusion branch during * expansion. Its member
                    //   ds_a1 has all backing indices in excludeNames, so nothing is added for it.
                    //   Its members ds_a2, ds_a3, ds_b1 have no excluded backing indices, so they
                    //   are added as individual DataStreams. alias_ab1 itself never enters the union.
                    // - The privilege evaluator never sees alias_ab1, so it cannot resolve it into
                    //   ds_a1, and the leak is prevented.
                    Meta.IndexLikeObject parentDS = indexMetadata.getIndexOrLike(indexLikeObject.parentDataStreamName());
                    if (parentDS != null) {
                        partiallyExcludedObjects.addAll(parentDS.parentAliasNames());
                    }
                }
                partiallyExcludedObjects.addAll(indexLikeObject.parentAliasNames());
                if (indexLikeObject instanceof Meta.IndexCollection) {
                    excludeNames.addAll(((Meta.IndexCollection) indexLikeObject).resolveDeepToNames(Meta.Alias.ResolutionMode.NORMAL));
                    if (indexLikeObject instanceof Meta.Alias) {
                        excludeNames.addAll(
                                ((Meta.Alias) indexLikeObject).members().stream().map(Meta.IndexLikeObject::name).collect(Collectors.toList()));
                    }
                }
            }
        }

    }

}