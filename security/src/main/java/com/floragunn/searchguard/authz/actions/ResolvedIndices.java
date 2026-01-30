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
                                request.indicesOptions().expandWildcardsHidden() //
                                        ? indexMetadata.dataStreams()//
                                        : indexMetadata.nonHiddenDataStreams(), //
                                ImmutableSet.empty());
                    } else if (request.expandWildcards) {
                        return resolveWithPatterns(request, indexMetadata);
                    } else if (request.createIndexRequest) {
                        return new Local(ImmutableSet.empty(), ImmutableSet.empty(),
                                ImmutableSet.of(resolveDateMathExpressions(request.localIndices).map(Meta.DataStream::nonExistent)),
                                ImmutableSet.empty());
                    } else {
                        return resolveDataStreamsWithoutPatterns(request, indexMetadata);
                    }
                } else if (request.scope == IndicesRequestInfo.Scope.ALIAS) {
                    if (request.isAll) {
                        return new Local(ImmutableSet.empty(), indexMetadata.aliases(), ImmutableSet.empty(), ImmutableSet.empty());
                    } else if (request.expandWildcards) {
                        return resolveWithPatterns(request, indexMetadata);
                    } else if (request.createIndexRequest) {
                        return new Local(ImmutableSet.empty(),
                                ImmutableSet.of(resolveDateMathExpressions(request.localIndices).map(Meta.Alias::nonExistent)), ImmutableSet.empty(),
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
            Set<String> partiallyExcludedObjects = new HashSet<>();

            // Note: We are going backwards through the list of indices in order to get negated patterns before the patterns they apply to
            for (int i = request.localIndices.size() - 1; i >= 0; i--) {
                String index = request.localIndices.get(i);

                if (index.startsWith("-")) {
                    index = index.substring(1);
                    index = DateMathExpressionResolver.resolveExpression(index);

                    if (index.contains("*")) {
                        Map<String, IndexAbstraction> matchedAbstractions = WildcardExpressionResolver.matches(metadata, indicesLookup, index,
                                request.indicesOptions(), includeDataStreams);

                        for (String resolvedIndex : matchedAbstractions.keySet()) {
                            resolveNegationUpAndDown(resolvedIndex, excludeNames, partiallyExcludedObjects, request, indexMetadata);
                        }
                    } else {
                        resolveNegationUpAndDown(index, excludeNames, partiallyExcludedObjects, request, indexMetadata);
                    }
                } else {
                    index = DateMathExpressionResolver.resolveExpression(index);

                    if (index.contains("*")) {

                        Map<String, IndexAbstraction> matchedAbstractions = WildcardExpressionResolver.matches(metadata, indicesLookup, index,
                                request.indicesOptions(), includeDataStreams);

                        for (Map.Entry<String, IndexAbstraction> entry : matchedAbstractions.entrySet()) {
                            if (excludeNames.contains(entry.getKey())) {
                                continue;
                            }

                            if (!partiallyExcludedObjects.contains(entry.getKey()) || scope == IndicesRequestInfo.Scope.ALIAS
                                    || scope == IndicesRequestInfo.Scope.DATA_STREAM || scope == IndicesRequestInfo.Scope.INDICES_DATA_STREAMS) {
                                // This is the happy case, just include the object
                                IndexAbstraction indexAbstraction = entry.getValue();

                                if (indexAbstraction instanceof DataStream) {
                                    if (includeDataStreams) {
                                        dataStreams.add((Meta.DataStream) indexMetadata.getIndexOrLike(entry.getKey()));
                                    }
                                } else if (indexAbstraction instanceof IndexAbstraction.Alias) {
                                    if (includeAliases) {
                                        aliases.add((Meta.Alias) indexMetadata.getIndexOrLike(entry.getKey()));
                                    }
                                } else {
                                    if (includeIndices) {
                                        Meta.Index indexMeta = (Meta.Index) indexMetadata.getIndexOrLike(entry.getKey());

                                        if (!indexMeta.isSystem() || request.systemIndexAccess.isAllowed(entry.getKey())) {
                                            indices.add(indexMeta);
                                        }
                                    }
                                }
                            } else {
                                // Oh dear, one of the negated elements is a member of this. Now we have to resolve this to perform the exclusion

                                Meta.IndexLikeObject indexLike = indexMetadata.getIndexOrLike(entry.getKey());

                                if (indexLike instanceof Meta.IndexCollection) {
                                    for (Meta.IndexLikeObject member : ((Meta.IndexCollection) indexLike).members()) {
                                        if (!excludeNames.contains(member.name())) {
                                            if (member instanceof Meta.Index) {
                                                if (includeIndices) {
                                                    Meta.Index indexMeta = (Meta.Index) member;
                                                    if (!indexMeta.isSystem() || request.systemIndexAccess.isAllowed(entry.getKey())) {
                                                        indices.add(indexMeta);
                                                    }
                                                }
                                            } else if (member instanceof Meta.DataStream) {
                                                if (includeDataStreams) {
                                                    Meta.DataStream dataStream = (Meta.DataStream) member;
                                                    if (dataStream.members().stream().anyMatch(dsMember -> excludeNames.contains(dsMember.name()))) {
                                                        for (Meta.IndexLikeObject dsMember : dataStream.members()) {
                                                            if (!excludeNames.contains(dsMember.name())) {
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
                        if (excludeNames.contains(index)) {
                            continue;
                        }

                        Meta.IndexLikeObject indexLikeObject = indexMetadata.getIndexOrLike(index);

                        if (indexLikeObject == null) {
                            if (scope == IndicesRequestInfo.Scope.DATA_STREAM) {
                                dataStreams.add(Meta.DataStream.nonExistent(index));
                            } else if (scope == IndicesRequestInfo.Scope.ALIAS) {
                                aliases.add(Meta.Alias.nonExistent(index));
                            } else {
                                nonExistingIndices.add(Meta.NonExistent.of(index));
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

                                if (!indexMeta.isSystem() || request.systemIndexAccess.isAllowed(indexMeta)) {
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

            pureIndices = pureIndices.matching(index -> !index.isSystem() || request.systemIndexAccess.isAllowed(index));

            return new Local(pureIndices, aliases, dataStreams, nonExistingIndices);
        }

        static ResolvedIndices.Local resolveWithoutPatterns(IndicesRequestInfo request, Meta indexMetadata) {
            IndicesRequestInfo.Scope scope = request.scope;

            ImmutableSet.Builder<Meta.Index> indices = new ImmutableSet.Builder<>();
            ImmutableSet.Builder<Meta.NonExistent> nonExistingIndices = new ImmutableSet.Builder<>();
            ImmutableSet.Builder<Meta.Alias> aliases = new ImmutableSet.Builder<>();
            ImmutableSet.Builder<Meta.DataStream> dataStreams = new ImmutableSet.Builder<>();

            for (String index : request.localIndices) {
                String resolved = DateMathExpressionResolver.resolveExpression(index);

                if (ActionRequestIntrospector.containsWildcard(resolved)) {
                    continue;
                }

                Meta.IndexLikeObject indexLike = indexMetadata.getIndexOrLike(resolved);

                if (scope == IndicesRequestInfo.Scope.INDEX) {
                    if (indexLike instanceof Meta.Index) {
                        indices.add((Meta.Index) indexLike);
                    } else {
                        indices.add(Meta.Index.nonExistent(resolved));
                    }
                } else if (scope == IndicesRequestInfo.Scope.ALIAS) {
                    if (indexLike instanceof Meta.Alias) {
                        aliases.add((Meta.Alias) indexLike);
                    } else {
                        aliases.add(Meta.Alias.nonExistent(resolved));
                    }
                } else if (scope == IndicesRequestInfo.Scope.DATA_STREAM) {
                    if (indexLike instanceof Meta.DataStream) {
                        dataStreams.add((Meta.DataStream) indexLike);
                    } else {
                        dataStreams.add(Meta.DataStream.nonExistent(resolved));
                    }
                } else {
                    if (indexLike == null) {
                        nonExistingIndices.add(Meta.NonExistent.of(resolved));
                    } else if (indexLike instanceof Meta.Alias) {
                        if (scope.includeAliases) {
                            aliases.add((Meta.Alias) indexLike);
                        } else {
                            nonExistingIndices.add(Meta.NonExistent.of(resolved));
                        }
                    } else if (indexLike instanceof Meta.DataStream) {
                        if (scope.includeDataStreams) {
                            dataStreams.add((Meta.DataStream) indexLike);
                        } else {
                            nonExistingIndices.add(Meta.NonExistent.of(resolved));
                        }
                    } else {
                        if (scope.includeIndices) {
                            indices.add((Meta.Index) indexLike);
                        } else {
                            nonExistingIndices.add(Meta.NonExistent.of(resolved));
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

            for (String index : request.localIndices) {
                String resolved = DateMathExpressionResolver.resolveExpression(index);

                if (ActionRequestIntrospector.containsWildcard(resolved)) {
                    continue;
                }

                Meta.IndexLikeObject indexLike = indexMetadata.getIndexOrLike(resolved);

                if (indexLike == null) {
                    dataStreams.add(Meta.DataStream.nonExistent(resolved));
                } else if (indexLike instanceof Meta.DataStream) {
                    dataStreams.add((Meta.DataStream) indexLike);
                }
            }

            return new Local(ImmutableSet.empty(), ImmutableSet.empty(), dataStreams.build(), ImmutableSet.empty());
        }

        static ResolvedIndices.Local resolveAliasesWithoutPatterns(IndicesRequestInfo request, Meta indexMetadata) {
            ImmutableSet.Builder<Meta.Alias> aliases = new ImmutableSet.Builder<>();

            for (String index : request.localIndices) {
                String resolved = DateMathExpressionResolver.resolveExpression(index);

                if (ActionRequestIntrospector.containsWildcard(resolved)) {
                    continue;
                }

                Meta.IndexLikeObject indexLike = indexMetadata.getIndexOrLike(resolved);

                if (indexLike == null) {
                    aliases.add(Meta.Alias.nonExistent(resolved));
                } else if (indexLike instanceof Meta.Alias) {
                    aliases.add((Meta.Alias) indexLike);
                }
            }

            return new Local(ImmutableSet.empty(), aliases.build(), ImmutableSet.empty(), ImmutableSet.empty());
        }

        static final ResolvedIndices.Local EMPTY = new Local(ImmutableSet.empty(), ImmutableSet.empty(), ImmutableSet.empty(), ImmutableSet.empty());

        private static ImmutableSet<String> resolveDateMathExpressions(Collection<String> indices) {
            ImmutableSet<String> result = ImmutableSet.empty();

            for (String index : indices) {
                result = result.with(DateMathExpressionResolver.resolveExpression(index));
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

            if (request.isNegationOnlyEffectiveForIndices() && !(indexLikeObject instanceof Meta.Index)) {
                // Negation is implemented in ES inconsistently:
                // Some actions (like search) only perform it properly on indicies, but not on aliases and data streams. Thus /my_datastreams_*,-my_datastream_1/ does not have the effect one might expect.
                // Other actions do implement it properly. The flag request.isNegationOnlyEffectiveForIndices() indicated the way we need to use
                return;
            }

            excludeNames.add(index);

            if (indexLikeObject != null) {
                if (indexLikeObject.parentDataStreamName() != null) {
                    partiallyExcludedObjects.add(indexLikeObject.parentDataStreamName());
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