ARG ES_VERSION


FROM docker.elastic.co/elasticsearch/elasticsearch:${ES_VERSION} as base

# Search Guard plugin
ARG SG_VERSION
ARG ES_VERSION

FROM base as flx
ENV SG_FULLVERSION=${SG_VERSION}-es-${ES_VERSION}
RUN elasticsearch-plugin install --batch https://maven.search-guard.com/search-guard-flx-release/com/floragunn/search-guard-flx-elasticsearch-plugin/${SG_FULLVERSION}/search-guard-flx-elasticsearch-plugin-${SG_FULLVERSION}.zip
