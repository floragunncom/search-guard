ARG ES_VERSION

# This is either "" or "-oss" and only relevant for ES 7.10.2
# For ES versions below 7.11.0 there was ASLv2 only oss builds
ARG ES_FLAVOUR

FROM docker.elastic.co/elasticsearch/elasticsearch${ES_FLAVOUR}:${ES_VERSION} as base

# Install repository-azure repository-gcs repository-hdfs analysis-icu plugins analysis-phonetic
# We want this for Helm charts to enable automatic backups
# For ES we skip this because the plugin are included in the ES8 image by default
RUN \ 
    if [ "$(echo $ES_VERSION | cut -c1-1)" = "7" ]; \
      then \
        for plugin in repository-s3 repository-azure repository-gcs repository-hdfs analysis-icu analysis-phonetic; do \
          bin/elasticsearch-plugin install --verbose --batch "https://artifacts.elastic.co/downloads/elasticsearch-plugins/${plugin}/${plugin}-${ES_VERSION}.zip"; \
        done; \
    fi

# Search Guard plugin
ARG SG_VERSION
ARG ES_VERSION
ARG ES_FLAVOUR

FROM base as non-flx
RUN echo $ES_VERSION | cut -c1-1 > /major
ENV SG_FULLVERSION=${ES_VERSION}-${SG_VERSION}
RUN elasticsearch-plugin install --batch https://maven.search-guard.com/search-guard-suite-release/com/floragunn/search-guard-suite-plugin/${SG_FULLVERSION}/search-guard-suite-plugin-${SG_FULLVERSION}.zip \
    && chmod +x plugins/search-guard-$(cat /major)/tools/*.sh

FROM base as flx
ENV SG_FULLVERSION=${SG_VERSION}-es-${ES_VERSION}
RUN elasticsearch-plugin install --batch https://maven.search-guard.com/search-guard-flx-release/com/floragunn/search-guard-flx-elasticsearch-plugin/${SG_FULLVERSION}/search-guard-flx-elasticsearch-plugin-${SG_FULLVERSION}.zip
