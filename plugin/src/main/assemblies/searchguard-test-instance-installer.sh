#!/bin/bash

set -e

echo "Welcome to the Search Guard demo instance installation tool!"
echo ""
echo "This tool will download Search Guard and Elasticsearch to the current directory on your computer and create a demo instance."
echo "All files downloaded here can be found on https://docs.search-guard.com/latest/search-guard-versions or on the websites of Elastic."
echo ""

# ------------------------------------------------------------------------
# Set up variables identifying the components to be used and the 
# installation directories
# ------------------------------------------------------------------------
# Mnemomic: 
# - SG: Search Guard
# - SB: Search Backend (i.e., Elasticsearch)
# - SF: Search Frontend (i.e., Kibana)
# ------------------------------------------------------------------------

BASE_DIR=$(pwd)
SG_VERSION_PRE="${project.version}"
SG_VERSION="${1:-$SG_VERSION_PRE}"
SGSF_VERSION="${2:-$SG_VERSION_PRE}"
SG_REPOSITORY="${3:-search-guard-flx-release}"
SG_PLUGIN_NAME="search-guard-flx"
SGCTL_VERSION="3.1.3"
TLS_TOOL_VERSION="3.1.3"

ARCH_DETECTED="$(arch)"
ARCH="x86_64"

if [[ "$ARCH_DETECTED" == *"arm"* ]] || [[ "$ARCH_DETECTED" == "aarch64" ]]; then
  ARCH="aarch64"
fi

ES_VERSION=$(echo $SG_VERSION | sed -n 's/^.*-es-\(.*\)*$/\1/p')
SB_NAME="Elasticsearch"
SB_LC_NAME="elasticsearch"
SF_NAME="Kibana"
SF_LC_NAME_CC="kibana"
SF_LC_NAME="kibana" 

if [[ "$OSTYPE"  == "linux"* ]]; then
  SB_ARCHIVE="elasticsearch-$ES_VERSION-linux-$ARCH.tar.gz"
  SF_ARCHIVE="kibana-$ES_VERSION-linux-$ARCH.tar.gz"
elif [[ "$OSTYPE" == "darwin"* ]]; then
  SB_ARCHIVE="elasticsearch-$ES_VERSION-darwin-$ARCH.tar.gz"
  SF_ARCHIVE="kibana-$ES_VERSION-darwin-$ARCH.tar.gz"
else
  echo "OS type $OSTYPE not supported"
  exit
fi   

SG_PLUGIN_FILE_NAME="search-guard-flx-$SB_LC_NAME-plugin-$SG_VERSION.zip"
SGSF_PLUGIN_FILE_NAME="search-guard-flx-$SF_LC_NAME-plugin-$SGSF_VERSION.zip"

SG_LINK="https://maven.search-guard.com/$SG_REPOSITORY/com/floragunn/search-guard-flx-$SB_LC_NAME-plugin/$SG_VERSION/$SG_PLUGIN_FILE_NAME"
SGSF_LINK="https://maven.search-guard.com/$SG_REPOSITORY/com/floragunn/search-guard-flx-$SF_LC_NAME-plugin/$SGSF_VERSION/$SGSF_PLUGIN_FILE_NAME"
SGCTL_LINK="https://maven.search-guard.com/search-guard-flx-release/com/floragunn/sgctl/$SGCTL_VERSION/sgctl-$SGCTL_VERSION.sh"

TLS_TOOL="search-guard-tlstool"
TLS_TOOL_ARCHIVE="$TLS_TOOL-${TLS_TOOL_VERSION}.tar.gz"
TLS_TOOL_LINK="https://maven.search-guard.com/artifactory/list/search-guard-tlstool/com/floragunn/search-guard-tlstool/${TLS_TOOL_VERSION}/$TLS_TOOL_ARCHIVE"

# ------------------------------------------------------------------------
# Download all necessary components
# ------------------------------------------------------------------------

echo "Downloading the Search Guard Elasticsearch plugin $SG_VERSION from $SG_LINK ... "
curl --fail "$SG_LINK" -o $SG_PLUGIN_FILE_NAME
echo

echo "Downloading the Search Guard Kibana plugin from $SGSF_LINK ... "
curl --fail "$SGSF_LINK" -o $SGSF_PLUGIN_FILE_NAME
echo

echo "Downloading Elasticsearch $ES_VERSION from https://artifacts.elastic.co/downloads/elasticsearch/$SB_ARCHIVE ..."
curl --fail "https://artifacts.elastic.co/downloads/elasticsearch/$SB_ARCHIVE" -o $SB_ARCHIVE
echo

echo "Downloading Kibana $ES_VERSION from https://artifacts.elastic.co/downloads/kibana/$SF_ARCHIVE ... "
curl --fail "https://artifacts.elastic.co/downloads/kibana/$SF_ARCHIVE" -o $SF_ARCHIVE

echo
echo "Downloading the TLS Tool from $TLS_TOOL_LINK ... "
curl --fail "$TLS_TOOL_LINK" -o $TLS_TOOL_ARCHIVE

echo
echo "Extracting $TLS_TOOL_ARCHIVE to $TLS_TOOL ... "
mkdir $TLS_TOOL
tar xfz "$TLS_TOOL_ARCHIVE" -C "$TLS_TOOL"

echo
echo "Downloading sgctl from $SGCTL_LINK"
curl --fail "$SGCTL_LINK" -o sgctl.sh
chmod u+x sgctl.sh

echo
echo "Extracting $SB_ARCHIVE to $SB_LC_NAME ... "

mkdir $SB_LC_NAME

tar xfz "$SB_ARCHIVE" -C "$SB_LC_NAME" --strip-components 1

cd "$SB_LC_NAME"
SB_INSTALL_DIR=$(pwd)

# ------------------------------------------------------------------------
# Install the Search Guard Plugin
# ------------------------------------------------------------------------

echo
echo "Installing the Search Guard plugin using bin/$SB_LC_NAME-plugin install -s -b file:///$BASE_DIR/$SG_PLUGIN_FILE_NAME ... "
bin/$SB_LC_NAME-plugin install -s -b file:///$BASE_DIR/$SG_PLUGIN_FILE_NAME

# ------------------------------------------------------------------------
# Create Demo Configuration
# ------------------------------------------------------------------------

# ------------------------------------------------------------------------
# JVM Settings memory settings
#
# We don't need lots of RAM for a demo instance; thus, we limit it here
# ------------------------------------------------------------------------
echo "-Xms1g" >>config/jvm.options
echo "-Xmx1g" >>config/jvm.options

cd ..

echo
echo "Applying demo configuration ... "

# ------------------------------------------------------------------------
# Create Certificates.
#
# All communication with the cluster and within the cluster is secured
# by TLS. This is setting up the necessary certificate files.
#
# Our offline TLS tool is used to generate all required certificates.
# The TLS Tool doc can be found
# here https://docs.search-guard.com/latest/search-guard-versions.
# ------------------------------------------------------------------------

# ------------------------------------------------------------------------
# The TLS tool uses a YAML configuration file to generate the certificates.
# ------------------------------------------------------------------------


cat >>$TLS_TOOL/config/tlsconfig.yml << EOM

ca:
   root:
      dn: CN=Example Com Inc. Root CA,OU=Example Com Inc. Root CA,O=Example Com Inc.,L=test,C=de
      keysize: 2048
      validityDays: 3650
      pkPassword: auto
      file: root-ca.pem

defaults:
      validityDays: 3650
      pkPassword: none
      generatedPasswordLength: 12
      nodeOid: "1.2.3.4.5.5"

nodes:
  - name: node
    dn: CN=node-0.example.com,OU=node,O=node,L=test,C=de
    dns: localhost
    ip: 127.0.0.1

clients:
  - name: admin
    dn: CN=kirk,OU=client,O=client,L=test,C=de
    pkPassword: auto
    admin: true

EOM

# ------------------------------------------------------------------------
# Generate the root, node and admin certificate.
# ------------------------------------------------------------------------

echo
echo "Generate certificates ... "
mkdir $TLS_TOOL/certificates
$TLS_TOOL/tools/sgtlstool.sh -c $TLS_TOOL/config/tlsconfig.yml -ca -crt -t $TLS_TOOL/certificates

mv $TLS_TOOL/certificates/root-ca.key $TLS_TOOL/certificates/root-ca-key.pem
mv $TLS_TOOL/certificates/node.key $TLS_TOOL/certificates/node-key.pem
mv $TLS_TOOL/certificates/admin.key $TLS_TOOL/certificates/admin-key.pem

ADMIN_PRIVATE_CERTIFICATE_PASSWORD=$(cat $TLS_TOOL/certificates/client-certificates.readme | grep "CN=kirk,OU=client,O=client,L=test,C=de Password:" | sed 's/CN=kirk,OU=client,O=client,L=test,C=de Password: //')

# ------------------------------------------------------------------------
# The TLS Tool has created the following files:
# root-ca.pem - the root certificate
# root-ca-key.pem - the private key for the root certificate
# node.pem - the node certificate
# node-key.pem - the private key for the node certificate
# admin.pem - the admin certificate
# admin-key.pem - the private key for the admin certificate
#
# ------------------------------------------------------------------------
# The file root-ca.pem is the certificate of the root CA which created all
# other certificates. By having (and trusting) this certificate, it is
# possibly to verify all other certificates.
#
# The files node.pem and node-key.pem are used by your only node
# in the cluster to identify itself.
#
# The files admin.pem and admin-key.pem can be used by administrators
# to get administrative access to the cluster.
#
# ------------------------------------------------------------------------

cp $TLS_TOOL/certificates/root-ca.pem $SB_LC_NAME/config/root-ca.pem

cp $TLS_TOOL/certificates/node.pem $SB_LC_NAME/config/node.pem
cp $TLS_TOOL/certificates/node-key.pem $SB_LC_NAME/config/node-key.pem

cp $TLS_TOOL/certificates/admin.pem admin.pem
cp $TLS_TOOL/certificates/admin-key.pem admin-key.pem

cd "$SB_LC_NAME"

# -----------------------------------------------------------
# Mark the start of the generated config in elasticsearch.yml
# -----------------------------------------------------------
cat >>config/$SB_LC_NAME.yml << EOM

# -----------------------------------------------------------
# Search Guard Demo Configuration
# -----------------------------------------------------------

EOM

# ------------------------------------------------------------
# These entries in elasticsearch.yml make the
# root CA known to Search Guard
# ------------------------------------------------------------

cat >>config/$SB_LC_NAME.yml << EOM
# -----
# The root certificate that is used to check the authenticity
# of all other certificates presented to Search Guard
#
searchguard.ssl.transport.pemtrustedcas_filepath: root-ca.pem
searchguard.ssl.http.pemtrustedcas_filepath: root-ca.pem

EOM

# ------------------------------------------------------------
# These entries in elasticsearch.yml tell the
# node to use the node certificates installed above
# ------------------------------------------------------------

cat >>config/$SB_LC_NAME.yml << EOM
# -----
# The current node uses this certificate and private key
# for identifying to other nodes and clients
#
searchguard.ssl.transport.pemcert_filepath: node.pem
searchguard.ssl.transport.pemkey_filepath: node-key.pem
searchguard.ssl.http.pemcert_filepath: node.pem
searchguard.ssl.http.pemkey_filepath: node-key.pem

EOM

# ------------------------------------------------------------------------
# The cluster identifies clients as administrators if:
# - The connection is trusted (untrusted connections would be refused anyway)
# - And if the distinguished name (DN) of the client certificate is
# listed in the configuration option searchguard.authcz.admin_dn
# ------------------------------------------------------------------------

cat >>config/$SB_LC_NAME.yml << EOM
# ---------
# In order to apply changes to the Search Guard configuration index, an admin
# TLS cetificate is required. This line tells Search Guard that the DN of the
# admin.pem certificate denotes an admin certificate and should be granted
# elevated privileges:
#
searchguard.authcz.admin_dn:
  - CN=kirk,OU=client,O=client,L=test,C=de
EOM

# ------------------------------------------------------------------------
# Use default demo configuration for users and roles
# ------------------------------------------------------------------------

cat >>config/$SB_LC_NAME.yml << EOM
# -----
# The setting searchguard.allow_default_init_sgindex makes Search Guard
# to intialize itself with default user and roles settings found in
# plugins/search-guard/sgconfig. In normal setups, Search Guard will
# initialize without any users (except users identified by admin certs)
#
searchguard.allow_default_init_sgindex: true

EOM

# ------------------------------------------------------------------------
# Basic Configuration
#
# Some more configuration to get the cluster running
# ------------------------------------------------------------------------

cat >>config/$SB_LC_NAME.yml << EOM
searchguard.ssl.transport.enforce_hostname_verification: false
searchguard.ssl.http.enabled: true
searchguard.restapi.roles_enabled: ["SGS_ALL_ACCESS"]
cluster.name: searchguard_demo
cluster.routing.allocation.disk.threshold_enabled: false
EOM

if [[ "$SB_LC_NAME"  == "elasticsearch"* ]]; then
  # Search Guard and X-Pack security cannot be active at the same time. Thus, X-Pack security needs to be disabled
  echo >>config/elasticsearch.yml xpack.security.enabled: false
fi

# ------------------------------------------------------------------------
# sgctl Configuration
#
# This configuration lets sgctl know how to connect to the cluster.
# This way, you don't have to specify connection settings when running
# sgtcl.
# ------------------------------------------------------------------------

mkdir -p ~/.searchguard/

cat >~/.searchguard/cluster_demo.yml << EOM
server: "localhost"
port: 9200
tls:
  trusted_cas: "#{file:$SB_INSTALL_DIR/config/root-ca.pem}"
  client_auth:
    certificate: "#{file:$BASE_DIR/admin.pem}"
    private_key: "#{file:$BASE_DIR/admin-key.pem}"
    private_key_password: $ADMIN_PRIVATE_CERTIFICATE_PASSWORD
EOM

echo >~/.searchguard/sgctl-selected-config.txt demo
  
cd ..

cp -r $SB_LC_NAME/plugins/$SG_PLUGIN_NAME/sgconfig my-sg-config

# ------------------------------------------------------------------------
# Kibana
# ------------------------------------------------------------------------

echo
echo "Extracting $SF_ARCHIVE to $SF_LC_NAME ... "

mkdir $SF_LC_NAME

tar xfz "$SF_ARCHIVE" -C "$SF_LC_NAME" --strip-components 1

cd "$SF_LC_NAME"
SF_INSTALL_DIR=$(pwd)

# ------------------------------------------------------------------------
# Install the Search Guard Plugin
# ------------------------------------------------------------------------

echo "Installing the Search Guard plugin using bin/$SF_LC_NAME-plugin install -s file:///$BASE_DIR/$SGSF_PLUGIN_FILE_NAME ... "
bin/$SF_LC_NAME-plugin install -s file:///$BASE_DIR/$SGSF_PLUGIN_FILE_NAME

# ------------------------------------------------------------------------
# Create Demo Configuration
# ------------------------------------------------------------------------

echo "Applying demo configuration ... "


# -----------------------------------------------------------
# Mark the start of the generated config in kibana.yml
# -----------------------------------------------------------
cat >>config/$SF_LC_NAME_CC.yml << EOM

# -----------------------------------------------------------
# Search Guard Demo Configuration
# -----------------------------------------------------------

EOM


# ------------------------------------------------------------------------
# Tell Kibana to connect by TLS and to provide credentials
# ------------------------------------------------------------------------
cat >>config/$SF_LC_NAME_CC.yml << EOM
# ----- 
# Use HTTPS instead of HTTP. Connections by HTTP are no longer possible.
#
$SB_LC_NAME.hosts: "https://localhost:9200"

# -----
# Connections to the backend must be now authenticated. This is the
# user Kibana uses for internal functionality. 
# This user is different to the users which will log in at Kibana.
#
$SB_LC_NAME.username: "kibanaserver"
$SB_LC_NAME.password: "kibanaserver"

# -----
# Disable SSL verification because we use self-signed demo certificates
#
$SB_LC_NAME.ssl.verificationMode: none

# Whitelist the Search Guard Multi Tenancy Header
$SB_LC_NAME.requestHeadersWhitelist: [ "Authorization", "sgtenant" ]

EOM

if [[ "$SB_LC_NAME"  == "elasticsearch"* ]]; then
  # Suppress Kibana warning about disable X-Pack security
  echo >>config/kibana.yml xpack.security.showInsecureClusterWarning: false
fi

cd ..


echo "Your test setup is now ready to start!"
echo 
echo "You can start $SB_NAME with the Search Guard plugin using this command:"
echo "$SB_LC_NAME/bin/$SB_LC_NAME"
echo
echo "You can start $SF_NAME with the Search Guard plugin using this command:"
echo "$SF_LC_NAME/bin/$SF_LC_NAME"
echo
echo "Note that $SB_NAME must be already running before you can start $SF_NAME. Upon the first start, $SF_NAME will take some time to build browser bundles before it is available."
echo
echo "In order to change the Search Guard configuration while $SB_NAME is running, edit one of the YML files in my-sg-config and upload it using ./sgctl.sh update-config my-sg-config"
echo
echo "You might also want to review the generated configuration files at:"
echo "$SB_LC_NAME/config/$SB_LC_NAME.yml"
echo "$SF_LC_NAME/config/$SF_LC_NAME_CC.yml"

