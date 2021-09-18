#!/bin/bash

set -e

BASE_DIR=$(pwd)
SG_VERSION_PRE="${project.version}"
SG_VERSION="${1:-$SG_VERSION_PRE}"
SGSF_VERSION="${2:-$SG_VERSION_PRE}"
SG_REPOSITORY="${3:-search-guard-suite-alpha}"
SG_PLUGIN_NAME="search-guard"
SGCTL_VERSION="0.1.0"

if [[ $SG_VERSION =~ .*-os-.* ]]; then
  OS_VERSION=$(echo $SG_VERSION | cut -d- -f3)
  SB_NAME="OpenSearch"
  SB_LC_NAME="opensearch"
  SF_NAME="OpenSearch Dashboards"
  SF_LC_NAME_CC="opensearch_dashboards"
  SF_LC_NAME="opensearch-dashboards" 
  	  
  if [[ "$OSTYPE"  == "linux"* ]]; then
    SB_ARCHIVE="opensearch-min-$OS_VERSION-linux-x64.tar.gz"
    SF_ARCHIVE="opensearch-dashboards-min-$OS_VERSION-linux-x64.tar.gz"
  else
    echo "OpenSearch is right now not available for type $OSTYPE"
    exit
  fi
else
  ES_VERSION=$(echo $SG_VERSION | cut -d- -f3)
  SB_NAME="Elasticsearch"
  SB_LC_NAME="elasticsearch"
  SG_PLUGIN_NAME="search-guard-7"
  SF_NAME="Kibana"
  SF_LC_NAME_CC="kibana"
  SF_LC_NAME="kibana" 
  
  if [[ "$OSTYPE"  == "linux"* ]]; then
    SB_ARCHIVE="elasticsearch-$ES_VERSION-linux-x86_64.tar.gz"
    SF_ARCHIVE="kibana-$ES_VERSION-linux-x86_64.tar.gz"
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    SB_ARCHIVE="elasticsearch-$ES_VERSION-darwin-x86_64.tar.gz"
    SF_ARCHIVE="kibana-$ES_VERSION-darwin-x86_64.tar.gz"
  else
    echo "OS type $OSTYPE not supported"
    exit
  fi
fi    

SG_PLUGIN_FILE_NAME="search-guard-$SB_LC_NAME-plugin-$SG_VERSION.zip"
SGSF_PLUGIN_FILE_NAME="search-guard-$SF_LC_NAME-plugin-$SGSF_VERSION.zip"

SG_LINK="https://maven.search-guard.com/$SG_REPOSITORY/com/floragunn/search-guard-$SB_LC_NAME-plugin/$SG_VERSION/$SG_PLUGIN_FILE_NAME"
SGSF_LINK="https://maven.search-guard.com/$SG_REPOSITORY/com/floragunn/search-guard-$SF_LC_NAME-plugin/$SGSF_VERSION/$SGSF_PLUGIN_FILE_NAME"
SGCTL_LINK="https://maven.search-guard.com/search-guard-suite-release/com/floragunn/sgctl/$SGCTL_VERSION/sgctl-$SGCTL_VERSION.sh"

echo "Welcome to the Search Guard test instance installation tool!"
echo ""
echo "This tool will download Search Guard and OpenSearch or Elasticsearch to the current directory on your computer and create a test instance."
echo "All files downloaded here can be found on https://docs.search-guard.com/latest/search-guard-versions or on the websites of Elastic or OpenSearch."
echo ""

# ------------------------------------------------------------------------
# Download all necessary components
# ------------------------------------------------------------------------


echo "Downloading Search Guard $SG_VERSION from $SG_LINK ... "
curl --fail "$SG_LINK" -o $SG_PLUGIN_FILE_NAME
echo

if [[ $SG_VERSION =~ .*-os-.* ]]; then
 
  echo "Downloading the Search Guard Search Frontend plugin for OpenSearch Dashboards $SGSF_LINK ... "  
  curl --fail "$SGSF_LINK" -o $SGSF_PLUGIN_FILE_NAME
  echo
  
  echo "Downloading OpenSearch Min $OS_VERSION from https://artifacts.opensearch.org/releases/core/opensearch/$OS_VERSION/$SB_ARCHIVE ..."
  curl --fail "https://artifacts.opensearch.org/releases/core/opensearch/$OS_VERSION/$SB_ARCHIVE" -o $SB_ARCHIVE
  echo
  
  echo "Downloading OpenSearch Dashboards ... "
  curl --fail "https://artifacts.opensearch.org/releases/core/opensearch-dashboards/$OS_VERSION/$SF_ARCHIVE" -o $SF_ARCHIVE

else
  
  echo "Downloading the Search Guard Search Frontend plugin for Kibana from $SGSF_LINK ... "
  curl --fail "$SGSF_LINK" -o $SGSF_PLUGIN_FILE_NAME
  echo
    
  echo "Downloading Elasticsearch $ES_VERSION from https://artifacts.elastic.co/downloads/elasticsearch/$SB_ARCHIVE ..."
  curl --fail "https://artifacts.elastic.co/downloads/elasticsearch/$SB_ARCHIVE" -o $SB_ARCHIVE
  echo  
    
  echo "Downloading Kibana $ES_VERSION from https://artifacts.elastic.co/downloads/kibana/$SF_ARCHIVE ... "
  curl --fail "https://artifacts.elastic.co/downloads/kibana/$SF_ARCHIVE" -o $SF_ARCHIVE
  
fi

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


echo "Applying demo configuration ... "

# ------------------------------------------------------------------------
# Create Demo Certificates. 
#
# All communication with the cluster and within the cluster is secured
# by TLS. This is setting up the necessary certificate files.
#
# In a real-world setup, you would of course generate your own 
# certficates. These prefabricated certificates are just for the demo.
# ------------------------------------------------------------------------

# ------------------------------------------------------------------------
# The file root-ca.pem the certificate of the root CA which created all
# other certificates. By having (and trusting) this certificate, it is 
# possibly to verify all other certificates. 
# ------------------------------------------------------------------------

cat >config/root-ca.pem << EOM
-----BEGIN CERTIFICATE-----
MIID/jCCAuagAwIBAgIBATANBgkqhkiG9w0BAQsFADCBjzETMBEGCgmSJomT8ixk
ARkWA2NvbTEXMBUGCgmSJomT8ixkARkWB2V4YW1wbGUxGTAXBgNVBAoMEEV4YW1w
bGUgQ29tIEluYy4xITAfBgNVBAsMGEV4YW1wbGUgQ29tIEluYy4gUm9vdCBDQTEh
MB8GA1UEAwwYRXhhbXBsZSBDb20gSW5jLiBSb290IENBMB4XDTE4MDQyMjAzNDM0
NloXDTI4MDQxOTAzNDM0NlowgY8xEzARBgoJkiaJk/IsZAEZFgNjb20xFzAVBgoJ
kiaJk/IsZAEZFgdleGFtcGxlMRkwFwYDVQQKDBBFeGFtcGxlIENvbSBJbmMuMSEw
HwYDVQQLDBhFeGFtcGxlIENvbSBJbmMuIFJvb3QgQ0ExITAfBgNVBAMMGEV4YW1w
bGUgQ29tIEluYy4gUm9vdCBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
ggEBAK/u+GARP5innhpXK0c0q7s1Su1VTEaIgmZr8VWI6S8amf5cU3ktV7WT9SuV
TsAm2i2A5P+Ctw7iZkfnHWlsC3HhPUcd6mvzGZ4moxnamM7r+a9otRp3owYoGStX
ylVTQusAjbq9do8CMV4hcBTepCd+0w0v4h6UlXU8xjhj1xeUIz4DKbRgf36q0rv4
VIX46X72rMJSETKOSxuwLkov1ZOVbfSlPaygXIxqsHVlj1iMkYRbQmaTib6XWHKf
MibDaqDejOhukkCjzpptGZOPFQ8002UtTTNv1TiaKxkjMQJNwz6jfZ53ws3fh1I0
RWT6WfM4oeFRFnyFRmc4uYTUgAkCAwEAAaNjMGEwDwYDVR0TAQH/BAUwAwEB/zAf
BgNVHSMEGDAWgBSSNQzgDx4rRfZNOfN7X6LmEpdAczAdBgNVHQ4EFgQUkjUM4A8e
K0X2TTnze1+i5hKXQHMwDgYDVR0PAQH/BAQDAgGGMA0GCSqGSIb3DQEBCwUAA4IB
AQBoQHvwsR34hGO2m8qVR9nQ5Klo5HYPyd6ySKNcT36OZ4AQfaCGsk+SecTi35QF
RHL3g2qffED4tKR0RBNGQSgiLavmHGCh3YpDupKq2xhhEeS9oBmQzxanFwWFod4T
nnsG2cCejyR9WXoRzHisw0KJWeuNlwjUdJY0xnn16srm1zL/M/f0PvCyh9HU1mF1
ivnOSqbDD2Z7JSGyckgKad1Omsg/rr5XYtCeyJeXUPcmpeX6erWJJNTUh6yWC/hY
G/dFC4xrJhfXwz6Z0ytUygJO32bJG4Np2iGAwvvgI9EfxzEv/KP+FGrJOvQJAq4/
BU36ZAa80W/8TBnqZTkNnqZV
-----END CERTIFICATE-----
EOM

# -----------------------------------------------------------
# Mark the start of the generated config in opensearch.yml,
# resp. elasticsearch.yml
# -----------------------------------------------------------
cat >>config/$SB_LC_NAME.yml << EOM

# -----------------------------------------------------------
# Search Guard Demo Configuration
# -----------------------------------------------------------

EOM

# ------------------------------------------------------------
# These entries in opensearch.yml/elasticsearch.yml make the
# root CA known to Search Guard
# ------------------------------------------------------------

cat >>config/$SB_LC_NAME.yml << EOM
# -----
# The root certificate that is used to check the authenticity
# of all other certificates presented to Search Guard
#
searchguard.ssl.transport.pemtrustedcas_filepath: root-ca.pem
searchguard.ssl.http.pemtrustedcas_filepath: root-ca.pem

# -----
# We need to use searchguard.allow_unsafe_democertificates
# because using these certificates in a production system
# would not be safe and would be thus now allowed by Search Guard
#
searchguard.allow_unsafe_democertificates: true

EOM

# ------------------------------------------------------------------------
# The files esnode.pem and esnode-key.pem are used by your only node
# in the cluster to identify itself. 
# ------------------------------------------------------------------------

cat >config/node.pem << EOM
-----BEGIN CERTIFICATE-----
MIIEyTCCA7GgAwIBAgIGAWLrc1O2MA0GCSqGSIb3DQEBCwUAMIGPMRMwEQYKCZIm
iZPyLGQBGRYDY29tMRcwFQYKCZImiZPyLGQBGRYHZXhhbXBsZTEZMBcGA1UECgwQ
RXhhbXBsZSBDb20gSW5jLjEhMB8GA1UECwwYRXhhbXBsZSBDb20gSW5jLiBSb290
IENBMSEwHwYDVQQDDBhFeGFtcGxlIENvbSBJbmMuIFJvb3QgQ0EwHhcNMTgwNDIy
MDM0MzQ3WhcNMjgwNDE5MDM0MzQ3WjBeMRIwEAYKCZImiZPyLGQBGRYCZGUxDTAL
BgNVBAcMBHRlc3QxDTALBgNVBAoMBG5vZGUxDTALBgNVBAsMBG5vZGUxGzAZBgNV
BAMMEm5vZGUtMC5leGFtcGxlLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBAJa+f476vLB+AwK53biYByUwN+40D8jMIovGXm6wgT8+9Sbs899dDXgt
9CE1Beo65oP1+JUz4c7UHMrCY3ePiDt4cidHVzEQ2g0YoVrQWv0RedS/yx/DKhs8
Pw1O715oftP53p/2ijD5DifFv1eKfkhFH+lwny/vMSNxellpl6NxJTiJVnQ9HYOL
gf2t971ITJHnAuuxUF48HcuNovW4rhtkXef8kaAN7cE3LU+A9T474ULNCKkEFPIl
ZAKN3iJNFdVsxrTU+CUBHzk73Do1cCkEvJZ0ZFjp0Z3y8wLY/gqWGfGVyA9l2CUq
eIZNf55PNPtGzOrvvONiui48vBKH1LsCAwEAAaOCAVkwggFVMIG8BgNVHSMEgbQw
gbGAFJI1DOAPHitF9k0583tfouYSl0BzoYGVpIGSMIGPMRMwEQYKCZImiZPyLGQB
GRYDY29tMRcwFQYKCZImiZPyLGQBGRYHZXhhbXBsZTEZMBcGA1UECgwQRXhhbXBs
ZSBDb20gSW5jLjEhMB8GA1UECwwYRXhhbXBsZSBDb20gSW5jLiBSb290IENBMSEw
HwYDVQQDDBhFeGFtcGxlIENvbSBJbmMuIFJvb3QgQ0GCAQEwHQYDVR0OBBYEFKyv
78ZmFjVKM9g7pMConYH7FVBHMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgXg
MCAGA1UdJQEB/wQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjA1BgNVHREELjAsiAUq
AwQFBYISbm9kZS0wLmV4YW1wbGUuY29tgglsb2NhbGhvc3SHBH8AAAEwDQYJKoZI
hvcNAQELBQADggEBAIOKuyXsFfGv1hI/Lkpd/73QNqjqJdxQclX57GOMWNbOM5H0
5/9AOIZ5JQsWULNKN77aHjLRr4owq2jGbpc/Z6kAd+eiatkcpnbtbGrhKpOtoEZy
8KuslwkeixpzLDNISSbkeLpXz4xJI1ETMN/VG8ZZP1bjzlHziHHDu0JNZ6TnNzKr
XzCGMCohFfem8vnKNnKUneMQMvXd3rzUaAgvtf7Hc2LTBlf4fZzZF1EkwdSXhaMA
1lkfHiqOBxtgeDLxCHESZ2fqgVqsWX+t3qHQfivcPW6txtDyrFPRdJOGhiMGzT/t
e/9kkAtQRgpTb3skYdIOOUOV0WGQ60kJlFhAzIs=
-----END CERTIFICATE-----
EOM

cat >config/node-key.pem << EOM
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCWvn+O+rywfgMC
ud24mAclMDfuNA/IzCKLxl5usIE/PvUm7PPfXQ14LfQhNQXqOuaD9fiVM+HO1BzK
wmN3j4g7eHInR1cxENoNGKFa0Fr9EXnUv8sfwyobPD8NTu9eaH7T+d6f9oow+Q4n
xb9Xin5IRR/pcJ8v7zEjcXpZaZejcSU4iVZ0PR2Di4H9rfe9SEyR5wLrsVBePB3L
jaL1uK4bZF3n/JGgDe3BNy1PgPU+O+FCzQipBBTyJWQCjd4iTRXVbMa01PglAR85
O9w6NXApBLyWdGRY6dGd8vMC2P4KlhnxlcgPZdglKniGTX+eTzT7Rszq77zjYrou
PLwSh9S7AgMBAAECggEABwiohxFoEIwws8XcdKqTWsbfNTw0qFfuHLuK2Htf7IWR
htlzn66F3F+4jnwc5IsPCoVFriCXnsEC/usHHSMTZkL+gJqxlNaGdin6DXS/aiOQ
nb69SaQfqNmsz4ApZyxVDqsQGkK0vAhDAtQVU45gyhp/nLLmmqP8lPzMirOEodmp
U9bA8t/ttrzng7SVAER42f6IVpW0iTKTLyFii0WZbq+ObViyqib9hVFrI6NJuQS+
IelcZB0KsSi6rqIjXg1XXyMiIUcSlhq+GfEa18AYgmsbPwMbExate7/8Ci7ZtCbh
lx9bves2+eeqq5EMm3sMHyhdcg61yzd5UYXeZhwJkQKBgQDS9YqrAtztvLY2gMgv
d+wOjb9awWxYbQTBjx33kf66W+pJ+2j8bI/XX2CpZ98w/oq8VhMqbr9j5b8MfsrF
EoQvedA4joUo8sXd4j1mR2qKF4/KLmkgy6YYusNP2UrVSw7sh77bzce+YaVVoO/e
0wIVTHuD/QZ6fG6MasOqcbl6hwKBgQC27cQruaHFEXR/16LrMVAX+HyEEv44KOCZ
ij5OE4P7F0twb+okngG26+OJV3BtqXf0ULlXJ+YGwXCRf6zUZkld3NMy3bbKPgH6
H/nf3BxqS2tudj7+DV52jKtisBghdvtlKs56oc9AAuwOs37DvhptBKUPdzDDqfys
Qchv5JQdLQKBgERev+pcqy2Bk6xmYHrB6wdseS/4sByYeIoi0BuEfYH4eB4yFPx6
UsQCbVl6CKPgWyZe3ydJbU37D8gE78KfFagtWoZ56j4zMF2RDUUwsB7BNCDamce/
OL2bCeG/Erm98cBG3lxufOX+z47I8fTNfkdY2k8UmhzoZwurLm73HJ3RAoGBAKsp
6yamuXF2FbYRhUXgjHsBbTD/vJO72/yO2CGiLRpi/5mjfkjo99269trp0C8sJSub
5PBiSuADXFsoRgUv+HI1UAEGaCTwxFTQWrRWdtgW3d0sE2EQDVWL5kmfT9TwSeat
mSoyAYR5t3tCBNkPJhbgA7pm4mASzHQ50VyxWs25AoGBAKPFx9X2oKhYQa+mW541
bbqRuGFMoXIIcr/aeM3LayfLETi48o5NDr2NDP11j4yYuz26YLH0Dj8aKpWuehuH
uB27n6j6qu0SVhQi6mMJBe1JrKbzhqMKQjYOoy8VsC2gdj5pCUP/kLQPW7zm9diX
CiKTtKgPIeYdigor7V3AHcVT
-----END PRIVATE KEY-----
EOM

# ------------------------------------------------------------
# These entries in opensearch.yml/elasticsearch.yml tell the
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
# The files admin.pem and admin-key.pem can be used by administrators
# to get administrative access to the cluster. 
# 
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
  - CN=kirk,OU=client,O=client,L=test, C=de
EOM


cat >../admin.pem << EOM
-----BEGIN CERTIFICATE-----
MIIEdzCCA1+gAwIBAgIGAWLrc1O4MA0GCSqGSIb3DQEBCwUAMIGPMRMwEQYKCZIm
iZPyLGQBGRYDY29tMRcwFQYKCZImiZPyLGQBGRYHZXhhbXBsZTEZMBcGA1UECgwQ
RXhhbXBsZSBDb20gSW5jLjEhMB8GA1UECwwYRXhhbXBsZSBDb20gSW5jLiBSb290
IENBMSEwHwYDVQQDDBhFeGFtcGxlIENvbSBJbmMuIFJvb3QgQ0EwHhcNMTgwNDIy
MDM0MzQ3WhcNMjgwNDE5MDM0MzQ3WjBNMQswCQYDVQQGEwJkZTENMAsGA1UEBwwE
dGVzdDEPMA0GA1UECgwGY2xpZW50MQ8wDQYDVQQLDAZjbGllbnQxDTALBgNVBAMM
BGtpcmswggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDCwgBOoO88uMM8
dREJsk58Yt4Jn0zwQ2wUThbvy3ICDiEWhiAhUbg6dTggpS5vWWJto9bvaaqgMVoh
ElfYHdTDncX3UQNBEP8tqzHON6BFEFSGgJRGLd6f5dri6rK32nCotYS61CFXBFxf
WumXjSukjyrcTsdkR3C5QDo2oN7F883MOQqRENPzAtZi9s3jNX48u+/e3yvJzXsB
GS9Qmsye6C71enbIujM4CVwDT/7a5jHuaUp6OuNCFbdRPnu/wLYwOS2/yOtzAqk7
/PFnPCe7YOa10ShnV/jx2sAHhp7ZQBJgFkkgnIERz9Ws74Au+EbptWnsWuB+LqRL
x5G02IzpAgMBAAGjggEYMIIBFDCBvAYDVR0jBIG0MIGxgBSSNQzgDx4rRfZNOfN7
X6LmEpdAc6GBlaSBkjCBjzETMBEGCgmSJomT8ixkARkWA2NvbTEXMBUGCgmSJomT
8ixkARkWB2V4YW1wbGUxGTAXBgNVBAoMEEV4YW1wbGUgQ29tIEluYy4xITAfBgNV
BAsMGEV4YW1wbGUgQ29tIEluYy4gUm9vdCBDQTEhMB8GA1UEAwwYRXhhbXBsZSBD
b20gSW5jLiBSb290IENBggEBMB0GA1UdDgQWBBRsdhuHn3MGDvZxOe22+1wliCJB
mDAMBgNVHRMBAf8EAjAAMA4GA1UdDwEB/wQEAwIF4DAWBgNVHSUBAf8EDDAKBggr
BgEFBQcDAjANBgkqhkiG9w0BAQsFAAOCAQEAkPrUTKKn+/6g0CjhTPBFeX8mKXhG
zw5z9Oq+xnwefZwxV82E/tgFsPcwXcJIBg0f43BaVSygPiV7bXqWhxASwn73i24z
lveIR4+z56bKIhP6c3twb8WWR9yDcLu2Iroin7dYEm3dfVUrhz/A90WHr6ddwmLL
3gcFF2kBu3S3xqM5OmN/tqRXFmo+EvwrdJRiTh4Fsf0tX1ZT07rrGvBFYktK7Kma
lqDl4UDCF1UWkiiFubc0Xw+DR6vNAa99E0oaphzvCmITU1wITNnYZTKzVzQ7vUCq
kLmXOFLTcxTQpptxSo5xDD3aTpzWGCvjExCKpXQtsITUOYtZc02AGjjPOQ==
-----END CERTIFICATE-----
EOM

cat >../admin-key.pem << EOM
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDCwgBOoO88uMM8
dREJsk58Yt4Jn0zwQ2wUThbvy3ICDiEWhiAhUbg6dTggpS5vWWJto9bvaaqgMVoh
ElfYHdTDncX3UQNBEP8tqzHON6BFEFSGgJRGLd6f5dri6rK32nCotYS61CFXBFxf
WumXjSukjyrcTsdkR3C5QDo2oN7F883MOQqRENPzAtZi9s3jNX48u+/e3yvJzXsB
GS9Qmsye6C71enbIujM4CVwDT/7a5jHuaUp6OuNCFbdRPnu/wLYwOS2/yOtzAqk7
/PFnPCe7YOa10ShnV/jx2sAHhp7ZQBJgFkkgnIERz9Ws74Au+EbptWnsWuB+LqRL
x5G02IzpAgMBAAECggEAEzwnMkeBbqqDgyRqFbO/PgMNvD7i0b/28V0dCtCPEVY6
klzrg3RCERP5V9AN8VVkppYjPkCzZ2A4b0JpMUu7ncOmr7HCnoSCj2IfEyePSVg+
4OHbbcBOAoDTHiI2myM/M9++8izNS34qGV4t6pfjaDyeQQ/5cBVWNBWnKjS34S5H
rJWpAcDgxYk5/ah2Xs2aULZlXDMxbSikjrv+n4JIYTKFQo8ydzL8HQDBRmXAFLjC
gNOSHf+5u1JdpY3uPIxK1ugVf8zPZ4/OEB23j56uu7c8+sZ+kZwfRWAQmMhFVG/y
OXxoT5mOruBsAw29m2Ijtxg252/YzSTxiDqFziB/eQKBgQDjeVAdi55GW/bvhuqn
xME/An8E3hI/FyaaITrMQJUBjiCUaStTEqUgQ6A7ZfY/VX6qafOX7sli1svihrXC
uelmKrdve/CFEEqzX9JWWRiPiQ0VZD+EQRsJvX85Tw2UGvVUh6dO3UGPS0BhplMD
jeVpyXgZ7Gy5we+DWjfwhYrCmwKBgQDbLmQhRy+IdVljObZmv3QtJ0cyxxZETWzU
MKmgBFvcRw+KvNwO+Iy0CHEbDu06Uj63kzI2bK3QdINaSrjgr8iftXIQpBmcgMF+
a1l5HtHlCp6RWd55nWQOEvn36IGN3cAaQkXuh4UYM7QfEJaAbzJhyJ+wXA3jWqUd
8bDTIAZ0ywKBgFuZ44gyTAc7S2JDa0Up90O/ZpT4NFLRqMrSbNIJg7d/m2EIRNkM
HhCzCthAg/wXGo3XYq+hCdnSc4ICCzmiEfoBY6LyPvXmjJ5VDOeWs0xBvVIK74T7
jr7KX2wdiHNGs9pZUidw89CXVhK8nptEzcheyA1wZowbK68yamph7HHXAoGBAK3x
7D9Iyl1mnDEWPT7f1Gh9UpDm1TIRrDvd/tBihTCVKK13YsFy2d+LD5Bk0TpGyUVR
STlOGMdloFUJFh4jA3pUOpkgUr8Uo/sbYN+x6Ov3+I3sH5aupRhSURVA7YhUIz/z
tqIt5R+m8Nzygi6dkQNvf+Qruk3jw0S3ahizwsvvAoGAL7do6dTLp832wFVxkEf4
gg1M6DswfkgML5V/7GQ3MkIX/Hrmiu+qSuHhDGrp9inZdCDDYg5+uy1+2+RBMRZ3
vDUUacvc4Fep05zp7NcjgU5y+/HWpuKVvLIlZAO1MBY4Xinqqii6RdxukIhxw7eT
C6TPL5KAcV1R/XAihDhI18Y=
-----END PRIVATE KEY-----
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
  trusted_cas: "\${file:$SB_INSTALL_DIR/config/root-ca.pem}"
  client_auth:
    certificate: "\${file:$SB_INSTALL_DIR/admin.pem}"
    private_key: "\${file:$SB_INSTALL_DIR/admin-key.pem}"
EOM

echo >~/.searchguard/sgctl-selected-config.txt demo
  
cd ..

cp -r $SB_LC_NAME/plugins/$SG_PLUGIN_NAME/sgconfig my-sg-config

# ------------------------------------------------------------------------
# OpenSearch Dasboards/Kibana
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
# Mark the start of the generated config in opensearch_dashboards.yml,
# resp. kibana.yml
# -----------------------------------------------------------
cat >>config/$SF_LC_NAME_CC.yml << EOM

# -----------------------------------------------------------
# Search Guard Demo Configuration
# -----------------------------------------------------------

EOM


# ------------------------------------------------------------------------
# Tell Dashboards/Kibana to connect by TLS and to provide credentials
# ------------------------------------------------------------------------
cat >>config/$SF_LC_NAME_CC.yml << EOM
# ----- 
# Use HTTPS instead of HTTP. Connections by HTTP are no longer possible.
#
$SB_LC_NAME.hosts: "https://localhost:9200"

# -----
# Connections to the backend must be now authenticated. This is the
# user OpenSearch/Kibana uses for internal functionality. 
# This user is different to the users which will log in at OpenSearch/Kibana.
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
  # Search Guard and X-Pack security cannot be active at the same time. Thus, X-Pack security needs to be disabled 
  echo >>config/kibana.yml xpack.security.enabled: false
  # Suppress Kibana warning about disable X-Pack security
  echo >>config/kibana.yml security.showInsecureClusterWarning: false
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
echo "In order to change the Search Guard configuration while $SB_NAME is running, edit one of the YML files in sgconfig and upload it using sgctl.sh update-config my-sg-config"
echo
echo "You might also want to review the generated configration file at:"
echo "$SB_LC_NAME/config/$SB_LC_NAME.yml"
echo "$SF_LC_NAME/config/$SF_LC_NAME_CC.yml"

