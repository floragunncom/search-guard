#!/bin/bash

set -e

MAIN_DIR=$(echo ~/searchguard-test)
DOWNLOAD_CACHE="$MAIN_DIR/download-cache"
INSTALL_DIR="$MAIN_DIR/es"
REPO_DIR=$(pwd)

if [ -d $REPO_DIR/plugin/target/releases/ ]; then
  rm $REPO_DIR/plugin/target/releases/*
fi

mvn install -Dmaven.test.skip.exec=true -Pquick

SG_SNAPSHOT=$(echo $REPO_DIR/plugin/target/releases/search-guard-flx-elasticsearch-plugin-*SNAPSHOT*.zip)

echo "Search Guard Snapshot: $SG_SNAPSHOT"

mkdir -p $DOWNLOAD_CACHE

ES_VERSION=$(xmlstarlet sel -t -m "/_:project/_:properties/_:elasticsearch.version" -v . pom.xml)

echo "ES version: $ES_VERSION"

if [[ "$OSTYPE"  == "linux"* ]]; then
  ES_ARCHIVE="elasticsearch-$ES_VERSION-linux-x86_64.tar.gz"
elif [[ "$OSTYPE" == "darwin"* ]]; then
  ES_ARCHIVE="elasticsearch-$ES_VERSION-darwin-x86_64.tar.gz"
else
  echo "OS type $OSTYPE not supported"
  exit
fi

if [ ! -f "$DOWNLOAD_CACHE/$ES_ARCHIVE" ]; then
	wget "https://artifacts.elastic.co/downloads/elasticsearch/$ES_ARCHIVE" -P $DOWNLOAD_CACHE
fi

if [ -d "$INSTALL_DIR" ]; then
   rm -rf "$INSTALL_DIR"
fi
   
mkdir -p "$INSTALL_DIR"

echo "Extracting $ES_ARCHIVE to $INSTALL_DIR"

tar xfz "$DOWNLOAD_CACHE/$ES_ARCHIVE" -C "$INSTALL_DIR" --strip-components 1

cd "$INSTALL_DIR"

echo "-Xms1g" >>config/jvm.options
echo "-Xmx1g" >>config/jvm.options

bin/elasticsearch-plugin install -v -b file:///$SG_SNAPSHOT

for param in "$@"
do
	if [[ "$param" =~ .*/sg_[^/]+\.yml ]]; then
		echo "Using config file $param"
		cp -v $param ./plugins/search-guard-flx/sgconfig/
	fi 	
done

cat >config/kirk.pem << EOM
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

cat >config/kirk-key.pem << EOM
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

cat >config/esnode.pem << EOM
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

cat >config/esnode-key.pem << EOM
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

cat >>config/elasticsearch.yml << EOM
searchguard.ssl.transport.pemcert_filepath: esnode.pem
searchguard.ssl.transport.pemkey_filepath: esnode-key.pem
searchguard.ssl.transport.pemtrustedcas_filepath: root-ca.pem
searchguard.ssl.transport.enforce_hostname_verification: false
searchguard.ssl.http.enabled: true
searchguard.ssl.http.pemcert_filepath: esnode.pem
searchguard.ssl.http.pemkey_filepath: esnode-key.pem
searchguard.ssl.http.pemtrustedcas_filepath: root-ca.pem
searchguard.allow_unsafe_democertificates: true
searchguard.allow_default_init_sgindex: true
searchguard.authcz.admin_dn:
  - CN=kirk,OU=client,O=client,L=test, C=de
searchguard.restapi.roles_enabled: ["SGS_ALL_ACCESS"]
cluster.name: searchguard_demo  
cluster.routing.allocation.disk.threshold_enabled: false
xpack.security.enabled: false
EOM

mkdir -p ~/.searchguard
cat >~/.searchguard/cluster_test.yml << EOM
server: "localhost"
port: 9200
tls:
  trusted_cas: "#{file:$INSTALL_DIR/config/root-ca.pem}"
  client_auth:
    certificate: "#{file:$INSTALL_DIR/config/kirk.pem}"
    private_key: "#{file:$INSTALL_DIR/config/kirk-key.pem}"
EOM

echo >~/.searchguard/sgctl-selected-config.txt test
  
echo "Starting ES"

bin/elasticsearch