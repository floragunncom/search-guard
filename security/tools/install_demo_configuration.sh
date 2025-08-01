#!/bin/bash
#install_demo_configuration.sh [-y]
SCRIPT_PATH="${BASH_SOURCE[0]}"
if ! [ -x "$(command -v realpath)" ]; then
    if [ -L "$SCRIPT_PATH" ]; then
        
        [ -x "$(command -v readlink)" ] || { echo "Not able to resolve symlink. Install realpath or readlink.";exit 1; }
        
        # try readlink (-f not needed because we know its a symlink)
        DIR="$( cd "$( dirname $(readlink "$SCRIPT_PATH") )" && pwd -P)"
    else
        DIR="$( cd "$( dirname "$SCRIPT_PATH" )" && pwd -P)"
    fi
else
    DIR="$( cd "$( dirname "$(realpath "$SCRIPT_PATH")" )" && pwd -P)"
fi
echo "Search Guard Demo Installer"
echo " ** Warning: Do not use on production or public reachable systems **"

OPTIND=1
assumeyes=0
initsg=0
cluster_mode=0
SGCTL_VERSION="3.1.0"
SGCTL_LINK="https://maven.search-guard.com/search-guard-flx-release/com/floragunn/sgctl/$SGCTL_VERSION/sgctl-$SGCTL_VERSION.sh"

function show_help() {
    echo "install_demo_configuration.sh [-y] [-i] [-c]"
    echo "  -h show help"
    echo "  -y confirm all installation dialogues automatically"
    echo "  -i initialize Search Guard with default configuration (default is to ask if -y is not given)"
    echo "  -c enable cluster mode by binding to all network interfaces (default is to ask if -y is not given)"
}

function init_search_guard_question() {
  if [ "$initsg" == 0 ]; then
    read -r -p "Initialize Search Guard? [y/N] " response
    case "$response" in
        [yY][eE][sS]|[yY])
          initsg=1
          ;;
        *)
        initsg=0
        ;;
    esac
  fi
}

function enable_custer_mode_question() {
  if [ "$cluster_mode" == 0 ]; then
    echo "Cluster mode requires maybe additional setup of:"
    echo "  - Virtual memory (vm.max_map_count)"
    echo "    See https://www.elastic.co/guide/en/elasticsearch/reference/current/vm-max-map-count.html"
    echo ""
    read -r -p "Enable cluster mode? [y/N] " response
    case "$response" in
        [yY][eE][sS]|[yY])
            cluster_mode=1
            ;;
        *)
            cluster_mode=0
            ;;
    esac
  fi
}

while getopts "h?yic" opt; do
    case "$opt" in
    h|\?)
        show_help
        exit 0
        ;;
    y)  assumeyes=1
        ;;
    i)  initsg=1
        ;;
    c)  cluster_mode=1
    esac
done

shift $((OPTIND-1))

[ "$1" = "--" ] && shift

if [ "$assumeyes" == 1 ]; then
  initsg=1
  cluster_mode=1
else
  init_search_guard_question
  enable_custer_mode_question
fi


set -e
BASE_DIR="$DIR/../../.."
if [ -d "$BASE_DIR" ]; then
	CUR="$(pwd)"
	cd "$BASE_DIR"
	BASE_DIR="$(pwd)"
	cd "$CUR"
	echo "Basedir: $BASE_DIR"
else
    echo "DEBUG: basedir does not exist"
fi
ES_CONF_FILE="$BASE_DIR/config/elasticsearch.yml"
ES_BIN_DIR="$BASE_DIR/bin"
ES_PLUGINS_DIR="$BASE_DIR/plugins"
ES_MODULES_DIR="$BASE_DIR/modules"
ES_LIB_PATH="$BASE_DIR/lib"
SUDO_CMD=""
ES_INSTALL_TYPE=".tar.gz"

#Check if its a rpm/deb install
if [ -f /usr/share/elasticsearch/bin/elasticsearch ]; then
    ES_CONF_FILE="/usr/share/elasticsearch/config/elasticsearch.yml"

    if [ ! -f "$ES_CONF_FILE" ]; then
        ES_CONF_FILE="/etc/elasticsearch/elasticsearch.yml"
    fi

    ES_BIN_DIR="/usr/share/elasticsearch/bin"
    ES_PLUGINS_DIR="/usr/share/elasticsearch/plugins"
    ES_MODULES_DIR="/usr/share/elasticsearch/modules"
    ES_LIB_PATH="/usr/share/elasticsearch/lib"

    if [ -x "$(command -v sudo)" ]; then
        SUDO_CMD="sudo"
        echo "This script maybe require your root password for 'sudo' privileges"
    fi

    ES_INSTALL_TYPE="rpm/deb"
fi

if [ $SUDO_CMD ]; then
    if ! [ -x "$(command -v $SUDO_CMD)" ]; then
        echo "Unable to locate 'sudo' command. Quit."
        exit 1
    fi
fi

if $SUDO_CMD test -f "$ES_CONF_FILE"; then
    :
else
    echo "Unable to determine Elasticsearch config directory. Quit."
    exit -1
fi

if [ ! -d "$ES_BIN_DIR" ]; then
	echo "Unable to determine Elasticsearch bin directory. Quit."
	exit -1
fi

if [ ! -d "$ES_PLUGINS_DIR" ]; then
	echo "Unable to determine Elasticsearch plugins directory. Quit."
	exit -1
fi

if [ ! -d "$ES_MODULES_DIR" ]; then
	echo "Unable to determine Elasticsearch modules directory. Quit."
	#exit -1
fi

if [ ! -d "$ES_LIB_PATH" ]; then
	echo "Unable to determine Elasticsearch lib directory. Quit."
	exit -1
fi

ES_CONF_DIR=$(dirname "${ES_CONF_FILE}")
ES_CONF_DIR=`cd "$ES_CONF_DIR" ; pwd`

if [ ! -d "$ES_PLUGINS_DIR/search-guard-flx" ]; then
  echo "Search Guard plugin not installed. Quit."
  exit -1
fi

OS=$(sb_release -ds 2>/dev/null || cat /etc/*release 2>/dev/null | head -n1 || uname -om)
echo "Elasticsearch install type: $ES_INSTALL_TYPE on $OS"
echo "Elasticsearch config dir: $ES_CONF_DIR"
echo "Elasticsearch config file: $ES_CONF_FILE"
echo "Elasticsearch bin dir: $ES_BIN_DIR"
echo "Elasticsearch plugins dir: $ES_PLUGINS_DIR"
echo "Elasticsearch lib dir: $ES_LIB_PATH"

if $SUDO_CMD grep --quiet -i searchguard "$ES_CONF_FILE"; then
  echo "$ES_CONF_FILE seems to be already configured for Search Guard. Quit."
  exit -1
fi

set +e

read -r -d '' SG_ADMIN_CERT << EOM
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

read -r -d '' SG_ADMIN_CERT_KEY << EOM
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

read -r -d '' NODE_CERT << EOM
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

read -r -d '' NODE_KEY << EOM
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

read -r -d '' ROOT_CA << EOM
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

set -e

echo "$SG_ADMIN_CERT" | $SUDO_CMD tee "$ES_CONF_DIR/kirk.pem" > /dev/null
echo "$NODE_CERT" | $SUDO_CMD tee "$ES_CONF_DIR/esnode.pem" > /dev/null 
echo "$ROOT_CA" | $SUDO_CMD tee "$ES_CONF_DIR/root-ca.pem" > /dev/null
echo "$NODE_KEY" | $SUDO_CMD tee "$ES_CONF_DIR/esnode-key.pem" > /dev/null
echo "$SG_ADMIN_CERT_KEY" | $SUDO_CMD tee "$ES_CONF_DIR/kirk-key.pem" > /dev/null

echo "" | $SUDO_CMD tee -a  "$ES_CONF_FILE"
echo "######## Start Search Guard Demo Configuration ########" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null 
echo "# WARNING: revise all the lines below before you go into production" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null 
echo "searchguard.ssl.transport.pemcert_filepath: esnode.pem" | $SUDO_CMD tee -a  "$ES_CONF_FILE" > /dev/null 
echo "searchguard.ssl.transport.pemkey_filepath: esnode-key.pem" | $SUDO_CMD tee -a  "$ES_CONF_FILE" > /dev/null 
echo "searchguard.ssl.transport.pemtrustedcas_filepath: root-ca.pem" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null 
echo "searchguard.ssl.transport.enforce_hostname_verification: false" | $SUDO_CMD tee -a  "$ES_CONF_FILE" > /dev/null 
echo "searchguard.ssl.http.enabled: true" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null 
echo "searchguard.ssl.http.pemcert_filepath: esnode.pem" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null
echo "searchguard.ssl.http.pemkey_filepath: esnode-key.pem" | $SUDO_CMD tee -a  "$ES_CONF_FILE" > /dev/null 
echo "searchguard.ssl.http.pemtrustedcas_filepath: root-ca.pem" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null 
echo "searchguard.allow_unsafe_democertificates: true" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null
if [ "$initsg" == 1 ]; then
    echo "searchguard.allow_default_init_sgindex: true" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null
fi
echo "searchguard.authcz.admin_dn:" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null 
echo "  - CN=kirk,OU=client,O=client,L=test, C=de" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null 
echo "" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null 
echo "searchguard.audit.type: internal_elasticsearch" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null
#echo "searchguard.enable_snapshot_restore_privilege: true" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null
echo "searchguard.check_snapshot_restore_write_privileges: true" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null
echo 'searchguard.restapi.roles_enabled: ["SGS_ALL_ACCESS"]' | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null

#cluster.routing.allocation.disk.threshold_enabled
if $SUDO_CMD grep --quiet -i "^cluster.routing.allocation.disk.threshold_enabled" "$ES_CONF_FILE"; then
	: #already present
else
    echo 'cluster.routing.allocation.disk.threshold_enabled: false' | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null
fi

#cluster.name
if $SUDO_CMD grep --quiet -i "^cluster.name" "$ES_CONF_FILE"; then
	: #already present
else
    echo "cluster.name: searchguard_demo" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null 
fi

#network.host
if $SUDO_CMD grep --quiet -i "^network.host" "$ES_CONF_FILE"; then
	: #already present
else
	if [ "$cluster_mode" == 1 ]; then
        echo "network.host: 0.0.0.0" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null
        echo "node.name: smoketestnode" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null
        echo "cluster.initial_master_nodes: smoketestnode" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null
    fi
fi

#xpack.security.enabled
if $SUDO_CMD grep --quiet -i "^xpack.security.enabled" "$ES_CONF_FILE"; then
	: #already present
else
    if [ -d "$ES_MODULES_DIR/x-pack-security" ];then
	    echo "xpack.security.enabled: false" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null
    fi
fi

#xpack.security.enabled
if $SUDO_CMD grep --quiet -i "^xpack.security.autoconfiguration.enabled" "$ES_CONF_FILE"; then
	: #already present
else
    if [ -d "$ES_MODULES_DIR/x-pack-security" ];then
	    echo "xpack.security.autoconfiguration.enabled: false" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null
    fi
fi

echo "######## End Search Guard Demo Configuration ########" | $SUDO_CMD tee -a "$ES_CONF_FILE" > /dev/null

ES_PLUGINS_DIR=`cd "$ES_PLUGINS_DIR" ; pwd`

echo
echo "Downloading sgctl from $SGCTL_LINK"
curl --fail "$SGCTL_LINK" -o "$ES_PLUGINS_DIR/search-guard-flx/sgctl.sh"
$SUDO_CMD chmod u+x "$ES_PLUGINS_DIR/search-guard-flx/sgctl.sh"

# Setup configuration for sgctl

mkdir -p ~/.searchguard
cat >~/.searchguard/cluster_demo.yml << EOM
server: "localhost"
port: 9200
tls:
  trusted_cas: "#{file:$ES_CONF_DIR/root-ca.pem}"
  client_auth:
    certificate: "#{file:$ES_CONF_DIR/kirk.pem}"
    private_key: "#{file:$ES_CONF_DIR/kirk-key.pem}"
EOM

echo >~/.searchguard/sgctl-selected-config.txt demo


echo "### Success"
echo "### Execute this script now on all your nodes and then start all nodes"

if [ "$initsg" == 0 ]; then
	echo "### After the whole cluster is up, you need to initialize the Search Guard configuration. You can achieve this by executing: "
    echo "### cd plugins/search-guard-flx"
	echo "### ./sgctl.sh update-config sgconfig/"
	echo "### See https://git.floragunn.com/search-guard/sgctl/-/blob/main/README.md for more information on using sgctl"
    echo "### After the initial initialization is complete, roles and users can be also edited using Search Guard Configuration GUI, see http://docs.search-guard.com/latest/configuration-gui"	
else
    echo "### Search Guard will be automatically initialized."
    echo "### If you like to change the runtime configuration "
    echo "### change the files in sgconfig and execute: "
    echo "### cd plugins/search-guard-flx"
	echo "### ./sgctl.sh update-config sgconfig/"
	echo "### See https://git.floragunn.com/search-guard/sgctl/-/blob/main/README.md for more information on using sgctl"
    echo "### Roles and users can be also edited using Search Guard Configuration GUI, see http://docs.search-guard.com/latest/configuration-gui"	
fi

echo "### To access your Search Guard secured cluster open https://<hostname>:<HTTP port> and log in with admin/admin."
echo "### (Ignore the SSL certificate warning because we installed self-signed demo certificates)"