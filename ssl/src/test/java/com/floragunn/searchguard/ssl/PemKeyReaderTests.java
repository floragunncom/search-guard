/*
 * Copyright 2015-2020 floragunn GmbH
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
package com.floragunn.searchguard.ssl;

import com.floragunn.searchguard.support.PemKeyReader;
import java.io.ByteArrayInputStream;
import java.security.Security;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.EncryptionException;
import org.bouncycastle.openssl.PasswordException;
import org.junit.Assert;
import org.junit.Test;

public class PemKeyReaderTests {

    static {

        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
        }

    }

    //openssl genpkey -algorithm RSA
    String pkcs8RsaUnencrypted = "-----BEGIN PRIVATE KEY-----\n" + "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCjoN8erWPxjtSr\n"
            + "7fyINsGEDmix7dmDud21SHIxIPLWAh28f1+67zYlySAW0RAQcH6mm6AgHUOTMJ1K\n"
            + "GeEKdUhMfJ9r0RTrFEYfCov75MvmkaXBt8Jr1LTqDZJFz96TZPNRa30j8L5LVS1E\n"
            + "tNT60dAjCsHJlkl8vGnj3cPZpPT5xhliNmfi11Gz/9QPajl5BtT88yyvu7A4a7J3\n"
            + "hn32hx6VNKU8VN96yqUx6BScN+0Q2WeT/+2IXvTLqSgfofx5ocdGxLusIWD6tIMC\n"
            + "4R9CrYT795qo7ZYHGR3YmnWiLrD5upPsMaa8geXEmNmekh1sTj2/otPwuOLvqtR/\n"
            + "4WoNd6wJAgMBAAECggEAToPH7Dl5BpTpuaIVleroSox7cj1WLR4Ho5Aisy5DN6uG\n"
            + "R0e7UMCt882hZzFkCu9f5mQwnphML5mZL9BhKpqCWalG+f42kmKFDyoJZ5IuwPFM\n"
            + "XzrkwMctFE30zSjkfUdodb4lKxS+yvkRIrG+rKf2vonP2QITOxZRnTRxYMMFhb/j\n"
            + "KXUhfHqhNagUJLlaLR2dqdMGcxDxzoO9J0JZ1ZuymKx8v60Gb+oa+aQ7B6rkDCRt\n"
            + "1zVH/2JefznI26NETpo3u+2mGnegk6NeS/+e+AiXoPi4MMWP9RgMXePBxrIjhdZv\n"
            + "1sek/ioQuZiqC0iEl9jPvVxVJzRBen1IEoAO5C+iTQKBgQDUkbPCu9SEYFOyIRtP\n"
            + "+D1kUDEEDQcnvHfwpTQSD5bOuJlrYrAnNVRXEDj+vVNNg7wUBLdwgRYpbUtmmpuD\n"
            + "F8dVmuRRe/+XvTAAxuv78PMeX8yHcXs6Xfz0griKpW1yNjMb4hRgy08Vp0vdfd37\n"
            + "umbaO5CILAigW7ewwOIdYa1LLwKBgQDFD1qgQT9WtOb/2wkEXQ8iYd/1pD+suQb6\n"
            + "3xlF7RjNUqltkyF1Y1NsXCghw9mxUF1rClYyFHYMpuv9Po78VZS9o5gslBG1OF+G\n"
            + "sTueyJwtXknBh2w69lcxpcpYYMvgB+cwje612orhNm+cMLQh89T6Vj86dPSSNDm2\n"
            + "+u+gIJjORwKBgQCPhYTCsYFOk59PAO1o9Cm3RjGiE43GNYkh0Nk0bJQdJSRpDevz\n"
            + "vR0h35Er5faNuqlNNtYIPxAQjnu/Xhory+1Jjlgj8D3lXNZBYA3LQKdV1cbJmERk\n"
            + "Zzs4d95TtlgMKi4d4Gpz5DGUZC1j8ezkXrm9FUSDvH9ijiqbS9AfmGih/wKBgDXH\n"
            + "C+diBwQFmGjIgLx1HPU4GqNxsILVd8cs7Tu9CaC250/k8COV+KVAvR8B0L3n/aaB\n"
            + "iqZeGR04zHwu/1xzioT6SUQZIIABI2iho3CgtKZY7e3npCyvH3qIESFvQRQhdAE3\n"
            + "KqbsPixZ67mHPRFq9nte4CB23Gut3vZFovFjE/BvAoGBALi19Kp3Ua3wOKJ+yInc\n"
            + "VMpxpZ++MrDtCilxItnicU8CiyQWI87wSH3tzuAyIAaT36BqhxKgtrvzP8fbtDWY\n"
            + "KIeOxMrUFHhB0SbrepXtGk9ciwQvPh1LeFK6+/WAuYxMxpLVyyM0BR8zwMbj7WvX\n" + "ev9LcAOIHj5p4Ba8qU2H79Pv\n" + "-----END PRIVATE KEY-----";

    //openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256
    String pkcs1EcUnencrypted = "-----BEGIN PRIVATE KEY-----\n" + "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgzIhO4afsWKZD3PYK\n"
            + "evG4vTKzwn00CXCebT2GlT2S2oOhRANCAATkXfix9bTFELlMMpIAuqElxoNv+196\n" + "K/Fz/5emiuM1D11jr0+xUHooX0FeK7bgc3Cyr9gHeu3V/oNKvGoCaRzj\n"
            + "-----END PRIVATE KEY-----";

    //openssl genpkey -algorithm x25519
    String ecx25519 = "-----BEGIN PRIVATE KEY-----\n" + "MC4CAQAwBQYDK2VuBCIEIIgx/WX8DixMbUqpfjTIHghpHI+SqcbLKq07kTIEJ2Rb\n"
            + "-----END PRIVATE KEY-----";

    //openssl genrsa -f4
    String pksc1Unencrypted = "-----BEGIN RSA PRIVATE KEY-----\n" + "MIIEpAIBAAKCAQEAxdIerNOqVWsOZ/pK7jevdsdYt6zMe+DJsP3vtjIqnG5NKbkZ\n"
            + "WSfLz9T9mn7idr34QdJGqQhqfheMPr+L7p/angBvIXIk6FzesKWyBO9BaTh2fIoP\n"
            + "KL+sa4cSTumFssWhAFEzycrh3uBXrY729TIq+rQnLtIwwTzJDwvhMei9Mcs2aBqo\n"
            + "c24BL8dqVleIU5aWTIqeSfanfUIv/C1OFtouho7OsDQhsVe5oxwtVH0un/y9aA4A\n"
            + "EUcDKRmdjfAvgP/Dcx/7DGmROnSp4woBnA7UPeuu7KSjfL50EHKDP6a2iaI2Sr5A\n"
            + "HX8wrLER3ffnf3DG3bu+bBbGV+v3XaAe89sPCwIDAQABAoIBAEW2GTdAVBtL4cTE\n"
            + "kFlIaF4MO/hmPZ0+BvWHV002eour1ydn4kXn30v7O++Q3U+I/I59gocYvyZ2N+b4\n"
            + "62AJFyps7ryzk8wKxjMsAQL6KWviCOc0Y8kxWpp9+/axFnt8Z5NMClsHUveOyXcR\n"
            + "FXgs2LaGe/Rk/+do+1WBEzfaU6nS2zDQ/yk1Ax7yYdzrPKgbk5G15ZxRSraepogp\n"
            + "vbFcQOLGiXDzbWhFSMNY+azs/WkBZ96I4p0yt+iFMi7DHY33OlKCQ7yOJzo486yl\n"
            + "uRj2ktG2BrEYCkVlXdoCdM7ev8SSJoDx9Ea1ZUnPxat+W1u4geZrHY1vWQK+15wb\n"
            + "kDZz/YECgYEA64SsShqEb5NDxKAlXta5RPdqAWP+VwLymSUOl7pG1ynturLeqpgc\n"
            + "T8inhXZ/qjOBiPf65csvwJpjJRGrjvSZ/wuU9V4FN/bMyiDSQAQC5Mzcnppkak/e\n"
            + "f1S6PWsLDlW+NnGheg/MymXu+al6PJaUKRyVWM96fLkKLYCvsLz4qksCgYEA1wYw\n"
            + "xXoFMv5GeSXn6/mnJo2+HHJvjTYojvjnKF26nIecXV1UEIkfvBgtjCVB2bWH+coh\n"
            + "CM2viiw5eOsOTDoB+uCaZNV3cy/movEOn7kEGdOA2rQZGgAbogx31CGyEv/wEdwO\n"
            + "S22BURSLRo7f9PRHzEQ5znSS56/MfbCcGE7kNkECgYEAvaJhG3XIhxJbcZnFHVzl\n"
            + "ZnuUtjPuWd88uUTWQCMz4RGYokhd85xwx2j4QgVM/B1mwoNxWM/Gzl1BUVqt+XhH\n"
            + "+s03jm264/nroSDWSccxEUtGpMoJ6nQO4hW1s7hZ+ZvegXXzIBZSvfFff1UP4HMa\n"
            + "LxQdyJmetYhEgqjoTfWKf4sCgYBVTg9rSM1nPZmX1oH8DD9T/Ee/4WaAD0xn7DdI\n"
            + "vGacUvCl2bDQHpaIeM8ZGm7VgD9Byf7xDyjlGEZ0wkwZ0amXbiPSfR22hPprCldS\n"
            + "5oPJ+y7TlT+gIQrVbzza28NZ5k/HxhWltvvmvs/CXkxbUc5qK2cDkRv27reh5b8H\n"
            + "pcTEAQKBgQCRzpGGJV+2FbdumvW0hEfn/0+2huQFNr+w8ZezU92FNkzF6X2kLXJm\n"
            + "nEHFuCV9LcMDIn84jCQ23C3GhRzNP5BiEOst9Qd2Mpa1Iw+tkFmwpnExcDXOJ4Vs\n" + "Vdb4KAyEoMUmu4qxPDoiPWXXJ/9ymhca01aBr5CME5n3T7+E2f2hUg==\n"
            + "-----END RSA PRIVATE KEY-----";

    //openssl genrsa -f4 -des
    String pkcs1DesEncrypted = "-----BEGIN RSA PRIVATE KEY-----\n" + "Proc-Type: 4,ENCRYPTED\n" + "DEK-Info: DES-CBC,E024957E1F58275A\n" + "\n"
            + "sPCVHvaSDzF0rXqVR1T6jkcwh/8MZ3hdNIO1vGnQM0r3wiMux/AhVjrMTJ8nKo9B\n"
            + "X/PLMn+Y+5JXjzkpLGuuWjugy7yHwA0YDGdnDAWcysubvgSpRfIggP8YRetB78Dy\n"
            + "FM5TVdxhLQ6r38tEizO+kUiel3GZh6cXrroGnmdj1J63e+hhH7qMs2r6sWtM25iP\n"
            + "dWk8wJoo5UW8G/zhH7umKp2NAuzvhjsFd7G/2Lxpi3/3XwcmsuGOVL71c8ZkxCp/\n"
            + "yJzXp3FIJrxPMxHwk23IIG0LBm4YfjEwxaVHRZ+IZ9fiuKUsjrsn3yZMv1p6+gkt\n"
            + "ryfoV0UOVw98hzZvuc+38u5eRHh3UBjqOftFJrRZmbjdYC+ZiEj2PDZevv5SdC6Q\n"
            + "8eIMskKW6BT3kvSbxVJq/0crufT3Z969Vrz6xZ0gdRBP2YmUKNm/cO5CT9syw6ab\n"
            + "ya6t/Nk1nFcG7NzNn6jAFZN4TqOrBpnIm5eb0/jwoEr+7Hpaql3WgzOlkzi2oZNF\n"
            + "sdHqag13IU36PI183KckhNwg/6cCuQnmkz1cM8Jk7kX34akAOIL0nadeEhar030E\n"
            + "/xUp8sPVH7GKuvrPEZd97AqwVHTc4hS5rPqz1II/uoiwSSSZc6zL3hDfEkjj8NmY\n"
            + "hes2UiCNUqXfceV/osLYFMa7kz2JihJRyjFzT5MOP2JCxIl84NaVmm7cFJwmJ9IJ\n"
            + "LGOBJU98VyfzlA8xBJn0jaeaCpB2GWZxqB8yqV7BwYpxom2sLNd8VndaEjtDbjGL\n"
            + "zij0Z0Vyy8b0YCVEqkX1H0iANA/HJtx94dGpjOdiyJJyd1grL/PMycfaL4wETqst\n"
            + "Sfmm0vkF8pFK621T9FvSH6kEXVxYD8mawVci1tle4Bv9ZpgBkWxVKg+juUkAS1cE\n"
            + "Sw744tW9aOlaj+Q8StPQppSj/xuIc2eJ/XKQp6JVTQRHEibm76mFB7TQ2Qpelpj0\n"
            + "3EuvGin9rbCDLqz77eJUhauVjPe/1vq85zuQABdCa8VN/cENsPbpuWKVps3GQ8rP\n"
            + "cDar15pt82ODFqvyu2FhKJwgsEjIASWhDBbnZEU/0cksJ40HLZmCORTpSDEa02b7\n"
            + "58s6nZfRYjeNH0IULawGz2jXDhklwyfckqSMPLv5KL4v4T+o8m6N9GSK0/MmRLCI\n"
            + "EehJl7SJJk0/WCMvzGEq+iW6XCYziNoC8GW9evKCduMBcJUh0YtdSj1Gtgz8iyLP\n"
            + "6NZBOYk/6OfOA1n/C4VDYmACgZM1jU9VhnVb2/ujB4wh/k1+e+4Th5nFtdWUbGwJ\n"
            + "Z5RMfjQ0nn2QhHJxtU/uT02Gs3RIgtq2jEDbE73TTRcj1AcAkVHixKXV+sFu97O5\n"
            + "1amySehlaq2XQgQ14KyAsUThhufYt9eEBuLAE6yOOI9bjVs1aIEUsqPuIjmHh4G+\n"
            + "qgDULkQZaxIHQ6VggVhlJ/5w/12XGi87+c5RmNGn0w5TkdWBwYhwJO3WynW2SrtM\n"
            + "drxDYoD3BptR93ug4FMTHeylpkJ4500U57pMl3YfefEZSKDvWYpdX6GAi4uXOKps\n" + "in5wjoqBKbZJUckJVZrNEti2DHuPu42jzyCwSw6HILeveX5nMyrpkw==\n"
            + "-----END RSA PRIVATE KEY-----";

    //openssl genrsa -f4 -aes128
    String pkcs1AesEncrypted = "-----BEGIN RSA PRIVATE KEY-----\n" + "Proc-Type: 4,ENCRYPTED\n"
            + "DEK-Info: AES-128-CBC,8D52BD039EE337E079C8BCC9B91D4AF4\n" + "\n" + "Sbbn6beIA2cDZi/5Q0uIhQnxSN3p/lQ6ofiyvW5vV8um6C1c8AtRFPmo61sCuJir\n"
            + "RlZ5BnCUkf39A9T8pC+YJXAMZhOrWLRluj4BjoGB2XmOLooP4xkOj8k3clrvbFp9\n"
            + "BbXdTl7dSRxqdH+4hbpy00ofgcqqTP4K1/UMYRcp/Caw9BfvLNXzHH2rPtgBBrho\n"
            + "iDO5eEzhigRx38hMJ06rzRypNPEzPvphCvVD55QcD8jV7XbqzJ+yHTuEog6tNDjC\n"
            + "u/rVvoVwZENTufABv1apg2VaQnVsMzKbVOkjp5TcYfOZ89UqUANgiB45mGqza2xB\n"
            + "ffa9AzAQXKR0efbwqMJss6aKsVfGfCrmKS9/ANACcnbFEjxqKykSK2VBzoGY1rOv\n"
            + "jAIzeo4BY9I42v0a61uaGgBz1FaEgm9VP2i8hRDaiYmJyGSe0zoMXDbOhL7yO2JE\n"
            + "WGFsGkrhDVD4Zan88c4clOvMumUoQhqYDSyk33rWZHArZun3NjQuaK7rrF3hSfZK\n"
            + "P0hUkPuDMgrTacD1ACPiXnmn7TN2J4lJZ55vdK2jnCru99xKVCly6brTIIfsoK7v\n"
            + "OH617yiVyMFVoI3+/U5cBLPOk3oZ8229MYMVlaq6kVaNAt7TUM0kyvnk7ZUwPL1L\n"
            + "tXeL4ny9KU6kbbTekdMJb9HjSgcECMN6yRkjTGwb4hToF4hdg3lJpiGJuJ4Z+CZk\n"
            + "pl/cy5MFGddsJVf8BFn088xHpMxOGyx156skzEjsRJMWCjuDUpE4sLvCiv4UDAE1\n"
            + "pb9H8pqhR5MmqXM5er32z1LBF5r6/1eOdULh9j0EW6RCkGk7JZWNSNJZlzvIAp8U\n"
            + "3YbMgaifEhcbe3H+Q8bjlgWHnCTW1fccvAL+v8hjx5XgTb6/nh9YY4vkV3yT0AQ0\n"
            + "c3lztjYnZKxueMKi8feE2jn69VYBwLcO2cUV+uTZ61oJXLRRa+XKPuPap5AZoCVh\n"
            + "fPU8p6FE+wqLEarcKSvQpnuZXWrqoxTVXO0tXopSyg48uWBhk4QB5V/AqRaSPaJd\n"
            + "imTnxHvL9rNA+2jWtdidw+dqxa9fLqq9bMuLyDs4+qoAaNMhPTlTRRwxdATCm29D\n"
            + "AoNh4s7NhpsgD1EF03sfkuf5ANBEwy5nw3OvPbHcuBWaOxJGgqo+fLBKLGj1cPVq\n"
            + "L6BtFacSPUPq9NzZfEcXzkvzO189tmM98IKtb5wdWPosJYdnf2icauwZUaHG13UR\n"
            + "m6kBJPttBFzS1SO+c7mTFaFADhWcBC9ew24djbQKDAefXzEbnzLPDWFvQH2QjxNc\n"
            + "fquGRvaMXNHaJtxsj+OvUQjF+1sfEhDEbfB2evgFiPdiQl6mMSKIySwo39e+52hw\n"
            + "ZB7ROOtNt5QhpvlkVEZvhvkoFke2Fp7VsINoYU1j/U+6eEfFE559rAEVUTtV2ES/\n"
            + "gfNwVRhdPzRbZtFf3mS10sh8wxjoMe7cHleGJe38eN8oWqJ0BUw8nEgOpQENLkMR\n"
            + "kUJv9wpoptnECBJijKRVkyYheRK+VBbxQoLHwHcA91iAZiaAxG+Khjm6W9Q8030f\n"
            + "YgeEyDSx999HE0AzfiDe/PgSeZ6+raOv9DBsULH/do7nvA6L/UNdDkVHapCr+5II\n" + "-----END RSA PRIVATE KEY-----";

    //openssl genpkey -algorithm RSA -aes128
    String pkcs8AesEncrypted = "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" + "MIIFLTBXBgkqhkiG9w0BBQ0wSjApBgkqhkiG9w0BBQwwHAQI2hef92wcFT4CAggA\n"
            + "MAwGCCqGSIb3DQIJBQAwHQYJYIZIAWUDBAECBBBDDiFq1Knrrxs2P+CgDdgiBIIE\n"
            + "0DqtZGpSh+iXjAM5hobm7GZkwlOHGgQV3i1Idd5n4BOXn13163zvLhYKxXaH94YQ\n"
            + "FQjCeLpxF/vz+n8cly0TiFhGF767565Fct/iK9CbTx5nCZDsIa8zXIXuQwRB7y18\n"
            + "2lI1g775+T2ydGiEhqKQ413+Qpyf+kVeh5e4kB+Z07F047kY3g49kSsbBYoGoEmK\n"
            + "BRb7EN+ovgTJ1aVNrDC/6/+6OUHaNhqLjW3m+ZrrIzDUGo0UPHzfdVQf4jLb9KhI\n"
            + "qGXuDcdRrs/vqTn9QhsmxzMfMxt8MzMcdPcWSMjt9EaYvo2fLDVBOz260bgVzkym\n"
            + "OsnNEgIehpB5VH19Y1xtVrO0ZoIFBozEs5gTsLN78WaRtG0sohu3np0scC52tk/J\n"
            + "qM9Otze4i2XC6dTh7ti+abUcpBF6dvdWTvpxnqdIc4v5vTPcDndQRtXzcbNs/K3r\n"
            + "uSRIWP7ZUoMwhB6aBDWx6eqg2lvuoRcHnjCbL2BB0CX/O5prDpSaiVR+IJt/xWK6\n"
            + "rpseZYVnryOk/QWrPZ8Cqaea8+Dua2/rZWYGOP2j1XbLmOfe/A7veIBsFPD1whmz\n"
            + "g46HFwH9UhbfwiGo4Ag5InoTtPVcgTkGo0VcuMQyl+Mbhhwfs92gqto2+zEWcoVp\n"
            + "nZ6zIjPbbCepRcArJLP2YHTxqr/mWNzVGedA1oiqbuhmlfbva1flZDCCQXqShJdl\n"
            + "u7tcPEoJobrhcGaC8NdxCAKSscwOmZ/QmYV+uFhCqMkMPzCi1w3kiIfn5SYF7rUS\n"
            + "6M6mZ8xbR8vi5ylLyJa340JFw9C8tpg6YWKlD7UiAzmIh6o/dl/40kYSN1RafPPS\n"
            + "6MbBZAQdB9hQ/oc8dY/acxVQgYUD/z7G1v3Ib5tX/38A3uqnqBv6lNbQq/lP2cS6\n"
            + "o3iZwq5QUx+WeVKF3hVxbFfSVUxmZ7Z9Ib7u6kQx1iRfCKTnEHFDoCtP5LA0SxPQ\n"
            + "U3ygWUem+LGkdWx/4c3zgyPQZsahHrKutJx+O+KdVk3oAo11DRZkR71Wdtyisd5U\n"
            + "8gl2ENEQ2ydiDyELRU38pPEQU2jlUNRNFnBem1NQmFwqMVsDHfSb5YbvHSWAnJzs\n"
            + "ya2N8YfM93s7PoiQv39niHTiG8tP6hEB+vqEvlWN68CSGNGaSv1YXWPNzhHpPJDu\n"
            + "gGfbEGBiCorFFFx9r5P7XfJL0bvApBbUXQYyyvMENMyN3Mz7ktV235BT7w/Wm+1V\n"
            + "u8xbZahicLIBe/EPyi0OUBzMv3JBNjuHgnw0+HkCYz/y1cNATdpdOFc1edljuwZA\n"
            + "fo8/xUWR8LfL0aIciMgC1oJa1MXBT2JaZ9/WJowWlIYx/yiOP/DRkiJFgyDXtWRZ\n"
            + "elobS9irlCxxWFtz0PllNK/2mHDB8a0F5iDJF5VnWXZZd07hjaBbmrX4sFdEDqac\n"
            + "C3TfjjcTocGzVw1KsOL6DD2vKbnddstu2sGMYWF6pA/oEmGl2Gxsh6qpcz5Gsf08\n"
            + "b3aeYuDRKD4bxN0CjhLUyJrg2oQlp/ldjTahE1qcv0WTFqoe6g+97djtAvtzTPIt\n"
            + "KFqEgPIdjPb3Aq7nwf+PDV93jDYOAJ3uMh9Pvw9/oyiG8WBIlfJWuyobkfiT81TZ\n" + "vrRRlhYXuQ5UqwvmUSbv5M0ty+deDAYtYS0mEGSfKMM2\n"
            + "-----END ENCRYPTED PRIVATE KEY-----";

    //openssl genpkey -algorithm RSA -des3
    String pkcs8Des3Enrypted = "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" + "MIIFHDBOBgkqhkiG9w0BBQ0wQTApBgkqhkiG9w0BBQwwHAQIYlEy1doijroCAggA\n"
            + "MAwGCCqGSIb3DQIJBQAwFAYIKoZIhvcNAwcECHSg+QpIE1bRBIIEyE/u7YuXxksq\n"
            + "6YZPo3flXYtJM8tYhe5ZxMdROhzquMdtz+VYa7S4slCl4U3d3xVWS/DocGsfO5am\n"
            + "Q1v2v3btZq/Px7wciOXZ7IcpMayVzdqUN0HhxGqBf1My0f6YPWjBOo/Y3tZqMxar\n"
            + "5gDsl7iwOwJwhSExfPril03f2ZC4v0jqLx4Jptj2ZyUw2HZzYzcTI7mYzIWCsFky\n"
            + "S6nbF+/MKjI2ZwEBMyiswV0AL/H3PEDgUnT0ERgxmUqpap8kj6jCvK8gFYnUaIti\n"
            + "Fi5RyfqNfPWEuVfBRjGNY8p5hl37hPnDWhuIrRjjDAnn7IDz073uaQRXPdfI8dZj\n"
            + "hdMRxmx3+lXlHWYRtpaFrJkF/YixAz+heH9OQtB/2dpCCAxlDdpfh2MGkVZm/4an\n"
            + "ZOkG1InSA5gckbIFVQRaq25xX+cUgsnoy9qPWI5djR305hvJj7mHLW3FB1Cs+dP+\n"
            + "Aspa/YfaccX5zM+y5xbREUEn3Pszm1fcUYd7p4Cw+uJuz4FAatAHfI2eiKdaPpwj\n"
            + "kbbF43+mH+6FRwg82OpTWjb2SA4Xl6ULfZMQntZzaIFG8n1p+bp6rO91hYlVzofq\n"
            + "eGPRwoR0aabyujNqxvj2gNQWp396NsaIZFyXo7+B5grWMSik9CYHNKParsbz8JZ1\n"
            + "1lCPMD4WKlVdyAgTVFkkBGzTeCRgfxGBGwM674zGW0rW+Z/92y8sAEj6kvfnop4t\n"
            + "iC7gcPKe2lktkge/NiAOz+V2+HH9coqzW2Im9sWAyu+A4X//MfrHlfKyf80bLcRD\n"
            + "R099zDMJTYXCHBA952S07VPUnQCFXlCWEaGv8cMMqmAIE5ubHAfa38pSIccf3qUe\n"
            + "PoOUnIykCLXByOm15CJoLxmrAXunalEQfjuhIenwMccUmjwigDFJ2IS2VyrfKBi3\n"
            + "q+J9Hj+K8Dw6fI3g33TnbAFoWrYjnUizphyH/AEVQiKifbhQhDcWEEgYpZDSKRqk\n"
            + "CYQq9DPUir815oA7VUyAt/njiTfu0Tltmf0dEScPPH31E96rM5OIfwdeFKyj2QhM\n"
            + "7VLFsrUVm2WKCRD7PIFHC6NihmNQc8nGyHATs8TE3B3DzHzahDOFlpwvzqkGDEMF\n"
            + "l+jZJi1YwWLQcEK7Ty32Js8xsNhSKNRMQEgpl+V/1MQ0N3wNMurvtBfHX0T20/IK\n"
            + "VH77EZLdcq5ClOQtRomBxvyyX7WwwxTk1MdM8rmzW2UL4Apu5LAAaJS+gI8ZfC19\n"
            + "IcZaspUIZYqdC80Q1LvTiAHn1IweuGPRVYFBXonfCUSXExZOP3ReBPelrKE12Dqt\n"
            + "7w3XQYH68GDKCLze38g4zFZC/qGxhUReDHB+R5rtPaUEeu9K/9b9cfx/q2Kpf9oy\n"
            + "3UKI+3r2mUjHXFSnYMIVbH8C+bpqKK/YvXykbX73I342b/jRvBEmXW1g7qpLVcoL\n"
            + "Z46YbCsa8zpVymrA1OfvGGYqD1isbr3M2vMzjlCHZz0baxJmSRmHyFhDcD12ijX8\n"
            + "NtbdMDSn+38aFBhgv/sCGTA/DaBBcTj1EZatbJ6BN3qYjl2RygAp+Q7FlpGWOBH6\n"
            + "ZdLOF6RMC/FtuXZHQ/pxvXcmIq5zDJ+zzpvGlvJKh3z4MG21pGOcluFmKGcyDGTE\n" + "c02nnVqDtm89hnum91EMaA==\n"
            + "-----END ENCRYPTED PRIVATE KEY-----";

    String pkcs1EcAesEncrypted = "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" + "MIHsMFcGCSqGSIb3DQEFDTBKMCkGCSqGSIb3DQEFDDAcBAh8rIfuvCTQLwICCAAw\n"
            + "DAYIKoZIhvcNAgkFADAdBglghkgBZQMEAQIEEJdOgBxCF+Megy0atscnDzkEgZBM\n"
            + "imiYV6lJfskLzbPwEk3S34deKplDptfsn7Q/lpBfHL3zrCHi6cIHR25ch0kbMS4R\n"
            + "6EAF+kkBX28UiBQwFBMIdMuuV4Li8nYJFlARiXTWdh69iS+wjHrKOyluu0AN4Lif\n"
            + "yXV/3zCBQk19QIFh+gM9Ot5toINLmJ+GfcKC7p6+HttEL5GQduR9lywou94lTGo=\n" + "-----END ENCRYPTED PRIVATE KEY-----";

    //openssl dsaparam -out dsaparam.pem 2048
    //openssl gendsa -out dsakey.pem dsaparam.pem
    //catdsakey.pem
    String dsaUnencrypted = "-----BEGIN DSA PRIVATE KEY-----\n" + "MIIDVwIBAAKCAQEA3RxQD245KfUv4C4Qjw7OBl3KbA1NveyMLGvUoXnJ0dYOLLib\n"
            + "A76Zflxv9hPbwVntqSBos47h2nXRk/aomYdzN+3I2rrUMKKI7HAiws3/o43/SgJm\n"
            + "NfJu/2dfqqQyh5+iEMFGQyV4Mvq0seGTbjGps0zYBKDKQUwZRHykIpX10cFc9dFR\n"
            + "b/aq/tO+onkD4hC0n5v1IfsctmpT5aq5OugbbNlDRT+0ef4Pjfa44WJfXyGQL1jL\n"
            + "UFBu7ObkhnA2C7LmuToafxdOKzc2PCpPO2ZeBxVHyLX1VUn4gOgQKhFiUlNyvbnB\n"
            + "fYWC31UP/8PwgfOMY6vxjTRaROI30mquT4JNpQIhAKDMJN7X5kcOgdGgJQCpg4zN\n"
            + "jb1pg+2+UQYx33aBUK85AoIBAQCZ+tPGH4EOxE8vjX4ghaAw7WWr+HWJbzzEIocD\n"
            + "89qM2uHYAvNJ7sHNe3oe34fNYESY/JqkUymx6Yh1YOD7dMc+ohlMsE8xT40YRGif\n"
            + "ADW6G8Jv4hZ3i7n36B0Llq/fMuhWDhi7ewwWWhrnaSqkWn+Idk/4Mzn9AYyipRoE\n"
            + "Hc1hHHqPkcWgv33mgEtq2KLivd3sL6Y2N5NvcwNguvoZK402kt2Ho00cuMsfNaQg\n"
            + "3iXK24rXS/ibnyuBsdH4UA4oBjg/1ZSlAh2BMhC9BkTg46r01pWNmu5dMaGu9mNh\n"
            + "JFCz8DMa20cP4D2qP/nplU8jET20r29f2sxuqk8xlAgvFMwBAoIBAQCYVzyuBbin\n"
            + "rNcmnBkUQh252W4K/1MgLNSH0rGXSa8eicIyxvCPNAWuallXu6ZlcRzHOWK97ehw\n"
            + "8io6aLh7xtgzknhD50Frgd1YXehHmm3MOc+w5gMv4LpOLRategsqSLvRJu0d37Gy\n"
            + "ZRGEjySXE/Ct+QtSSnP5siL+TnrTVYBDSM7gNcKmWXTYZPQPEtL76YYOGBIekMAO\n"
            + "cfFBoWBkrOu2ZCwx9vJuSOLddNONXKSa3aSTNNO6a6Y4VW3CrwdQRL/t+4jp9xKL\n"
            + "J9Gu3wbKQEVLRzDjp50vFNh21g8YRkoVPw4tXiZbMBXE3IbrtJP0Sh/8wtLhRqLc\n" + "FQ6ByXx0LSjiAiAYyFt8RA11JwKBr1Ac3JEESuMWxqL2fHH9UUrxHidDAQ==\n"
            + "-----END DSA PRIVATE KEY-----";

    String invalid = "----BEGIN PRIVATE KEY-----\n" + "XX4CAQAwBQYDK2VuBCIEIIgx/WX8DixMbUqpgjTIHghpHI+SqcbLKq07kTIEJ2xx\n"
            + "-----END PRIVATE KEY-----";

    @Test
    public void testKeys() throws Exception {

        Assert.assertNotNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream(pkcs8RsaUnencrypted.getBytes()), null));
        Assert.assertNotNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream(pkcs1EcUnencrypted.getBytes()), null));
        Assert.assertNotNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream(pksc1Unencrypted.getBytes()), null));
        Assert.assertNotNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream(pkcs1DesEncrypted.getBytes()), "12345"));
        Assert.assertNotNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream(pkcs1AesEncrypted.getBytes()), "12345"));
        Assert.assertNotNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream(pkcs8AesEncrypted.getBytes()), "12345"));
        Assert.assertNotNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream(pkcs8Des3Enrypted.getBytes()), "12345"));
        Assert.assertNotNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream(pkcs1EcAesEncrypted.getBytes()), "12345"));
        Assert.assertNotNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream(dsaUnencrypted.getBytes()), null));
        Assert.assertNotNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream(ecx25519.getBytes()), null));

        try {
            Assert.assertNotNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream(pkcs1DesEncrypted.getBytes()), null));
        } catch (PasswordException e) {
            //assumed
        } catch (Exception e) {
            Assert.fail(e.toString());
        }

        try {
            Assert.assertNotNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream(pkcs1DesEncrypted.getBytes()), "wrong"));
        } catch (EncryptionException e) {
            //assumed
        } catch (Exception e) {
            Assert.fail(e.toString());
        }

        try {
            Assert.assertNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream(invalid.getBytes()), null));
        } catch (Exception e) {
            Assert.fail(e.toString());
        }

        try {
            Assert.assertNull(PemKeyReader.toPrivateKey(new ByteArrayInputStream("".getBytes()), null));
        } catch (Exception e) {
            Assert.fail(e.toString());
        }

    }

}
