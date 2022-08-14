package com.floragunn.searchguard.enterprise.encrypted_indices.utils;

/*
*
* Copyright  vt-middleware (https://www.middleware.vt.edu/)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*
*  */

//copied from https://raw.githubusercontent.com/vt-middleware/cryptacular/master/src/main/java/org/cryptacular/util/KeyPairUtil.java
//https://www.middleware.vt.edu/


import org.bouncycastle.crypto.CryptoException;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.params.DSAParameters;
import org.bouncycastle.crypto.params.DSAPrivateKeyParameters;
import org.bouncycastle.crypto.params.DSAPublicKeyParameters;
import org.bouncycastle.crypto.params.RSAKeyParameters;
import org.bouncycastle.crypto.signers.DSASigner;
import org.bouncycastle.crypto.signers.ECDSASigner;
import org.bouncycastle.crypto.signers.RSADigestSigner;
import org.bouncycastle.jcajce.provider.asymmetric.util.ECUtil;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.DSAPrivateKey;
import java.security.interfaces.DSAPublicKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;

/**
 * Utility methods for public/private key pairs used for asymmetric encryption.
 *
 * @author  Middleware Services
 */
public final class KeyPairUtil
{

    /** Data used to verify key pairs. */
    private static final byte[] SIGN_BYTES = ("Mr. Watson--come here--I want to see you.").getBytes(StandardCharsets.UTF_8);

    /** Private constructor of utility class. */
    private KeyPairUtil() {}

    public static BigInteger getModulus(final PublicKey pubKey)
    {
        final String alg = pubKey.getAlgorithm();

        switch (alg) {

            //case "DSA":
            //    return ((DSAPublicKey) pubKey).getParams().getG()

            case "RSA":
                return  ((RSAPublicKey) pubKey).getModulus();


            //case "EC":
            //    return  ((ECPublicKey) pubKey).getParams().


            default:
                throw new IllegalArgumentException(alg + " not supported.");
        }
    }

    public static BigInteger getModulus(final PrivateKey pubKey)
    {
        final String alg = pubKey.getAlgorithm();

        switch (alg) {

            //case "DSA":
            //    return ((DSAPublicKey) pubKey).getParams().getG()

            case "RSA":
                return  ((RSAPrivateKey) pubKey).getModulus();


            //case "EC":
            //    return  ((ECPublicKey) pubKey).getParams().


            default:
                throw new IllegalArgumentException(alg + " not supported.");
        }
    }


    /**
     * Determines whether the given public and private keys form a proper key pair by computing and verifying a digital
     * signature with the keys.
     *
     * @param  pubKey  DSA, RSA or EC public key.
     * @param  privKey  DSA, RSA, or EC private key.
     *
     * @return  True if the keys form a functioning keypair, false otherwise. Errors during signature verification are
     *          treated as false.
     *
     */
    public static boolean isKeyPair(final PublicKey pubKey, final PrivateKey privKey) throws CryptoException, InvalidKeyException {
        final String alg = pubKey.getAlgorithm();
        if (!alg.equals(privKey.getAlgorithm())) {
            return false;
        }

        // Dispatch onto the algorithm-specific method
        final boolean result;
        switch (alg) {

            case "DSA":
                result = isKeyPair((DSAPublicKey) pubKey, (DSAPrivateKey) privKey);
                break;

            case "RSA":
                result = isKeyPair((RSAPublicKey) pubKey, (RSAPrivateKey) privKey);
                break;

            case "EC":
                result = isKeyPair((ECPublicKey) pubKey, (ECPrivateKey) privKey);
                break;

            default:
                throw new IllegalArgumentException(alg + " not supported.");
        }
        return result;
    }


    /**
     * Determines whether the given DSA public and private keys form a proper key pair by computing and verifying a
     * digital signature with the keys.
     *
     * @param  pubKey  DSA public key.
     * @param  privKey  DSA private key.
     *
     * @return  True if the keys form a functioning keypair, false otherwise. Errors during signature verification are
     *          treated as false.
     *
     */
    public static boolean isKeyPair(final DSAPublicKey pubKey, final DSAPrivateKey privKey)
    {
        final DSASigner signer = new DSASigner();
        final DSAParameters params = new DSAParameters(
                privKey.getParams().getP(),
                privKey.getParams().getQ(),
                privKey.getParams().getG());

        try {
            signer.init(true, new DSAPrivateKeyParameters(privKey.getX(), params));
            final BigInteger[] sig = signer.generateSignature(SIGN_BYTES);
            signer.init(false, new DSAPublicKeyParameters(pubKey.getY(), params));
            return signer.verifySignature(SIGN_BYTES, sig[0], sig[1]);
        } catch (RuntimeException e) {
            throw e;
        }
    }


    /**
     * Determines whether the given RSA public and private keys form a proper key pair by computing and verifying a
     * digital signature with the keys.
     *
     * @param  pubKey  RSA public key.
     * @param  privKey  RSA private key.
     *
     * @return  True if the keys form a functioning keypair, false otherwise. Errors during signature verification are
     *          treated as false.
     *
     */
    public static boolean isKeyPair(final RSAPublicKey pubKey, final RSAPrivateKey privKey) throws CryptoException {
        final RSADigestSigner signer = new RSADigestSigner(new SHA256Digest());
        try {
            signer.init(true, new RSAKeyParameters(true, privKey.getModulus(), privKey.getPrivateExponent()));
            signer.update(SIGN_BYTES, 0, SIGN_BYTES.length);
            final byte[] sig = signer.generateSignature();
            signer.init(false, new RSAKeyParameters(false, pubKey.getModulus(), pubKey.getPublicExponent()));
            signer.update(SIGN_BYTES, 0, SIGN_BYTES.length);
            return signer.verifySignature(sig);
        } catch (Exception e) {
            throw e;
        }
    }


    /**
     * Determines whether the given EC public and private keys form a proper key pair by computing and verifying a digital
     * signature with the keys.
     *
     * @param  pubKey  EC public key.
     * @param  privKey  EC private key.
     *
     * @return  True if the keys form a functioning keypair, false otherwise. Errors during signature verification are
     *          treated as false.
     *
     */
    public static boolean isKeyPair(final ECPublicKey pubKey, final ECPrivateKey privKey) throws InvalidKeyException {
        final ECDSASigner signer = new ECDSASigner();
        try {
            signer.init(true, ECUtil.generatePrivateKeyParameter(privKey));

            final BigInteger[] sig = signer.generateSignature(SIGN_BYTES);
            signer.init(false, ECUtil.generatePublicKeyParameter(pubKey));
            return signer.verifySignature(SIGN_BYTES, sig[0], sig[1]);
        } catch (Exception e) {
            throw e;
        }
    }


}

