/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
 * 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 
 * This software is free of charge for non-commercial and academic use. 
 * For commercial use in a production environment you have to obtain a license 
 * from https://floragunn.com
 * 
 */

package com.floragunn.searchguard.dlic.rest.validation;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.codec.binary.Hex;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;

import com.floragunn.codova.documents.BasicJsonPathDefaultConfiguration;
import com.floragunn.searchguard.lpg.Blake2bDigest;
import com.google.common.base.Splitter;
import com.google.common.primitives.Bytes;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

public class RolesValidator extends AbstractConfigurationValidator {

	public RolesValidator(final RestRequest request, final BytesReference ref, final Settings esSettings, Object... param) {
		super(request, ref, esSettings, param);
		this.payloadMandatory = true;
		allowedKeys.put("cluster_permissions", DataType.ARRAY);
		allowedKeys.put("tenant_permissions", DataType.ARRAY);
		allowedKeys.put("index_permissions", DataType.ARRAY);
        allowedKeys.put("exclude_cluster_permissions", DataType.ARRAY);
		allowedKeys.put("description", DataType.STRING);
        allowedKeys.put("alias_permissions", DataType.ARRAY);
        allowedKeys.put("data_stream_permissions", DataType.ARRAY);
	}

    @Override
    public boolean validate() {

        if (!super.validate()) {
            return false;
        }
        
        boolean valid=true;

        if (this.content != null && this.content.length() > 0) {

            final ReadContext ctx = JsonPath.parse(this.content.utf8ToString(), BasicJsonPathDefaultConfiguration.defaultConfiguration());
            final List<String> maskedFields = ctx.read("$..masked_fields[*]");

            if (maskedFields != null) {
                
                for (String mf : maskedFields) {
                    if (!validateMaskedFieldSyntax(mf)) {
                        valid = false;
                    }
                }
            }
        }
        
        if(!valid) {
           this.errorType = ErrorType.WRONG_DATATYPE;
        }

        return valid;
    }

    private boolean validateMaskedFieldSyntax(String mf) {
        try {
            new MaskedField(mf, new byte[] {1,2,3,4,5,1,2,3,4,5,1,2,3,4,5,6}, null, null).isValid();
        } catch (Exception e) {
            wrongDatatypes.put("Masked field not valid: "+mf, e.getMessage());
            return false;
        }
        return true;
    }

    private static class MaskedField {

        private String algo = null;
        private List<RegexReplacement> regexReplacements;
        private final byte[] defaultSalt;
        private final byte[] salt2;
        private final byte[] prefix;

        public MaskedField(final String value, final byte[] salt, final byte[] salt2, final byte[] prefix) {
            this.defaultSalt = salt;
            this.salt2 = salt2;
            this.prefix = prefix;
            final List<String> tokens = Splitter.on("::").splitToList(Objects.requireNonNull(value));
            final int tokenCount = tokens.size();
            if (tokenCount == 2) {
                algo = tokens.get(1);
            } else if (tokenCount >= 3 && tokenCount % 2 == 1) {
                regexReplacements = new ArrayList<>((tokenCount - 1) / 2);
                for (int i = 1; i < tokenCount - 1; i = i + 2) {
                    regexReplacements.add(new RegexReplacement(tokens.get(i), tokens.get(i + 1)));
                }
            } else if (tokenCount != 1) {
                throw new IllegalArgumentException("Expected 1 or 2 or >=3 (but then odd count) tokens, got " + tokenCount);
            }
        }

        public final void isValid() throws Exception {
            mask(new byte[] { 1, 2, 3, 4, 5 });
        }

        public byte[] mask(byte[] value) {
            if (isDefault()) {
                return blake2bHash(value);
            } else {
                return customHash(value);
            }
        }

        private boolean isDefault() {
            return regexReplacements == null && algo == null;
        }

        private byte[] customHash(byte[] in) {
            if (algo != null) {
                try {
                    MessageDigest digest = MessageDigest.getInstance(algo);

                    if (prefix != null) {
                        return Bytes.concat(prefix, Hex.encodeHexString(digest.digest(in)).getBytes());
                    }

                    return Hex.encodeHexString(digest.digest(in)).getBytes();
                } catch (NoSuchAlgorithmException e) {
                    throw new IllegalArgumentException(e);
                }
            } else if (regexReplacements != null) {
                String cur = new String(in, StandardCharsets.UTF_8);
                for (RegexReplacement rr : regexReplacements) {
                    cur = cur.replaceAll(rr.getRegex(), rr.getReplacement());
                }

                if (prefix != null) {
                    return Bytes.concat(prefix, cur.getBytes(StandardCharsets.UTF_8));
                }

                return cur.getBytes(StandardCharsets.UTF_8);

            } else {
                throw new IllegalArgumentException();
            }
        }

        private byte[] blake2bHash(byte[] in) {
            final Blake2bDigest hash = new Blake2bDigest(null, 32, salt2, defaultSalt);
            hash.update(in, 0, in.length);
            final byte[] out = new byte[hash.getDigestSize()];
            hash.doFinal(out, 0);

            if (prefix != null) {
                return Bytes.concat(prefix, Hex.encodeHexString(out).getBytes());
            }

            return Hex.encodeHexString(out).getBytes();
        }

        private static class RegexReplacement {
            private final String regex;
            private final String replacement;

            public RegexReplacement(String regex, String replacement) {
                super();
                this.regex = regex.substring(1).substring(0, regex.length() - 2);
                this.replacement = replacement;
            }

            public String getRegex() {
                return regex;
            }

            public String getReplacement() {
                return replacement;
            }

            @Override
            public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + ((regex == null) ? 0 : regex.hashCode());
                result = prime * result + ((replacement == null) ? 0 : replacement.hashCode());
                return result;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj)
                    return true;
                if (obj == null)
                    return false;
                if (getClass() != obj.getClass())
                    return false;
                RegexReplacement other = (RegexReplacement) obj;
                if (regex == null) {
                    if (other.regex != null)
                        return false;
                } else if (!regex.equals(other.regex))
                    return false;
                if (replacement == null) {
                    if (other.replacement != null)
                        return false;
                } else if (!replacement.equals(other.replacement))
                    return false;
                return true;
            }

            @Override
            public String toString() {
                return "RegexReplacement [regex=" + regex + ", replacement=" + replacement + "]";
            }

        }
    }
}
