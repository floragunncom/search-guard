/*
 * Copyright 2021 floragunn GmbH
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

package com.floragunn.searchguard.configuration.variables;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;

import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.fluent.collections.ImmutableMap;
import com.floragunn.searchsupport.cstate.ComponentState;
import com.floragunn.searchsupport.cstate.ComponentStateProvider;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocWriter;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Files;

public class EncryptionKeys implements ComponentStateProvider {
    private final static Logger log = LogManager.getLogger(EncryptionKeys.class);

    public static final Setting<?> ENCRYPTION_KEYS_SETTING = Setting.groupSetting("searchguard.config_vars.encryption_keys.", Property.NodeScope);

    private static final Entry DEFAULT_ENTRY = new Entry("default", "AES/CBC/PKCS5Padding",
            new SecretKeySpec(BaseEncoding.base64().decode("v9hGHVFiTgj+eAhjJrDgAEy5GUoTBUwXkAKEpfCL6dQ="), "AES"), false);

    private final Map<String, Entry> entries;
    private final Entry active;
    private final ComponentState componentState = new ComponentState(1000, null, "encryption_keys", EncryptionKeys.class);

    static class Entry {
        final String id;
        final SecretKeySpec secretKeySpec;
        final String cipher;
        final boolean gcm;
        final boolean active;

        Entry(String id, String cipher, SecretKeySpec secretKeySpec, boolean active) {
            this.id = id;
            this.cipher = cipher;
            this.secretKeySpec = secretKeySpec;
            this.gcm = cipher.contains("/GCM/");
            this.active = active;
        }

        static Entry fromFile(String id, String cipher, File file, boolean active) throws IOException {

            String content = Files.asCharSource(file, Charset.defaultCharset()).read();

            return new Entry(id, cipher, new SecretKeySpec(BaseEncoding.base64().decode(content), "AES"), active);
        }

        static Entry fromSettings(String id, Settings settings) throws IOException {
            String cipher = settings.get("cipher", "AES/CBC/NoPadding");
            boolean active = settings.getAsBoolean("active", false);

            if (settings.hasValue("key_file")) {
                return fromFile(id, cipher, new File(settings.get("file")), active);
            } else if (settings.hasValue("key")) {
                return new Entry(id, cipher, new SecretKeySpec(BaseEncoding.base64().decode(settings.get("key")), "AES"), active);
            } else {
                throw new RuntimeException("Encryption key must be specified as key_file or key");
            }
        }
    }

    public EncryptionKeys(Settings settings) {
        this.entries = createEntryMap(settings, componentState);
        this.active = getActive(this.entries);
        componentState.updateStateFromParts();
        componentState.setMessage("active: " + this.active.id);
    }

    Map<String, Object> getEncryptedData(Object value) throws EncryptionException {
        byte[] plainBytes = DocWriter.json().writeAsBytes(value);

        try {
            Entry entry = this.active;

            if (entry == null) {
                throw new EncryptionException("Could not find active encryption key");
            }

            Cipher cipher = Cipher.getInstance(entry.cipher);
            byte iv[] = null;

            if (entry.gcm) {
                SecureRandom random = new SecureRandom();
                iv = new byte[16];
                random.nextBytes(iv);
                cipher.init(Cipher.ENCRYPT_MODE, entry.secretKeySpec, new GCMParameterSpec(16 * 8, iv));
            } else {
                SecureRandom random = new SecureRandom();
                iv = new byte[16];
                random.nextBytes(iv);
                cipher.init(Cipher.ENCRYPT_MODE, entry.secretKeySpec, new IvParameterSpec(iv));
            }

            byte[] encryptedBytes = cipher.doFinal(plainBytes);

            return ImmutableMap.ofNonNull("value", BaseEncoding.base64().encode(encryptedBytes), "key", entry.id, "iv",
                    iv != null ? BaseEncoding.base16().encode(iv) : null);

        } catch (InvalidKeyException | InvalidAlgorithmParameterException | NoSuchPaddingException | NoSuchAlgorithmException
                | IllegalBlockSizeException | BadPaddingException e) {
            throw new EncryptionException(e);
        } finally {
            Arrays.fill(plainBytes, (byte) 0);
        }
    }

    Object getDecryptedData(Map<String, Object> source) throws EncryptionException, DocumentParseException {
        Map<?, ?> encrypted = (Map<?, ?>) source.get("encrypted");
        return getDecryptedData((String) encrypted.get("value"), (String) encrypted.get("key"), (String) encrypted.get("iv"));
    }

    Object getDecryptedData(ConfigVar configVar) throws DocumentParseException, EncryptionException {
        return getDecryptedData(configVar.getEncValue(), configVar.getEncKey(), configVar.getEncIv());
    }

    Object getDecryptedData(String value, String key, String iv) throws EncryptionException, DocumentParseException {
        byte[] encryptedBytes = BaseEncoding.base64().decode(value);

        try {
            Entry entry = this.entries.get(key);

            if (entry == null) {
                throw new EncryptionException("Unknown encryption key: " + key);
            }

            Cipher cipher = Cipher.getInstance(entry.cipher);

            if (entry.gcm) {
                cipher.init(Cipher.DECRYPT_MODE, entry.secretKeySpec, new GCMParameterSpec(16 * 8, BaseEncoding.base16().decode(iv)));
            } else {
                cipher.init(Cipher.DECRYPT_MODE, entry.secretKeySpec, new IvParameterSpec(BaseEncoding.base16().decode(iv)));
            }

            byte[] plainBytes = cipher.doFinal(encryptedBytes);

            try {
                return DocReader.json().read(plainBytes);
            } finally {
                Arrays.fill(plainBytes, (byte) 0);
            }
        } catch (InvalidKeyException | InvalidAlgorithmParameterException | NoSuchAlgorithmException | NoSuchPaddingException
                | IllegalBlockSizeException | BadPaddingException e) {
            throw new EncryptionException(e);
        }
    }

    private static Map<String, Entry> createEntryMap(Settings settings, ComponentState componentState) {
        Map<String, Settings> groups = settings.getGroups(ENCRYPTION_KEYS_SETTING.getKey());
        Map<String, Entry> map = new HashMap<>();
        map.put(DEFAULT_ENTRY.id, DEFAULT_ENTRY);

        for (Map.Entry<String, Settings> settingsGroup : groups.entrySet()) {
            String id = settingsGroup.getKey();

            try {
                map.put(id, Entry.fromSettings(id, settingsGroup.getValue()));
                componentState.getOrCreatePart("encryption_key", id).setInitialized();
            } catch (Exception e) {
                componentState.getOrCreatePart("encryption_key", id).setFailed(e);
                log.error("Error while creating encryption key " + id, e);
            }
        }

        return Collections.unmodifiableMap(map);
    }

    private static Entry getActive(Map<String, Entry> map) {
        for (Entry entry : map.values()) {
            if (entry.active) {
                return entry;
            }
        }

        return map.get("default");
    }

    @Override
    public ComponentState getComponentState() {
        return null;
    }

}
