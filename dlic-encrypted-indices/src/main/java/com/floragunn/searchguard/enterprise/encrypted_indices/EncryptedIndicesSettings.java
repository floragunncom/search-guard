package com.floragunn.searchguard.enterprise.encrypted_indices;

import com.floragunn.searchsupport.StaticSettings;

public class EncryptedIndicesSettings {

    private EncryptedIndicesSettings() {

    }

    public static final StaticSettings.Attribute<Boolean> INDEX_ENCRYPTION_ENABLED =
            StaticSettings.Attribute
                    .define("index.encryption_enabled")
                    .indexScoped()
                    .withDefault(false)
                    .asBoolean();

    public static final StaticSettings.Attribute<String> INDEX_ENCRYPTION_KEY =
            StaticSettings.Attribute
                    .define("index.encryption_key")
                    .indexScoped()
                    .withDefault((String) null)
                    .asString();

    public static final StaticSettings.Attribute<String> INDEX_ENCRYPTION_ALGO =
            StaticSettings.Attribute
                    .define("index.encryption_algo")
                    .indexScoped()
                    .withDefault("auto")
                    .asString();

    public static final StaticSettings.Attribute<Boolean> INDEX_ENCRYPTION_FAIL_ON_MISSING_KEY =
            StaticSettings.Attribute
                    .define("index.encryption_fail_on_missing_key")
                    .indexScoped()
                    .withDefault(false)
                    .asBoolean();


    public static final StaticSettings.Attribute<Boolean> INDEX_ENCRYPTION_FAIL_ON_DECRYPT_ERROR =
            StaticSettings.Attribute
                    .define("index.encryption_fail_on_decrypt_error")
                    .indexScoped()
                    .withDefault(false)
                    .asBoolean();

    static final StaticSettings.Attribute[] attributes =
            new StaticSettings.Attribute[] {
                    INDEX_ENCRYPTION_ENABLED,
                    INDEX_ENCRYPTION_KEY,
                    INDEX_ENCRYPTION_ALGO,
                    INDEX_ENCRYPTION_FAIL_ON_MISSING_KEY,
                    INDEX_ENCRYPTION_FAIL_ON_DECRYPT_ERROR

            };
}
