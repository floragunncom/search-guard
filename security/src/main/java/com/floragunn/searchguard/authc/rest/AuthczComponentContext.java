package com.floragunn.searchguard.authc.rest;

import java.nio.file.Path;

import org.opensearch.common.settings.Settings;

import com.floragunn.codova.validation.VariableResolvers;

public class AuthczComponentContext {
    private final Path configPath;
    private final Settings esSettings;
    private final VariableResolvers configVariableProviders;

    public AuthczComponentContext(Path configPath, Settings esSettings, VariableResolvers configVariableProviders) {
        this.configPath = configPath;
        this.esSettings = esSettings;
        this.configVariableProviders = configVariableProviders;
    }

    public Path getConfigPath() {
        return configPath;
    }

    public Settings getEsSettings() {
        return esSettings;
    }

    public VariableResolvers getConfigVariableProviders() {
        return configVariableProviders;
    }

}
