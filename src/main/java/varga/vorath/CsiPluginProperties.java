package varga.vorath;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "csi.plugin")
public class CsiPluginProperties {
    private String name;
    private String vendorVersion;
    private Map<String, String> manifest;

    // Getters and Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVendorVersion() {
        return vendorVersion;
    }

    public void setVendorVersion(String vendorVersion) {
        this.vendorVersion = vendorVersion;
    }

    public Map<String, String> getManifest() {
        return manifest;
    }

    public void setManifest(Map<String, String> manifest) {
        this.manifest = manifest;
    }
}