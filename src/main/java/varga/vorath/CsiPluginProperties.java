package varga.vorath;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "csi.plugin")
public class CsiPluginProperties {
    private String name;
    private String vendorVersion;
    private Map<String, String> manifest;
}