package varga.vorath;

import java.net.URI;

public class Utils {

    // Extract cluster URI (scheme://authority) when a full HDFS path is provided; otherwise return null
    public static String extractClusterUri(String maybeFullPath) {
        try {
            if (maybeFullPath == null || maybeFullPath.isEmpty()) return null;
            URI uri = URI.create(maybeFullPath);
            if (uri.getScheme() != null && uri.getAuthority() != null) {
                return uri.getScheme() + "://" + uri.getAuthority();
            }
            return null;
        } catch (Exception e) {
            logger.warn("Failed to parse location '{}': {}", maybeFullPath, e.getMessage());
            return null;
        }
    }

    public static String normalizePath(String path) {
        if (path == null || path.isEmpty()) return "/volumes";
        if (path.endsWith("/")) return path.substring(0, path.length() - 1);
        return path;
    }
}
