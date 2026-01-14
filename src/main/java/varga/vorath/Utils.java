package varga.vorath;

/*-
 * #%L
 * Vorath
 * %%
 * Copyright (C) 2025 - 2026 Varga Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import lombok.extern.slf4j.Slf4j;

import java.net.URI;

@Slf4j
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
            log.warn("Failed to parse location '{}': {}", maybeFullPath, e.getMessage());
            return null;
        }
    }

    public static String normalizePath(String path) {
        if (path == null || path.isEmpty()) return "/volumes";
        if (path.endsWith("/")) return path.substring(0, path.length() - 1);
        return path;
    }
}
