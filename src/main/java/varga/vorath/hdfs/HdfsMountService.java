package varga.vorath.hdfs;

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

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import varga.vorath.kubernetes.KubernetesVolumeAttachmentClient;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class HdfsMountService {

    private final KubernetesVolumeAttachmentClient volumeAttachmentClient;
    private final Map<String, HdfsVirtualFileSystem> hdfsVfsInstances = new ConcurrentHashMap<>();

    /**
     * Initializes the service by querying Kubernetes VolumeAttachment resources and performing
     * necessary volume mount and unmount operations.
     */
    @PostConstruct
    public void init() {
        log.info("Initializing HdfsMountService...");

        try {
            // Query the VolumeAttachment objects for the current node
            Map<String, VolumeAttachmentInfo> volumeAttachments = this.volumeAttachmentClient.getVolumeAttachmentsForCurrentNode();

            for (Map.Entry<String, VolumeAttachmentInfo> entry : volumeAttachments.entrySet()) {
                String targetPath = entry.getKey();
                VolumeAttachmentInfo attachmentInfo = entry.getValue();

                if (!Files.exists(Paths.get(targetPath))) {
                    // Mount volumes for any missing target paths
                    log.info("Target path '{}' is missing. Attempting to mount...", targetPath);
                    try {
                        mountVolume(attachmentInfo.getHdfsConnection(), attachmentInfo.getHdfsUri(), targetPath);
                    } catch (Exception e) {
                        log.error("Failed to mount volume for target path '{}': {}", targetPath, e.getMessage(), e);
                    }
                } else {
                    log.info("Target path '{}' already exists. Skipping mount.", targetPath);
                }
            }

            // Clean up stale mounts not listed in VolumeAttachments
            this.cleanupStaleMounts(volumeAttachments.keySet());

            log.info("Initialization completed.");
        } catch (Exception e) {
            log.error("Error during initialization: {}", e.getMessage(), e);
        }
    }

    /**
     * Mounts an HDFS volume to a local target path, creating an HdfsVirtualFileSystem instance.
     *
     * @param hdfsUri    The HDFS URI (e.g., hdfs://localhost:8020/path).
     * @param targetPath The local target path where the volume should be mounted.
     */
    public void mountVolume(HdfsConnection hdfsConnection, String hdfsUri, String targetPath) {
        log.info("Mounting HDFS volume '{}' to local path '{}'", hdfsUri, targetPath);

        Path target = Paths.get(targetPath);
        if (this.hdfsVfsInstances.containsKey(targetPath)) {
            log.warn("Volume at path '{}' is already mounted. Skipping...", targetPath);
            return;
        }

        // Create and mount with a new HdfsVirtualFileSystem instance
        HdfsVirtualFileSystem hdfsVirtualFileSystem = new HdfsVirtualFileSystem(hdfsUri, hdfsConnection);
        try {
            hdfsVirtualFileSystem.mount(target, false, true);
            this.hdfsVfsInstances.put(targetPath, hdfsVirtualFileSystem); // Track the instance
            log.info("Successfully mounted HDFS volume '{}' to '{}'", hdfsUri, targetPath);
        } catch (Exception e) {
            log.error("Failed to mount HDFS volume '{}' to '{}': {}", hdfsUri, targetPath, e.getMessage());
            throw e;
        }
    }

    /**
     * Unmounts an HDFS volume from a local target path, and removes its HdfsVirtualFileSystem instance.
     *
     * @param targetPath The local target path to be unmounted.
     */
    public void unmountVolume(String targetPath) {
        log.info("Unmounting volume at '{}'", targetPath);

        Path target = Paths.get(targetPath);
        HdfsVirtualFileSystem hdfsVirtualFileSystem = this.hdfsVfsInstances.get(targetPath);

        if (hdfsVirtualFileSystem == null) {
            log.warn("No HDFS mount found for path '{}'. Skipping unmount.", targetPath);
            return;
        }

        try {
            hdfsVirtualFileSystem.umount();
            this.hdfsVfsInstances.remove(targetPath); // Remove from the map
            log.info("Successfully unmounted volume at '{}'", targetPath);
        } catch (Exception e) {
            log.error("Failed to unmount volume at '{}': {}", targetPath, e.getMessage());
            throw e;
        }
    }

    /**
     * Cleans up stale mounts (paths mounted on the current node that are not part of VolumeAttachments).
     *
     * @param validMountPaths The set of target paths that should remain mounted.
     */
    private void cleanupStaleMounts(Set<String> validMountPaths) {
        log.info("Cleaning up stale mount points...");

        // Iterate over tracked mounts
        for (String mountPath : this.hdfsVfsInstances.keySet()) {
            if (!validMountPaths.contains(mountPath)) {
                log.info("Mount point '{}' is not valid. Attempting to unmount...", mountPath);
                try {
                    unmountVolume(mountPath);
                } catch (Exception e) {
                    log.error("Failed to unmount stale mount point '{}': {}", mountPath, e.getMessage());
                }
            }
        }

        log.info("Stale mount cleanup completed.");
    }
}
