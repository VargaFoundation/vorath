package varga.vorath.hdfs;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import varga.vorath.kubernetes.KubernetesVolumeAttachmentClient;

import javax.annotation.PostConstruct;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class HdfsMountService {

    private static final Logger logger = LoggerFactory.getLogger(HdfsMountService.class);

    private final KubernetesVolumeAttachmentClient volumeAttachmentClient;
    private final Map<String, HdfsVirtualFileSystem> hdfsVfsInstances = new ConcurrentHashMap<>();

    /**
     * Initializes the service by querying Kubernetes VolumeAttachment resources and performing
     * necessary volume mount and unmount operations.
     */
    @PostConstruct
    public void init() {
        logger.info("Initializing HdfsMountService...");

        try {
            // Query the VolumeAttachment objects for the current node
            Map<String, VolumeAttachmentInfo> volumeAttachments = this.volumeAttachmentClient.getVolumeAttachmentsForCurrentNode();

            for (Map.Entry<String, VolumeAttachmentInfo> entry : volumeAttachments.entrySet()) {
                String targetPath = entry.getKey();
                VolumeAttachmentInfo attachmentInfo = entry.getValue();

                if (!Files.exists(Paths.get(targetPath))) {
                    // Mount volumes for any missing target paths
                    logger.info("Target path '{}' is missing. Attempting to mount...", targetPath);
                    try {
                        mountVolume(attachmentInfo.getHdfsUri(), targetPath);
                    } catch (Exception e) {
                        logger.error("Failed to mount volume for target path '{}': {}", targetPath, e.getMessage(), e);
                    }
                } else {
                    logger.info("Target path '{}' already exists. Skipping mount.", targetPath);
                }
            }

            // Clean up stale mounts not listed in VolumeAttachments
            this.cleanupStaleMounts(volumeAttachments.keySet());

            logger.info("Initialization completed.");
        } catch (Exception e) {
            logger.error("Error during initialization: {}", e.getMessage(), e);
        }
    }

    /**
     * Mounts an HDFS volume to a local target path, creating an HdfsVirtualFileSystem instance.
     *
     * @param hdfsUri    The HDFS URI (e.g., hdfs://localhost:8020/path).
     * @param targetPath The local target path where the volume should be mounted.
     */
    public void mountVolume(String hdfsUri, String targetPath) {
        logger.info("Mounting HDFS volume '{}' to local path '{}'", hdfsUri, targetPath);

        Path target = Paths.get(targetPath);
        if (this.hdfsVfsInstances.containsKey(targetPath)) {
            logger.warn("Volume at path '{}' is already mounted. Skipping...", targetPath);
            return;
        }

        // Create and mount with a new HdfsVirtualFileSystem instance
        HdfsVirtualFileSystem hdfsVirtualFileSystem = new HdfsVirtualFileSystem(hdfsUri);
        try {
            hdfsVirtualFileSystem.mount(target, false, true);
            this.hdfsVfsInstances.put(targetPath, hdfsVirtualFileSystem); // Track the instance
            logger.info("Successfully mounted HDFS volume '{}' to '{}'", hdfsUri, targetPath);
        } catch (Exception e) {
            logger.error("Failed to mount HDFS volume '{}' to '{}': {}", hdfsUri, targetPath, e.getMessage());
            throw e;
        }
    }

    /**
     * Unmounts an HDFS volume from a local target path, and removes its HdfsVirtualFileSystem instance.
     *
     * @param targetPath The local target path to be unmounted.
     */
    public void unmountVolume(String targetPath) {
        logger.info("Unmounting volume at '{}'", targetPath);

        Path target = Paths.get(targetPath);
        HdfsVirtualFileSystem hdfsVirtualFileSystem = this.hdfsVfsInstances.get(targetPath);

        if (hdfsVirtualFileSystem == null) {
            logger.warn("No HDFS mount found for path '{}'. Skipping unmount.", targetPath);
            return;
        }

        try {
            hdfsVirtualFileSystem.umount();
            this.hdfsVfsInstances.remove(targetPath); // Remove from the map
            logger.info("Successfully unmounted volume at '{}'", targetPath);
        } catch (Exception e) {
            logger.error("Failed to unmount volume at '{}': {}", targetPath, e.getMessage());
            throw e;
        }
    }

    /**
     * Cleans up stale mounts (paths mounted on the current node that are not part of VolumeAttachments).
     *
     * @param validMountPaths The set of target paths that should remain mounted.
     */
    private void cleanupStaleMounts(Set<String> validMountPaths) {
        logger.info("Cleaning up stale mount points...");

        // Iterate over tracked mounts
        for (String mountPath : this.hdfsVfsInstances.keySet()) {
            if (!validMountPaths.contains(mountPath)) {
                logger.info("Mount point '{}' is not valid. Attempting to unmount...", mountPath);
                try {
                    unmountVolume(mountPath);
                } catch (Exception e) {
                    logger.error("Failed to unmount stale mount point '{}': {}", mountPath, e.getMessage());
                }
            }
        }

        logger.info("Stale mount cleanup completed.");
    }
}
