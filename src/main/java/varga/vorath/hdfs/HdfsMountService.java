package varga.vorath.hdfs;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;

@Component
@RequiredArgsConstructor
public class HdfsMountService {

    private static final Logger logger = LoggerFactory.getLogger(HdfsMountService.class);

    private final KubernetesVolumeAttachmentClient volumeAttachmentClient;
    private final HdfsVirtualFileSystem hdfsVirtualFileSystem;

    /**
     * Initializes the service by querying Kubernetes VolumeAttachment resources and performing
     * necessary volume mount and unmount operations.
     */
    @PostConstruct
    public void init() {
        logger.info("Initializing HdfsMountService...");

        try {
            // Query the VolumeAttachment objects for the current node
            Map<String, VolumeAttachmentInfo> volumeAttachments = volumeAttachmentClient.getVolumeAttachmentsForCurrentNode();

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
                    // Skip mounting if the path already exists
                    logger.info("Target path '{}' already exists. Skipping mount.", targetPath);
                }
            }

            // Optionally: Unmount stale paths not listed in VolumeAttachments
            cleanupStaleMounts(volumeAttachments.keySet());

            logger.info("Initialization completed.");
        } catch (Exception e) {
            logger.error("Error during initialization: {}", e.getMessage(), e);
        }
    }

    /**
     * Mounts an HDFS volume to a local target path.
     *
     * @param hdfsUri    The HDFS URI for the volume (e.g., hdfs://localhost:8020/path).
     * @param targetPath The local path where the volume should be mounted.
     * @throws Exception If an error occurs while mounting.
     */
    public void mountVolume(String hdfsUri, String targetPath) throws Exception {
        logger.info("Mounting HDFS volume '{}' to local path '{}'", hdfsUri, targetPath);

        Path target = Paths.get(targetPath);

        try {
            // Use HdfsVirtualFileSystem to mount the volume
            hdfsVirtualFileSystem.mount(target, hdfsUri, true, false); // Debug and foreground mode for demo purposes
            logger.info("Successfully mounted HDFS volume '{}' to '{}'", hdfsUri, targetPath);
        } catch (Exception e) {
            logger.error("Failed to mount HDFS volume '{}' to '{}': {}", hdfsUri, targetPath, e.getMessage());
            throw e;
        }
    }

    /**
     * Unmounts an HDFS volume from a local target path.
     *
     * @param targetPath The local target path to be unmounted.
     * @throws Exception If an error occurs while unmounting.
     */
    public void unmountVolume(String targetPath) throws Exception {
        logger.info("Unmounting volume at '{}'", targetPath);

        Path target = Paths.get(targetPath);

        try {
            // Use HdfsVirtualFileSystem to unmount the volume
            hdfsVirtualFileSystem.umount(target);
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

        // Query the list of all current mount points
        Set<String> currentMountPaths = hdfsVirtualFileSystem.listMounts();

        for (String mountPath : currentMountPaths) {
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

}