package varga.vorath.node;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;

@Component
public class NodeUnstageVolumeHandler {

    private static final Logger logger = LoggerFactory.getLogger(NodeUnstageVolumeHandler.class);

    /**
     * Handles the NodeUnstageVolume request to clean up a staged volume on this node.
     *
     * @param request           The NodeUnstageVolumeRequest from the client.
     * @param responseObserver  The response observer to send the result.
     */
    public void handleNodeUnstageVolume(Csi.NodeUnstageVolumeRequest request, StreamObserver<Csi.NodeUnstageVolumeResponse> responseObserver) {
        String volumeId = request.getVolumeId();
        String stagingTargetPath = request.getStagingTargetPath();

        try {
            logger.info("Processing NodeUnstageVolume request for volumeId: {}, stagingTargetPath: {}", volumeId, stagingTargetPath);

            // Validate the input parameters
            if (volumeId == null || volumeId.isEmpty() || stagingTargetPath == null || stagingTargetPath.isEmpty()) {
                throw new IllegalArgumentException("VolumeId and StagingTargetPath must not be empty.");
            }

            // Ensure that the staging target path exists
            Path targetPath = Paths.get(stagingTargetPath);
            if (!Files.exists(targetPath)) {
                logger.warn("StagingTargetPath does not exist. Assuming volume is already cleaned up. Path: {}", stagingTargetPath);
            } else {
                // Simulate the unstage logic (e.g., unmount directory or clean up resources)
                unstageVolume(targetPath);
            }

            // Respond successfully
            Csi.NodeUnstageVolumeResponse response = Csi.NodeUnstageVolumeResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            logger.info("NodeUnstageVolume successfully completed for volumeId: {}", volumeId);

        } catch (Exception e) {
            logger.error("Error handling NodeUnstageVolume for volumeId: {}: {}", volumeId, e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * Simulates the logic to unstage a volume.
     *
     * @param targetPath  The staging target path to clean up.
     * @throws IOException  If an error occurs during the cleanup.
     */
    private void unstageVolume(Path targetPath) throws IOException {
        // Example cleanup logic (this can be customized for real use cases)
        logger.info("Cleaning up staging target path: {}", targetPath.toString());

        // If there is a mounted volume, unmount it (example placeholder logic)
        // For now, we'll assume all resources are "staged" within this directory.
        if (Files.isDirectory(targetPath)) {
            Files.delete(targetPath);
            logger.info("Successfully cleaned up staging target path: {}", targetPath.toString());
        } else {
            throw new IOException("Staging target path is not a directory: " + targetPath.toString());
        }
    }
}