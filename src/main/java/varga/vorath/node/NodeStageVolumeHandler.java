package varga.vorath.node;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Component
public class NodeStageVolumeHandler {

    private static final Logger logger = LoggerFactory.getLogger(NodeStageVolumeHandler.class);

    /**
     * Handles the NodeStageVolume request to prepare a volume for use on this node.
     *
     * @param request           The NodeStageVolumeRequest from the client.
     * @param responseObserver  The response observer to send the result.
     */
    public void handleNodeStageVolume(Csi.NodeStageVolumeRequest request, StreamObserver<Csi.NodeStageVolumeResponse> responseObserver) {
        String volumeId = request.getVolumeId();
        String stagingTargetPath = request.getStagingTargetPath();

        try {
            logger.info("Processing NodeStageVolume request for volumeId: {}, stagingTargetPath: {}", volumeId, stagingTargetPath);

            // Example check: Ensure the staging target path exists or create it
            Path targetPath = Paths.get(stagingTargetPath);
            if (!Files.exists(targetPath)) {
                Files.createDirectories(targetPath);
                logger.info("Created staging target path: {}", stagingTargetPath);
            }

            // Simulate logic for "staging" the volume (e.g., mount or prepare volume)
            stageVolume(volumeId, targetPath);

            // Respond successfully
            Csi.NodeStageVolumeResponse response = Csi.NodeStageVolumeResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            logger.info("NodeStageVolume successfully completed for volumeId: {}", volumeId);

        } catch (Exception e) {
            logger.error("Error handling NodeStageVolume for volumeId: {}: {}", volumeId, e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * Simulate the logic to stage a volume.
     *
     * @param volumeId    The ID of the volume to stage.
     * @param targetPath  The staging target path where the volume will be prepared.
     * @throws Exception  If any issue occurs during staging.
     */
    private void stageVolume(String volumeId, Path targetPath) throws Exception {
        // Example logic: You can replace this with actual mounting or preparation logic
        logger.info("Staging volumeId: {} at path: {}", volumeId, targetPath.toString());

        // For example, if dealing with HDFS, you might set up mount points with FUSE or other tools
        // This is a placeholder for volume staging
        if (!Files.isDirectory(targetPath)) {
            throw new RuntimeException("Target path is not a directory: " + targetPath.toString());
        }
    }
}