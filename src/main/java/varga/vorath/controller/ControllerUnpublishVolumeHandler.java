package varga.vorath.controller;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ControllerUnpublishVolumeHandler {

    private static final Logger logger = LoggerFactory.getLogger(ControllerUnpublishVolumeHandler.class);

    /**
     * Handles the ControllerUnpublishVolume request to detach a volume from a node.
     *
     * @param request           The ControllerUnpublishVolumeRequest from the client.
     * @param responseObserver  The response observer to send the result.
     */
    public void handleControllerUnpublishVolume(Csi.ControllerUnpublishVolumeRequest request,
                                                StreamObserver<Csi.ControllerUnpublishVolumeResponse> responseObserver) {
        String volumeId = request.getVolumeId();
        String nodeId = request.getNodeId();

        try {
            logger.info("Processing ControllerUnpublishVolume request for volumeId: {}, nodeId: {}", volumeId, nodeId);

            // Validate inputs
            if (volumeId == null || volumeId.isEmpty()) {
                throw new IllegalArgumentException("Volume ID cannot be null or empty.");
            }
            if (nodeId == null || nodeId.isEmpty()) {
                throw new IllegalArgumentException("Node ID cannot be null or empty.");
            }

            // Simulate unpublishing logic (e.g., update metadata, clean up resources on the node, etc.)
            unpublishVolume(volumeId, nodeId);

            // Respond with success
            Csi.ControllerUnpublishVolumeResponse response = Csi.ControllerUnpublishVolumeResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            logger.info("ControllerUnpublishVolume successfully completed for volumeId: {}", volumeId);

        } catch (Exception e) {
            logger.error("Error handling ControllerUnpublishVolume for volumeId: {}: {}", volumeId, e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * Simulates the logic to unpublish/detach a volume from a specific node.
     *
     * @param volumeId The ID of the volume to unpublish.
     * @param nodeId   The ID of the node from which the volume is being detached.
     */
    private void unpublishVolume(String volumeId, String nodeId) {
        // Placeholder for real unpublish logic
        // Examples:
        // - Update metadata in a database to mark the volume as "detached"
        // - Notify the node to detach the volume (via specific APIs or messages)
        logger.info("Simulating unpublishing volume with volumeId: {} from nodeId: {}", volumeId, nodeId);

        // In a real implementation, this would include logic like:
        // - Verifying associations in a backend system
        // - Removing any node-specific metadata for this volume
    }
}