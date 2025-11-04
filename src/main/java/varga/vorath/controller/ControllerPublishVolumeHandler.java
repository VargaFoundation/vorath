package varga.vorath.controller;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import varga.vorath.hdfs.HdfsVolumeService;

import java.io.IOException;

@Component
public class ControllerPublishVolumeHandler {

    private static final Logger logger = LoggerFactory.getLogger(ControllerPublishVolumeHandler.class);
    private final HdfsVolumeService hdfsVolumeService;

    public ControllerPublishVolumeHandler(HdfsVolumeService hdfsVolumeService) {
        this.hdfsVolumeService = hdfsVolumeService;
    }

    /**
     * Handles the ControllerPublishVolume gRPC request.
     *
     * @param request          The ControllerPublishVolumeRequest from the client.
     * @param responseObserver The response observer to send the result.
     */
    public void handleControllerPublishVolume(Csi.ControllerPublishVolumeRequest request,
                                              StreamObserver<Csi.ControllerPublishVolumeResponse> responseObserver) {
        String volumeId = request.getVolumeId();
        String nodeId = request.getNodeId();

        logger.info("Handling ControllerPublishVolume for Volume ID: {}, Node ID: {}", volumeId, nodeId);

        try {
            // Check if the volume exists in HDFS
            if (!hdfsVolumeService.volumeExists(volumeId)) {
                throw new IllegalArgumentException("Volume with ID '" + volumeId + "' does not exist in HDFS.");
            }

            // Simulate binding the volume to the node (could involve updating metadata, etc.)
            logger.info("Volume '{}' successfully published to Node '{}'.", volumeId, nodeId);

            // Send a successful response
            responseObserver.onNext(Csi.ControllerPublishVolumeResponse.newBuilder().build());
            responseObserver.onCompleted();

        } catch (IllegalArgumentException e) {
            logger.error("Volume binding failed: {}", e.getMessage());
            responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (IOException e) {
            logger.error("Error accessing HDFS for volume '{}': {}", volumeId, e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Internal error accessing HDFS: " + e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            logger.error("Unexpected error occurred during volume publication: {}", e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.UNKNOWN
                    .withDescription("Unexpected error: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}