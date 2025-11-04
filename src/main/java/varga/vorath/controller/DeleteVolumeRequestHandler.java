package varga.vorath.controller;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import varga.vorath.hdfs.HdfsVolumeService;

import java.util.HashMap;
import java.util.Map;

@Component
public class DeleteVolumeRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(DeleteVolumeRequestHandler.class);

    private final Map<String, Csi.Volume> volumeStore = new HashMap<>();
    private final HdfsVolumeService hdfsVolumeService;

    public DeleteVolumeRequestHandler(HdfsVolumeService hdfsVolumeService) {
        this.hdfsVolumeService = hdfsVolumeService;
    }

    public void handleDeleteVolume(Csi.DeleteVolumeRequest request,
                                   StreamObserver<Csi.DeleteVolumeResponse> responseObserver) {
        String volumeId = request.getVolumeId();

        try {
            // Validate input
            if (volumeId == null || volumeId.isEmpty()) {
                throw new IllegalArgumentException("Volume ID is missing!");
            }

            // Find the volume in the store
            Csi.Volume volumeToDelete = volumeStore.values()
                    .stream()
                    .filter(volume -> volume.getVolumeId().equals(volumeId))
                    .findFirst()
                    .orElse(null);

            if (volumeToDelete == null) {
                throw new IllegalArgumentException("Volume ID not found: " + volumeId);
            }

            // Delegate HDFS deletion logic to HdfsVolumeService
            String hdfsPath = volumeToDelete.getVolumeContextMap().get("hdfsPath");
            if (hdfsPath == null || hdfsPath.isEmpty()) {
                throw new IllegalArgumentException("HDFS path is missing in volume context!");
            }

            hdfsVolumeService.deleteVolume(hdfsPath);

            // Remove the volume from the in-memory store
            volumeStore.values().removeIf(volume -> volume.getVolumeId().equals(volumeId));

            // Send deletion confirmation to the client
            Csi.DeleteVolumeResponse response = Csi.DeleteVolumeResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.info("Volume successfully deleted, ID: {}", volumeId);

        } catch (Exception e) {
            // Capture and log errors
            logger.error("Error while deleting volume: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}