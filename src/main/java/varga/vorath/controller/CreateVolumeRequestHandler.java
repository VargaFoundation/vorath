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
public class CreateVolumeRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(CreateVolumeRequestHandler.class);

    private final Map<String, Csi.Volume> volumeStore = new HashMap<>();
    private final HdfsVolumeService hdfsVolumeService;

    public CreateVolumeRequestHandler(HdfsVolumeService hdfsVolumeService) {
        this.hdfsVolumeService = hdfsVolumeService;
    }

    public void handleCreateVolume(Csi.CreateVolumeRequest request,
                                   StreamObserver<Csi.CreateVolumeResponse> responseObserver) {
        String volumeName = request.getName();
        long requiredBytes = request.getCapacityRange().getRequiredBytes();

        try {
            // Validate input
            if (volumeName == null || volumeName.isEmpty()) {
                throw new IllegalArgumentException("Volume name is missing!");
            }

            if (requiredBytes <= 0) {
                throw new IllegalArgumentException("Requested capacity is invalid!");
            }

            // Check if a volume with the same name already exists
            if (volumeStore.containsKey(volumeName)) {
                Csi.Volume existingVolume = volumeStore.get(volumeName);
                Csi.CreateVolumeResponse response = Csi.CreateVolumeResponse.newBuilder()
                        .setVolume(existingVolume)
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
                logger.info("Returning existing volume: {}", volumeName);
                return;
            }

            // Delegate HDFS logic to HdfsVolumeService
            hdfsVolumeService.createVolume(volumeName);

            // Create volume metadata and store it
            String volumeId = "vol-" + System.currentTimeMillis();
            Csi.Volume newVolume = Csi.Volume.newBuilder()
                    .setVolumeId(volumeId)
                    .setCapacityBytes(requiredBytes)
                    .putVolumeContext("storageClass", request.getParametersMap().getOrDefault("storageClass", "default"))
                    .putVolumeContext("hdfsPath", "/volumes/" + volumeName)
                    .build();

            volumeStore.put(volumeName, newVolume);

            // Send the response
            Csi.CreateVolumeResponse response = Csi.CreateVolumeResponse.newBuilder()
                    .setVolume(newVolume)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.info("Successfully created volume: {} (ID: {})", volumeName, volumeId);
        } catch (Exception e) {
            // Handle errors and send an error response back to the gRPC caller
            logger.error("Error while creating volume: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}