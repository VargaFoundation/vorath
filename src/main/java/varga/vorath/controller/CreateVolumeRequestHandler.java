package varga.vorath.controller;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import varga.vorath.hdfs.HdfsVolumeService;
import varga.vorath.kubernetes.KubernetesVolumeService;

import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class CreateVolumeRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(CreateVolumeRequestHandler.class);

    private final HdfsVolumeService hdfsVolumeService;
    private final KubernetesVolumeService kubernetesVolumeService;

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
            if (kubernetesVolumeService.getPersistentVolumeByName(volumeName).isPresent()) {
                logger.info("Volume '{}' already exists, returning existing volume.", volumeName);
                Csi.Volume existingVolume = kubernetesVolumeService.buildCsiVolumeFromPersistentVolume(volumeName);
                Csi.CreateVolumeResponse response = Csi.CreateVolumeResponse.newBuilder()
                        .setVolume(existingVolume)
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            // Delegate HDFS logic to HdfsVolumeService
            this.hdfsVolumeService.createVolume(volumeName);

            // Generate the HDFS path and metadata
            String volumeId = "vol-" + System.currentTimeMillis();
            String hdfsPath = "/volumes/" + volumeName;
            Map<String, String> volumeContext = Map.of(
                    "hdfsPath", hdfsPath,
                    "storageClass", request.getParametersMap().getOrDefault("storageClass", "default")
            );

            // Create the PersistentVolume in Kubernetes
            this.kubernetesVolumeService.createPersistentVolume(volumeId, volumeName, hdfsPath, requiredBytes, volumeContext);


            // Create the response
            Csi.Volume newVolume = Csi.Volume.newBuilder()
                    .setVolumeId(volumeId)
                    .setCapacityBytes(requiredBytes)
                    .putAllVolumeContext(volumeContext)
                    .build();

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