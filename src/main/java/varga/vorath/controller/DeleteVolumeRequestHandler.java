package varga.vorath.controller;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import varga.vorath.hdfs.HdfsVolumeService;
import varga.vorath.kubernetes.KubernetesVolumeService;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class DeleteVolumeRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(DeleteVolumeRequestHandler.class);

    private final HdfsVolumeService hdfsVolumeService;
    private final KubernetesVolumeService kubernetesVolumeService;

    public void handleDeleteVolume(Csi.DeleteVolumeRequest request,
                                   StreamObserver<Csi.DeleteVolumeResponse> responseObserver) {
        String volumeId = request.getVolumeId();

        try {
            // Validate input
            if (volumeId == null || volumeId.isEmpty()) {
                throw new IllegalArgumentException("Volume ID is missing!");
            }

            // Fetch the PersistentVolume from Kubernetes
            Optional<V1PersistentVolume> persistentVolumeOpt = kubernetesVolumeService.getPersistentVolumeById(volumeId);

            if (persistentVolumeOpt.isEmpty()) {
                throw new IllegalArgumentException("Volume ID not found in Kubernetes: " + volumeId);
            }

            V1PersistentVolume persistentVolume = persistentVolumeOpt.get();

            // Delegate HDFS deletion logic to HdfsVolumeService
            String hdfsPath = persistentVolume.getSpec().getCsi().getVolumeAttributes().get("hdfsPath");

            if (hdfsPath == null || hdfsPath.isEmpty()) {
                throw new IllegalArgumentException("HDFS path is missing in volume context!");
            }

            hdfsVolumeService.deleteVolume(hdfsPath);

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