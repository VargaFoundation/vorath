package varga.vorath.controller;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import varga.vorath.Utils;
import varga.vorath.hdfs.HdfsConnection;
import varga.vorath.hdfs.HdfsVolumeService;
import varga.vorath.kubernetes.KubernetesVolumeService;

import java.net.URI;
import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
public class DeleteVolumeRequestHandler {

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
            Optional<V1PersistentVolume> persistentVolumeOpt = this.kubernetesVolumeService.getPersistentVolumeById(volumeId);

            if (persistentVolumeOpt.isEmpty()) {
                throw new IllegalArgumentException("Volume ID not found in Kubernetes: " + volumeId);
            }

            V1PersistentVolume persistentVolume = persistentVolumeOpt.get();

            // Delegate HDFS deletion logic to HdfsVolumeService
            String hdfsPath = persistentVolume.getSpec().getCsi().getVolumeAttributes().get("hdfsPath");

            if (hdfsPath == null || hdfsPath.isEmpty()) {
                throw new IllegalArgumentException("HDFS path is missing in volume context!");
            }

            String location = persistentVolume.getSpec().getCsi().getVolumeAttributes().get("location"); // optional path (full HDFS path)
            String secretName = persistentVolume.getSpec().getCsi().getVolumeAttributes().get("secretName");
            String secretNamespace = persistentVolume.getSpec().getCsi().getVolumeAttributes().get("secretNamespace");

            // Secrets are mandatory according to requirements
            HdfsConnection hdfsConnection = HdfsConnection.createHdfsConnection(secretName, secretNamespace, Utils.extractClusterUri(location));

            // For deletion, we can operate without rebuilding an HDFS connection since we delete by path
            this.hdfsVolumeService.deleteVolume(hdfsConnection, hdfsPath);

            // Send deletion confirmation to the client
            Csi.DeleteVolumeResponse response = Csi.DeleteVolumeResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            log.info("Volume successfully deleted, ID: {}", volumeId);

        } catch (Exception e) {
            // Capture and log errors
            log.error("Error while deleting volume: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

}