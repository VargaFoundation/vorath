package varga.vorath.node;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import varga.vorath.Utils;
import varga.vorath.hdfs.HdfsConnection;
import varga.vorath.hdfs.HdfsMountService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class NodePublishVolumeHandler {

    private final HdfsMountService hdfsMountService;

    /**
     * Handles the NodePublishVolume request.
     *
     * @param request          The request for publishing the volume.
     * @param responseObserver The observer to send the response to the client.
     */
    public void handleNodePublishVolume(Csi.NodePublishVolumeRequest request,
                                        StreamObserver<Csi.NodePublishVolumeResponse> responseObserver) {

        String volumeId = request.getVolumeId();
        String targetPath = request.getTargetPath();
        Map<String, String> volumeContext = request.getVolumeContextMap();

        if (volumeId == null || volumeId.isEmpty()) {
            throw new IllegalArgumentException("Volume ID is missing.");
        }

        if (targetPath == null || targetPath.isEmpty()) {
            throw new IllegalArgumentException("Target path is missing.");
        }

        String location = volumeContext.get("location");
        String secretName = volumeContext.get("secretName");
        String secretNamespace = volumeContext.get("secretNamespace");

        String hdfsPath = request.getVolumeContextMap().get("hdfsPath");

        //apiVersion: v1
        //kind: PersistentVolume
        //metadata:
        //  annotations:
        //    pv.kubernetes.io/provisioned-by: hdfs.csi.varga
        //  name: hdfsexample
        //spec:
        //  capacity:
        //    storage: 5Gi
        //  accessModes:
        //    - ReadWriteMany
        //  persistentVolumeReclaimPolicy: Retain
        //  storageClassName: ""
        //  csi:
        //    driver: hdfs.csi.varga
        //    volumeHandle: "hdfs://xxx:9090/path"  # make sure this volumeid is unique for every identical share in the cluster
        //    volumeAttributes:
        //      auth: kerberos
        //    nodeStageSecretRef:
        //      name: hdfs-secret
        //      namespace: expense

        log.info("Handling NodePublishVolume for Volume ID: {} at Target Path: {}", volumeId, targetPath);

        try {
            HdfsConnection hdfsConnection = HdfsConnection.createHdfsConnection(secretName, secretNamespace, Utils.extractClusterUri(location));

            Path path = Paths.get(targetPath);
            if (Files.exists(path)) {
                log.info("Target path '{}' already exists for volume '{}'. Assuming idempotent request.", targetPath, volumeId);
                Csi.NodePublishVolumeResponse response = Csi.NodePublishVolumeResponse.newBuilder().build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            Files.createDirectories(path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x")));
            log.info("Target directory created at path: {}", targetPath);

            // Mount the volume with HDFS path
            this.hdfsMountService.mountVolume(hdfsConnection, hdfsPath, targetPath);

            log.info("Volume {} is successfully published to {}", volumeId, targetPath);

            Csi.NodePublishVolumeResponse response = Csi.NodePublishVolumeResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error while publishing volume {} to target path {}: {}", volumeId, targetPath, e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}