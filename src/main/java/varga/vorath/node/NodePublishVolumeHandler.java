package varga.vorath.node;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import varga.vorath.hdfs.HdfsConnection;
import varga.vorath.hdfs.HdfsMountService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class NodePublishVolumeHandler {

    private static final Logger logger = LoggerFactory.getLogger(NodePublishVolumeHandler.class);

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
        String authType = volumeContext.getOrDefault("auth", "simple");



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

        logger.info("Handling NodePublishVolume for Volume ID: {} at Target Path: {}", volumeId, targetPath);

        HdfsConnection.createHdfsConnection()

        "location", location);

        // Add secrets used for provisioning (if applicable)
        if (secretName != null && secretNamespace != null) {
            volumeBuilder.putVolumeContext("secretName", secretName);
            volumeBuilder.putVolumeContext("secretNamespace", secretNamespace);
        }


        try {
            Path path = Paths.get(targetPath);
            if (Files.exists(path)) {
                logger.info("Target path '{}' already exists for volume '{}'. Assuming idempotent request.", targetPath, volumeId);
                Csi.NodePublishVolumeResponse response = Csi.NodePublishVolumeResponse.newBuilder().build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            Files.createDirectories(path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x")));
            logger.info("Target directory created at path: {}", targetPath);

            // Mount the volume with HDFS path
            this.hdfsMountService.mountVolume(hdfsPath, targetPath);

            logger.info("Volume {} is successfully published to {}", volumeId, targetPath);

            Csi.NodePublishVolumeResponse response = Csi.NodePublishVolumeResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error while publishing volume {} to target path {}: {}", volumeId, targetPath, e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}