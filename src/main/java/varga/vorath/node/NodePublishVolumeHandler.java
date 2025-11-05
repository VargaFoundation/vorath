package varga.vorath.node;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import varga.vorath.hdfs.HdfsMountService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;

@Component
@RequiredArgsConstructor
public class NodePublishVolumeHandler {

    private static final Logger logger = LoggerFactory.getLogger(NodePublishVolumeHandler.class);

    private final HdfsMountService hdfsMountService;

    /**
     * Handles the NodePublishVolume request.
     *
     * @param request           The request for publishing the volume.
     * @param responseObserver  The observer to send the response to the client.
     */
    public void handleNodePublishVolume(Csi.NodePublishVolumeRequest request,
                                        StreamObserver<Csi.NodePublishVolumeResponse> responseObserver) {
        String volumeId = request.getVolumeId();
        String targetPath = request.getTargetPath();
        String hdfsPath = request.getVolumeContextMap().get("hdfsPath");

        if (volumeId == null || volumeId.isEmpty()) {
            throw new IllegalArgumentException("Volume ID is missing.");
        }

        if (targetPath == null || targetPath.isEmpty()) {
            throw new IllegalArgumentException("Target path is missing.");
        }

        if (!request.getVolumeContextMap().containsKey("hdfsPath")) {
            throw new IllegalArgumentException("HDFS path is missing in volume context.");
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