package varga.vorath.node;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;

@Component
public class NodePublishVolumeHandler {

    private static final Logger logger = LoggerFactory.getLogger(NodePublishVolumeHandler.class);

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

        if (volumeId == null || volumeId.isEmpty() || targetPath == null || targetPath.isEmpty()) {
            logger.error("Volume ID or Target Path is missing. Volume ID: {}, Target Path: {}", volumeId, targetPath);
            responseObserver.onError(new IllegalArgumentException("Volume ID or Target Path is missing."));
            return;
        }

        try {
            Path path = Paths.get(targetPath);
            if (!Files.exists(path)) {
                Files.createDirectories(path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x")));
                logger.info("Target directory created at path: {}", targetPath);
            }

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