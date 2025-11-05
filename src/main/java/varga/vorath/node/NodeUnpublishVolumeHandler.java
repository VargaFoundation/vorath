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

@Component
@RequiredArgsConstructor
public class NodeUnpublishVolumeHandler {

    private static final Logger logger = LoggerFactory.getLogger(NodeUnpublishVolumeHandler.class);
    private final HdfsMountService hdfsMountService;

    /**
     * Handles the release of a volume in response to the NodeUnpublishVolume request.
     *
     * @param request           The request to unpublish the volume.
     * @param responseObserver  The observer to send the response back to the client.
     */
    public void handleNodeUnpublishVolume(Csi.NodeUnpublishVolumeRequest request,
                                          StreamObserver<Csi.NodeUnpublishVolumeResponse> responseObserver) {
        String volumeId = request.getVolumeId();
        String targetPath = request.getTargetPath();

        try {
            // Validate input parameters
            if (volumeId == null || volumeId.isEmpty() || targetPath == null || targetPath.isEmpty()) {
                throw new IllegalArgumentException("Volume ID or Target Path is missing. Volume ID: " + volumeId + ", Target Path: " + targetPath);
            }

            logger.info("Request received to unpublish volume. Volume ID: {}, Target Path: {}", volumeId, targetPath);

            Path path = Paths.get(targetPath);

            // Check idempotency: If the path doesn't exist, no additional action is needed
            if (!Files.exists(path)) {
                logger.warn("The target path '{}' does not exist. The volume is already unpublished or not mounted.", targetPath);
                Csi.NodeUnpublishVolumeResponse response = Csi.NodeUnpublishVolumeResponse.newBuilder().build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            // Use the service to unmount the volume
            this.hdfsMountService.unmountVolume(targetPath);

            // Respond to the client confirming the success
            Csi.NodeUnpublishVolumeResponse response = Csi.NodeUnpublishVolumeResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.info("Volume '{}' successfully unpublished and target path '{}' cleaned up.", volumeId, targetPath);
        } catch (IllegalArgumentException e) {
            logger.error("Error in input parameters: {}", e.getMessage());
            responseObserver.onError(e);
        } catch (Exception e) {
            logger.error("Unexpected error while unpublishing the volume: {}", e.getMessage(), e);
            responseObserver.onError(new RuntimeException("Error while unpublishing the volume.", e));
        }
    }
}