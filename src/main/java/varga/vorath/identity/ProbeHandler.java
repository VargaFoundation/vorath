package varga.vorath.identity;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import varga.vorath.hdfs.HdfsVolumeService;

@Component
@RequiredArgsConstructor
public class ProbeHandler {

    private static final Logger logger = LoggerFactory.getLogger(ProbeHandler.class);

    private final HdfsVolumeService hdfsVolumeService;

    /**
     * Handles the Probe request by constructing a standard response.
     *
     * @param request           The received Probe request.
     * @param responseObserver  The observer to send the response to the client.
     */
    public void handleProbe(Csi.ProbeRequest request, StreamObserver<Csi.ProbeResponse> responseObserver) {
        try {
            // Check if the system is operational by verifying the existence of a well-known volume or path.
            String testVolume = "probe-volume";  // A predefined volume name for the probe check

            if (hdfsVolumeService.volumeExists(testVolume)) {
                // If the volume exists, build and send a successful Probe response.
                Csi.ProbeResponse response = Csi.ProbeResponse.newBuilder().build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

                logger.info("Probe handled successfully. HDFS is operational.");
            } else {
                // Volume does not exist, indicating potential issues; throw an error.
                throw new IllegalStateException("Probe failed: HDFS test volume does not exist");
            }
        } catch (Exception e) {
            // Log the error and send the exception to the client.
            logger.error("Error in handling Probe: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

}