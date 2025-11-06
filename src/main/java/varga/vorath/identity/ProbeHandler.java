package varga.vorath.identity;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ProbeHandler {

    private static final Logger logger = LoggerFactory.getLogger(ProbeHandler.class);

    /**
     * Handles the Probe request by constructing a standard response.
     *
     * @param request          The received Probe request.
     * @param responseObserver The observer to send the response to the client.
     */
    public void handleProbe(Csi.ProbeRequest request, StreamObserver<Csi.ProbeResponse> responseObserver) {
        try {

            Csi.ProbeResponse response = Csi.ProbeResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            // Log the error and send the exception to the client.
            logger.error("Error in handling Probe: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

}