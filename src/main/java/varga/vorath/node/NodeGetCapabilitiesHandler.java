package varga.vorath.node;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class NodeGetCapabilitiesHandler {

    private static final Logger logger = LoggerFactory.getLogger(NodeGetCapabilitiesHandler.class);

    /**
     * Handles the retrieval of node capabilities.
     *
     * @param request           The request to get node capabilities.
     * @param responseObserver  The observer to send the response to the client.
     */
    public void handleNodeGetCapabilities(Csi.NodeGetCapabilitiesRequest request,
                                          StreamObserver<Csi.NodeGetCapabilitiesResponse> responseObserver) {
        try {
            // Build node capabilities
            List<Csi.NodeServiceCapability> capabilities = getNodeCapabilities();

            // Build the response
            Csi.NodeGetCapabilitiesResponse response = Csi.NodeGetCapabilitiesResponse.newBuilder()
                    .addAllCapabilities(capabilities)
                    .build();

            // Send the response to the client
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.info("Node capabilities successfully sent.");
        } catch (Exception e) {
            // Handle any error and notify the client
            logger.error("Error while retrieving node capabilities: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * Provides the available capabilities of the node.
     *
     * @return List of available capabilities.
     */
    private List<Csi.NodeServiceCapability> getNodeCapabilities() {
        // Example capability: STAGE_UNSTAGE_VOLUME
        Csi.NodeServiceCapability stageUnstageCapability = Csi.NodeServiceCapability.newBuilder()
                .setRpc(Csi.NodeServiceCapability.RPC.newBuilder()
                        .setType(Csi.NodeServiceCapability.RPC.Type.STAGE_UNSTAGE_VOLUME)
                        .build())
                .build();

        // Return a single capability (add more as needed)
        return Collections.singletonList(stageUnstageCapability);
    }
}