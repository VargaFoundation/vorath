package varga.vorath.node;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class NodeGetInfoHandler {

    private final String nodeId; // Unique identifier for this Node
    private final int maxVolumesPerNode; // Maximum volumes attachable to this Node

    public NodeGetInfoHandler(String nodeId, int maxVolumesPerNode) {
        this.nodeId = nodeId;
        this.maxVolumesPerNode = maxVolumesPerNode;
    }

    /**
     * Handles the NodeGetInfo request from the CSI Node Service.
     *
     * @param request           The NodeGetInfoRequest from the client.
     * @param responseObserver  The response observer to send the result.
     */
    public void handleNodeGetInfo(Csi.NodeGetInfoRequest request, StreamObserver<Csi.NodeGetInfoResponse> responseObserver) {
        try {
            log.info("Processing NodeGetInfo request...");

            // Construct the NodeGetInfo response
            Csi.NodeGetInfoResponse response = Csi.NodeGetInfoResponse.newBuilder()
                    .setNodeId(nodeId)
                    .setMaxVolumesPerNode(maxVolumesPerNode)
                    .build();

            // Send the response
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            log.info("NodeGetInfo response sent successfully. Node ID: {}, Max Volumes Per Node: {}", nodeId, maxVolumesPerNode);

        } catch (Exception e) {
            log.error("Error handling NodeGetInfo request: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}