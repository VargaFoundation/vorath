package varga.vorath.controller;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
public class ControllerGetCapabilitiesRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(ControllerGetCapabilitiesRequestHandler.class);

    /**
     * Handles the gRPC request to get the controller capabilities.
     *
     * @param request           The request to get controller capabilities.
     * @param responseObserver  The gRPC response observer to send the response to the client.
     */
    public void handleControllerGetCapabilities(Csi.ControllerGetCapabilitiesRequest request,
                                                StreamObserver<Csi.ControllerGetCapabilitiesResponse> responseObserver) {
        try {
            // Retrieve the list of controller capabilities
            List<Csi.ControllerServiceCapability> capabilities = getControllerCapabilities();

            // Build the response
            Csi.ControllerGetCapabilitiesResponse response = Csi.ControllerGetCapabilitiesResponse.newBuilder()
                    .addAllCapabilities(capabilities)
                    .build();

            // Send the response to the client
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.info("Controller capabilities successfully sent.");
        } catch (Exception e) {
            // Handle any errors and notify the client
            logger.error("Error while retrieving controller capabilities: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * Get the list of controller capabilities for the CSI driver. Update this list to match the driver capabilities.
     *
     * @return List of controller service capabilities.
     */
    private List<Csi.ControllerServiceCapability> getControllerCapabilities() {
        List<Csi.ControllerServiceCapability> capabilities = new ArrayList<>();

        // Capability 1: Create and delete volumes
        Csi.ControllerServiceCapability createDeleteVolumeCapability = Csi.ControllerServiceCapability.newBuilder()
                .setRpc(Csi.ControllerServiceCapability.RPC.newBuilder()
                        .setType(Csi.ControllerServiceCapability.RPC.Type.CREATE_DELETE_VOLUME)
                        .build())
                .build();
        capabilities.add(createDeleteVolumeCapability);

        // Capability 2: Publish and unpublish volumes
        Csi.ControllerServiceCapability publishUnpublishVolumeCapability = Csi.ControllerServiceCapability.newBuilder()
                .setRpc(Csi.ControllerServiceCapability.RPC.newBuilder()
                        .setType(Csi.ControllerServiceCapability.RPC.Type.PUBLISH_UNPUBLISH_VOLUME)
                        .build())
                .build();
        capabilities.add(publishUnpublishVolumeCapability);

        // Capability 3: Expand volumes (if your driver supports it)
        Csi.ControllerServiceCapability expandVolumeCapability = Csi.ControllerServiceCapability.newBuilder()
                .setRpc(Csi.ControllerServiceCapability.RPC.newBuilder()
                        .setType(Csi.ControllerServiceCapability.RPC.Type.EXPAND_VOLUME)
                        .build())
                .build();
        capabilities.add(expandVolumeCapability);

        // Add more capabilities as supported by your driver
        // For example: LIST_VOLUMES, CLONE_VOLUME, etc.

        logger.debug("Controller capabilities prepared: {}", capabilities);
        return capabilities;
    }
}