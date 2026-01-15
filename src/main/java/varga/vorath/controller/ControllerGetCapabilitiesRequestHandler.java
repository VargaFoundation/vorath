package varga.vorath.controller;

/*-
 * #%L
 * Vorath
 * %%
 * Copyright (C) 2025 - 2026 Varga Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class ControllerGetCapabilitiesRequestHandler {

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

            log.info("Controller capabilities successfully sent.");
        } catch (Exception e) {
            // Handle any errors and notify the client
            log.error("Error while retrieving controller capabilities: {}", e.getMessage(), e);
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

        // Capability 2: List volumes
        Csi.ControllerServiceCapability listVolumesCapability = Csi.ControllerServiceCapability.newBuilder()
                .setRpc(Csi.ControllerServiceCapability.RPC.newBuilder()
                        .setType(Csi.ControllerServiceCapability.RPC.Type.LIST_VOLUMES)
                        .build())
                .build();
        capabilities.add(listVolumesCapability);

        // Capability 3: Get capacity
        Csi.ControllerServiceCapability getCapacityCapability = Csi.ControllerServiceCapability.newBuilder()
                .setRpc(Csi.ControllerServiceCapability.RPC.newBuilder()
                        .setType(Csi.ControllerServiceCapability.RPC.Type.GET_CAPACITY)
                        .build())
                .build();
        capabilities.add(getCapacityCapability);

        // Add more capabilities as supported by your driver
        // For example: LIST_VOLUMES, CLONE_VOLUME, etc.

        log.debug("Controller capabilities prepared: {}", capabilities);
        return capabilities;
    }
}
