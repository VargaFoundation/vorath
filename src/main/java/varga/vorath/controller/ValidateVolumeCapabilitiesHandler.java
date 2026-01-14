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
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class ValidateVolumeCapabilitiesHandler {

    /**
     * Handles the ValidateVolumeCapabilities request to verify if the volume supports the requested capabilities.
     *
     * @param request           The ValidateVolumeCapabilitiesRequest from the client.
     * @param responseObserver  The response observer to send the result.
     */
    public void handleValidateVolumeCapabilities(Csi.ValidateVolumeCapabilitiesRequest request,
                                                 StreamObserver<Csi.ValidateVolumeCapabilitiesResponse> responseObserver) {
        String volumeId = request.getVolumeId();
        List<Csi.VolumeCapability> requestedCapabilities = request.getVolumeCapabilitiesList();

        try {
            log.info("Processing ValidateVolumeCapabilities request for volumeId: {}", volumeId);
            log.info("Requested Volume Capabilities: {}", requestedCapabilities);

            // Validate the volume ID
            if (volumeId == null || volumeId.isEmpty()) {
                throw new IllegalArgumentException("VolumeId cannot be empty.");
            }

            // Simulate volume validation (normally, volume details are fetched from the backend or database)
            if (!isVolumeAvailable(volumeId)) {
                throw new IllegalArgumentException("Volume with ID " + volumeId + " does not exist.");
            }

            // Validate requested capabilities
            List<Csi.VolumeCapability> unsupportedCapabilities = validateCapabilities(requestedCapabilities);

            // Build the response
            Csi.ValidateVolumeCapabilitiesResponse.Builder responseBuilder = Csi.ValidateVolumeCapabilitiesResponse.newBuilder();
            if (unsupportedCapabilities.isEmpty()) {
                responseBuilder.setConfirmed(Csi.ValidateVolumeCapabilitiesResponse.Confirmed.newBuilder()
                        .addAllVolumeCapabilities(requestedCapabilities)
                        .build()
                );
                log.info("All requested capabilities are supported for volumeId: {}", volumeId);
            } else {
                log.warn("Some requested capabilities are not supported for volumeId: {}", volumeId);
                unsupportedCapabilities.forEach(capability ->
                        log.warn("Unsupported capability: {}", capability));
            }

            // Send the response
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error handling ValidateVolumeCapabilities for volumeId {}: {}", volumeId, e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * Simulates validating a volume's capabilities.
     *
     * @param requestedCapabilities The capabilities provided in the request.
     * @return A list of unsupported volume capabilities, if any.
     */
    private List<Csi.VolumeCapability> validateCapabilities(List<Csi.VolumeCapability> requestedCapabilities) {
        // Example: Define the supported capabilities
        List<Csi.VolumeCapability> supportedCapabilities = getSupportedCapabilities();
        List<Csi.VolumeCapability> unsupportedCapabilities = new ArrayList<>();

        // Check if each requested capability is supported
        for (Csi.VolumeCapability requestedCapability : requestedCapabilities) {
            if (!supportedCapabilities.contains(requestedCapability)) {
                unsupportedCapabilities.add(requestedCapability);
            }
        }
        return unsupportedCapabilities;
    }

    /**
     * Simulates checking if a volume exists.
     *
     * @param volumeId The ID of the volume to check.
     * @return True if the volume exists, false otherwise.
     */
    private boolean isVolumeAvailable(String volumeId) {
        // Simulated logic - only volumes with IDs like "volume-123" exist
        return volumeId.startsWith("volume-");
    }

    /**
     * Simulates the list of supported volume capabilities.
     *
     * @return List of supported volume capabilities.
     */
    private List<Csi.VolumeCapability> getSupportedCapabilities() {
        // Example: Only Block and Mount capabilities are supported
        List<Csi.VolumeCapability> supportedCapabilities = new ArrayList<>();
        supportedCapabilities.add(Csi.VolumeCapability.newBuilder()
                .setAccessMode(Csi.VolumeCapability.AccessMode.newBuilder()
                        .setMode(Csi.VolumeCapability.AccessMode.Mode.SINGLE_NODE_WRITER)
                        .build())
                .build());
        supportedCapabilities.add(Csi.VolumeCapability.newBuilder()
                .setAccessMode(Csi.VolumeCapability.AccessMode.newBuilder()
                        .setMode(Csi.VolumeCapability.AccessMode.Mode.MULTI_NODE_READER_ONLY)
                        .build())
                .build());
        return supportedCapabilities;
    }
}
