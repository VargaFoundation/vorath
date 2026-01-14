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

@Component
@Slf4j
public class GetCapacityHandler {

    /**
     * Handles the GetCapacity request to return available capacity based on the parameters provided.
     *
     * @param request           The GetCapacityRequest from the client.
     * @param responseObserver  The response observer to send the result.
     */
    public void handleGetCapacity(Csi.GetCapacityRequest request,
                                  StreamObserver<Csi.GetCapacityResponse> responseObserver) {
        try {
            String accessibleTopology = request.hasAccessibleTopology() ?
                    request.getAccessibleTopology().toString() : "N/A";
            String volumeCapabilities = request.getVolumeCapabilitiesList().toString();

            log.info("Processing GetCapacity request with accessibleTopology: {}, volumeCapabilities: {}",
                    accessibleTopology, volumeCapabilities);

            // Simulate capacity calculation based on the request parameters
            long availableCapacity = calculateAvailableCapacity(request);

            // Build the response
            Csi.GetCapacityResponse response = Csi.GetCapacityResponse.newBuilder()
                    .setAvailableCapacity(availableCapacity)
                    .build();

            // Send response to the client
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            log.info("GetCapacity completed successfully. Available capacity: {} bytes", availableCapacity);

        } catch (Exception e) {
            log.error("Error handling GetCapacity request: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * Simulates the logic to calculate available capacity.
     *
     * @param request The GetCapacityRequest containing parameters for the calculation.
     * @return Available capacity in bytes.
     */
    private long calculateAvailableCapacity(Csi.GetCapacityRequest request) {
        // Placeholder example for the capacity calculation
        // You can extend this to include real logic, such as querying storage backends

        // Example: Assume a static maximum capacity of 10 TB
        long staticMaxCapacity = 10L * 1024 * 1024 * 1024 * 1024; // 10 TB in bytes

        // Optional: Subtract capacity already used by analyzing the request parameters
        // For example, based on storage class, region, etc.

        // Here, returning the static value for demonstration
        return staticMaxCapacity;
    }
}
