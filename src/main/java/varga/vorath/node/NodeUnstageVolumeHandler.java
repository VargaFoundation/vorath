package varga.vorath.node;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;

@Slf4j
@Component
public class NodeUnstageVolumeHandler {

    /**
     * Handles the NodeUnstageVolume request to clean up a staged volume on this node.
     *
     * @param request           The NodeUnstageVolumeRequest from the client.
     * @param responseObserver  The response observer to send the result.
     */
    public void handleNodeUnstageVolume(Csi.NodeUnstageVolumeRequest request, StreamObserver<Csi.NodeUnstageVolumeResponse> responseObserver) {
        String volumeId = request.getVolumeId();
        String stagingTargetPath = request.getStagingTargetPath();

        try {
            log.info("Processing NodeUnstageVolume request for volumeId: {}, stagingTargetPath: {}", volumeId, stagingTargetPath);

            // Validate the input parameters
            if (volumeId == null || volumeId.isEmpty() || stagingTargetPath == null || stagingTargetPath.isEmpty()) {
                throw new IllegalArgumentException("VolumeId and StagingTargetPath must not be empty.");
            }

            // Ensure that the staging target path exists
            Path targetPath = Paths.get(stagingTargetPath);
            if (!Files.exists(targetPath)) {
                log.warn("StagingTargetPath does not exist. Assuming volume is already cleaned up. Path: {}", stagingTargetPath);
            } else {
                // Simulate the unstage logic (e.g., unmount directory or clean up resources)
                unstageVolume(targetPath);
            }

            // Respond successfully
            Csi.NodeUnstageVolumeResponse response = Csi.NodeUnstageVolumeResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            log.info("NodeUnstageVolume successfully completed for volumeId: {}", volumeId);

        } catch (Exception e) {
            log.error("Error handling NodeUnstageVolume for volumeId: {}: {}", volumeId, e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * Simulates the logic to unstage a volume.
     *
     * @param targetPath  The staging target path to clean up.
     * @throws IOException  If an error occurs during the cleanup.
     */
    private void unstageVolume(Path targetPath) throws IOException {
        // Example cleanup logic (this can be customized for real use cases)
        log.info("Cleaning up staging target path: {}", targetPath.toString());

        // If there is a mounted volume, unmount it (example placeholder logic)
        // For now, we'll assume all resources are "staged" within this directory.
        if (Files.isDirectory(targetPath)) {
            Files.delete(targetPath);
            log.info("Successfully cleaned up staging target path: {}", targetPath.toString());
        } else {
            throw new IOException("Staging target path is not a directory: " + targetPath.toString());
        }
    }
}
