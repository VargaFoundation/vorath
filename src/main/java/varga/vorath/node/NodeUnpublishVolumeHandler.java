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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import varga.vorath.hdfs.HdfsMountService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
@Component
@RequiredArgsConstructor
public class NodeUnpublishVolumeHandler {

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

            log.info("Request received to unpublish volume. Volume ID: {}, Target Path: {}", volumeId, targetPath);

            Path path = Paths.get(targetPath);

            // Check idempotency: If the path doesn't exist, no additional action is needed
            if (!Files.exists(path)) {
                log.warn("The target path '{}' does not exist. The volume is already unpublished or not mounted.", targetPath);
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

            log.info("Volume '{}' successfully unpublished and target path '{}' cleaned up.", volumeId, targetPath);
        } catch (IllegalArgumentException e) {
            log.error("Error in input parameters: {}", e.getMessage());
            responseObserver.onError(e);
        } catch (Exception e) {
            log.error("Unexpected error while unpublishing the volume: {}", e.getMessage(), e);
            responseObserver.onError(new RuntimeException("Error while unpublishing the volume.", e));
        }
    }
}
