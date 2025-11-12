package varga.vorath.controller;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import varga.vorath.Utils;
import varga.vorath.hdfs.HdfsConnection;
import varga.vorath.hdfs.HdfsVolumeService;

@Slf4j
@Component
@RequiredArgsConstructor
public class CreateVolumeRequestHandler {

    private final HdfsVolumeService hdfsVolumeService;

    @Value("${csi.storage.basePath:/volumes}")
    private String defaultBasePath;

    /**
     * Handles the CreateVolume gRPC request.
     *
     * @param request          The CreateVolumeRequest from the orchestrator.
     * @param responseObserver The gRPC response observer to send the response.
     */
    public void handleCreateVolumeRequest(Csi.CreateVolumeRequest request,
                                          StreamObserver<Csi.CreateVolumeResponse> responseObserver) {
        try {
            log.info("Received CreateVolumeRequest for volume: {}", request.getName());

            // Step 1: Validate the request
            validateCreateVolumeRequest(request);

            // Step 2: Extract necessary details from the request
            String volumeName = request.getName();
            long requiredCapacity = request.hasCapacityRange() ? request.getCapacityRange().getRequiredBytes() : 0L;

            // Ex:
            //apiVersion: storage.k8s.io/v1
            //kind: StorageClass
            //metadata:
            //  name: hdfs-sc
            //provisioner: hdfs.csi.varga
            //parameters:
            //  location: "hdfs://xxx:9090"
            //  secretName: "hdfs-secret"
            //  secretNamespace: "expense"
            //reclaimPolicy: Delete # Retain
            //volumeBindingMode: WaitForFirstConsumer # Immediate
            //mountOptions:
            //  - -o allow_other
            //  - --file-cache-timeout-in-seconds=120
            //  - --use-attr-cache=true

            String location = request.getParametersMap().get("location"); // optional path (full HDFS path)
            String secretName = request.getParametersMap().get("secretName");
            String secretNamespace = request.getParametersMap().get("secretNamespace");

            // Secrets are mandatory according to requirements
            HdfsConnection hdfsConnection = HdfsConnection.createHdfsConnection(secretName, secretNamespace, Utils.extractClusterUri(location));

            // Compute the HDFS path to create (volumeId)
            String hdfsPath;
            if (location != null && !location.isEmpty()) {
                hdfsPath = location; // location used directly as the volume path
            } else {
                hdfsPath = Utils.normalizePath(this.defaultBasePath) + "/" + volumeName;
            }

            // Step 3: Create the volume in the backend
            String volumeId = this.hdfsVolumeService.createVolume(
                    hdfsConnection,
                    hdfsPath
            );

            log.info("Volume {} created successfully with ID: {}", volumeName, volumeId);

            // Create the volume structure
            Csi.Volume.Builder volumeBuilder = Csi.Volume.newBuilder()
                    .setVolumeId(hdfsPath) // Return HDFS path as the Volume ID
                    .setCapacityBytes(requiredCapacity) // Reported capacity of the volume
                    .putVolumeContext("provisioner", "hdfs.csi.varga")
                    .putVolumeContext("location", location == null ? "" : location)
                    .putVolumeContext("hdfsPath", hdfsPath);

            // Add secrets used for provisioning (if applicable)
            if (secretName != null && secretNamespace != null) {
                volumeBuilder.putVolumeContext("secretName", secretName);
                volumeBuilder.putVolumeContext("secretNamespace", secretNamespace);
            }

            // Step 4: Build the response
            Csi.CreateVolumeResponse response = Csi.CreateVolumeResponse.newBuilder()
                    .setVolume(volumeBuilder.build())
                    .build();

            // Step 5: Send the response back to the orchestrator
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (IllegalArgumentException e) {
            log.error("Invalid CreateVolumeRequest: {}", e.getMessage(), e);
            responseObserver.onError(e);
        } catch (Exception e) {
            log.error("Error while creating volume: {}", e.getMessage(), e);
            responseObserver.onError(new RuntimeException("Failed to create volume."));
        }
    }


    /**
     * Validates the CreateVolumeRequest.
     *
     * @param request The gRPC request.
     */
    private void validateCreateVolumeRequest(Csi.CreateVolumeRequest request) {
        if (request.getName() == null || request.getName().isEmpty()) {
            throw new IllegalArgumentException("Volume name is required.");
        }
        // location is optional; if provided, it must not be empty
        if (request.getParametersMap().containsKey("location") && request.getParametersMap().get("location").isEmpty()) {
            throw new IllegalArgumentException("location cannot be empty when provided.");
        }
        // Secrets are mandatory
        if (!request.getParametersMap().containsKey("secretName") || !request.getParametersMap().containsKey("secretNamespace")) {
            throw new IllegalArgumentException("secretName and secretNamespace are required.");
        }
        if (request.hasCapacityRange() && request.getCapacityRange().getRequiredBytes() <= 0) {
            throw new IllegalArgumentException("CapacityRange.requiredBytes must be > 0 when provided.");
        }

        log.debug("CreateVolumeRequest validated successfully.");
    }
}