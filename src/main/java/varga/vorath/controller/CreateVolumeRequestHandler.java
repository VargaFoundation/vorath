package varga.vorath.controller;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.util.Config;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import varga.vorath.hdfs.HdfsConnection;
import varga.vorath.hdfs.HdfsVolumeService;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class CreateVolumeRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(CreateVolumeRequestHandler.class);

    private final HdfsVolumeService hdfsVolumeService;

    /**
     * Handles the CreateVolume gRPC request.
     *
     * @param request          The CreateVolumeRequest from the orchestrator.
     * @param responseObserver The gRPC response observer to send the response.
     */
    public void handleCreateVolumeRequest(Csi.CreateVolumeRequest request,
                                          StreamObserver<Csi.CreateVolumeResponse> responseObserver) {
        try {
            logger.info("Received CreateVolumeRequest for volume: {}", request.getName());

            // Step 1: Validate the request
            validateCreateVolumeRequest(request);

            // Step 2: Extract necessary details from the request
            String volumeName = request.getName();
            long requiredCapacity = request.getCapacityRange().getRequiredBytes();

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

            String location = request.getParametersMap().get("location");
            String secretName = request.getParametersMap().get("secretName");
            String secretNamespace = request.getParametersMap().get("secretNamespace");

            HdfsConnection hdfsConnection = HdfsConnection.createHdfsConnection(secretName, secretNamespace, location);
            hdfsConnection.getConfiguration();


            // Step 3: Create the volume in the backend
            String volumeId = this.hdfsVolumeService.createVolume(
                    volumeName
//                    ,
//                    requiredCapacity,
//                    storageClass,
//                    volumeType
            );

            logger.info("Volume {} created successfully with ID: {}", volumeName, volumeId);

            // Create the volume structure
            Csi.Volume.Builder volumeBuilder = Csi.Volume.newBuilder()
                    .setVolumeId(volumeId) // The globally unique volume ID
                    .setCapacityBytes(requiredCapacity) // Reported capacity of the volume
                    .putVolumeContext("provisioner", "hdfs.csi.varga")
                    .putVolumeContext("location", location);

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
            logger.error("Invalid CreateVolumeRequest: {}", e.getMessage(), e);
            responseObserver.onError(e);
        } catch (Exception e) {
            logger.error("Error while creating volume: {}", e.getMessage(), e);
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
        if (!request.getParametersMap().containsKey("location")) {
            throw new IllegalArgumentException("location parameter is required.");
        }
        if (!request.getParametersMap().containsKey("secretName") ||
                !request.getParametersMap().containsKey("secretNamespace")) {
            throw new IllegalArgumentException("Both secretName and secretNamespace must be provided.");
        }

        logger.debug("CreateVolumeRequest validated successfully.");
    }
}