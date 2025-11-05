package varga.vorath.kubernetes;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class KubernetesVolumeService {

    private static final Logger logger = LoggerFactory.getLogger(KubernetesVolumeService.class);

    private final CoreV1Api coreV1Api;

    public KubernetesVolumeService() throws IOException {
        ApiClient client = ClientBuilder.defaultClient();
        Configuration.setDefaultApiClient(client);
        this.coreV1Api = new CoreV1Api(client);
    }

    /**
     * Retrieves a PersistentVolume with the given volume ID.
     *
     * @param volumeId The ID of the volume to retrieve.
     * @return An Optional containing the PersistentVolume if found, otherwise empty.
     */
    public Optional<V1PersistentVolume> getPersistentVolumeById(String volumeId) {
        try {
            V1PersistentVolumeList pvList = coreV1Api.listPersistentVolume(
                    null, null, null, "spec.csi.volumeHandle=" + volumeId,
                    null, null, null, null, null, false);

            if (pvList.getItems().isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(pvList.getItems().get(0)); // Assume volume IDs are unique for simplicity
        } catch (Exception e) {
            logger.error("Error retrieving PersistentVolume with ID '{}': {}", volumeId, e.getMessage(), e);
            return Optional.empty();
        }
    }


    /**
     * Check if a PersistentVolume with the given name already exists in Kubernetes.
     *
     * @param volumeName The name of the volume.
     * @return An Optional containing the PersistentVolume if found, otherwise empty.
     */
    public Optional<V1PersistentVolume> getPersistentVolumeByName(String volumeName) {
        try {
            V1PersistentVolumeList pvList = coreV1Api.listPersistentVolume(
                    null, null, null, "metadata.name=" + volumeName,
                    null, null, null, null, null, false);

            if (pvList.getItems().isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(pvList.getItems().get(0));
        } catch (Exception e) {
            logger.error("Error retrieving PersistentVolume with name '{}': {}", volumeName, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Creates a PersistentVolume in Kubernetes.
     *
     * @param volumeId      The ID of the volume.
     * @param volumeName    The name of the volume.
     * @param hdfsPath      The HDFS path for the volume.
     * @param capacityBytes The capacity of the volume in bytes.
     * @param volumeContext Additional volume metadata.
     */
    public void createPersistentVolume(String volumeId, String volumeName, String hdfsPath,
                                       long capacityBytes, Map<String, String> volumeContext) {
        try {
            V1PersistentVolume pv = new V1PersistentVolume()
                    .metadata(new V1ObjectMeta()
                            .name(volumeName)
                            .putLabelsItem("volume-id", volumeId))
                    .spec(new V1PersistentVolumeSpec()
                            .capacity(Map.of("storage", Quantity.fromString(capacityBytes + "B")))
                            .accessModes(List.of("ReadWriteOnce"))
                            .csi(new V1CSIPersistentVolumeSource()
                                    .driver("your.csi.driver")
                                    .volumeHandle(volumeId)
                                    .volumeAttributes(volumeContext)));

            coreV1Api.createPersistentVolume(pv, null, null, null);
            logger.info("PersistentVolume '{}' created successfully.", volumeName);
        } catch (Exception e) {
            logger.error("Error creating PersistentVolume '{}': {}", volumeName, e.getMessage(), e);
            throw new RuntimeException("Could not create PersistentVolume", e);
        }
    }

    /**
     * Builds a CSI Volume object from a PersistentVolume in Kubernetes.
     *
     * @param volumeName The name of the PersistentVolume.
     * @return The CSI Volume object.
     */
    public Csi.Volume buildCsiVolumeFromPersistentVolume(String volumeName) {
        Optional<V1PersistentVolume> pvOpt = getPersistentVolumeByName(volumeName);

        if (pvOpt.isEmpty()) {
            throw new IllegalArgumentException("PersistentVolume not found: " + volumeName);
        }

        V1PersistentVolume pv = pvOpt.get();

        return Csi.Volume.newBuilder()
                .setVolumeId(pv.getMetadata().getLabels().get("volume-id"))
                .setCapacityBytes(Quantity.getAmountInBytes(pv.getSpec().getCapacity().get("storage")).longValue())
                .putAllVolumeContext(pv.getSpec().getCsi().getVolumeAttributes())
                .build();
    }

}