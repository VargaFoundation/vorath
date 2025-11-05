package varga.vorath.hdfs;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.StorageV1Api;
import io.kubernetes.client.openapi.models.V1VolumeAttachment;
import io.kubernetes.client.openapi.models.V1VolumeAttachmentList;
import io.kubernetes.client.util.ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class KubernetesVolumeAttachmentClient {

    private static final Logger logger = LoggerFactory.getLogger(KubernetesVolumeAttachmentClient.class);

    private static final String CSI_ATTACHER_NAME = "hdfs-csi-driver";
    private final String currentNodeName;
    private final ApiClient apiClient;

    public KubernetesVolumeAttachmentClient() throws IOException {
        ApiClient client = ClientBuilder.defaultClient();
        Configuration.setDefaultApiClient(client);
        this.apiClient = client;

        // Retrieve the current node name from the environment (set in the Pod by Kubernetes)
        this.currentNodeName = System.getenv("NODE_NAME"); // Kubernetes default variable
        if (this.currentNodeName == null || this.currentNodeName.isEmpty()) {
            throw new IllegalStateException("Env variable NODE_NAME is not set. Ensure the Pod has access to the node name.");
        }
    }

    /**
     * Fetches all VolumeAttachments associated with the current node for the HDFS CSI driver.
     *
     * @return A map of targetPath to VolumeAttachmentInfo for volumes that need to be mounted on the current node.
     */
    public Map<String, VolumeAttachmentInfo> getVolumeAttachmentsForCurrentNode() {
        logger.info("Fetching VolumeAttachments for current node '{}' and CSI attacher '{}'", currentNodeName, CSI_ATTACHER_NAME);

        Map<String, VolumeAttachmentInfo> volumeAttachments = new HashMap<>();
        StorageV1Api storageApi = new StorageV1Api(apiClient);

        try {
            // Fetch all VolumeAttachment objects
            V1VolumeAttachmentList attachmentList = storageApi.listVolumeAttachment(
                    null,  // pretty
                    null,  // _continue
                    null,  // fieldSelector
                    null,  // labelSelector
                    null,  // limit
                    null,  // resourceVersion
                    null,  // resourceVersionMatch
                    null,  // timeoutSeconds
                    null,  // timeoutSeconds
                    false  // watch
            );

            // Iterate over items and filter for current node and CSI attacher
            List<V1VolumeAttachment> attachments = attachmentList.getItems();
            for (V1VolumeAttachment attachment : attachments) {
                try {
                    String attacher = attachment.getSpec().getAttacher();
                    String nodeName = attachment.getSpec().getNodeName();

                    if (attacher.equals(CSI_ATTACHER_NAME) && nodeName.equals(currentNodeName)) {
                        String pvName = attachment.getSpec().getSource().getPersistentVolumeName();
                        Map<String, String> metadata = attachment.getStatus().getAttachmentMetadata();

                        if (pvName == null || metadata == null) {
                            logger.warn("Skipping VolumeAttachment '{}' due to missing PV name or metadata.", attachment.getMetadata().getName());
                            continue;
                        }

                        // Extract target path and HDFS URI from metadata
                        String targetPath = metadata.get("mountPath");
                        String hdfsUri = metadata.get("hdfsUri");

                        if (targetPath == null || hdfsUri == null) {
                            logger.warn("Skipping VolumeAttachment '{}' due to missing mountPath or hdfsUri in metadata.", attachment.getMetadata().getName());
                            continue;
                        }

                        // Add to result map
                        volumeAttachments.put(targetPath, new VolumeAttachmentInfo(hdfsUri));
                    }
                } catch (Exception e) {
                    logger.error("Error processing VolumeAttachment '{}': {}", attachment.getMetadata().getName(), e.getMessage(), e);
                }
            }

        } catch (ApiException e) {
            logger.error("Failed to fetch VolumeAttachments from Kubernetes API: {}", e.getResponseBody(), e);
            throw new RuntimeException("Error fetching VolumeAttachment resources", e);
        }

        logger.info("Found {} VolumeAttachments for node '{}'", volumeAttachments.size(), currentNodeName);
        return volumeAttachments;
    }
}