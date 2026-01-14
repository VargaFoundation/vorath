package varga.vorath.kubernetes;

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

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeList;
import io.kubernetes.client.util.ClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Optional;

@Slf4j
@Service
public class KubernetesVolumeService {

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
            log.error("Error retrieving PersistentVolume with ID '{}': {}", volumeId, e.getMessage(), e);
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
            log.error("Error retrieving PersistentVolume with name '{}': {}", volumeName, e.getMessage(), e);
            return Optional.empty();
        }
    }

}
