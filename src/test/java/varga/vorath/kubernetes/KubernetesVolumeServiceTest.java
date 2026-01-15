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

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

public class KubernetesVolumeServiceTest {

    @Mock
    private CoreV1Api coreV1Api;

    @InjectMocks
    private KubernetesVolumeService kubernetesVolumeService;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetPersistentVolumeById_Found() throws ApiException {
        String volumeId = "test-volume";
        V1PersistentVolume pv = new V1PersistentVolume();
        V1PersistentVolumeList pvList = new V1PersistentVolumeList();
        pvList.setItems(Collections.singletonList(pv));

        when(coreV1Api.listPersistentVolume(
                any(), any(), any(), any(),
                any(), any(), any(), any(), any(), any()))
                .thenReturn(pvList);
        
        Optional<V1PersistentVolume> result = kubernetesVolumeService.getPersistentVolumeById(volumeId);

        // assertTrue(result.isPresent());
        // assertEquals(pv, result.get());
    }

    @Test
    public void testGetPersistentVolumeById_NotFound() throws ApiException {
        String volumeId = "not-found";
        V1PersistentVolumeList pvList = new V1PersistentVolumeList();
        pvList.setItems(Collections.emptyList());

        when(coreV1Api.listPersistentVolume(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(pvList);

        Optional<V1PersistentVolume> result = kubernetesVolumeService.getPersistentVolumeById(volumeId);

        assertTrue(result.isEmpty());
    }

    private static <T> T eq(T value) {
        return org.mockito.ArgumentMatchers.eq(value);
    }
}
