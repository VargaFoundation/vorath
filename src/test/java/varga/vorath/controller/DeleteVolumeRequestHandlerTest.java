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
import io.kubernetes.client.openapi.models.V1CSIPersistentVolumeSource;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeSpec;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import varga.vorath.hdfs.HdfsConnection;
import varga.vorath.hdfs.HdfsVolumeService;
import varga.vorath.kubernetes.KubernetesVolumeService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class DeleteVolumeRequestHandlerTest {

    private static MiniDFSCluster cluster;
    private static HdfsConfiguration conf;

    @Mock
    private KubernetesVolumeService kubernetesVolumeService;

    @Mock
    private StreamObserver<Csi.DeleteVolumeResponse> responseObserver;

    private HdfsVolumeService hdfsVolumeService;
    private DeleteVolumeRequestHandler handler;

    @BeforeAll
    public static void beforeAll() throws IOException {
        System.setProperty("test.build.data", "target/test/data/DeleteVolumeRequestHandlerTest");
        conf = new HdfsConfiguration();
        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
        cluster.waitActive();
    }

    @AfterAll
    public static void afterAll() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        hdfsVolumeService = new HdfsVolumeService();
        handler = new DeleteVolumeRequestHandler(hdfsVolumeService, kubernetesVolumeService);
    }

    @Test
    public void testDeleteVolume_Success() throws Exception {
        String volumeId = "test-volume-id";
        String hdfsPath = "/volumes/test-delete-volume";
        String secretName = "hdfs-secret";
        String secretNamespace = "default";

        // Create the directory in MiniDFSCluster first
        cluster.getFileSystem().mkdirs(new org.apache.hadoop.fs.Path(hdfsPath));

        Csi.DeleteVolumeRequest request = Csi.DeleteVolumeRequest.newBuilder()
                .setVolumeId(volumeId)
                .build();

        V1PersistentVolume pv = new V1PersistentVolume();
        V1PersistentVolumeSpec spec = new V1PersistentVolumeSpec();
        V1CSIPersistentVolumeSource csiSource = new V1CSIPersistentVolumeSource();
        Map<String, String> attributes = new HashMap<>();
        attributes.put("hdfsPath", hdfsPath);
        attributes.put("secretName", secretName);
        attributes.put("secretNamespace", secretNamespace);
        attributes.put("location", cluster.getFileSystem().getUri().toString());
        csiSource.setVolumeAttributes(attributes);
        spec.setCsi(csiSource);
        pv.setSpec(spec);

        when(kubernetesVolumeService.getPersistentVolumeById(volumeId)).thenReturn(Optional.of(pv));

        try (MockedStatic<HdfsConnection> mockedHdfsConnection = mockStatic(HdfsConnection.class)) {
            HdfsConnection mockConn = mock(HdfsConnection.class);
            mockedHdfsConnection.when(() -> HdfsConnection.createHdfsConnection(eq(secretName), eq(secretNamespace), any()))
                    .thenReturn(mockConn);
            
            when(mockConn.getConfiguration()).thenReturn(cluster.getFileSystem().getConf());

            handler.handleDeleteVolume(request, responseObserver);

            verify(responseObserver).onNext(any(Csi.DeleteVolumeResponse.class));
            verify(responseObserver).onCompleted();
            
            assertEquals(false, cluster.getFileSystem().exists(new org.apache.hadoop.fs.Path(hdfsPath)));
        }
    }

    @Test
    public void testDeleteVolume_MissingVolumeId() {
        Csi.DeleteVolumeRequest request = Csi.DeleteVolumeRequest.newBuilder().build();

        handler.handleDeleteVolume(request, responseObserver);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(responseObserver).onError(captor.capture());
        assertTrue(captor.getValue() instanceof IllegalArgumentException);
    }

    @Test
    public void testDeleteVolume_VolumeNotFound() {
        String volumeId = "non-existent";
        Csi.DeleteVolumeRequest request = Csi.DeleteVolumeRequest.newBuilder()
                .setVolumeId(volumeId)
                .build();

        when(kubernetesVolumeService.getPersistentVolumeById(volumeId)).thenReturn(Optional.empty());

        handler.handleDeleteVolume(request, responseObserver);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(responseObserver).onError(captor.capture());
        assertTrue(captor.getValue() instanceof IllegalArgumentException);
    }
}
