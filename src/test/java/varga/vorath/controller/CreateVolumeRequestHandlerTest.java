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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import varga.vorath.hdfs.HdfsConnection;
import varga.vorath.hdfs.HdfsVolumeService;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class CreateVolumeRequestHandlerTest {

    private static MiniDFSCluster cluster;
    private static HdfsConfiguration conf;
    private HdfsVolumeService hdfsVolumeService;
    private CreateVolumeRequestHandler handler;

    @BeforeAll
    public static void beforeAll() throws IOException {
        System.setProperty("test.build.data", "target/test/data/CreateVolumeRequestHandlerTest");
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
        hdfsVolumeService = new HdfsVolumeService();
        handler = new CreateVolumeRequestHandler(hdfsVolumeService);
    }

    @Test
    public void testHandleCreateVolumeRequest_Success() throws Exception {
        Csi.CreateVolumeRequest request = Csi.CreateVolumeRequest.newBuilder()
                .setName("test-volume")
                .putParameters("secretName", "hdfs-secret")
                .putParameters("secretNamespace", "default")
                .build();
        StreamObserver<Csi.CreateVolumeResponse> responseObserver = mock(StreamObserver.class);

        HdfsConnection mockConnection = mock(HdfsConnection.class);
        try (MockedStatic<HdfsConnection> mockedHdfsConnection = mockStatic(HdfsConnection.class)) {
            mockedHdfsConnection.when(() -> HdfsConnection.createHdfsConnection(anyString(), anyString(), any()))
                    .thenReturn(mockConnection);
            
            when(mockConnection.getConfiguration()).thenReturn(cluster.getFileSystem().getConf());

            handler.handleCreateVolumeRequest(request, responseObserver);

            ArgumentCaptor<Csi.CreateVolumeResponse> captor = ArgumentCaptor.forClass(Csi.CreateVolumeResponse.class);
            verify(responseObserver).onNext(captor.capture());
            verify(responseObserver).onCompleted();

            Csi.CreateVolumeResponse response = captor.getValue();
            assertEquals("/volumes/test-volume", response.getVolume().getVolumeId());
            
            org.apache.hadoop.fs.Path expectedPath = new org.apache.hadoop.fs.Path("/volumes/test-volume");
            assertTrue(cluster.getFileSystem().exists(expectedPath), "Path /volumes/test-volume should exist");
        }
    }

    @Test
    public void testHandleCreateVolumeRequest_MissingName() {
        Csi.CreateVolumeRequest request = Csi.CreateVolumeRequest.newBuilder().build();
        StreamObserver<Csi.CreateVolumeResponse> responseObserver = mock(StreamObserver.class);

        handler.handleCreateVolumeRequest(request, responseObserver);

        verify(responseObserver).onError(any(IllegalArgumentException.class));
    }
}
