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
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import varga.vorath.hdfs.HdfsConnection;
import varga.vorath.hdfs.HdfsMountService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class NodePublishVolumeHandlerTest {

    private static MiniDFSCluster cluster;
    private static HdfsConfiguration conf;
    private HdfsMountService hdfsMountService;
    private NodePublishVolumeHandler handler;

    @BeforeAll
    public static void beforeAll() throws IOException {
        System.setProperty("test.build.data", "target/test/data/NodePublishVolumeHandlerTest");
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
        hdfsMountService = mock(HdfsMountService.class);
        handler = new NodePublishVolumeHandler(hdfsMountService);
    }

    @Test
    public void testHandleNodePublishVolume_Success() throws Exception {
        Path tempDir = Files.createTempDirectory("vorath-test");
        String targetPath = tempDir.resolve("mount").toString();
        
        Csi.NodePublishVolumeRequest request = Csi.NodePublishVolumeRequest.newBuilder()
                .setVolumeId("test-volume")
                .setTargetPath(targetPath)
                .putVolumeContext("location", cluster.getFileSystem().getUri().toString() + "/test-volume")
                .putVolumeContext("secretName", "hdfs-secret")
                .putVolumeContext("secretNamespace", "default")
                .build();
        
        StreamObserver<Csi.NodePublishVolumeResponse> responseObserver = mock(StreamObserver.class);

        HdfsConnection mockConnection = mock(HdfsConnection.class);
        try (MockedStatic<HdfsConnection> mockedHdfsConnection = mockStatic(HdfsConnection.class)) {
            mockedHdfsConnection.when(() -> HdfsConnection.createHdfsConnection(anyString(), anyString(), any()))
                    .thenReturn(mockConnection);

            handler.handleNodePublishVolume(request, responseObserver);

            verify(hdfsMountService).mountVolume(eq(mockConnection), anyString(), eq(targetPath));
            verify(responseObserver).onNext(any());
            verify(responseObserver).onCompleted();
            
            assertTrue(Files.exists(java.nio.file.Paths.get(targetPath)));
        } finally {
            Files.deleteIfExists(java.nio.file.Paths.get(targetPath));
            Files.deleteIfExists(tempDir);
        }
    }
}
