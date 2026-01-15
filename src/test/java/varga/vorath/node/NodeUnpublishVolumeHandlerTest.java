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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import varga.vorath.hdfs.HdfsMountService;

import java.io.File;
import java.nio.file.Path;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class NodeUnpublishVolumeHandlerTest {

    @Mock
    private HdfsMountService hdfsMountService;

    @Mock
    private StreamObserver<Csi.NodeUnpublishVolumeResponse> responseObserver;

    private NodeUnpublishVolumeHandler handler;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new NodeUnpublishVolumeHandler(hdfsMountService);
    }

    @Test
    public void testHandleNodeUnpublishVolume_Success() {
        String volumeId = "test-volume";
        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();
        String targetPath = targetDir.getAbsolutePath();

        Csi.NodeUnpublishVolumeRequest request = Csi.NodeUnpublishVolumeRequest.newBuilder()
                .setVolumeId(volumeId)
                .setTargetPath(targetPath)
                .build();

        handler.handleNodeUnpublishVolume(request, responseObserver);

        verify(hdfsMountService).unmountVolume(targetPath);
        verify(responseObserver).onNext(any(Csi.NodeUnpublishVolumeResponse.class));
        verify(responseObserver).onCompleted();
    }

    @Test
    public void testHandleNodeUnpublishVolume_PathNotExists() {
        String volumeId = "test-volume";
        String targetPath = tempDir.resolve("non-existent").toFile().getAbsolutePath();

        Csi.NodeUnpublishVolumeRequest request = Csi.NodeUnpublishVolumeRequest.newBuilder()
                .setVolumeId(volumeId)
                .setTargetPath(targetPath)
                .build();

        handler.handleNodeUnpublishVolume(request, responseObserver);

        verify(hdfsMountService, never()).unmountVolume(anyString());
        verify(responseObserver).onNext(any(Csi.NodeUnpublishVolumeResponse.class));
        verify(responseObserver).onCompleted();
    }

    @Test
    public void testHandleNodeUnpublishVolume_MissingParams() {
        Csi.NodeUnpublishVolumeRequest request = Csi.NodeUnpublishVolumeRequest.newBuilder().build();

        handler.handleNodeUnpublishVolume(request, responseObserver);

        verify(responseObserver).onError(any(IllegalArgumentException.class));
    }
}
