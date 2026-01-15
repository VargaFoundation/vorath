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

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

public class NodeUnstageVolumeHandlerTest {

    @Mock
    private StreamObserver<Csi.NodeUnstageVolumeResponse> responseObserver;

    private NodeUnstageVolumeHandler handler;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new NodeUnstageVolumeHandler();
    }

    @Test
    public void testHandleNodeUnstageVolume_Success() throws Exception {
        String volumeId = "test-volume";
        Path stagingPath = tempDir.resolve("staging");
        Files.createDirectory(stagingPath);

        Csi.NodeUnstageVolumeRequest request = Csi.NodeUnstageVolumeRequest.newBuilder()
                .setVolumeId(volumeId)
                .setStagingTargetPath(stagingPath.toAbsolutePath().toString())
                .build();

        handler.handleNodeUnstageVolume(request, responseObserver);

        assertFalse(Files.exists(stagingPath));
        verify(responseObserver).onNext(any(Csi.NodeUnstageVolumeResponse.class));
        verify(responseObserver).onCompleted();
    }

    @Test
    public void testHandleNodeUnstageVolume_NotExists() {
        String volumeId = "test-volume";
        String stagingPath = tempDir.resolve("non-existent").toAbsolutePath().toString();

        Csi.NodeUnstageVolumeRequest request = Csi.NodeUnstageVolumeRequest.newBuilder()
                .setVolumeId(volumeId)
                .setStagingTargetPath(stagingPath)
                .build();

        handler.handleNodeUnstageVolume(request, responseObserver);

        verify(responseObserver).onNext(any(Csi.NodeUnstageVolumeResponse.class));
        verify(responseObserver).onCompleted();
    }
}
