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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

public class NodeStageVolumeHandlerTest {

    @Mock
    private StreamObserver<Csi.NodeStageVolumeResponse> responseObserver;

    private NodeStageVolumeHandler handler;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new NodeStageVolumeHandler();
    }

    @Test
    public void testHandleNodeStageVolume_Success() {
        String volumeId = "test-volume";
        String stagingPath = tempDir.resolve("staging").toAbsolutePath().toString();

        Csi.NodeStageVolumeRequest request = Csi.NodeStageVolumeRequest.newBuilder()
                .setVolumeId(volumeId)
                .setStagingTargetPath(stagingPath)
                .build();

        handler.handleNodeStageVolume(request, responseObserver);

        assertTrue(Files.exists(tempDir.resolve("staging")));
        verify(responseObserver).onNext(any(Csi.NodeStageVolumeResponse.class));
        verify(responseObserver).onCompleted();
    }
}
