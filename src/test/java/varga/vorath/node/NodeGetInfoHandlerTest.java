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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

public class NodeGetInfoHandlerTest {

    @Mock
    private StreamObserver<Csi.NodeGetInfoResponse> responseObserver;

    private NodeGetInfoHandler handler;
    private final String nodeId = "test-node-id";
    private final int maxVolumes = 10;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new NodeGetInfoHandler(nodeId, maxVolumes);
    }

    @Test
    public void testHandleNodeGetInfo() {
        Csi.NodeGetInfoRequest request = Csi.NodeGetInfoRequest.newBuilder().build();

        handler.handleNodeGetInfo(request, responseObserver);

        ArgumentCaptor<Csi.NodeGetInfoResponse> responseCaptor = ArgumentCaptor.forClass(Csi.NodeGetInfoResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        Csi.NodeGetInfoResponse response = responseCaptor.getValue();
        assertEquals(nodeId, response.getNodeId());
        assertEquals(maxVolumes, response.getMaxVolumesPerNode());
    }
}
