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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

public class NodeGetCapabilitiesHandlerTest {

    @Mock
    private StreamObserver<Csi.NodeGetCapabilitiesResponse> responseObserver;

    private NodeGetCapabilitiesHandler handler;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new NodeGetCapabilitiesHandler();
    }

    @Test
    public void testHandleNodeGetCapabilities() {
        Csi.NodeGetCapabilitiesRequest request = Csi.NodeGetCapabilitiesRequest.newBuilder().build();

        handler.handleNodeGetCapabilities(request, responseObserver);

        ArgumentCaptor<Csi.NodeGetCapabilitiesResponse> responseCaptor = ArgumentCaptor.forClass(Csi.NodeGetCapabilitiesResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        Csi.NodeGetCapabilitiesResponse response = responseCaptor.getValue();
        List<Csi.NodeServiceCapability> capabilities = response.getCapabilitiesList();

        assertFalse(capabilities.isEmpty());

        boolean hasStageUnstage = capabilities.stream()
                .anyMatch(c -> c.getRpc().getType() == Csi.NodeServiceCapability.RPC.Type.STAGE_UNSTAGE_VOLUME);
        boolean hasGetStats = capabilities.stream()
                .anyMatch(c -> c.getRpc().getType() == Csi.NodeServiceCapability.RPC.Type.GET_VOLUME_STATS);

        assertTrue(hasStageUnstage);
        assertTrue(hasGetStats);
    }
}
