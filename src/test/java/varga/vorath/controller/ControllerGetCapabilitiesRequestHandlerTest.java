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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ControllerGetCapabilitiesRequestHandlerTest {

    @Mock
    private StreamObserver<Csi.ControllerGetCapabilitiesResponse> responseObserver;

    private ControllerGetCapabilitiesRequestHandler handler;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new ControllerGetCapabilitiesRequestHandler();
    }

    @Test
    public void testHandleControllerGetCapabilities() {
        Csi.ControllerGetCapabilitiesRequest request = Csi.ControllerGetCapabilitiesRequest.newBuilder().build();

        handler.handleControllerGetCapabilities(request, responseObserver);

        ArgumentCaptor<Csi.ControllerGetCapabilitiesResponse> responseCaptor = ArgumentCaptor.forClass(Csi.ControllerGetCapabilitiesResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        Csi.ControllerGetCapabilitiesResponse response = responseCaptor.getValue();
        List<Csi.ControllerServiceCapability> capabilities = response.getCapabilitiesList();

        assertFalse(capabilities.isEmpty());
        
        boolean hasCreateDelete = capabilities.stream()
                .anyMatch(c -> c.getRpc().getType() == Csi.ControllerServiceCapability.RPC.Type.CREATE_DELETE_VOLUME);
        boolean hasListVolumes = capabilities.stream()
                .anyMatch(c -> c.getRpc().getType() == Csi.ControllerServiceCapability.RPC.Type.LIST_VOLUMES);
        boolean hasGetCapacity = capabilities.stream()
                .anyMatch(c -> c.getRpc().getType() == Csi.ControllerServiceCapability.RPC.Type.GET_CAPACITY);

        assertTrue(hasCreateDelete);
        assertTrue(hasListVolumes);
        assertTrue(hasGetCapacity);
    }
}
