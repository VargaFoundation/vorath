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

public class ValidateVolumeCapabilitiesHandlerTest {

    @Mock
    private StreamObserver<Csi.ValidateVolumeCapabilitiesResponse> responseObserver;

    private ValidateVolumeCapabilitiesHandler handler;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new ValidateVolumeCapabilitiesHandler();
    }

    @Test
    public void testValidateVolumeCapabilities_Supported() {
        Csi.VolumeCapability capability = Csi.VolumeCapability.newBuilder()
                .setAccessMode(Csi.VolumeCapability.AccessMode.newBuilder()
                        .setMode(Csi.VolumeCapability.AccessMode.Mode.SINGLE_NODE_WRITER)
                        .build())
                .build();

        Csi.ValidateVolumeCapabilitiesRequest request = Csi.ValidateVolumeCapabilitiesRequest.newBuilder()
                .setVolumeId("volume-123")
                .addVolumeCapabilities(capability)
                .build();

        handler.handleValidateVolumeCapabilities(request, responseObserver);

        ArgumentCaptor<Csi.ValidateVolumeCapabilitiesResponse> responseCaptor = ArgumentCaptor.forClass(Csi.ValidateVolumeCapabilitiesResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        Csi.ValidateVolumeCapabilitiesResponse response = responseCaptor.getValue();
        assertTrue(response.hasConfirmed());
        assertEquals(1, response.getConfirmed().getVolumeCapabilitiesCount());
    }

    @Test
    public void testValidateVolumeCapabilities_Unsupported() {
        Csi.VolumeCapability capability = Csi.VolumeCapability.newBuilder()
                .setAccessMode(Csi.VolumeCapability.AccessMode.newBuilder()
                        .setMode(Csi.VolumeCapability.AccessMode.Mode.MULTI_NODE_MULTI_WRITER)
                        .build())
                .build();

        Csi.ValidateVolumeCapabilitiesRequest request = Csi.ValidateVolumeCapabilitiesRequest.newBuilder()
                .setVolumeId("volume-123")
                .addVolumeCapabilities(capability)
                .build();

        handler.handleValidateVolumeCapabilities(request, responseObserver);

        ArgumentCaptor<Csi.ValidateVolumeCapabilitiesResponse> responseCaptor = ArgumentCaptor.forClass(Csi.ValidateVolumeCapabilitiesResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());

        Csi.ValidateVolumeCapabilitiesResponse response = responseCaptor.getValue();
        assertFalse(response.hasConfirmed());
    }

    @Test
    public void testValidateVolumeCapabilities_InvalidVolume() {
        Csi.ValidateVolumeCapabilitiesRequest request = Csi.ValidateVolumeCapabilitiesRequest.newBuilder()
                .setVolumeId("bad-id")
                .build();

        handler.handleValidateVolumeCapabilities(request, responseObserver);

        verify(responseObserver).onError(any(IllegalArgumentException.class));
    }
}
