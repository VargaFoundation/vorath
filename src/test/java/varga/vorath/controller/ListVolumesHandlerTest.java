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

public class ListVolumesHandlerTest {

    @Mock
    private StreamObserver<Csi.ListVolumesResponse> responseObserver;

    private ListVolumesHandler handler;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new ListVolumesHandler();
    }

    @Test
    public void testListVolumes_FirstPage() {
        Csi.ListVolumesRequest request = Csi.ListVolumesRequest.newBuilder()
                .setMaxEntries(10)
                .build();

        handler.handleListVolumes(request, responseObserver);

        ArgumentCaptor<Csi.ListVolumesResponse> responseCaptor = ArgumentCaptor.forClass(Csi.ListVolumesResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        Csi.ListVolumesResponse response = responseCaptor.getValue();
        assertEquals(10, response.getEntriesCount());
        assertEquals("0", response.getNextToken());
        assertEquals("volume-1", response.getEntries(0).getVolume().getVolumeId());
    }

    @Test
    public void testListVolumes_Pagination() {
        Csi.ListVolumesRequest request = Csi.ListVolumesRequest.newBuilder()
                .setMaxEntries(10)
                .setStartingToken("10")
                .build();

        handler.handleListVolumes(request, responseObserver);

        ArgumentCaptor<Csi.ListVolumesResponse> responseCaptor = ArgumentCaptor.forClass(Csi.ListVolumesResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());

        Csi.ListVolumesResponse response = responseCaptor.getValue();
        assertEquals(10, response.getEntriesCount());
        assertEquals("11", response.getNextToken());
        assertEquals("volume-11", response.getEntries(0).getVolume().getVolumeId());
    }

    @Test
    public void testListVolumes_LastPage() {
        Csi.ListVolumesRequest request = Csi.ListVolumesRequest.newBuilder()
                .setMaxEntries(10)
                .setStartingToken("45")
                .build();

        handler.handleListVolumes(request, responseObserver);

        ArgumentCaptor<Csi.ListVolumesResponse> responseCaptor = ArgumentCaptor.forClass(Csi.ListVolumesResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());

        Csi.ListVolumesResponse response = responseCaptor.getValue();
        assertEquals(5, response.getEntriesCount());
        assertEquals("", response.getNextToken());
    }
}
