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
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class GetCapacityHandlerTest {

    @Test
    public void testHandleGetCapacity() {
        GetCapacityHandler handler = new GetCapacityHandler();
        Csi.GetCapacityRequest request = Csi.GetCapacityRequest.newBuilder().build();
        StreamObserver<Csi.GetCapacityResponse> responseObserver = mock(StreamObserver.class);

        handler.handleGetCapacity(request, responseObserver);

        ArgumentCaptor<Csi.GetCapacityResponse> captor = ArgumentCaptor.forClass(Csi.GetCapacityResponse.class);
        verify(responseObserver).onNext(captor.capture());
        verify(responseObserver).onCompleted();

        Csi.GetCapacityResponse response = captor.getValue();
        // 10 TB = 10 * 1024^4
        long expectedCapacity = 10L * 1024 * 1024 * 1024 * 1024;
        assertEquals(expectedCapacity, response.getAvailableCapacity());
    }
}
