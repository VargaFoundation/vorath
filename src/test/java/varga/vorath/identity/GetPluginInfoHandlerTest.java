package varga.vorath.identity;

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
import varga.vorath.CsiPluginProperties;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class GetPluginInfoHandlerTest {

    @Test
    public void testHandleGetPluginInfo() {
        CsiPluginProperties properties = new CsiPluginProperties();
        properties.setName("vorath-plugin");
        properties.setVendorVersion("1.0.0");
        properties.setManifest(Map.of("description", "HDFS CSI Plugin"));

        GetPluginInfoHandler handler = new GetPluginInfoHandler(properties);
        Csi.GetPluginInfoRequest request = Csi.GetPluginInfoRequest.newBuilder().build();
        StreamObserver<Csi.GetPluginInfoResponse> responseObserver = mock(StreamObserver.class);

        handler.handleGetPluginInfo(request, responseObserver);

        ArgumentCaptor<Csi.GetPluginInfoResponse> captor = ArgumentCaptor.forClass(Csi.GetPluginInfoResponse.class);
        verify(responseObserver).onNext(captor.capture());
        verify(responseObserver).onCompleted();

        Csi.GetPluginInfoResponse response = captor.getValue();
        assertEquals("vorath-plugin", response.getName());
        assertEquals("1.0.0", response.getVendorVersion());
        assertEquals("HDFS CSI Plugin", response.getManifestOrThrow("description"));
    }
}
