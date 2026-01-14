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
import csi.v1.IdentityGrpc;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdentityGrpcService extends IdentityGrpc.IdentityImplBase {

    private final GetPluginInfoHandler pluginInfoHandler;
    private final GetPluginCapabilitiesHandler pluginCapabilitiesHandler;
    private final ProbeHandler probeHandler;

    @Override
    public void getPluginInfo(Csi.GetPluginInfoRequest request,
                              StreamObserver<Csi.GetPluginInfoResponse> responseObserver) {
        this.pluginInfoHandler.handleGetPluginInfo(request, responseObserver);
    }

    @Override
    public void getPluginCapabilities(Csi.GetPluginCapabilitiesRequest request,
                                      StreamObserver<Csi.GetPluginCapabilitiesResponse> responseObserver) {

        Csi.GetPluginCapabilitiesResponse response = Csi.GetPluginCapabilitiesResponse.newBuilder()
                .addAllCapabilities(this.pluginCapabilitiesHandler.handleGetPluginCapabilities())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }

    @Override
    public void probe(Csi.ProbeRequest request, StreamObserver<Csi.ProbeResponse> responseObserver) {
        this.probeHandler.handleProbe(request, responseObserver);
    }

}
