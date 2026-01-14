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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import varga.vorath.CsiPluginProperties;

@Slf4j
@Component
@RequiredArgsConstructor
public class GetPluginInfoHandler {

    private final CsiPluginProperties csiPluginProperties;

    public void handleGetPluginInfo(Csi.GetPluginInfoRequest request,
                                    StreamObserver<Csi.GetPluginInfoResponse> responseObserver) {
        try {
            Csi.GetPluginInfoResponse response = Csi.GetPluginInfoResponse.newBuilder()
                    .setName(this.csiPluginProperties.getName())
                    .setVendorVersion(this.csiPluginProperties.getVendorVersion())
                    .putManifest("description", this.csiPluginProperties.getManifest().get("description"))
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            log.info("GetPluginInfo request processed successfully.");

        } catch (Exception e) {
            log.error("An error occurred while processing GetPluginInfo: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}
