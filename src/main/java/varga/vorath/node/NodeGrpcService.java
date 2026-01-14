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
import csi.v1.NodeGrpc;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class NodeGrpcService extends NodeGrpc.NodeImplBase {

    private final NodeGetInfoHandler nodeGetInfoHandler;
    private final NodePublishVolumeHandler nodePublishVolumeHandler;
    private final NodeUnpublishVolumeHandler nodeUnpublishVolumeHandler;
    private final NodeGetCapabilitiesHandler nodeGetCapabilitiesHandler;
    private final NodeStageVolumeHandler nodeStageVolumeHandler;
    private final NodeUnstageVolumeHandler nodeUnstageVolumeHandler;

    @Override
    public void nodeGetCapabilities(Csi.NodeGetCapabilitiesRequest request,
                                    StreamObserver<Csi.NodeGetCapabilitiesResponse> responseObserver) {
        this.nodeGetCapabilitiesHandler.handleNodeGetCapabilities(request, responseObserver);
    }

    @Override
    public void nodeUnpublishVolume(Csi.NodeUnpublishVolumeRequest request,
                                    StreamObserver<Csi.NodeUnpublishVolumeResponse> responseObserver) {
        this.nodeUnpublishVolumeHandler.handleNodeUnpublishVolume(request, responseObserver);
    }

    @Override
    public void nodePublishVolume(Csi.NodePublishVolumeRequest request,
                                  StreamObserver<Csi.NodePublishVolumeResponse> responseObserver) {
        this.nodePublishVolumeHandler.handleNodePublishVolume(request, responseObserver);
    }

    @Override
    public void nodeStageVolume(Csi.NodeStageVolumeRequest request, StreamObserver<Csi.NodeStageVolumeResponse> responseObserver) {
        this.nodeStageVolumeHandler.handleNodeStageVolume(request, responseObserver);
    }

    @Override
    public void nodeUnstageVolume(Csi.NodeUnstageVolumeRequest request, StreamObserver<Csi.NodeUnstageVolumeResponse> responseObserver) {
        this.nodeUnstageVolumeHandler.handleNodeUnstageVolume(request, responseObserver);
    }

    @Override
    public void nodeGetInfo(Csi.NodeGetInfoRequest request, StreamObserver<Csi.NodeGetInfoResponse> responseObserver) {
        this.nodeGetInfoHandler.handleNodeGetInfo(request, responseObserver);
    }
}
