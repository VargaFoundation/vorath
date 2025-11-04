package varga.vorath.node;

import csi.v1.Csi;
import csi.v1.NodeGrpc;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class NodeGrpcService extends NodeGrpc.NodeImplBase {

    private final NodePublishVolumeHandler nodePublishVolumeHandler;
    private final NodeUnpublishVolumeHandler nodeUnpublishVolumeHandler;
    private final NodeGetCapabilitiesHandler nodeGetCapabilitiesHandler;

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
}
