package varga.vorath.node;

import csi.v1.NodeGrpc;
import io.grpc.stub.StreamObserver;
import csi.v1.Csi;

public class NodeService extends NodeGrpc.NodeImplBase {
    @Override
    public void nodePublishVolume(Csi.NodePublishVolumeRequest request,
                                  StreamObserver<Csi.NodePublishVolumeResponse> responseObserver) {
        // Préparation d'un volume pour un accès sur le nœud
        System.out.println("Attachement du volume : " + request.getVolumeId());

        responseObserver.onNext(Csi.NodePublishVolumeResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void nodeUnpublishVolume(Csi.NodeUnpublishVolumeRequest request,
                                    StreamObserver<Csi.NodeUnpublishVolumeResponse> responseObserver) {
        // Libération d'un volume
        System.out.println("Détachement du volume : " + request.getVolumeId());

        responseObserver.onNext(Csi.NodeUnpublishVolumeResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void nodeGetCapabilities(Csi.NodeGetCapabilitiesRequest request,
                                    StreamObserver<Csi.NodeGetCapabilitiesResponse> responseObserver) {
        // Retourner une réponse vide pour indiquer des capacités de base
        Csi.NodeGetCapabilitiesResponse response = Csi.NodeGetCapabilitiesResponse.newBuilder().build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
