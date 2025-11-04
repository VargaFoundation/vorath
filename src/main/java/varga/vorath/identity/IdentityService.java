package varga.vorath.identity;


import csi.v1.Csi;
import csi.v1.IdentityGrpc;
import io.grpc.stub.StreamObserver;

public class IdentityService extends IdentityGrpc.IdentityImplBase {
    @Override
    public void getPluginInfo(Csi.GetPluginInfoRequest request,
                              StreamObserver<Csi.GetPluginInfoResponse> responseObserver) {
        GetPluginInfoHandler handler = new GetPluginInfoHandler();
        handler.handleGetPluginInfo(request, responseObserver);
    }

    @Override
    public void getPluginCapabilities(Csi.GetPluginCapabilitiesRequest request,
                                      StreamObserver<Csi.GetPluginCapabilitiesResponse> responseObserver) {
        Csi.GetPluginCapabilitiesResponse response = Csi.GetPluginCapabilitiesResponse.newBuilder()
                .addCapabilities(Csi.PluginCapability.newBuilder()
                        .setService(Csi.PluginCapability.Service.newBuilder()
                                .setType(Csi.PluginCapability.Service.Type.CONTROLLER_SERVICE)
                                .build())
                ).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void probe(Csi.ProbeRequest request,
                      StreamObserver<Csi.ProbeResponse> responseObserver) {
        // Vérification de l'état de santé
        Csi.ProbeResponse response = Csi.ProbeResponse.newBuilder().build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
