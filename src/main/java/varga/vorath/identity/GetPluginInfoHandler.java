package varga.vorath.identity;


import csi.v1.Csi;
import io.grpc.stub.StreamObserver;

public class GetPluginInfoHandler {

    public void handleGetPluginInfo(Csi.GetPluginInfoRequest request,
                                    StreamObserver<Csi.GetPluginInfoResponse> responseObserver) {
        try {
            Csi.GetPluginInfoResponse response = Csi.GetPluginInfoResponse.newBuilder()
                    .setName("example.csi.plugin")
                    .setVendorVersion("1.0.0")
                    .putManifest("description", "Un plugin CSI de démonstration pour Kubernetes")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            System.out.println("GetPluginInfo appelé avec succès. Nom: example.csi.plugin, Version: 1.0.0");

        } catch (Exception e) {
            System.err.println("Erreur lors de l'exécution de GetPluginInfo: " + e.getMessage());
            responseObserver.onError(e);
        }
    }
}
