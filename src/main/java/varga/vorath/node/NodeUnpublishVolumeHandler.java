package varga.vorath.node;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

@Component
public class NodeUnpublishVolumeHandler {

    /**
     * Gère la libération d'un volume en répondant à la requête NodeUnpublishVolume.
     *
     * @param request           La requête reçue pour détacher un volume.
     * @param responseObserver  L'observateur pour envoyer la réponse au client.
     */
    public void handleNodeUnpublishVolume(Csi.NodeUnpublishVolumeRequest request,
                                          StreamObserver<Csi.NodeUnpublishVolumeResponse> responseObserver) {
        try {
            // Récupération de l'identifiant du volume à détacher
            String volumeId = request.getVolumeId();
            System.out.println("Détachement du volume : " + volumeId);

            // Logique de détachement (ajoutez ici toutes les opérations nécessaires si applicables)

            // Envoi de la réponse pour confirmer que le volume a été détaché
            Csi.NodeUnpublishVolumeResponse response = Csi.NodeUnpublishVolumeResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            System.out.println("Volume détaché avec succès : " + volumeId);
        } catch (Exception e) {
            // Gestion des erreurs et transmission au client
            System.err.println("Erreur lors du détachement du volume : " + e.getMessage());
            responseObserver.onError(e);
        }
    }
}