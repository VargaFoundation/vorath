package varga.vorath.node;


import csi.v1.Csi;
import io.grpc.stub.StreamObserver;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;

public class NodePublishVolumeHandler {

    public void handleNodePublishVolume(Csi.NodePublishVolumeRequest request,
                                        StreamObserver<Csi.NodePublishVolumeResponse> responseObserver) {
        String volumeId = request.getVolumeId();
        String targetPath = request.getTargetPath();

        if (volumeId == null || volumeId.isEmpty() || targetPath == null || targetPath.isEmpty()) {
            responseObserver.onError(new IllegalArgumentException("Volume ID ou Target Path manquant."));
            return;
        }

        try {
            Path path = Paths.get(targetPath);
            if (!Files.exists(path)) {

                Files.createDirectories(path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x")));
                System.out.println("Le répertoire cible a été créé : " + targetPath);
            }

            System.out.println("Le volume " + volumeId + " est publié sur " + targetPath);

            Csi.NodePublishVolumeResponse response = Csi.NodePublishVolumeResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            System.err.println("Erreur lors de la publication du volume : " + e.getMessage());
            responseObserver.onError(e);
        }
    }
}
