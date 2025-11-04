package varga.vorath.controller;


import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class VolumeRequestHandler {

    private final Map<String, Csi.Volume> volumeStore = new HashMap<>();
    private final Configuration hdfsConfig;
    private final FileSystem hdfs;

    public VolumeRequestHandler() {
        // Chargement de la configuration HDFS
        hdfsConfig = new Configuration();
        hdfsConfig.set("fs.defaultFS", "hdfs://namenode:8020"); // Remplacez par votre URL HDFS
        try {
            hdfs = FileSystem.get(URI.create(hdfsConfig.get("fs.defaultFS")), hdfsConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void handleCreateVolume(Csi.CreateVolumeRequest request,
                                   StreamObserver<Csi.CreateVolumeResponse> responseObserver) {
        String volumeName = request.getName();
        long requiredBytes = request.getCapacityRange().getRequiredBytes();

        try {
            // Validation de l'entrée
            if (volumeName == null || volumeName.isEmpty()) {
                throw new IllegalArgumentException("Le nom du volume est manquant !");
            }

            if (requiredBytes <= 0) {
                throw new IllegalArgumentException("La capacité demandée est invalide !");
            }

            // Vérifie si un volume avec le même nom existe déjà
            if (volumeStore.containsKey(volumeName)) {
                Csi.Volume existingVolume = volumeStore.get(volumeName);
                Csi.CreateVolumeResponse response = Csi.CreateVolumeResponse.newBuilder()
                        .setVolume(existingVolume)
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
                System.out.println("Volume existant renvoyé : " + volumeName);
                return;
            }

            // Emplacement du volume dans HDFS
            Path volumePath = new Path("/volumes/" + volumeName);

            // Vérifie si le répertoire existe déjà
            if (hdfs.exists(volumePath)) {
                throw new IllegalArgumentException("Un volume avec ce nom existe déjà dans HDFS : " + volumeName);
            }

            // Création du répertoire dans HDFS
            hdfs.mkdirs(volumePath);

            // Crée le volume dans HDFS et ajoute les informations dans le store
            String volumeId = "vol-" + System.currentTimeMillis();
            Csi.Volume newVolume = Csi.Volume.newBuilder()
                    .setVolumeId(volumeId)
                    .setCapacityBytes(requiredBytes)
                    .putVolumeContext("storageClass", request.getParametersMap().getOrDefault("storageClass", "default"))
                    .putVolumeContext("hdfsPath", volumePath.toString())
                    .build();

            volumeStore.put(volumeName, newVolume);

            // Envoi de la réponse
            Csi.CreateVolumeResponse response = Csi.CreateVolumeResponse.newBuilder()
                    .setVolume(newVolume)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            System.out.println("Volume créé avec succès dans HDFS : " + volumeName + " (Path: " + volumePath + ")");

        } catch (Exception e) {
            // Gère les erreurs et envoie une erreur à l'appelant gRPC
            System.err.println("Erreur lors de la création du volume dans HDFS : " + e.getMessage());
            responseObserver.onError(e);
        }
    }

    public void handleDeleteVolume(Csi.DeleteVolumeRequest request,
                                   StreamObserver<Csi.DeleteVolumeResponse> responseObserver) {
        String volumeId = request.getVolumeId();

        try {
            // Validation de l'entrée
            if (volumeId == null || volumeId.isEmpty()) {
                throw new IllegalArgumentException("Volume ID est manquant !");
            }

            // Recherche du volume correspondant
            Csi.Volume volumeToDelete = volumeStore.values()
                    .stream()
                    .filter(volume -> volume.getVolumeId().equals(volumeId))
                    .findFirst()
                    .orElse(null);

            if (volumeToDelete == null) {
                throw new IllegalArgumentException("Volume ID introuvable : " + volumeId);
            }

            // Suppression du répertoire dans HDFS
            String volumeHdfsPath = volumeToDelete.getVolumeContextMap().get("hdfsPath");
            if (volumeHdfsPath != null) {
                Path hdfsPath = new Path(volumeHdfsPath);
                if (hdfs.exists(hdfsPath)) {
                    hdfs.delete(hdfsPath, true); // Supprime le dossier de façon récursive
                } else {
                    throw new IllegalArgumentException("Le chemin HDFS n'existe pas : " + volumeHdfsPath);
                }
            }

            // Suppression dans le store local
            volumeStore.values().removeIf(volume -> volume.getVolumeId().equals(volumeId));

            // Envoi de la réponse concernant la suppression du volume
            Csi.DeleteVolumeResponse response = Csi.DeleteVolumeResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            System.out.println("Volume supprimé avec succès dans HDFS : " + volumeId);

        } catch (Exception e) {
            // Gestion des erreurs
            System.err.println("Erreur lors de la suppression du volume dans HDFS : " + e.getMessage());
            responseObserver.onError(e);
        }
    }
}


