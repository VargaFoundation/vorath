package varga.vorath.controller;


import csi.v1.ControllerGrpc;
import io.grpc.stub.StreamObserver;
import csi.v1.Csi;

public class ControllerService extends ControllerGrpc.ControllerImplBase {
    @Override
    public void createVolume(Csi.CreateVolumeRequest request,
                             StreamObserver<Csi.CreateVolumeResponse> responseObserver) {
        VolumeRequestHandler handler = new VolumeRequestHandler();
        handler.handleCreateVolume(request, responseObserver);

    }

    @Override
    public void deleteVolume(Csi.DeleteVolumeRequest request,
                             StreamObserver<Csi.DeleteVolumeResponse> responseObserver) {
        VolumeRequestHandler handler = new VolumeRequestHandler();
        handler.handleDeleteVolume(request, responseObserver);
    }

}
