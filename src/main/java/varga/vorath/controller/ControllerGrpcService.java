package varga.vorath.controller;


import csi.v1.ControllerGrpc;
import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ControllerGrpcService extends ControllerGrpc.ControllerImplBase {

    private final CreateVolumeRequestHandler createVolumeRequestHandler;
    private final DeleteVolumeRequestHandler deleteVolumeRequestHandler;

    @Override
    public void createVolume(Csi.CreateVolumeRequest request,
                             StreamObserver<Csi.CreateVolumeResponse> responseObserver) {
        this.createVolumeRequestHandler.handleCreateVolume(request, responseObserver);

    }

    @Override
    public void deleteVolume(Csi.DeleteVolumeRequest request,
                             StreamObserver<Csi.DeleteVolumeResponse> responseObserver) {
        this.deleteVolumeRequestHandler.handleDeleteVolume(request, responseObserver);
    }
}
