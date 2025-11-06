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
    private final GetCapacityHandler getCapacityHandler;
    private final ListVolumesHandler listVolumesHandler;
    private final ValidateVolumeCapabilitiesHandler validateVolumeCapabilitiesHandler;
    private final ControllerGetCapabilitiesRequestHandler controllerGetCapabilitiesRequestHandler;

    @Override
    public void controllerGetCapabilities(csi.v1.Csi.ControllerGetCapabilitiesRequest request, StreamObserver<csi.v1.Csi.ControllerGetCapabilitiesResponse> responseObserver) {
        this.controllerGetCapabilitiesRequestHandler.handleControllerGetCapabilities(request, responseObserver);
    }

    @Override
    public void createVolume(Csi.CreateVolumeRequest request,
                             StreamObserver<Csi.CreateVolumeResponse> responseObserver) {
        this.createVolumeRequestHandler.handleCreateVolumeRequest(request, responseObserver);

    }

    @Override
    public void deleteVolume(Csi.DeleteVolumeRequest request,
                             StreamObserver<Csi.DeleteVolumeResponse> responseObserver) {
        this.deleteVolumeRequestHandler.handleDeleteVolume(request, responseObserver);
    }

    @Override
    public void listVolumes(Csi.ListVolumesRequest request, StreamObserver<Csi.ListVolumesResponse> responseObserver) {
        this.listVolumesHandler.handleListVolumes(request, responseObserver);
    }

    @Override
    public void getCapacity(Csi.GetCapacityRequest request, StreamObserver<Csi.GetCapacityResponse> responseObserver) {
        this.getCapacityHandler.handleGetCapacity(request, responseObserver);
    }

//    @Override
//    public void controllerUnpublishVolume(Csi.ControllerUnpublishVolumeRequest request, StreamObserver<Csi.ControllerUnpublishVolumeResponse> responseObserver) {
//        this.controllerUnpublishVolumeHandler.handleControllerUnpublishVolume(request, responseObserver);
//    }
//
//    @Override
//    public void controllerPublishVolume(Csi.ControllerPublishVolumeRequest request, StreamObserver<Csi.ControllerPublishVolumeResponse> responseObserver) {
//        this.controllerPublishVolumeHandler.handleControllerPublishVolume(request, responseObserver);
//    }

    @Override
    public void validateVolumeCapabilities(Csi.ValidateVolumeCapabilitiesRequest request, StreamObserver<Csi.ValidateVolumeCapabilitiesResponse> responseObserver) {
        this.validateVolumeCapabilitiesHandler.handleValidateVolumeCapabilities(request, responseObserver);
    }
}
