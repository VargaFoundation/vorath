package varga.vorath.identity;


import csi.v1.Csi;
import csi.v1.IdentityGrpc;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class IdentityGrpcService extends IdentityGrpc.IdentityImplBase {

    private final GetPluginInfoHandler pluginInfoHandler;
    private final GetPluginCapabilitiesHandler pluginCapabilitiesHandler;
    private final ProbeHandler probeHandler;

    @Override
    public void getPluginInfo(Csi.GetPluginInfoRequest request,
                              StreamObserver<Csi.GetPluginInfoResponse> responseObserver) {
        this.pluginInfoHandler.handleGetPluginInfo(request, responseObserver);
    }

    @Override
    public void getPluginCapabilities(Csi.GetPluginCapabilitiesRequest request,
                                      StreamObserver<Csi.GetPluginCapabilitiesResponse> responseObserver) {

        Csi.GetPluginCapabilitiesResponse response = Csi.GetPluginCapabilitiesResponse.newBuilder()
                .addAllCapabilities(this.pluginCapabilitiesHandler.handleGetPluginCapabilities())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }

    @Override
    public void probe(Csi.ProbeRequest request, StreamObserver<Csi.ProbeResponse> responseObserver) {
        this.probeHandler.handleProbe(request, responseObserver);
    }

}
