package varga.vorath.identity;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import varga.vorath.CsiPluginProperties;

@Slf4j
@Component
@RequiredArgsConstructor
public class GetPluginInfoHandler {

    private final CsiPluginProperties csiPluginProperties;

    public void handleGetPluginInfo(Csi.GetPluginInfoRequest request,
                                    StreamObserver<Csi.GetPluginInfoResponse> responseObserver) {
        try {
            Csi.GetPluginInfoResponse response = Csi.GetPluginInfoResponse.newBuilder()
                    .setName(this.csiPluginProperties.getName())
                    .setVendorVersion(this.csiPluginProperties.getVendorVersion())
                    .putManifest("description", this.csiPluginProperties.getManifest().get("description"))
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            log.info("GetPluginInfo request processed successfully.");

        } catch (Exception e) {
            log.error("An error occurred while processing GetPluginInfo: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}