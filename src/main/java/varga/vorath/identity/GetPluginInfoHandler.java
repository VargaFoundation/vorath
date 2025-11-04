package varga.vorath.identity;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class GetPluginInfoHandler {

    private static final Logger logger = LoggerFactory.getLogger(GetPluginInfoHandler.class);
    public static final String HDFS_CSI_PLUGIN = "hdfs.csi.plugin";
    public static final String O_1 = "1.0.0";

    public void handleGetPluginInfo(Csi.GetPluginInfoRequest request,
                                    StreamObserver<Csi.GetPluginInfoResponse> responseObserver) {
        try {
            Csi.GetPluginInfoResponse response = Csi.GetPluginInfoResponse.newBuilder()
                    .setName("hdfs.csi.plugin")
                    .setVendorVersion(O_1)
                    .putManifest("description", "A CSI plugin for HDFS")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.info("GetPluginInfo request processed successfully. Name: {}, Version: {}",
                    HDFS_CSI_PLUGIN, O_1);

        } catch (Exception e) {
            logger.error("An error occurred while processing GetPluginInfo: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }
}