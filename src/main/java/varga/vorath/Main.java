package varga.vorath;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import varga.vorath.controller.ControllerService;
import varga.vorath.identity.IdentityService;
import varga.vorath.node.NodeService;

public class Main {
    public static void main(String[] args) throws Exception {
        Server server = ServerBuilder.forPort(50051)
                .addService(new IdentityService())
                .addService(new ControllerService())
                .addService(new NodeService())
                .build();

        System.out.println("CSI server started on port 50051");
        server.start();
        server.awaitTermination();
    }
}
