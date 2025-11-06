package varga.vorath;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import varga.vorath.controller.ControllerGrpcService;
import varga.vorath.identity.IdentityGrpcService;
import varga.vorath.node.NodeGrpcService;

import java.io.File;

@SpringBootApplication
public class Application {

    // Define the logger
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public CommandLineRunner runGrpcServer(ApplicationContext applicationContext) {
        return args -> {
            // Use the configured CSI endpoint (Unix socket)
            String csiEndpoint = System.getenv("CSI_ENDPOINT");

            if (csiEndpoint == null || !csiEndpoint.startsWith("unix://")) {
                logger.error("Invalid CSI_ENDPOINT: must use the 'unix://' scheme for file sockets");
                throw new IllegalArgumentException("CSI_ENDPOINT must use the 'unix://' scheme for file sockets");
            }

            String socketPath = csiEndpoint.substring("unix://".length());
            File socketFile = new File(socketPath);

            // Ensure the socket file does not already exist
            if (socketFile.exists() && !socketFile.delete()) {
                logger.error("Failed to delete existing socket: {}", socketPath);
                return; // Exit the runner if the socket file cannot be removed
            }

            // Create and start the gRPC server
            Server server = NettyServerBuilder.forAddress(new DomainSocketAddress(socketPath))
                    .addService(applicationContext.getBean(IdentityGrpcService.class))
                    .addService(applicationContext.getBean(ControllerGrpcService.class))
                    .addService(applicationContext.getBean(NodeGrpcService.class))
                    .build();

            logger.info("gRPC server started on Unix socket: {}", socketPath);
            server.start();

            // Add a shutdown hook to gracefully stop the server on JVM shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown initiated: attempting to stop gRPC server...");
                try {
                    if (server != null) {
                        server.shutdown();
                        server.awaitTermination();
                        logger.info("gRPC server stopped successfully.");
                    }
                } catch (InterruptedException e) {
                    logger.error("Error during server shutdown: {}", e.getMessage(), e);
                    Thread.currentThread().interrupt();
                }
            }));

            // Wait for the server to terminate
            server.awaitTermination();
        };
    }
}