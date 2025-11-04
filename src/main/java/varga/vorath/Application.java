package varga.vorath;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import varga.vorath.identity.IdentityGrpcService;
import varga.vorath.controller.ControllerGrpcService;
import varga.vorath.node.NodeGrpcService;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public CommandLineRunner runGrpcServer(ApplicationContext applicationContext,
                                           @Value("${grpc.server.port:50051}") int grpcServerPort) {
        return args -> {
            // Création du serveur gRPC en récupérant les services depuis le contexte de Spring
            Server server = ServerBuilder.forPort(grpcServerPort)
                    .addService(applicationContext.getBean(IdentityGrpcService.class))
                    .addService(applicationContext.getBean(ControllerGrpcService.class))
                    .addService(applicationContext.getBean(NodeGrpcService.class))
                    .build();

            System.out.println("gRPC server started on port " + grpcServerPort);

            server.start();
            server.awaitTermination();
        };
    }
}