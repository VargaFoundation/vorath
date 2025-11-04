package varga.vorath.controller;

import csi.v1.Csi;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class ListVolumesHandler {

    private static final Logger logger = LoggerFactory.getLogger(ListVolumesHandler.class);

    /**
     * Handles the ListVolumes request to return a paginated list of existing volumes.
     *
     * @param request           The ListVolumesRequest from the client.
     * @param responseObserver  The response observer to send the result.
     */
    public void handleListVolumes(Csi.ListVolumesRequest request,
                                  StreamObserver<Csi.ListVolumesResponse> responseObserver) {
        try {
            logger.info("Processing ListVolumes request. StartingToken: {}, MaxEntries: {}",
                    request.getStartingToken(), request.getMaxEntries());

            // Simulate retrieving volume information (this would normally come from a backend or database)
            List<Csi.ListVolumesResponse.Entry> volumeEntries = fetchVolumes(request);

            // Build response
            Csi.ListVolumesResponse.Builder responseBuilder = Csi.ListVolumesResponse.newBuilder()
                    .addAllEntries(volumeEntries);

            // Check if pagination is needed and set the next token
            if (!volumeEntries.isEmpty() && volumeEntries.size() >= request.getMaxEntries()) {
                String nextToken = generateNextToken(request.getStartingToken());
                responseBuilder.setNextToken(nextToken);
            } else {
                responseBuilder.setNextToken(""); // No more volumes to list
            }

            // Send response to the client
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            logger.info("ListVolumes completed successfully. Returned volumes: {}", volumeEntries.size());

        } catch (Exception e) {
            logger.error("Error handling ListVolumes request: {}", e.getMessage(), e);
            responseObserver.onError(e);
        }
    }

    /**
     * Simulates fetching a list of volumes.
     *
     * @param request The ListVolumesRequest containing pagination parameters.
     * @return A list of volume entries.
     */
    private List<Csi.ListVolumesResponse.Entry> fetchVolumes(Csi.ListVolumesRequest request) {
        // Example data: Normally you'd query a backend or database for real volume data
        List<Csi.ListVolumesResponse.Entry> allVolumes = new ArrayList<>();

        for (int i = 1; i <= 50; i++) {
            allVolumes.add(Csi.ListVolumesResponse.Entry.newBuilder()
                    .setVolume(Csi.Volume.newBuilder()
                            .setVolumeId("volume-" + i)
                            .setCapacityBytes(1024L * 1024 * 1024 * 100) // 100 GiB
                            .build())
                    .build());
        }

        // Handle pagination
        int startIndex;
        try {
            startIndex = request.getStartingToken().isEmpty() ? 0 : Integer.parseInt(request.getStartingToken());
        } catch (NumberFormatException e) {
            startIndex = 0;
        }

        int endIndex = request.getMaxEntries() > 0
                ? Math.min(startIndex + request.getMaxEntries(), allVolumes.size())
                : allVolumes.size();

        return allVolumes.subList(startIndex, endIndex);
    }

    /**
     * Generates the next token for paginated requests.
     *
     * @param currentToken The current token from the request.
     * @return The next token for pagination.
     */
    private String generateNextToken(String currentToken) {
        int nextToken = currentToken.isEmpty() ? 0 : Integer.parseInt(currentToken) + 1;
        return String.valueOf(nextToken);
    }
}