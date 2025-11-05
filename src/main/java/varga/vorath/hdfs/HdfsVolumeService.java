package varga.vorath.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class HdfsVolumeService {

    private static final Logger logger = LoggerFactory.getLogger(HdfsVolumeService.class);
    private final FileSystem hdfs;

    // Simulated state store
    private final ConcurrentMap<String, String> volumeNodeBindings = new ConcurrentHashMap<>();

    /**
     * Checks whether the volume is already published to the given node.
     *
     * @param volumeId The ID of the volume.
     * @param nodeId   The ID of the node.
     * @return true if the volume is already published to the node, false otherwise.
     */
    public boolean isAlreadyPublished(String volumeId, String nodeId) {
        String boundNodeId = volumeNodeBindings.get(volumeId);
        return nodeId.equals(boundNodeId);
    }

    /**
     * Marks a volume as published to a specific node.
     *
     * @param volumeId The ID of the volume.
     * @param nodeId   The ID of the node.
     */
    public void markVolumeAsPublished(String volumeId, String nodeId) {
        logger.info("Marking volume '{}' as published to node '{}'.", volumeId, nodeId);
        volumeNodeBindings.put(volumeId, nodeId); // Bind volume to the node
    }

    public HdfsVolumeService(@Value("${hdfs.url}") String hdfsUrl,
                             @Value("${hdfs.user}") String hdfsUser) throws IOException, InterruptedException {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", hdfsUrl);
        this.hdfs = FileSystem.get(URI.create(hdfsUrl), config, hdfsUser);
        logger.info("Connected to HDFS at {} with user {}", hdfsUrl, hdfsUser);
    }

    /**
     * Create a volume directory in HDFS.
     *
     * @param volumeName Name of the volume to be created.
     * @throws IOException If there is a problem creating the volume.
     */
    public void createVolume(String volumeName) throws IOException {
        Path volumePath = new Path("/volumes/" + volumeName);
        if (hdfs.exists(volumePath)) {
            throw new IllegalArgumentException("Volume already exists in HDFS: " + volumeName);
        }
        hdfs.mkdirs(volumePath);
        logger.info("Volume '{}' successfully created at path: {}", volumeName, volumePath);
    }

    /**
     * Check if a volume exists in HDFS.
     *
     * @param volumeName Name of the volume.
     * @return True if the volume exists, false otherwise.
     * @throws IOException If there is an error during the check.
     */
    public boolean volumeExists(String volumeName) throws IOException {
        Path volumePath = new Path("/volumes/" + volumeName);
        return hdfs.exists(volumePath);
    }

    /**
     * Delete a volume directory in HDFS.
     *
     * @param volumeName Name of the volume to be deleted.
     * @throws IOException If there is a problem deleting the volume.
     */
    public void deleteVolume(String volumeName) throws IOException {
        Path volumePath = new Path("/volumes/" + volumeName);
        if (hdfs.exists(volumePath)) {
            hdfs.delete(volumePath, true);
            logger.info("Volume '{}' successfully deleted from path: {}", volumeName, volumePath);
        } else {
            logger.warn("Volume '{}' does not exist at path: {}", volumeName, volumePath);
        }
    }
}