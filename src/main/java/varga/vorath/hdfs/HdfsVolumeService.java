package varga.vorath.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class HdfsVolumeService {

    private static final Logger logger = LoggerFactory.getLogger(HdfsVolumeService.class);

    /**
     * Resolve a FileSystem to operate on. If an HdfsConnection is provided, use its
     * Configuration (and authentication) to obtain a FileSystem instance; otherwise,
     * fall back to the default configuration on the classpath/JVM.
     */
    private FileSystem resolveFileSystem(HdfsConnection hdfsConnection) throws IOException {
        if (hdfsConnection != null) {
            return FileSystem.get(hdfsConnection.getConfiguration());
        }
        return FileSystem.get(new Configuration());
    }

    /**
     * Create a volume directory in HDFS.
     *
     * @param volumeName Name of the volume to be created.
     * @throws IOException If there is a problem creating the volume.
     */
    public String createVolume(HdfsConnection hdfsConnection, String volumeName) throws IOException {
        FileSystem fs = resolveFileSystem(hdfsConnection);
        Path volumePath = new Path("/volumes/" + volumeName);
        if (fs.exists(volumePath)) {
            throw new IllegalArgumentException("Volume already exists in HDFS: " + volumeName);
        }
        fs.mkdirs(volumePath);
        logger.info("Volume '{}' successfully created at path: {}", volumeName, volumePath);

        // Use volume name as stable ID for now
        return volumeName;
    }

    /**
     * Delete a volume directory in HDFS.
     *
     * @throws IOException If there is a problem deleting the volume.
     */
    public void deleteVolume(HdfsConnection hdfsConnection, String pathOrVolumeName) throws IOException {
        FileSystem fs = resolveFileSystem(hdfsConnection);

        // If a full path is provided (e.g., starts with / or contains scheme), use it as is.
        Path volumePath;
        if (pathOrVolumeName.startsWith("/") || pathOrVolumeName.contains(":")) {
            volumePath = new Path(pathOrVolumeName);
        } else {
            volumePath = new Path("/volumes/" + pathOrVolumeName);
        }

        if (fs.exists(volumePath)) {
            fs.delete(volumePath, true);
            logger.info("HDFS path '{}' successfully deleted", volumePath);
        } else {
            logger.warn("HDFS path '{}' does not exist; nothing to delete", volumePath);
        }
    }
}