package varga.vorath.hdfs;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class HdfsVolumeService {

    private static final Logger logger = LoggerFactory.getLogger(HdfsVolumeService.class);

    /**
     * Create a volume directory in HDFS.
     *
     * @param volumeName Name of the volume to be created.
     * @throws IOException If there is a problem creating the volume.
     */
    public String createVolume(String volumeName) throws IOException {
        Path volumePath = new Path("/volumes/" + volumeName);
        if (hdfs.exists(volumePath)) {
            throw new IllegalArgumentException("Volume already exists in HDFS: " + volumeName);
        }
        hdfs.mkdirs(volumePath);
        logger.info("Volume '{}' successfully created at path: {}", volumeName, volumePath);

        return "volumeId"; // TODO
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