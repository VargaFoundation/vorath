package varga.vorath.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;

@Service
public class HdfsVolumeService {

    private final FileSystem hdfs;

    public HdfsVolumeService(@Value("${hdfs.url}") String hdfsUrl,
                             @Value("${hdfs.user}") String hdfsUser) throws IOException, InterruptedException {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", hdfsUrl);
        hdfs = FileSystem.get(URI.create(hdfsUrl), config, hdfsUser);
    }

    public void createVolume(String volumeName) throws IOException {
        Path volumePath = new Path("/volumes/" + volumeName);
        if (!hdfs.exists(volumePath)) {
            hdfs.mkdirs(volumePath);
        }
    }

    public void deleteVolume(String volumeName) throws IOException {
        Path volumePath = new Path("/volumes/" + volumeName);
        if (hdfs.exists(volumePath)) {
            hdfs.delete(volumePath, true);
        }
    }
}
