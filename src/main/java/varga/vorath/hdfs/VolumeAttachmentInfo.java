package varga.vorath.hdfs;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class VolumeAttachmentInfo {
    private final String hdfsUri;
    private final HdfsConnection hdfsConnection;

}