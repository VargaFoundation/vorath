package varga.vorath.hdfs;

import lombok.Getter;

@Getter
public class VolumeAttachmentInfo {
    private final String hdfsUri;

    public VolumeAttachmentInfo(String hdfsUri) {
        this.hdfsUri = hdfsUri;
    }

}