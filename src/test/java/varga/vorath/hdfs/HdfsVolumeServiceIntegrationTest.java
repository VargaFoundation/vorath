package varga.vorath.hdfs;

/*-
 * #%L
 * Vorath
 * %%
 * Copyright (C) 2025 - 2026 Varga Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HdfsVolumeServiceIntegrationTest {

    private static MiniDFSCluster cluster;
    private static HdfsConfiguration conf;
    private static HdfsVolumeService service;

    @BeforeAll
    public static void setUp() throws IOException {
        System.setProperty("test.build.data", "target/test/data");
        conf = new HdfsConfiguration();
        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
        cluster.waitActive();
        service = new HdfsVolumeService();
    }

    @AfterAll
    public static void tearDown() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test
    public void testCreateAndDeleteVolume() throws IOException {
        String volumeName = "test-integration-volume";
        
        // Pass the cluster configuration to the service via a mock connection or by using the cluster's FS
        FileSystem fs = cluster.getFileSystem();
        
        // We need to make sure the service uses the SAME configuration as the cluster
        HdfsConnection mockConnection = mock(HdfsConnection.class);
        when(mockConnection.getConfiguration()).thenReturn(fs.getConf());

        String volumeId = service.createVolume(mockConnection, volumeName);
        assertEquals(volumeName, volumeId);

        Path expectedPath = new Path("/volumes/" + volumeName);
        assertTrue(fs.exists(expectedPath), "Volume path should exist in HDFS");
        assertTrue(fs.isDirectory(expectedPath));

        // Test duplicate creation
        assertThrows(IllegalArgumentException.class, () -> service.createVolume(mockConnection, volumeName));

        // Test deletion
        service.deleteVolume(mockConnection, volumeName);
        assertFalse(fs.exists(expectedPath));
    }
}
