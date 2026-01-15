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

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import varga.vorath.kubernetes.KubernetesVolumeAttachmentClient;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class HdfsMountServiceTest {

    private static MiniDFSCluster cluster;
    private static HdfsConfiguration conf;

    @Mock
    private KubernetesVolumeAttachmentClient volumeAttachmentClient;

    @InjectMocks
    private HdfsMountService hdfsMountService;

    @TempDir
    Path tempDir;

    @BeforeAll
    public static void beforeAll() throws IOException {
        System.setProperty("test.build.data", "target/test/data/HdfsMountServiceTest");
        conf = new HdfsConfiguration();
        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
        cluster.waitActive();
    }

    @AfterAll
    public static void afterAll() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testInit_NoAttachments() {
        when(volumeAttachmentClient.getVolumeAttachmentsForCurrentNode()).thenReturn(Collections.emptyMap());
        
        hdfsMountService.init();
        
        verify(volumeAttachmentClient).getVolumeAttachmentsForCurrentNode();
    }

    @Test
    public void testMountVolume_Failure() throws IOException {
        HdfsConnection mockConn = mock(HdfsConnection.class);
        when(mockConn.getConfiguration()).thenReturn(cluster.getFileSystem().getConf());
        
        String hdfsUri = cluster.getFileSystem().getUri().toString() + "/test";
        String targetPath = tempDir.resolve("mount").toAbsolutePath().toString();

        // HdfsVirtualFileSystem creation and mount call is hard to mock without refactoring or PowerMock
        // But we can check that it fails as expected if we can't mount (e.g. no FUSE)
        assertThrows(Throwable.class, () -> hdfsMountService.mountVolume(mockConn, hdfsUri, targetPath));
    }

    @Test
    public void testUnmountVolume_NotMounted() {
        hdfsMountService.unmountVolume("/not/mounted");
        // Should just log a warning
    }
}
