package varga.vorath;

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

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class UtilsTest {

    @Test
    public void testExtractClusterUri() {
        assertEquals("hdfs://namenode:8020", Utils.extractClusterUri("hdfs://namenode:8020/user/data"));
        assertEquals("webhdfs://namenode:50070", Utils.extractClusterUri("webhdfs://namenode:50070/"));
        assertNull(Utils.extractClusterUri("/local/path"));
        assertNull(Utils.extractClusterUri(null));
        assertNull(Utils.extractClusterUri(""));
    }

    @Test
    public void testNormalizePath() {
        assertEquals("/volumes", Utils.normalizePath(null));
        assertEquals("/volumes", Utils.normalizePath(""));
        assertEquals("/my/path", Utils.normalizePath("/my/path/"));
        assertEquals("/my/path", Utils.normalizePath("/my/path"));
    }
}
