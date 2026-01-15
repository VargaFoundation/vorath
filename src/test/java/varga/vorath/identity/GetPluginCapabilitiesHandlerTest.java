package varga.vorath.identity;

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

import csi.v1.Csi;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class GetPluginCapabilitiesHandlerTest {

    @Test
    public void testHandleGetPluginCapabilities() {
        GetPluginCapabilitiesHandler handler = new GetPluginCapabilitiesHandler();
        List<Csi.PluginCapability> capabilities = handler.handleGetPluginCapabilities();

        assertFalse(capabilities.isEmpty());
        assertEquals(Csi.PluginCapability.Service.Type.CONTROLLER_SERVICE, 
                     capabilities.get(0).getService().getType());
    }
}
