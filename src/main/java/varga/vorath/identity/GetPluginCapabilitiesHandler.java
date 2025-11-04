package varga.vorath.identity;

import csi.v1.Csi;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class GetPluginCapabilitiesHandler {

    public List<Csi.PluginCapability> handleGetPluginCapabilities() {
        Csi.PluginCapability controllerServiceCapability = Csi.PluginCapability.newBuilder()
                .setService(Csi.PluginCapability.Service.newBuilder()
                        .setType(Csi.PluginCapability.Service.Type.CONTROLLER_SERVICE)
                        .build())
                .build();

        return Collections.singletonList(controllerServiceCapability);
    }

}
