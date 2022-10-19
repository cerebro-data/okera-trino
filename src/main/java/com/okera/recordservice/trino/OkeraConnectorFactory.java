package com.okera.recordservice.trino;

import com.google.inject.Injector;
import com.okera.recordservice.util.Preconditions;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;
import java.util.Map;

public class OkeraConnectorFactory implements ConnectorFactory {
  private static final Logger LOG = Logger.get(OkeraConnectorFactory.class);

  private final String name;
  private final String sampling;

  public OkeraConnectorFactory(String name, String sampling) {
    this.name = name;
    this.sampling = sampling;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Connector create(final String connectorId, Map<String, String> requiredConfig,
      ConnectorContext context) {
    LOG.info("Creating Okera connector: " + connectorId);
    Preconditions.checkNotNull(requiredConfig, "requiredConfig is null");

    try {
      Bootstrap app = new Bootstrap(
          new JsonModule(),
          new RecordServiceModule(sampling, name),
          binder -> {
            binder.bind(TypeManager.class).toInstance(context.getTypeManager());
            binder.bind(NodeManager.class).toInstance(context.getNodeManager());
        });

      Injector injector = app
          .doNotInitializeLogging()
          .setRequiredConfigurationProperties(requiredConfig)
          .initialize();
      return injector.getInstance(RecordServiceConnector.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
