// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.okera.recordservice.trino;

import static java.util.Objects.requireNonNull;

import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import java.util.List;
import javax.inject.Inject;

public class RecordServiceConnector implements Connector {
  private static final Logger LOG = Logger.get(RecordServiceConnector.class);

  private final LifeCycleManager lifeCycleManager;
  private final RecordServiceMetadata metadata;
  private final ConnectorSplitManager splitManager;
  private final RecordServicePageSourceProvider pageSourceProvider;
  private final RecordServiceSessionProperties sessionProperties;

  @Inject
  public RecordServiceConnector(
      LifeCycleManager lifeCycleManager,
      NodeManager nodeManager,
      RecordServiceMetadata metadata,
      RecordServiceConfig config,
      RecordServiceSessionProperties sessionProperties,
      RecordServicePageSourceProvider pageSourceProvider) {
    this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    this.metadata = requireNonNull(metadata, "RecordService metadata is null");
    this.pageSourceProvider = requireNonNull(pageSourceProvider,
        "RecordService pageSourceProvider is null");
    this.sessionProperties = requireNonNull(sessionProperties,
        "RecordService sessionProperties is null");
    this.splitManager = new RecordServiceSplitManagerWithStrategy(config, nodeManager);
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel level, boolean readonly) {
    return RecordServiceTransactionHandle.INSTANCE;
  }

  @Override
  public ConnectorMetadata getMetadata(ConnectorTransactionHandle h) {
    return metadata;
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    return splitManager;
  }

  @Override
  public ConnectorPageSourceProvider getPageSourceProvider() {
    return pageSourceProvider;
  }

  @Override
  public List<PropertyMetadata<?>> getSessionProperties() {
    return sessionProperties.getSessionProperties();
  }

  @Override
  public final void shutdown() {
    try {
      lifeCycleManager.stop();
    } catch (Exception e) {
      LOG.error(e, "Error shutting down connector");
    }
  }
}
