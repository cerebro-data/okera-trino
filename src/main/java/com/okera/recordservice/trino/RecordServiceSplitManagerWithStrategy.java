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

import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

public class RecordServiceSplitManagerWithStrategy implements ConnectorSplitManager {
  private final RecordServiceSplitManagerImpl impl;

  public RecordServiceSplitManagerWithStrategy(
      RecordServiceConfig config, NodeManager nodeManager) {
    this.impl = new RecordServiceSplitManagerImpl(config, nodeManager);
  }

  @Override
  public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, 
      ConnectorSession session, ConnectorTableHandle table, 
      DynamicFilter dynamicFilter, Constraint constraint) {
    return impl.getSplits(transaction, session, table, dynamicFilter, constraint);
  }
}
