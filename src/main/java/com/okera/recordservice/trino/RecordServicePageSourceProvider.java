package com.okera.recordservice.trino;

import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordPageSource;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordServicePageSourceProvider implements ConnectorPageSourceProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(RecordServicePageSourceProvider.class);

  private final NodeManager nodeManager;

  @Inject
  public RecordServicePageSourceProvider(
      NodeManager nodeManager) {
    this.nodeManager = nodeManager;
  }

  @Override
  public ConnectorPageSource createPageSource(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorSplit split,
      ConnectorTableHandle table,
      List<ColumnHandle> columnHandles,
      DynamicFilter dynamicFilter) {
    if (!nodeManager.getAllNodes().isEmpty()) {
      for (Node node : nodeManager.getAllNodes()) {
        RecordServiceUtil.setVersion(node.getVersion());
        LOG.debug("Setting Trino version to: " + node.getVersion());
        break;
      }
    } else {
      LOG.debug("No version info available, defaulting to assuming 338 or earlier.");
    }

    List<RecordServiceColumnHandle> columns = new ArrayList<RecordServiceColumnHandle>();
    for (ColumnHandle handle : columnHandles) {
      RecordServiceColumnHandle rsHandle = (RecordServiceColumnHandle)handle;
      columns.add(rsHandle);
    }

    // If the data does not contain complex types, use the page source,
    // otherwise fallback to the older implementation.
    // TODO: delete the older one when the new impl is done.
    final RecordServiceSplit rsSplit = (RecordServiceSplit)split;
    if (rsSplit.isContainsComplexTypes()) {
      RecordServiceRecordSet rs = new RecordServiceRecordSet(session, rsSplit, columns);
      return new RecordPageSource(rs);
    } else {
      return new RecordServicePageSource(session, rsSplit, columns);
    }
  }
}
