package com.okera.recordservice.trino;

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;

import io.airlift.log.Logger;
import org.apache.commons.lang.StringEscapeUtils;

public class OkeraEventListener implements EventListener {
  private static final Logger LOG = Logger.get(OkeraEventListener.class);

  private static final boolean ENABLE_QUERY_LOGGING;
  private static final String TRINO_ENABLE_QUERY_LOGGING_ENV =
      "TRINO_ENABLE_QUERY_LOGGING";

  static {
    if (System.getenv().containsKey(TRINO_ENABLE_QUERY_LOGGING_ENV)) {
      ENABLE_QUERY_LOGGING =
        "true".equalsIgnoreCase(System.getenv(TRINO_ENABLE_QUERY_LOGGING_ENV));
    } else {
      ENABLE_QUERY_LOGGING = false;
    }
  }

  @Override
  public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
    String queryId = queryCompletedEvent.getMetadata().getQueryId();
    String userName = queryCompletedEvent.getContext().getUser();
    long createTime = queryCompletedEvent.getCreateTime().toEpochMilli();
    long executionStartTime = queryCompletedEvent.getExecutionStartTime().toEpochMilli();
    long endTime = queryCompletedEvent.getEndTime().toEpochMilli();
    long totalExecutionTime = endTime - executionStartTime;
    boolean succeeded = !queryCompletedEvent.getFailureInfo().isPresent();
    String errorCode = queryCompletedEvent.getFailureInfo()
        .map(f -> f.getErrorCode().toString())
        .orElse("None");
    String failureMessage = queryCompletedEvent.getFailureInfo()
        .flatMap(f -> f.getFailureMessage())
        .orElse("None");
    String query = StringEscapeUtils.escapeJavaScript(
        queryCompletedEvent.getMetadata().getQuery());

    String logMessageFormat =
        "QueryId: %s; " +
        "User: %s; " +
        "CreateTime: %d; " +
        "ExecutionStartTime: %d; " +
        "EndTime: %d; " +
        "TotalExecutionTime: %d; " +
        "Succeeded: %b; " +
        "ErrorCode: %s; " +
        "FailureMessage: %s;" +
        "Query: %s; ";

    log(
        logMessageFormat,
        queryId,
        userName,
        createTime,
        executionStartTime,
        endTime,
        totalExecutionTime,
        succeeded,
        errorCode,
        failureMessage,
        query);
  }

  // This is a separate method to make it easier to change how we persist log info
  private void log(String format, Object...args) {
    if (ENABLE_QUERY_LOGGING) {
      LOG.info(format, args);
    }
  }
}
