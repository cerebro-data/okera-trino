package com.okera.recordservice.trino;

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.Map;

public class OkeraEventListenerFactory implements EventListenerFactory {

  @Override
  public EventListener create(Map<String, String> config) {
    return new OkeraEventListener();
  }

  @Override
  public String getName() {
    return "okera-query-logger";
  }

}
