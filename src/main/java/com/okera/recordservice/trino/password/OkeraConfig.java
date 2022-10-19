package com.okera.recordservice.trino.password;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

public class OkeraConfig {
  private Duration cacheTtl = new Duration(1, TimeUnit.HOURS);
  private Duration authTimeout = new Duration(10, TimeUnit.SECONDS);

  @NotNull
  public Duration getCacheTtl() {
    return cacheTtl;
  }

  @NotNull
  public Duration getAuthTimeout() {
    return authTimeout;
  }

  @Config("okera.cache-ttl")
  public OkeraConfig setCacheTtl(Duration cacheTtl) {
    this.cacheTtl = cacheTtl;
    return this;
  }

  @Config("okera.auth-timeout")
  public OkeraConfig setAuthTimeout(Duration timeout) {
    this.authTimeout = timeout;
    return this;
  }
}
