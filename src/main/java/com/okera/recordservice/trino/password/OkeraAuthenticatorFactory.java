package com.okera.recordservice.trino.password;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.configuration.ConfigBinder.configBinder;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.security.PasswordAuthenticator;
import io.trino.spi.security.PasswordAuthenticatorFactory;
import java.util.Map;

public class OkeraAuthenticatorFactory implements PasswordAuthenticatorFactory {
  @Override
  public String getName() {
    return "okera";
  }

  @Override
  public PasswordAuthenticator create(Map<String, String> config) {
    try {
      Bootstrap app = new Bootstrap(
          binder -> {
            configBinder(binder).bindConfig(OkeraConfig.class);
            binder.bind(OkeraAuthenticator.class).in(Scopes.SINGLETON);
          });

      Injector injector = app
              .doNotInitializeLogging()
              .setRequiredConfigurationProperties(config)
              .initialize();

      return injector.getInstance(OkeraAuthenticator.class);
    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }
}
