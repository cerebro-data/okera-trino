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

import com.okera.recordservice.trino.password.OkeraAuthenticatorFactory;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.security.PasswordAuthenticatorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.okera.recordservice.trino.udfs.CurrentDatabaseUdf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RecordServicePlugin implements Plugin {
  private Map<String, String> optionalConfig = new HashMap<String, String>();

  private static final boolean ENABLE_UDFS;

  static {
    if (System.getenv().containsKey("OKERA_ENABLE_UDFS")) {
      ENABLE_UDFS = "true".equalsIgnoreCase(System.getenv("OKERA_ENABLE_UDFS"));
    } else {
      ENABLE_UDFS = true;
    }
  }

  public synchronized Map<String, String> getOptionalConfig() {
    return optionalConfig;
  }

  @Override
  public Iterable<EventListenerFactory> getEventListenerFactories() {
    return ImmutableList.of(new OkeraEventListenerFactory());
  }

  @Override
  public Iterable<PasswordAuthenticatorFactory> getPasswordAuthenticatorFactories() {
    List<PasswordAuthenticatorFactory> result = new ArrayList<>();
    result.add(new OkeraAuthenticatorFactory());
    return result;
  }

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    List<ConnectorFactory> result = new ArrayList<ConnectorFactory>();
    result.add(new OkeraConnectorFactory("okera", null));
    result.add(new OkeraConnectorFactory("okera_sampled_10mb", "10mb"));
    result.add(new OkeraConnectorFactory("okera_sampled_100mb", "100mb"));
    return result;
  }

  @Override
  public Set<Class<?>> getFunctions() {
    if (ENABLE_UDFS) {
      return ImmutableSet.<Class<?>>builder()
          .add(CurrentDatabaseUdf.class)
          .build();
    } else {
      return new HashSet<>();
    }
  }
}
