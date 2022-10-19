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

import static io.airlift.configuration.ConfigBinder.configBinder;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;

public class RecordServiceModule implements Module {
  private final String sampling;
  private final String catalog;

  @Inject
  public RecordServiceModule(String sampling, String catalog) {
    this.sampling = sampling;
    this.catalog = catalog;
  }

  public String getCatalog() { return catalog; }

  @Override
  public void configure(Binder binder) {
    binder.bind(RecordServiceMetadata.class).in(Scopes.SINGLETON);
    binder.bind(RecordServiceModule.class).toInstance(this);

    binder.bind(RecordServicePageSourceProvider.class).in(Scopes.SINGLETON);
    binder.bind(RecordServiceConnector.class).in(Scopes.SINGLETON);
    binder.bind(RecordServiceSessionProperties.class).in(Scopes.SINGLETON);
    configBinder(binder).bindConfigGlobalDefaults(RecordServiceConfig.class,
        config -> { config.setLastNBytes(sampling); });
    configBinder(binder).bindConfig(RecordServiceConfig.class);
  }
}
