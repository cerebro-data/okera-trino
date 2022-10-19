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

import io.trino.spi.session.PropertyMetadata;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;

/**
 * Properties that can be set from the presto-cli to set per session values.
 * For example:
 * presto> SET SESSION recordservice.token='part1.part2.part2'
 */
public class RecordServiceSessionProperties {
  public static final String ACCESS_TOKEN = "token";
  public static final String SAMPLING_VALUE = "sampling_value";
  public static final String LIMIT = "limit";
  public static final String MAX_TASKS = "max_tasks";
  public static final String STATS_MODE = "stats_mode";
  private static final long DEFAULT_PROP_VALUE = -1;
  private static final int DEFAULT_INT_PROP_VALUE = -1;
  private final List<PropertyMetadata<?>> sessionProperties;

  @Inject
  public RecordServiceSessionProperties() {
    PropertyMetadata<String> token = PropertyMetadata.stringProperty(ACCESS_TOKEN,
        "Access token for ODAS services.", null, true);
    PropertyMetadata<Long> samplingValue = PropertyMetadata.longProperty(SAMPLING_VALUE,
        "Sampling value in bytes to use when querying data.", DEFAULT_PROP_VALUE,
        false);
    PropertyMetadata<Long> limit = PropertyMetadata.longProperty(LIMIT,
        "Limit to be applied to the query results", DEFAULT_PROP_VALUE,
        false);
    PropertyMetadata<Integer> maxTasks = PropertyMetadata.integerProperty(MAX_TASKS,
        "Maximum number of tasks to use (-1 to use configured default)",
        DEFAULT_INT_PROP_VALUE,
        false);
    PropertyMetadata<String> statsMode = PropertyMetadata.stringProperty(STATS_MODE,
        "Which stats mode to use (empty string to use configured default)", "",
        false);
    List<PropertyMetadata<?>> propList = new ArrayList<PropertyMetadata<?>>();
    propList.add(token);
    propList.add(samplingValue);
    propList.add(limit);
    propList.add(maxTasks);
    propList.add(statsMode);
    sessionProperties = propList;
  }

  public List<PropertyMetadata<?>> getSessionProperties() {
    return sessionProperties;
  }
}
