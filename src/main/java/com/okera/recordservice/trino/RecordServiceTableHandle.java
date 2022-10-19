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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class RecordServiceTableHandle implements ConnectorTableHandle {
  private final SchemaTableName schemaTableName;
  private final TupleDomain<ColumnHandle> constraint;
  private final Optional<List<ColumnHandle>> desiredColumns;
  private final boolean isDeleteHandle;

  // Cache of the schema for this table. Populated lazily.
  private List<ColumnMetadata> schema;

  // Cache of stats for this table. Populated lazily.
  private TableStatistics stats;

  @JsonCreator
  public RecordServiceTableHandle(
      @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
      @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
      @JsonProperty("desiredColumns") Optional<List<ColumnHandle>> desiredColumns,
      @JsonProperty("isDeleteHandle") boolean isDeleteHandle) {
    this.schemaTableName = schemaTableName;
    this.constraint = constraint;
    this.desiredColumns = desiredColumns;
    this.isDeleteHandle = isDeleteHandle;
  }

  @JsonProperty
  public SchemaTableName getSchemaTableName() { 
    return schemaTableName; 
  }

  @JsonProperty
  public TupleDomain<ColumnHandle> getConstraint() {
      return constraint;
  }

  @JsonProperty
  public Optional<List<ColumnHandle>> getDesiredColumns() {
      return desiredColumns;
  }

  @JsonProperty
  public boolean isDeleteHandle() {
      return isDeleteHandle;
  }

  public List<ColumnMetadata> getSchema() { return schema; }

  public TableStatistics getStats() { return stats; }

  public void setSchema(List<ColumnMetadata> schema) {
    this.schema = schema;
  }

  public void setStats(TableStatistics stats) {
    this.stats = stats;
  }

  @Override
  public boolean equals(Object obj){
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    RecordServiceTableHandle other = (RecordServiceTableHandle) obj;
    return Objects.equals(this.schemaTableName, other.schemaTableName) &&
    Objects.equals(this.constraint, other.constraint) &&
    Objects.equals(this.desiredColumns, other.desiredColumns) &&
    Objects.equals(this.isDeleteHandle, other.isDeleteHandle);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.schemaTableName);
  }

  @Override
  public String toString() {
    return schemaTableName.toString();
  }
}
