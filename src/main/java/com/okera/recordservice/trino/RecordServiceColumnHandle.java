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
import com.google.common.base.MoreObjects;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

public class RecordServiceColumnHandle implements ColumnHandle {
  private final String columnName;
  private final Type columnType;
  private final String comment;
  private final int columnIdx; // Index into the base table.

  @JsonCreator
  public RecordServiceColumnHandle(
      @JsonProperty("columnName") String colName,
      @JsonProperty("columnType") Type colType,
      @JsonProperty("comment") String comment,
      @JsonProperty("columnIdx") int colIdx) {
    this.columnName = colName;
    this.columnType = colType;
    this.comment = comment;
    this.columnIdx = colIdx;
  }

  @JsonProperty
  public String columnName() { return columnName; }

  @JsonProperty
  public Type columnType() { return columnType; }

  @JsonProperty
  public String comment() { return comment; }

  @JsonProperty
  public int columnIdx() { return columnIdx; }

  public ColumnMetadata metadata() {
    return ColumnMetadata.builder()
      .setName(columnName)
      .setType(columnType)
      .setComment(Optional.of(comment))
      .setHidden(false).build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("columnName", columnName)
        .add("columnType", columnType)
        .add("comment", comment)
        .add("columnIdx", columnIdx)
        .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    RecordServiceColumnHandle that = (RecordServiceColumnHandle) other;
    return Objects.equals(this.columnName, that.columnName) &&
        Objects.equals(this.columnType, that.columnType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, columnType);
  }
}
