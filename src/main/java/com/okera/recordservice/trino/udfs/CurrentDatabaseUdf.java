package com.okera.recordservice.trino.udfs;

import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class CurrentDatabaseUdf {
  @Description("Returns the current catalog")
  @ScalarFunction("current_database")
  @SqlType(StandardTypes.VARCHAR)
  public static Slice currentDatabase() {
    return Slices.utf8Slice("okera");
  }
}