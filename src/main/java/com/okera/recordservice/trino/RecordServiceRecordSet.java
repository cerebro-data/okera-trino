package com.okera.recordservice.trino;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

import com.okera.recordservice.core.ByteArray;
import com.okera.recordservice.core.Decimal;
import com.okera.recordservice.core.RecordServiceException;
import com.okera.recordservice.core.Records;
import com.okera.recordservice.core.Schema;
import com.okera.recordservice.util.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import java.io.IOException;
import java.util.List;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

public class RecordServiceRecordSet implements RecordSet {
  // private static final Logger LOG =
  //     LoggerFactory.getLogger(RecordServiceRecordSet.class);

  private final ConnectorSession session;
  private final RecordServiceSplit split;
  private final List<RecordServiceColumnHandle> columns;

  public RecordServiceRecordSet(ConnectorSession session, RecordServiceSplit split,
      List<RecordServiceColumnHandle> columns) {
    this.session = Preconditions.checkNotNull(session);
    this.split = Preconditions.checkNotNull(split);
    this.columns = Preconditions.checkNotNull(columns);
  }

  @Override
  public List<Type> getColumnTypes() {
    return RecordServiceUtil.getColumnTypes(columns);
  }

  @SuppressWarnings("resource")
  @Override
  public RecordCursor cursor() {
    Records cursorRecords = RecordServiceUtil.createRecords(session, split, null);
    return new RecordServiceCursor(columns, cursorRecords);
  }

  private static final class RecordServiceCursor implements RecordCursor {
    private final List<RecordServiceColumnHandle> cols;
    private List<Schema.ColumnDesc> rsCols;
    private final Records records;
    private final Schema.Type[] types;
    private Records.Record record;

    // We need to cache the results of the row on calls to advanceNextPosition. The
    // underlying record api does not support random access but the calls from presto
    // will skip rows depending on the predicates. This means it's very easy to mis-index
    // into the results.
    // Each of these arrays is index by field (i.e. column idx). Only one of these will
    // be populated per column idx depending on the type.
    private final boolean[] booleanValues;
    private final long[] longValues;
    private final double[] doubleValues;
    private final Decimal[] decimalValues;
    private final ByteArray[] byteArrayValues;
    private final Block[] complexValues;

    public RecordServiceCursor(List<RecordServiceColumnHandle> cols, Records records) {
      this.cols = Preconditions.checkNotNull(cols);
      this.records = Preconditions.checkNotNull(records);
      this.rsCols = records.getSchema().cols;
      if (records.getSchema().nestedCols != null) {
        this.rsCols = records.getSchema().nestedCols;
      }

      booleanValues = new boolean[rsCols.size()];
      longValues = new long[rsCols.size()];
      doubleValues = new double[rsCols.size()];
      decimalValues = new Decimal[rsCols.size()];
      byteArrayValues = new ByteArray[rsCols.size()];
      complexValues = new Block[rsCols.size()];

      // Extract the type ids of each column.
      types = new Schema.Type[rsCols.size()];
      for (int i = 0; i < types.length; i++) {
        types[i] = rsCols.get(i).type.typeId;
      }
    }

    @Override
    public Type getType(int field) { return cols.get(field).columnType(); }

    @Override
    public boolean advanceNextPosition() {
      try {
        if (!records.hasNext()) {
          close();
          return false;
        }
        record = records.next();
        populateRecord();
        return true;
      } catch (RecordServiceException e) {
        exceptionHandler(new TrinoException(GENERIC_INTERNAL_ERROR,
            "Unable to fetch records.\n" + e.getMessage(), e));
      } catch (IOException e) {
        exceptionHandler(new TrinoException(GENERIC_INTERNAL_ERROR,
            "Unable to fetch records.\n" + e.getMessage(), e));
      }
      // Should never be reached in practice, but necessary to appease the Java gods.
      return false;
    }

    @Override
    public boolean isNull(int field) {
      // Records are non-nullable so always return false for them.
      if (types[field] == Schema.Type.RECORD) return false;
      return record.isNull(field);
    }

    @Override
    public boolean getBoolean(int field) {
      return booleanValues[field];
    }

    @Override
    public long getLong(int field) {
      return longValues[field];
    }

    @Override
    public double getDouble(int field) {
      switch (types[field]) {
        case FLOAT:
        case DOUBLE:
          return doubleValues[field];
        case DECIMAL:
          return decimalValues[field].toBigDecimal().doubleValue();
        default:
          exceptionHandler(new TrinoException(GENERIC_INTERNAL_ERROR,
              "Invalid type for getDouble(): " + types[field]));
          // Not reachable in practice, but needed to appease the compiler.
          return -1.0;
      }
    }

    @Override
    public Slice getSlice(int field) {
      switch (types[field]) {
        case CHAR:
        case STRING:
        case VARCHAR:
          return RecordServiceUtil.toSlice(byteArrayValues[field]);
        case DECIMAL:
          return Slices.wrappedBuffer(decimalValues[field].toBigInteger().toByteArray()); // TODO
        default:
          exceptionHandler(new TrinoException(GENERIC_INTERNAL_ERROR,
              "Invalid type for getSlice(): " + types[field]));
          // Not reachable in practice, but needed to appease the compiler.
          return null;
      }
    }

    @Override
    public Object getObject(int field) {
      return complexValues[field];
    }

    @Override
    public void close() {
      records.close();
      record = null;
    }

    @Override
    public long getReadTimeNanos() {
      return 0;
    }

    @Override
    public long getCompletedBytes() {
      return 0;
    }

    // Fills values with the current record.
    private void populateRecord() {
      for (int field = 0; field < cols.size(); field++) {
        if (types[field] == Schema.Type.RECORD) {
          // Records are non-nullable
          complexValues[field] = RecordServiceUtil.convertRecord(record.nextStruct(field),
              cols.get(field).columnType(), rsCols.get(field), null);
          continue;
        }
        if (record.isNull(field)) continue;
        switch (types[field]) {
          case BOOLEAN:
            booleanValues[field] = record.nextBoolean(field);
            break;
          case TINYINT:
            longValues[field] = record.nextByte(field);
            break;
          case SMALLINT:
            longValues[field] = record.nextShort(field);
            break;
          case DATE:
          case INT:
            longValues[field] = record.nextInt(field);
            break;
          case BIGINT:
            longValues[field] = record.nextLong(field);
            break;
          case FLOAT:
            doubleValues[field] = record.nextFloat(field);
            break;
          case DOUBLE:
            doubleValues[field] = record.nextDouble(field);
            break;
          case DECIMAL:
            decimalValues[field] = record.nextDecimal(field);
            if (((DecimalType)cols.get(field).columnType()).isShort()) {
              // If the decimal is small, trino processes it as a long so set that.
              longValues[field] = decimalValues[field].toBigInteger().longValue();
            }
            break;
          case TIMESTAMP_NANOS:
            longValues[field] =
                record.nextTimestampNanos(field).getMillisSinceEpoch() * RecordServiceUtil.TIMESTAMP_SCALE_FACTOR;
            break;
          case CHAR:
          case STRING:
          case VARCHAR:
            byteArrayValues[field] = record.nextByteArray(field);
            break;

          case ARRAY:
            complexValues[field] = RecordServiceUtil.convertArray(record.getArray(field),
                cols.get(field).columnType(),
                rsCols.get(field), null);
            break;

          case MAP:
            complexValues[field] = RecordServiceUtil.convertMap(record.getMap(field),
                cols.get(field).columnType(), rsCols.get(field), null);
            break;

          case RECORD:
            // Should not have gotten here (Handled above)
            Preconditions.checkState(false);
            break;

          default:
            throw new RuntimeException("Unsupported type: " + types[field]);
        }
      }
    }

    // We aggressively close the connection on any run-time exception and then propagate
    // the original exception.
    private void exceptionHandler(RuntimeException originalException) {
      try {
        close();
      } catch (Exception innerException) {
        originalException.addSuppressed(innerException);
      }
      throw originalException;
    }
  }
}
