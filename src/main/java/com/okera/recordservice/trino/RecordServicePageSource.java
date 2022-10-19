package com.okera.recordservice.trino;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

import com.okera.recordservice.core.Decimal;
import com.okera.recordservice.core.FetchResult;
import com.okera.recordservice.core.FetchResult.RecordFormat;
import com.okera.recordservice.core.RecordServiceException;
import com.okera.recordservice.core.Records;
import com.okera.recordservice.core.Schema;
import com.okera.recordservice.util.Preconditions;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class RecordServicePageSource implements ConnectorPageSource {
  private static final Logger LOG = Logger.get(RecordServicePageSource.class);

  public static final int PAGE_SIZE = 3000;

  private final ConnectorSession session;
  private final List<RecordServiceColumnHandle> columns;
  private final RecordServiceSplit split;
  private final PageBuilderStatus pageBuilderStatus;
  private Records records;
  private boolean closed;

  public RecordServicePageSource(ConnectorSession session, RecordServiceSplit split,
      List<RecordServiceColumnHandle> columns) {
    this.session = Preconditions.checkNotNull(session);
    this.columns = Preconditions.checkNotNull(columns);
    this.split = Preconditions.checkNotNull(split);
    this.pageBuilderStatus = new PageBuilderStatus(1024 * 1024);
  }

  @Override
  public boolean isFinished() {
    return closed;
  }

  @Override
  public void close() {
    closed = true;
    if (records != null) {
      LOG.debug("Closing page source.");
      records.close();
      records = null;
    }
  }

  @Override
  public Page getNextPage() {
    try {
      if (closed) return null;

      if (records == null) {
        // Create on first fetch
        this.records = RecordServiceUtil.createRecords(session, split,
            FetchResult.RecordFormat.Columnar);
        // count(*) is a special case where the columns metadata is empty.
        if (this.records.getSchema().isCountStar) {
          Preconditions.checkState(this.columns.isEmpty());
          this.columns.add(new RecordServiceColumnHandle(
              "count(*)", BigintType.BIGINT, null, 0));
        }
      }

      if (!records.hasNext()) {
        close();
        return null;
      }

      // Convert the batch to a page.
      final Records.Record record = records.next();
      final int numValues = Math.min(records.numRecordsBuffered() + 1, PAGE_SIZE);
      final Block[] blocks = new Block[record.getSchema().cols.size()];

      if (record.getSchema().isCountStar) {
        // Handle count(*)
        Preconditions.checkState(blocks.length == 1);
        blocks[0] = new LongArrayBlock(
            numValues, Optional.of(new boolean[numValues]), new long[numValues]);
        records.advance(numValues - 1);
        return new Page(blocks);
      }

      for (int col = 0; col < record.getSchema().cols.size(); col++) {
        blocks[col] = getBlock(record, col, numValues);
      }

      // All done with this batch, advance
      records.advance(numValues - 1);
      return new Page(blocks);
    } catch (IOException | RecordServiceException e) {
      exceptionHandler(new TrinoException(GENERIC_INTERNAL_ERROR,
          "Unable to get next page. records.\n" + e.getMessage(), e));
      close();
      return null;
    }
  }

  /**
   * Construct one column. Note that this is intentionally written this way
   * to make the inner loop as tight as possible.
   */
  private Block getBlock(Records.Record record, int col, int numValues) {
    final Schema.TypeDesc type = record.getSchema().cols.get(col).type;
    final Type trinoType = columns.get(col).columnType();
    final boolean[] nulls = new boolean[numValues];

    // Set up the underlying buffer state
    boolean containsNulls = record.containsNulls(col);
    final byte[] srcArray = record.underlyingArray(col);
    long srcOffset = record.underlyingArrayOffset(col);

    // Process nulls
    if (containsNulls) {
      // Older servers didn't set this, this is cheap to recompute and maintain
      containsNulls = false;
      int nullIdx = record.recordIdx();
      final byte[] nullArray = record.nullsArray(col);
      for (int i = 0; i < numValues; ++i) {
        nulls[i] = nullArray[nullIdx++] != 0;
        containsNulls |= nulls[i];
      }
    }

    switch (type.typeId) {
      case CHAR: {
        final int[] offsets = new int[numValues + 1];
        final Slice slice = Slices.wrappedBuffer(srcArray, 0, srcArray.length);
        int totalLen = (int)record.underlyingLenOffset(col);
        offsets[0] = totalLen;
        for (int i = 0; i < numValues; ++i) {
          if (containsNulls && nulls[i]) {
            offsets[i + 1] = totalLen;
            continue;
          }
          offsets[i + 1] = totalLen + type.len;
          totalLen += type.len;
        }
        record.setUnderlyingLenOffset(col, totalLen);
        return new VariableWidthBlock(
            numValues, slice, offsets,
            containsNulls ? Optional.of(nulls) : Optional.empty());
      }

      case VARCHAR:
      case BINARY:
      case STRING: {
        if (records.getFormat() == RecordFormat.Columnar) {
          final BlockBuilder output = trinoType.createBlockBuilder(
              pageBuilderStatus.createBlockBuilderStatus(), numValues);
          for (int i = 0; i < numValues; ++i) {
            if (containsNulls && nulls[i]) {
              output.appendNull();
              continue;
            }
            trinoType.writeSlice(output,
                RecordServiceUtil.toSlice(record.nextByteArray(col)));
          }
          return output.build();
        } else {
          Preconditions.checkState(records.getFormat() == RecordFormat.Columnar2);
          final int[] offsets = new int[numValues + 1];
          final int sliceOffset = records.totalBatchSize() * 4;
          final Slice slice = Slices.wrappedBuffer(
              srcArray, sliceOffset, srcArray.length - sliceOffset);
          int totalLen = (int)record.underlyingLenOffset(col);
          offsets[0] = totalLen;
          for (int i = 0; i < numValues; ++i) {
            int len = 0;
            if (!containsNulls || !nulls[i]) {
              len = Records.unsafe.getInt(srcArray, srcOffset);
            }
            offsets[i + 1] = totalLen + len;
            srcOffset += 4;
            totalLen += len;
          }
          record.setUnderlyingArrayOffset(col, srcOffset);
          record.setUnderlyingLenOffset(col, totalLen);
          return new VariableWidthBlock(
              numValues, slice, offsets,
              containsNulls ? Optional.of(nulls) : Optional.empty());
        }
      }

      case BOOLEAN: {
        final byte[] v = new byte[numValues];
        for (int i = 0; i < numValues; ++i) {
          if (containsNulls && nulls[i]) continue;
          v[i] = Records.unsafe.getByte(srcArray, srcOffset);
          srcOffset += 1;
        }
        record.setUnderlyingArrayOffset(col, srcOffset);
        return new ByteArrayBlock(numValues,
            containsNulls ? Optional.of(nulls) : Optional.empty(), v);
      }

      case TINYINT: {
        final int[] v = new int[numValues];
        for (int i = 0; i < numValues; ++i) {
          if (containsNulls && nulls[i]) continue;
          v[i] = Records.unsafe.getByte(srcArray, srcOffset);
          srcOffset += 1;
        }
        record.setUnderlyingArrayOffset(col, srcOffset);
        return new IntArrayBlock(numValues,
            containsNulls ? Optional.of(nulls) : Optional.empty(), v);
      }

      case SMALLINT: {
        final int[] v = new int[numValues];
        for (int i = 0; i < numValues; ++i) {
          if (containsNulls && nulls[i]) continue;
          v[i] = Records.unsafe.getShort(srcArray, srcOffset);
          srcOffset += 2;
        }
        record.setUnderlyingArrayOffset(col, srcOffset);
        return new IntArrayBlock(numValues,
            containsNulls ? Optional.of(nulls) : Optional.empty(), v);
      }

      case INT:
      case DATE: {
        final int[] v = new int[numValues];
        for (int i = 0; i < numValues; ++i) {
          if (containsNulls && nulls[i]) continue;
          v[i] = Records.unsafe.getInt(srcArray, srcOffset);
          srcOffset += 4;
        }
        record.setUnderlyingArrayOffset(col, srcOffset);
        return new IntArrayBlock(numValues,
            containsNulls ? Optional.of(nulls) : Optional.empty(), v);
      }

      case BIGINT: {
        final long[] v = new long[numValues];
        for (int i = 0; i < numValues; ++i) {
          if (containsNulls && nulls[i]) continue;
          v[i] = Records.unsafe.getLong(srcArray, srcOffset);
          srcOffset += 8;
        }
        record.setUnderlyingArrayOffset(col, srcOffset);
        return new LongArrayBlock(numValues,
            containsNulls ? Optional.of(nulls) : Optional.empty(), v);
      }

      case FLOAT: {
        long dstOffset = Records.unsafe.arrayBaseOffset(long[].class);
        final long[] v = new long[numValues];
        for (int i = 0; i < numValues; ++i) {
          if (containsNulls && nulls[i]) {
            // If the float at current offset is null, we still want to advance
            // the destination array as that contains an entry for every row.
            dstOffset += 8;
          } else {
            Records.unsafe.putDouble(v, dstOffset,
                Records.unsafe.getFloat(srcArray, srcOffset));
            srcOffset += 4;
            dstOffset += 8;
          }
        }
        record.setUnderlyingArrayOffset(col, srcOffset);
        return new LongArrayBlock(numValues,
            containsNulls ? Optional.of(nulls) : Optional.empty(), v);
      }

      case DOUBLE: {
        long dstOffset = Records.unsafe.arrayBaseOffset(long[].class);
        final long[] v = new long[numValues];
        if (containsNulls) {
          for (int i = 0; i < numValues; ++i) {
            if (!nulls[i]) {
              Records.unsafe.putDouble(v, dstOffset,
                  Records.unsafe.getDouble(srcArray, srcOffset));
              srcOffset += 8;
            }
            // If the double at current offset is null, we still want to advance
            // the destination array as that contains an entry for every row.
            dstOffset += 8;
          }
          record.setUnderlyingArrayOffset(col, srcOffset);
          return new LongArrayBlock(numValues, Optional.of(nulls), v);
        } else {
          // Copy the entire batch in one go
          Records.unsafe.copyMemory(
              srcArray, srcOffset, v, dstOffset, 8L * numValues);
          srcOffset += 8L * numValues;
          record.setUnderlyingArrayOffset(col, srcOffset);
          return new LongArrayBlock(numValues, Optional.empty(), v);
        }
      }

      case DECIMAL: {
        DecimalType dt = (DecimalType) trinoType;
        if (dt.isShort()) {
          // In this case, the decimal is either 4 or 8 bytes and we can
          // deserialize it by directly going to the underlying int value.
          final long[] v = new long[numValues];
          final int byteSize = Decimal.computeByteSize(
              dt.getPrecision(), dt.getScale());
          if (byteSize == 4) {
            for (int i = 0; i < numValues; ++i) {
              if (containsNulls && nulls[i]) continue;
              v[i] = Records.unsafe.getInt(srcArray, srcOffset);
              srcOffset += 4;
            }
          } else {
            Preconditions.checkState(byteSize == 8);
            for (int i = 0; i < numValues; ++i) {
              if (containsNulls && nulls[i]) continue;
              v[i] = Records.unsafe.getLong(srcArray, srcOffset);
              srcOffset += 8;
            }
          }
          record.setUnderlyingArrayOffset(col, srcOffset);
          return new LongArrayBlock(numValues,
              containsNulls ? Optional.of(nulls) : Optional.empty(), v);
        } else {
          final BlockBuilder output = trinoType.createBlockBuilder(
              pageBuilderStatus.createBlockBuilderStatus(), numValues);
          for (int i = 0; i < numValues; ++i) {
            if (containsNulls && nulls[i]) {
              output.appendNull();
              continue;
            }
            dt.writeObject(output, Int128.valueOf(
                record.nextDecimal(col).toBigInteger()));
          }
          return output.build();
        }
      }

      case TIMESTAMP_NANOS: {
        final long[] v = new long[numValues];
        for (int i = 0; i < numValues; ++i) {
          if (containsNulls && nulls[i]) continue;
          // Starting in Presto 339 this is now in epoch micros
          v[i] = Records.unsafe.getLong(srcArray, srcOffset) * RecordServiceUtil.TIMESTAMP_SCALE_FACTOR;
          // 8 bytes for the micros we're reading and 4 bytes for nanos
          // that we just drop
          srcOffset += 12;
        }
        record.setUnderlyingArrayOffset(col, srcOffset);
        return new LongArrayBlock(numValues,
            containsNulls ? Optional.of(nulls) : Optional.empty(), v);
      }

      default:
        throw new RuntimeException("NYI");
    }
  }

  @Override
  public long getCompletedBytes() {
    return 0;
  }

  @Override
  public long getReadTimeNanos() {
    return 0;
  }

  @Override
  public long getMemoryUsage() {
    return 0;
  }

  // We aggressively close the connection on any run-time exception and then propagate
  // the original exception.
  private void exceptionHandler(RuntimeException e) {
    try {
      close();
    } catch (Exception innerException) {
      e.addSuppressed(innerException);
    }
    throw e;
  }
}
