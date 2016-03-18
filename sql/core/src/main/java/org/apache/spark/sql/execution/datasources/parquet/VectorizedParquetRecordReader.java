/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;

import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;

/**
 * A specialized RecordReader that reads into InternalRows or ColumnarBatches directly using the
 * Parquet column APIs. This is somewhat based on parquet-mr's ColumnReader.
 *
 * TODO: handle complex types, decimal requiring more than 8 bytes, INT96. Schema mismatch.
 * All of these can be handled efficiently and easily with codegen.
 *
 * This class can either return InternalRows or ColumnarBatches. With whole stage codegen
 * enabled, this class returns ColumnarBatches which offers significant performance gains.
 * TODO: make this always return ColumnarBatches.
 */
public class VectorizedParquetRecordReader extends SpecificParquetRecordReaderBase<Object> {
  /**
   * Batch of rows that we assemble and the current index we've returned. Every time this
   * batch is used up (batchIdx == numBatched), we populated the batch.
   */
  private int batchIdx = 0;
  private int numBatched = 0;

  /**
   * For each request column, the reader to read this column.
   */
  private VectorizedColumnReader[] columnReaders;

  /**
   * The number of rows that have been returned.
   */
  private long rowsReturned;

  /**
   * The number of rows that have been reading, including the current in flight row group.
   */
  private long totalCountLoadedSoFar = 0;

  /**
   * columnBatch object that is used for batch decoding. This is created on first use and triggers
   * batched decoding. It is not valid to interleave calls to the batched interface with the row
   * by row RecordReader APIs.
   * This is only enabled with additional flags for development. This is still a work in progress
   * and currently unsupported cases will fail with potentially difficult to diagnose errors.
   * This should be only turned on for development to work on this feature.
   *
   * When this is set, the code will branch early on in the RecordReader APIs. There is no shared
   * code between the path that uses the MR decoders and the vectorized ones.
   *
   * TODOs:
   *  - Implement v2 page formats (just make sure we create the correct decoders).
   */
  private ColumnarBatch columnarBatch;

  /**
   * If true, this class returns batches instead of rows.
   */
  private boolean returnColumnarBatch;

  /**
   * The default config on whether columnarBatch should be offheap.
   */
  private static final MemoryMode DEFAULT_MEMORY_MODE = MemoryMode.ON_HEAP;

  /**
   * Tries to initialize the reader for this split. Returns true if this reader supports reading
   * this split and false otherwise.
   */
  public boolean tryInitialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    try {
      initialize(inputSplit, taskAttemptContext);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Implementation of RecordReader API.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    super.initialize(inputSplit, taskAttemptContext);
    initializeInternal();
  }

  /**
   * Utility API that will read all the data in path. This circumvents the need to create Hadoop
   * objects to use this class. `columns` can contain the list of columns to project.
   */
  @Override
  public void initialize(String path, List<String> columns) throws IOException {
    super.initialize(path, columns);
    initializeInternal();
  }

  @Override
  public void close() throws IOException {
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
    super.close();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    resultBatch();

    if (returnColumnarBatch) return nextBatch();

    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    ++batchIdx;
    return true;
  }

  @Override
  public Object getCurrentValue() throws IOException, InterruptedException {
    if (returnColumnarBatch) return columnarBatch;
    return columnarBatch.getRow(batchIdx - 1);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float) rowsReturned / totalRowCount;
  }

  /**
   * Returns the ColumnarBatch object that will be used for all rows returned by this reader.
   * This object is reused. Calling this enables the vectorized reader. This should be called
   * before any calls to nextKeyValue/nextBatch.
   */
  public ColumnarBatch resultBatch() {
    return resultBatch(DEFAULT_MEMORY_MODE);
  }

  public ColumnarBatch resultBatch(MemoryMode memMode) {
    if (columnarBatch == null) {
      columnarBatch = ColumnarBatch.allocate(sparkSchema, memMode);
    }
    return columnarBatch;
  }

  /**
   * Can be called before any rows are returned to enable returning columnar batches directly.
   */
  public void enableReturningBatches() {
    returnColumnarBatch = true;
  }

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  public boolean nextBatch() throws IOException {
    columnarBatch.reset();
    if (rowsReturned >= totalRowCount) return false;
    checkEndOfRowGroup();

    int num = (int) Math.min((long) columnarBatch.capacity(), totalCountLoadedSoFar - rowsReturned);
    for (int i = 0; i < columnReaders.length; ++i) {
      columnReaders[i].readBatch(num, columnarBatch.column(i));
    }
    rowsReturned += num;
    columnarBatch.setNumRows(num);
    numBatched = num;
    batchIdx = 0;
    return true;
  }

  private void initializeInternal() throws IOException {
    /**
     * Check that the requested schema is supported.
     */
    OriginalType[] originalTypes = new OriginalType[requestedSchema.getFieldCount()];
    for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
      Type t = requestedSchema.getFields().get(i);
      if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
        throw new IOException("Complex types not supported.");
      }
      PrimitiveType primitiveType = t.asPrimitiveType();

      originalTypes[i] = t.getOriginalType();

      // TODO: Be extremely cautious in what is supported. Expand this.
      if (originalTypes[i] != null && originalTypes[i] != OriginalType.DECIMAL &&
          originalTypes[i] != OriginalType.UTF8 && originalTypes[i] != OriginalType.DATE &&
          originalTypes[i] != OriginalType.INT_8 && originalTypes[i] != OriginalType.INT_16) {
        throw new IOException("Unsupported type: " + t);
      }
      if (originalTypes[i] == OriginalType.DECIMAL &&
          primitiveType.getDecimalMetadata().getPrecision() > Decimal.MAX_LONG_DIGITS()) {
        throw new IOException("Decimal with high precision is not supported.");
      }
      if (primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
        throw new IOException("Int96 not supported.");
      }
      ColumnDescriptor fd = fileSchema.getColumnDescription(requestedSchema.getPaths().get(i));
      if (!fd.equals(requestedSchema.getColumns().get(i))) {
        throw new IOException("Schema evolution not supported.");
      }
    }
  }

  /**
   * Decoder to return values from a single column.
   */
  private class VectorizedColumnReader {
    /**
     * Total number of values read.
     */
    private long valuesRead;

    /**
     * value that indicates the end of the current page. That is,
     * if valuesRead == endOfPageValueCount, we are at the end of the page.
     */
    private long endOfPageValueCount;

    /**
     * The dictionary, if this column has dictionary encoding.
     */
    private final Dictionary dictionary;

    /**
     * If true, the current page is dictionary encoded.
     */
    private boolean useDictionary;

    /**
     * Maximum definition level for this column.
     */
    private final int maxDefLevel;

    /**
     * Repetition/Definition/Value readers.
     */
    private IntIterator repetitionLevelColumn;
    private IntIterator definitionLevelColumn;
    private ValuesReader dataColumn;

    // Only set if vectorized decoding is true. This is used instead of the row by row decoding
    // with `definitionLevelColumn`.
    private VectorizedRleValuesReader defColumn;

    /**
     * Total number of values in this column (in this row group).
     */
    private final long totalValueCount;

    /**
     * Total values in the current page.
     */
    private int pageValueCount;

    private final PageReader pageReader;
    private final ColumnDescriptor descriptor;

    public VectorizedColumnReader(ColumnDescriptor descriptor, PageReader pageReader)
        throws IOException {
      this.descriptor = descriptor;
      this.pageReader = pageReader;
      this.maxDefLevel = descriptor.getMaxDefinitionLevel();

      DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
      if (dictionaryPage != null) {
        try {
          this.dictionary = dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
          this.useDictionary = true;
        } catch (IOException e) {
          throw new IOException("could not decode the dictionary for " + descriptor, e);
        }
      } else {
        this.dictionary = null;
        this.useDictionary = false;
      }
      this.totalValueCount = pageReader.getTotalValueCount();
      if (totalValueCount == 0) {
        throw new IOException("totalValueCount == 0");
      }
    }

    /**
     * TODO: Hoist the useDictionary branch to decode*Batch and make the batch page aligned.
     */
    public boolean nextBoolean() {
      if (!useDictionary) {
        return dataColumn.readBoolean();
      } else {
        return dictionary.decodeToBoolean(dataColumn.readValueDictionaryId());
      }
    }

    public int nextInt() {
      if (!useDictionary) {
        return dataColumn.readInteger();
      } else {
        return dictionary.decodeToInt(dataColumn.readValueDictionaryId());
      }
    }

    public long nextLong() {
      if (!useDictionary) {
        return dataColumn.readLong();
      } else {
        return dictionary.decodeToLong(dataColumn.readValueDictionaryId());
      }
    }

    public float nextFloat() {
      if (!useDictionary) {
        return dataColumn.readFloat();
      } else {
        return dictionary.decodeToFloat(dataColumn.readValueDictionaryId());
      }
    }

    public double nextDouble() {
      if (!useDictionary) {
        return dataColumn.readDouble();
      } else {
        return dictionary.decodeToDouble(dataColumn.readValueDictionaryId());
      }
    }

    public Binary nextBinary() {
      if (!useDictionary) {
        return dataColumn.readBytes();
      } else {
        return dictionary.decodeToBinary(dataColumn.readValueDictionaryId());
      }
    }

    /**
     * Advances to the next value. Returns true if the value is non-null.
     */
    private boolean next() throws IOException {
      if (valuesRead >= endOfPageValueCount) {
        if (valuesRead >= totalValueCount) {
          // How do we get here? Throw end of stream exception?
          return false;
        }
        readPage();
      }
      ++valuesRead;
      // TODO: Don't read for flat schemas
      //repetitionLevel = repetitionLevelColumn.nextInt();
      return definitionLevelColumn.nextInt() == maxDefLevel;
    }

    /**
     * Reads `total` values from this columnReader into column.
     */
    private void readBatch(int total, ColumnVector column) throws IOException {
      int rowId = 0;
      while (total > 0) {
        // Compute the number of values we want to read in this page.
        int leftInPage = (int)(endOfPageValueCount - valuesRead);
        if (leftInPage == 0) {
          readPage();
          leftInPage = (int)(endOfPageValueCount - valuesRead);
        }
        int num = Math.min(total, leftInPage);
        if (useDictionary) {
          // Read and decode dictionary ids.
          ColumnVector dictionaryIds = column.reserveDictionaryIds(total);
          defColumn.readIntegers(
              num, dictionaryIds, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
          decodeDictionaryIds(rowId, num, column, dictionaryIds);
        } else {
          column.setDictionary(null);
          switch (descriptor.getType()) {
            case BOOLEAN:
              readBooleanBatch(rowId, num, column);
              break;
            case INT32:
              readIntBatch(rowId, num, column);
              break;
            case INT64:
              readLongBatch(rowId, num, column);
              break;
            case FLOAT:
              readFloatBatch(rowId, num, column);
              break;
            case DOUBLE:
              readDoubleBatch(rowId, num, column);
              break;
            case BINARY:
              readBinaryBatch(rowId, num, column);
              break;
            case FIXED_LEN_BYTE_ARRAY:
              readFixedLenByteArrayBatch(rowId, num, column, descriptor.getTypeLength());
              break;
            default:
              throw new IOException("Unsupported type: " + descriptor.getType());
          }
        }

        valuesRead += num;
        rowId += num;
        total -= num;
      }
    }

    /**
     * Reads `num` values into column, decoding the values from `dictionaryIds` and `dictionary`.
     */
    private void decodeDictionaryIds(int rowId, int num, ColumnVector column,
                                     ColumnVector dictionaryIds) {
      switch (descriptor.getType()) {
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
        case BINARY:
          column.setDictionary(dictionary);
          break;

        case FIXED_LEN_BYTE_ARRAY:
          // DecimalType written in the legacy mode
          if (DecimalType.is32BitDecimalType(column.dataType())) {
            for (int i = rowId; i < rowId + num; ++i) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getInt(i));
              column.putInt(i, (int) CatalystRowConverter.binaryToUnscaledLong(v));
            }
          } else if (DecimalType.is64BitDecimalType(column.dataType())) {
            for (int i = rowId; i < rowId + num; ++i) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getInt(i));
              column.putLong(i, CatalystRowConverter.binaryToUnscaledLong(v));
            }
          } else {
            throw new NotImplementedException();
          }
          break;

        default:
          throw new NotImplementedException("Unsupported type: " + descriptor.getType());
      }
    }

    /**
     * For all the read*Batch functions, reads `num` values from this columnReader into column. It
     * is guaranteed that num is smaller than the number of values left in the current page.
     */

    private void readBooleanBatch(int rowId, int num, ColumnVector column) throws IOException {
      assert(column.dataType() == DataTypes.BooleanType);
      defColumn.readBooleans(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    }

    private void readIntBatch(int rowId, int num, ColumnVector column) throws IOException {
      // This is where we implement support for the valid type conversions.
      // TODO: implement remaining type conversions
      if (column.dataType() == DataTypes.IntegerType || column.dataType() == DataTypes.DateType ||
        DecimalType.is32BitDecimalType(column.dataType())) {
        defColumn.readIntegers(
            num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
      } else if (column.dataType() == DataTypes.ByteType) {
        defColumn.readBytes(
            num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
      } else if (column.dataType() == DataTypes.ShortType) {
        defColumn.readShorts(
            num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
      } else {
        throw new NotImplementedException("Unimplemented type: " + column.dataType());
      }
    }

    private void readLongBatch(int rowId, int num, ColumnVector column) throws IOException {
      // This is where we implement support for the valid type conversions.
      if (column.dataType() == DataTypes.LongType ||
          DecimalType.is64BitDecimalType(column.dataType())) {
        defColumn.readLongs(
            num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
      } else {
        throw new UnsupportedOperationException("Unsupported conversion to: " + column.dataType());
      }
    }

    private void readFloatBatch(int rowId, int num, ColumnVector column) throws IOException {
      // This is where we implement support for the valid type conversions.
      // TODO: support implicit cast to double?
      if (column.dataType() == DataTypes.FloatType) {
        defColumn.readFloats(
            num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
      } else {
        throw new UnsupportedOperationException("Unsupported conversion to: " + column.dataType());
      }
    }

    private void readDoubleBatch(int rowId, int num, ColumnVector column) throws IOException {
      // This is where we implement support for the valid type conversions.
      // TODO: implement remaining type conversions
      if (column.dataType() == DataTypes.DoubleType) {
        defColumn.readDoubles(
            num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
      } else {
        throw new NotImplementedException("Unimplemented type: " + column.dataType());
      }
    }

    private void readBinaryBatch(int rowId, int num, ColumnVector column) throws IOException {
      // This is where we implement support for the valid type conversions.
      // TODO: implement remaining type conversions
      if (column.isArray()) {
        defColumn.readBinarys(
            num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
      } else {
        throw new NotImplementedException("Unimplemented type: " + column.dataType());
      }
    }

    private void readFixedLenByteArrayBatch(int rowId, int num,
                                            ColumnVector column, int arrayLen) throws IOException {
      VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
      // This is where we implement support for the valid type conversions.
      // TODO: implement remaining type conversions
      if (DecimalType.is32BitDecimalType(column.dataType())) {
        for (int i = 0; i < num; i++) {
          if (defColumn.readInteger() == maxDefLevel) {
            column.putInt(rowId + i,
              (int) CatalystRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
          } else {
            column.putNull(rowId + i);
          }
        }
      } else if (DecimalType.is64BitDecimalType(column.dataType())) {
        for (int i = 0; i < num; i++) {
          if (defColumn.readInteger() == maxDefLevel) {
            column.putLong(rowId + i,
                CatalystRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
          } else {
            column.putNull(rowId + i);
          }
        }
      } else {
        throw new NotImplementedException("Unimplemented type: " + column.dataType());
      }
    }

    private void readPage() throws IOException {
      DataPage page = pageReader.readPage();
      // TODO: Why is this a visitor?
      page.accept(new DataPage.Visitor<Void>() {
        @Override
        public Void visit(DataPageV1 dataPageV1) {
          try {
            readPageV1(dataPageV1);
            return null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public Void visit(DataPageV2 dataPageV2) {
          try {
            readPageV2(dataPageV2);
            return null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
    }

    private void initDataReader(Encoding dataEncoding, byte[] bytes, int offset) throws IOException {
      this.endOfPageValueCount = valuesRead + pageValueCount;
      if (dataEncoding.usesDictionary()) {
        this.dataColumn = null;
        if (dictionary == null) {
          throw new IOException(
              "could not read page in col " + descriptor +
                  " as the dictionary was missing for encoding " + dataEncoding);
        }
        @SuppressWarnings("deprecation")
        Encoding plainDict = Encoding.PLAIN_DICTIONARY; // var to allow warning suppression
        if (dataEncoding != plainDict && dataEncoding != Encoding.RLE_DICTIONARY) {
          throw new NotImplementedException("Unsupported encoding: " + dataEncoding);
        }
        this.dataColumn = new VectorizedRleValuesReader();
        this.useDictionary = true;
      } else {
        if (dataEncoding != Encoding.PLAIN) {
          throw new NotImplementedException("Unsupported encoding: " + dataEncoding);
        }
        this.dataColumn = new VectorizedPlainValuesReader();
        this.useDictionary = false;
      }

      try {
        dataColumn.initFromPage(pageValueCount, bytes, offset);
      } catch (IOException e) {
        throw new IOException("could not read page in col " + descriptor, e);
      }
    }

    private void readPageV1(DataPageV1 page) throws IOException {
      this.pageValueCount = page.getValueCount();
      ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
      ValuesReader dlReader;

      // Initialize the decoders.
      if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
        throw new NotImplementedException("Unsupported encoding: " + page.getDlEncoding());
      }
      int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
      this.defColumn = new VectorizedRleValuesReader(bitWidth);
      dlReader = this.defColumn;
      this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
      this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
      try {
        byte[] bytes = page.getBytes().toByteArray();
        rlReader.initFromPage(pageValueCount, bytes, 0);
        int next = rlReader.getNextOffset();
        dlReader.initFromPage(pageValueCount, bytes, next);
        next = dlReader.getNextOffset();
        initDataReader(page.getValueEncoding(), bytes, next);
      } catch (IOException e) {
        throw new IOException("could not read page " + page + " in col " + descriptor, e);
      }
    }

    private void readPageV2(DataPageV2 page) throws IOException {
      this.pageValueCount = page.getValueCount();
      this.repetitionLevelColumn = createRLEIterator(descriptor.getMaxRepetitionLevel(),
          page.getRepetitionLevels(), descriptor);

      int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
      this.defColumn = new VectorizedRleValuesReader(bitWidth);
      this.definitionLevelColumn = new ValuesReaderIntIterator(this.defColumn);
      this.defColumn.initFromBuffer(
          this.pageValueCount, page.getDefinitionLevels().toByteArray());
      try {
        initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0);
      } catch (IOException e) {
        throw new IOException("could not read page " + page + " in col " + descriptor, e);
      }
    }
  }

  private void checkEndOfRowGroup() throws IOException {
    if (rowsReturned != totalCountLoadedSoFar) return;
    PageReadStore pages = reader.readNextRowGroup();
    if (pages == null) {
      throw new IOException("expecting more rows but reached last block. Read "
          + rowsReturned + " out of " + totalRowCount);
    }
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    columnReaders = new VectorizedColumnReader[columns.size()];
    for (int i = 0; i < columns.size(); ++i) {
      columnReaders[i] = new VectorizedColumnReader(columns.get(i),
          pages.getPageReader(columns.get(i)));
    }
    totalCountLoadedSoFar += pages.getRowCount();
  }
}
