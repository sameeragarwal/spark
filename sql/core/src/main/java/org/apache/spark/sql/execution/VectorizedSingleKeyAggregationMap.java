package org.apache.spark.sql.execution;

import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;

import java.util.Arrays;

import static org.apache.spark.sql.types.DataTypes.LongType;

public class VectorizedSingleKeyAggregationMap {
  public ColumnarBatch batch;
  private int[] buckets;
  private int numBuckets = 65536 * 4;
  private int numRows = 0;
  private int MAX_STEPS = 3;

  public VectorizedSingleKeyAggregationMap() {
    StructType schema = new StructType()
        .add("key", LongType)
        .add("value", LongType);
    batch = ColumnarBatch.allocate(schema, ColumnarBatch.DEFAULT_MEMORY_MODE, numBuckets/4);
    buckets = new int[numBuckets];
    Arrays.fill(buckets, -1);
  }

  private int findOrInsert(long key1) {
    long h = hash(key1);
    int step = 1;
    int idx = (int) h & (numBuckets - 1);

    while (step < MAX_STEPS) {
      if (buckets[idx] == -1) {
        batch.column(0).putLong(numRows, key1);
        batch.column(1).putLong(numRows, 0);
        buckets[idx] = numRows++;
        // batch.setNumRows(numRows);
        return idx;
      } else {
        if (equals(idx, key1)) {
          return idx;
        }
      }

      idx = (idx + step) & (numBuckets - 1);
      //step += 1;
    }

    // Didn't find it
    return -1;
  }

  private int find(long key1) {
    long h = hash(key1);
    int step = 1;
    int idx = (int) h & (numBuckets - 1);

    while (step < MAX_STEPS) {
      if (buckets[idx] == -1) {
        return -1;
      } else {
        if (equals(idx, key1)) {
          return idx;
        }
      }

      idx = (idx + step) & (numBuckets - 1);
      //step += 1;
    }

    // Didn't find it
    return -1;
  }

  public void incrementCount(long key1) {
    int idx = findOrInsert(key1);
    if (idx == -1) {
      return;
    }
    batch.column(1).putLong(buckets[idx], batch.column(1).getLong(buckets[idx]) + 1);
    //return batch.getRow(buckets[idx]);
    //return "";
  }

  public long get(long key) {
    int idx = find(key);
    if (idx == -1) {
      return -1L;
    }
    return batch.column(1).getLong(buckets[idx]);
  }

  private long hash(long key) {
    return key;
  }

  private boolean equals(int idx, long key1) {
    return batch.column(0).getLong(buckets[idx]) == key1;
  }
}
