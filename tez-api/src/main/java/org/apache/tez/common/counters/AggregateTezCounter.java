package org.apache.tez.common.counters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AggregateTezCounter<T extends TezCounter> extends AbstractCounter {

  private final T child;
  private long min = Long.MAX_VALUE;
  private long max = Long.MIN_VALUE;

  public AggregateTezCounter(T child) {
    this.child = child;
  }
  
  @Override
  public String getName() {
    return String.format("%s_aggregate", child.getName());
  }

  @Override
  public String getDisplayName() {
    return String.format("Aggregate(%s)", child.getDisplayName());
  }

  @Override
  public long getValue() {
    return child.getValue();
  }

  @Override
  public void setValue(long value) {
    this.child.setValue(value);
  }

  @Override
  public void increment(long incr) {
    throw new IllegalArgumentException("Cannot increment an aggregate counter");
  }
  
  @Override 
  public synchronized void aggregate(TezCounter other) {
    final long val = other.getValue();
    this.child.increment(val);
    if (this.min == Long.MAX_VALUE) {
      this.min = this.max = other.getValue();
      return;
    }
    this.min = Math.min(this.min, val);
    this.max = Math.max(this.max, val);
  }

  @Override
  public TezCounter getUnderlyingCounter() {
    return this.child;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    throw new IllegalArgumentException("Cannot deserialize an aggregate counter");
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    this.child.write(arg0);
  }

  public synchronized long getMin() {
    return min;
  }
  
  public synchronized long getMax() {
    return max;
  }
}
