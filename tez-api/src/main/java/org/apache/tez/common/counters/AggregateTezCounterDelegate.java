package org.apache.tez.common.counters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AggregateTezCounterDelegate<T extends TezCounter> extends AbstractCounter implements AggregateTezCounter {

  private final T child;
  private long min = Long.MAX_VALUE;
  private long max = Long.MIN_VALUE;

  public AggregateTezCounterDelegate(T child) {
    this.child = child;
  }
  
  @Override
  public String getName() {
    return child.getName(); // this is a pass-through
  }

  @Override
  public String getDisplayName() {
    return child.getDisplayName();
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
    throw new UnsupportedOperationException("Cannot increment an aggregate counter");
  }
  
  /* (non-Javadoc)
   * @see org.apache.tez.common.counters.AggregateTezCounter#aggregate(org.apache.tez.common.counters.TezCounter)
   */
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
    throw new UnsupportedOperationException("Cannot deserialize an aggregate counter");
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    throw new UnsupportedOperationException("Cannot deserialize an aggregate counter");
  }

  @Override
  public long getMin() {
    return min;
  }

  @Override
  public long getMax() {
    return max;
  }
}
