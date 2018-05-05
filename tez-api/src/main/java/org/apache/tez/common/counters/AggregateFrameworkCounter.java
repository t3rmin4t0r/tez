package org.apache.tez.common.counters;

import org.apache.tez.common.counters.FrameworkCounterGroup.FrameworkCounter;

@SuppressWarnings("rawtypes")
public class AggregateFrameworkCounter<T extends Enum<T>> extends FrameworkCounter implements AggregateTezCounter  {
  
  private long min = Long.MAX_VALUE;
  private long max = Long.MIN_VALUE;

  @SuppressWarnings("unchecked")
  public AggregateFrameworkCounter(Enum<T> ref, String groupName) {
    super(ref, groupName);
  }

  @Override
  public void increment(long incr) {
    throw new IllegalArgumentException("Cannot increment an aggregate counter directly");
  }
  
  @Override
  public void aggregate(TezCounter other) {
    final long val = other.getValue();
    super.increment(val);
    if (min == Long.MAX_VALUE) {
      this.min = this.max = val;
      return;
    }
    // these are only called from the AM within a lock
    this.min = Math.min(this.min, val);
    this.max = Math.max(this.max, val);
  }

  @Override
  public long getMin() {
    return min;
  }

  @Override
  public long getMax() {
    return max;
  }
  
  @SuppressWarnings("unchecked")
  public FrameworkCounter<T> asFrameworkCounter() {
    return ((FrameworkCounter<T>)this);
  }

}
