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
    final long othermax;
    final long othermin;
    if (other instanceof AggregateTezCounter) {
      othermax = ((AggregateTezCounter) other).getMax();
      othermin = ((AggregateTezCounter) other).getMin();
    } else {
      othermin = othermax = val;
    }
    super.increment(val);
    if (this.min == Long.MAX_VALUE) {
      this.min = othermin;
      this.max = othermax;
      return;
    }
    this.min = Math.min(this.min, othermin);
    this.max = Math.max(this.max, othermax);
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
