package org.apache.tez.common.counters;

public interface AggregateTezCounter {

  public abstract void aggregate(TezCounter other);

  public abstract long getMin();

  public abstract long getMax();

}