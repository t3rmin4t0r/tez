package org.apache.tez.common.counters;

public class AggregateTezCounters extends TezCounters {
  
  private static final GroupFactory groupFactory = new GroupFactory();
  
  public AggregateTezCounters() {
    super(groupFactory);
  }
  
  // Mix framework group implementation into CounterGroup interface
  private static class AggregateFrameworkGroupImpl<T extends Enum<T>>
      extends FrameworkCounterGroup<T, TezCounter> implements CounterGroup {

    AggregateFrameworkGroupImpl(Class<T> cls) {
      super(cls);
    }

    @Override
    protected FrameworkCounter<T> newCounter(T key) {
      return new FrameworkCounter<T>(key, getName());
    }

    @Override
    public CounterGroupBase<TezCounter> getUnderlyingGroup() {
      return this;
    }
  }

  // Mix generic group implementation into CounterGroup interface
  // and provide some mandatory group factory methods.
  private static class AggregateGenericGroup extends AbstractCounterGroup<TezCounter>
      implements CounterGroup {

    AggregateGenericGroup(String name, String displayName, Limits limits) {
      super(name, displayName, limits);
    }

    @Override
    protected TezCounter newCounter(String name, String displayName, long value) {
      return new AggregateTezCounter<GenericCounter>(new GenericCounter(name, displayName, value));
    }

    @Override
    protected TezCounter newCounter() {
      return new AggregateTezCounter<GenericCounter>(new GenericCounter());
    }

    @Override
    public CounterGroupBase<TezCounter> getUnderlyingGroup() {
      return this;
    }
  }

  // Mix file system group implementation into the CounterGroup interface
  private static class AggregateFileSystemGroup extends FileSystemCounterGroup<TezCounter>
      implements CounterGroup {

    @Override
    protected TezCounter newCounter(String scheme, FileSystemCounter key) {
      return new AggregateTezCounter<FSCounter>(new FSCounter(scheme, key));
    }

    @Override
    public CounterGroupBase<TezCounter> getUnderlyingGroup() {
      return this;
    }
  }

  /**
   * Provide factory methods for counter group factory implementation.
   * See also the GroupFactory in
   *  {@link org.apache.hadoop.TezCounters.Counters mapred.Counters}
   */
  private static class GroupFactory
      extends CounterGroupFactory<TezCounter, CounterGroup> {

    @Override
    protected <T extends Enum<T>>
    FrameworkGroupFactory<CounterGroup>
        newFrameworkGroupFactory(final Class<T> cls) {
      return new FrameworkGroupFactory<CounterGroup>() {
        @Override public CounterGroup newGroup(String name) {
          return new AggregateFrameworkGroupImpl<T>(cls); // impl in this package
        }
      };
    }

    @Override
    protected CounterGroup newGenericGroup(String name, String displayName,
                                           Limits limits) {
      return new AggregateGenericGroup(name, displayName, limits);
    }

    @Override
    protected CounterGroup newFileSystemGroup() {
      return new AggregateFileSystemGroup();
    }
  }
}
