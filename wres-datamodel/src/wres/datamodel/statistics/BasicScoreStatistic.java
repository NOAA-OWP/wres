package wres.datamodel.statistics;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.config.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.ScoreStatistic.ScoreComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.SummaryStatistic;

/**
 * An abstract base class for an immutable score output.
 *
 * @author James Brown
 */

abstract class BasicScoreStatistic<S, T extends ScoreComponent<?>> implements ScoreStatistic<S, T>
{
    /** Null output message. */
    private static final String NULL_OUTPUT_MESSAGE = "Specify a non-null statistic.";

    /** Null metadata message. */
    private static final String NULL_METADATA_MESSAGE = "Specify non-null metadata for the statistic.";

    /** The statistic. */
    private final S score;

    /** A convenient mapping to internal types. */
    private final Map<MetricConstants, T> internal;

    /** The metadata associated with the statistic. */
    private final PoolMetadata metadata;

    /** The summary statistic, if any. */
    private final SummaryStatistic summaryStatistic;

    @Override
    public PoolMetadata getPoolMetadata()
    {
        return this.metadata;
    }

    @Override
    public SummaryStatistic getSummaryStatistic()
    {
        return this.summaryStatistic;
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( !( o instanceof BasicScoreStatistic<?, ?> s ) )
        {
            return false;
        }

        boolean start = Objects.equals( this.getSummaryStatistic(), s.getSummaryStatistic() );

        if ( !start )
        {
            return false;
        }

        start = this.getPoolMetadata()
                    .equals( s.getPoolMetadata() );

        if ( !start )
        {
            return false;
        }

        return this.getStatistic()
                   .equals( s.getStatistic() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getStatistic(), this.getPoolMetadata(), this.getSummaryStatistic() );
    }

    @Override
    public S getStatistic()
    {
        return this.score;
    }

    @Override
    public Iterator<T> iterator()
    {
        return Collections.unmodifiableCollection( this.internal.values() )
                          .iterator();
    }

    @Override
    public Set<MetricConstants> getComponents()
    {
        return Collections.unmodifiableSet( this.internal.keySet() );
    }

    @Override
    public T getComponent( MetricConstants component )
    {
        if ( !this.internal.containsKey( component ) )
        {
            throw new StatisticException( "The component " + component + " is not defined in this context." );
        }

        return this.internal.get( component );
    }

    /**
     * A wrapper for a {@link DoubleScoreStatisticComponent}.
     *
     * @author James Brown
     */

    abstract static class BasicScoreComponent<S> implements ScoreComponent<S>
    {
        /** The component. */
        private final S component;

        /** The metadata. */
        private final PoolMetadata metadata;

        /** Mapper to a pretty string. */
        private final Function<S, String> mapper;

        /** The summary statistic, if any. */
        private final SummaryStatistic summaryStatistic;

        /**
         * Hidden constructor.
         * @param component the score component
         * @param metadata the metadata
         * @param mapper a mapper to a pretty string
         * @param summaryStatistic the summary statistic or null
         * @throws NullPointerException if any input is null
         */

        BasicScoreComponent( S component,
                             PoolMetadata metadata,
                             Function<S, String> mapper,
                             SummaryStatistic summaryStatistic )
        {
            Objects.requireNonNull( component );
            Objects.requireNonNull( metadata );
            Objects.requireNonNull( mapper );

            this.component = component;
            this.metadata = metadata;
            this.mapper = mapper;
            this.summaryStatistic = summaryStatistic;
        }

        @Override
        public S getStatistic()
        {
            return this.component;
        }

        @Override
        public PoolMetadata getPoolMetadata()
        {
            return this.metadata;
        }

        @Override
        public SummaryStatistic getSummaryStatistic()
        {
            return this.summaryStatistic;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( o == this )
            {
                return true;
            }

            if ( !( o instanceof BasicScoreComponent<?> c ) )
            {
                return false;
            }

            return Objects.equals( this.getSummaryStatistic(), c.getSummaryStatistic() )
                   && Objects.equals( this.getMetricName(), c.getMetricName() )
                   && Objects.equals( this.getStatistic(), c.getStatistic() )
                   && Objects.equals( this.getPoolMetadata(), c.getPoolMetadata() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( this.getMetricName(),
                                 this.getStatistic(),
                                 this.getPoolMetadata(),
                                 this.getSummaryStatistic() );
        }

        @Override
        public String toString()
        {
            String pretty = this.mapper.apply( this.getStatistic() );
            return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                    .append( "name", this.getMetricName()
                                         .name() )
                    .append( "value", pretty )
                    .build();
        }
    }

    /**
     * Returns the component mapping for building a friendly string representation of the score.
     *
     * @return the component mapping
     */

    Map<MetricConstants, T> getInternalMapping()
    {
        return this.internal;
    }

    /**
     * Construct the output.
     *
     * @param score the verification score
     * @param internal the internal mapping between score component names and component values
     * @param metadata the metadata
     * @param summaryStatistic the summary statistic or null
     */

    BasicScoreStatistic( S score, Map<MetricConstants, T> internal,
                         PoolMetadata metadata,
                         SummaryStatistic summaryStatistic )
    {
        Objects.requireNonNull( metadata, NULL_METADATA_MESSAGE );
        Objects.requireNonNull( score, NULL_OUTPUT_MESSAGE );

        this.score = score;
        this.metadata = metadata;
        this.internal = Collections.unmodifiableMap( internal );
        this.summaryStatistic = summaryStatistic;
    }
}
