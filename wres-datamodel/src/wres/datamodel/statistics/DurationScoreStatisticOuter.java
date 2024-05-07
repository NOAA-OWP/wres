package wres.datamodel.statistics;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import net.jcip.annotations.Immutable;

import wres.config.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DurationScoreStatisticOuter.DurationScoreComponentOuter;
import wres.statistics.MessageFactory;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.SummaryStatistic;

/**
 * An immutable score statistic that wraps a {@link DurationScoreStatistic}.
 *
 * @author James Brown
 */

@Immutable
public class DurationScoreStatisticOuter
        extends BasicScoreStatistic<DurationScoreStatistic, DurationScoreComponentOuter>
{
    /** The metric name. */
    private final MetricConstants metricName;

    /**
     * Construct the statistic.
     *
     * @param statistic the verification statistic
     * @param metadata the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DurationScoreStatisticOuter of( DurationScoreStatistic statistic, PoolMetadata metadata )
    {
        return new DurationScoreStatisticOuter( statistic, metadata, null );
    }

    /**
     * Construct the statistic as a summary statistic.
     *
     * @param statistic the verification statistic
     * @param metadata the metadata
     * @param summaryStatistic the summary statistic or null
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DurationScoreStatisticOuter of( DurationScoreStatistic statistic,
                                                  PoolMetadata metadata,
                                                  SummaryStatistic summaryStatistic )
    {
        return new DurationScoreStatisticOuter( statistic, metadata, summaryStatistic );
    }

    /**
     * A wrapper for a {@link DurationScoreStatisticComponent}.
     *
     * @author James Brown
     */

    public static class DurationScoreComponentOuter extends BasicScoreComponent<DurationScoreStatisticComponent>
    {
        /** The component name. */
        private final MetricConstants metricName;

        /**
         * Hidden constructor.
         * @param component the score component
         * @param metadata the metadata
         * @param summaryStatistic the summary statistic or null
         */

        private DurationScoreComponentOuter( DurationScoreStatisticComponent component,
                                             PoolMetadata metadata,
                                             SummaryStatistic summaryStatistic )
        {
            super( component, metadata, next -> MessageFactory.getDuration( next.getValue() ).toString(), summaryStatistic );

            this.metricName = MetricConstants.valueOf( component.getMetric().getName().name() );
        }

        /**
         * Create a component.
         *
         * @param component the score component
         * @param metadata the metadata
         * @return a component
         * @throws NullPointerException if any input is null 
         */

        public static DurationScoreComponentOuter of( DurationScoreStatisticComponent component,
                                                      PoolMetadata metadata )
        {
            return new DurationScoreComponentOuter( component, metadata, null );
        }

        /**
         * Create a component with a summary statistic.
         *
         * @param component the score component
         * @param metadata the metadata
         * @param summaryStatistic the summary statistic or null
         * @return a component
         */

        public static DurationScoreComponentOuter of( DurationScoreStatisticComponent component,
                                                      PoolMetadata metadata,
                                                      SummaryStatistic summaryStatistic )
        {
            return new DurationScoreComponentOuter( component, metadata, summaryStatistic );
        }

        @Override
        public MetricConstants getMetricName()
        {
            return this.metricName;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( !super.equals( o ) )
            {
                return false;
            }

            if ( !( o instanceof DurationScoreComponentOuter inner ) )
            {
                return false;
            }

            if ( o == this )
            {
                return true;
            }

            return Objects.equals( this.getMetricName(), inner.getMetricName() );
        }

        @Override
        public int hashCode()
        {
            return 31 * this.getMetricName().hashCode() + super.hashCode();
        }
    }

    @Override
    public MetricConstants getMetricName()
    {
        return this.metricName;
    }

    @Override
    public String toString()
    {
        ToStringBuilder builder = new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE );

        builder.append( "metric", this.getStatistic().getMetric().getName() );

        this.getInternalMapping().forEach( ( key, value ) -> builder.append( "value", value ) );

        builder.append( "metadata", this.getPoolMetadata() );

        return builder.toString();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( !super.equals( o ) )
        {
            return false;
        }

        if ( !( o instanceof DurationScoreStatisticOuter inner ) )
        {
            return false;
        }

        if ( o == this )
        {
            return true;
        }

        return Objects.equals( this.getMetricName(), inner.getMetricName() );
    }

    @Override
    public int hashCode()
    {
        return 31 * this.getMetricName().hashCode() + super.hashCode();
    }

    /**
     * Hidden constructor.
     *
     * @param score the score
     * @param metadata the pool metadata
     * @param summaryStatistic the summary statistic or null
     */

    private DurationScoreStatisticOuter( DurationScoreStatistic score,
                                         PoolMetadata metadata,
                                         SummaryStatistic summaryStatistic )
    {
        super( score,
               DurationScoreStatisticOuter.createInternalMapping( score, metadata, summaryStatistic ),
               metadata,
               summaryStatistic );

        this.metricName = MetricConstants.valueOf( score.getMetric().getName().name() );
    }

    /**
     * Creates an internal mapping between score components and their identifiers.
     *
     * @param score the score
     * @param metadata the metadata
     * @param summaryStatistic the summary statistic
     * @return the internal mapping for re-use
     */

    private static Map<MetricConstants, DurationScoreComponentOuter>
    createInternalMapping( DurationScoreStatistic score,
                           PoolMetadata metadata,
                           SummaryStatistic summaryStatistic )
    {
        Map<MetricConstants, DurationScoreComponentOuter> returnMe = new EnumMap<>( MetricConstants.class );

        List<DurationScoreStatisticComponent> components = score.getStatisticsList();
        for ( DurationScoreStatisticComponent next : components )
        {
            MetricConstants name = MetricConstants.valueOf( next.getMetric().getName().name() );
            DurationScoreComponentOuter nextOuter = DurationScoreComponentOuter.of( next,
                                                                                    metadata,
                                                                                    summaryStatistic );

            returnMe.put( name, nextOuter );
        }

        return Collections.unmodifiableMap( returnMe );
    }
}
