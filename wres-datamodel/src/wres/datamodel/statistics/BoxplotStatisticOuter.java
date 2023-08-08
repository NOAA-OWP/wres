package wres.datamodel.statistics;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import net.jcip.annotations.Immutable;
import wres.config.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.statistics.generated.BoxplotStatistic;

/**
 * Immutable store of several box plot statistics.
 * 
 * @author James Brown
 */

@Immutable
public class BoxplotStatisticOuter implements Statistic<BoxplotStatistic>
{
    /** The statistics metadata. */
    private final PoolMetadata metadata;

    /** The statistics. */
    private final BoxplotStatistic statistic;

    /** The metric name. */
    private final MetricConstants metricName;

    /**
     * Returns an instance from the inputs.
     * 
     * @param statistic the box plot data
     * @param metadata the metadata
     * @throws StatisticException if any of the inputs is invalid
     * @throws NullPointerException if any of the inputs is null
     * @return an instance of the output
     */

    public static BoxplotStatisticOuter of( BoxplotStatistic statistic,
                                            PoolMetadata metadata )
    {
        return new BoxplotStatisticOuter( statistic, metadata );
    }

    @Override
    public PoolMetadata getPoolMetadata()
    {
        return this.metadata;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof BoxplotStatisticOuter p ) )
        {
            return false;
        }

        if ( o == this )
        {
            return true;
        }

        if ( !this.getStatistic().equals( p.getStatistic() ) )
        {
            return false;
        }

        return this.getPoolMetadata().equals( p.getPoolMetadata() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getStatistic(), this.getPoolMetadata() );
    }

    @Override
    public String toString()
    {
        ToStringBuilder builder = new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE );

        builder.append( "metric name", this.getStatistic().getMetric().getName() );
        builder.append( "linked value type", this.getStatistic().getMetric().getLinkedValueType() );
        builder.append( "quantile value type", this.getStatistic().getMetric().getQuantileValueType() );

        this.getStatistic()
            .getStatisticsList()
            .forEach( component -> builder.append( "linked value:", component.getLinkedValue() )
                                          .append( "box:", component.getQuantilesList() ) );

        builder.append( "metadata", this.getPoolMetadata() );

        return builder.toString();
    }

    @Override
    public BoxplotStatistic getStatistic()
    {
        return this.statistic; // Rendered immutable on construction
    }

    @Override
    public MetricConstants getMetricName()
    {
        return this.metricName;
    }

    @Override
    public Double getSampleQuantile()
    {
        return null;
    }

    /**
     * Hidden constructor.
     * 
     * @param statistic the box plot data
     * @param metadata the metadata
     * @throws NullPointerException if any input is null
     */

    private BoxplotStatisticOuter( BoxplotStatistic statistic,
                                   PoolMetadata metadata )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( statistic );

        this.metadata = metadata;
        this.statistic = statistic;
        this.metricName = MetricConstants.valueOf( statistic.getMetric()
                                                            .getName()
                                                            .name() );
    }

}
