package wres.datamodel.statistics;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import net.jcip.annotations.Immutable;

import wres.config.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.SummaryStatistic;

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

    /** The summary statistic. */
    private final SummaryStatistic summaryStatistic;

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
        return new BoxplotStatisticOuter( statistic, metadata, null );
    }

    /**
     * Returns an instance from the inputs.
     *
     * @param statistic the box plot data
     * @param metadata the metadata
     * @param summaryStatistic the optional summary statistic
     * @throws StatisticException if any of the inputs is invalid
     * @throws NullPointerException if any of the inputs is null
     * @return an instance of the output
     */

    public static BoxplotStatisticOuter of( BoxplotStatistic statistic,
                                            PoolMetadata metadata,
                                            SummaryStatistic summaryStatistic )
    {
        return new BoxplotStatisticOuter( statistic, metadata, summaryStatistic );
    }

    @Override
    public PoolMetadata getPoolMetadata()
    {
        return this.metadata;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( !( o instanceof BoxplotStatisticOuter p ) )
        {
            return false;
        }

        if ( o == this )
        {
            return true;
        }

        if ( !this.getStatistic()
                  .equals( p.getStatistic() ) )
        {
            return false;
        }

        if ( !Objects.equals( this.getSummaryStatistic(), p.getSummaryStatistic() ) )
        {
            return false;
        }

        return this.getPoolMetadata()
                   .equals( p.getPoolMetadata() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getStatistic(),
                             this.getPoolMetadata(),
                             this.getSummaryStatistic() );
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

        return builder.append( "metadata", this.getPoolMetadata() )
                      .append( "summaryStatistic", this.getSummaryStatistic() )
                      .toString();
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
    public SummaryStatistic getSummaryStatistic()
    {
        return this.summaryStatistic;
    }

    /**
     * Hidden constructor.
     *
     * @param statistic the box plot data
     * @param metadata the metadata
     * @param summaryStatistic the optional summary statistic
     * @throws NullPointerException if any input is null
     */

    private BoxplotStatisticOuter( BoxplotStatistic statistic,
                                   PoolMetadata metadata,
                                   SummaryStatistic summaryStatistic )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( statistic );

        this.metadata = metadata;
        this.statistic = statistic;
        this.summaryStatistic = summaryStatistic;
        this.metricName = MetricConstants.valueOf( statistic.getMetric()
                                                            .getName()
                                                            .name() );
    }

}
