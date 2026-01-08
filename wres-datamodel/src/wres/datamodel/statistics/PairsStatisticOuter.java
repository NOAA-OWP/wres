package wres.datamodel.statistics;

import java.util.Objects;

import net.jcip.annotations.Immutable;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.config.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.PairsStatistic;
import wres.statistics.generated.SummaryStatistic;

/**
 * An immutable score statistic that wraps a {@link DoubleScoreStatistic}.
 *
 * @author James Brown
 */

@Immutable
public class PairsStatisticOuter implements Statistic<PairsStatistic>
{
    /** The metric name. */
    private final MetricConstants metricName;

    /** The pairs statistic. */
    private final PairsStatistic pairsStatistic;

    /** The metadata associated with the statistic. */
    private final PoolMetadata metadata;

    /**
     * Construct the statistic.
     *
     * @param statistic the verification statistic
     * @param metadata the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static PairsStatisticOuter of( PairsStatistic statistic, PoolMetadata metadata )
    {
        return new PairsStatisticOuter( statistic, metadata );
    }

    @Override
    public PairsStatistic getStatistic()
    {
        return this.pairsStatistic;
    }

    @Override
    public PoolMetadata getPoolMetadata()
    {
        return this.metadata;
    }

    @Override
    public MetricConstants getMetricName()
    {
        return this.metricName;
    }

    @Override
    public SummaryStatistic getSummaryStatistic()
    {
        return null;
    }

    @Override
    public boolean isSummaryStatistic()
    {
        return Statistic.super.isSummaryStatistic();
    }

    @Override
    public String toString()
    {
        ToStringBuilder builder = new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE );

        builder.append( "metric", this.getStatistic()
                                      .getMetric()
                                      .getName() );

        builder.append( "metadata", this.getPoolMetadata() );
        builder.append( "leftNames", this.getStatistic()
                                         .getStatistics()
                                         .getLeftVariableNamesList() );
        builder.append( "rightNames", this.getStatistic()
                                         .getStatistics()
                                         .getRightVariableNamesList() );

        return builder.toString();
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( o == this )
        {
            return true;
        }

        if ( !( o instanceof final PairsStatisticOuter v ) )
        {
            return false;
        }

        return this.getPoolMetadata()
                   .equals( v.getPoolMetadata() )
               && this.getStatistic()
                      .equals( v.getStatistic() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getPoolMetadata(), this.getStatistic() );
    }

    /**
     * Hidden constructor.
     *
     * @param pairsStatistic the statistic
     * @param metadata the metadata
     */

    private PairsStatisticOuter( PairsStatistic pairsStatistic,
                                 PoolMetadata metadata )
    {
        if ( Objects.isNull( pairsStatistic ) )
        {
            throw new StatisticException( "Specify non-null pair statistics." );
        }
        if ( Objects.isNull( metadata ) )
        {
            throw new StatisticException( "Specify non-null metadata." );
        }

        this.pairsStatistic = pairsStatistic;
        this.metadata = metadata;
        this.metricName = MetricConstants.valueOf( pairsStatistic.getMetric()
                                                                 .getName()
                                                                 .name() );
    }
}
