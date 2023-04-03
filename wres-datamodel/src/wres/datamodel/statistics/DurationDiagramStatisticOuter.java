package wres.datamodel.statistics;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;

import net.jcip.annotations.Immutable;
import wres.config.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.statistics.generated.DurationDiagramStatistic;

/**
 * A statistic that comprises a list of pairs.
 * 
 * @author James Brown
 */

@Immutable
public class DurationDiagramStatisticOuter implements Statistic<DurationDiagramStatistic>
{

    /**
     * The statistic.
     */

    private final DurationDiagramStatistic statistic;

    /**
     * The metadata associated with the statistic.
     */

    private final PoolMetadata metadata;
    
    /**
     * The metric name.
     */

    private final MetricConstants metricName;

    /**
     * Construct the statistic.
     * 
     * @param statistic the statistic
     * @param metadata the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DurationDiagramStatisticOuter of( DurationDiagramStatistic statistic,
                                                    PoolMetadata metadata )
    {
        return new DurationDiagramStatisticOuter( statistic, metadata );
    }

    @Override
    public PoolMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( o == this )
        {
            return true;
        }

        if ( ! ( o instanceof DurationDiagramStatisticOuter v ) )
        {
            return false;
        }

        return this.getData().equals( v.getData() ) && this.getMetadata().equals( v.getMetadata() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( statistic, metadata );
    }

    @Override
    public DurationDiagramStatistic getData()
    {
        return statistic;
    }
    
    @Override
    public MetricConstants getMetricName()
    {
        return this.metricName;
    }

    /**
     * Returns the pairs of instants and durations in a friendly format.
     * 
     * @return the pairs
     */

    public List<Pair<Instant, Duration>> getPairs()
    {
        return this.getData()
                   .getStatisticsList()
                   .stream()
                   .map( next -> Pair.of( Instant.ofEpochSecond( next.getTime().getSeconds(),
                                                                 next.getTime().getNanos() ),
                                          Duration.ofSeconds( next.getDuration().getSeconds(),
                                                              next.getDuration().getNanos() ) ) )
                   .toList();
    }

    @Override
    public String toString()
    {
        ToStringBuilder builder = new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE );

        List<Pair<Instant, Duration>> pairs = this.getPairs();

        builder.append( "metric", this.getData().getMetric().getName() )
               .append( "statistic", pairs )
               .append( "metadata", this.getMetadata() );

        return builder.toString();
    }

    /**
     * Construct the output.
     * 
     * @param statistic the statistic
     * @param metadata the metadata
     * @throws StatisticException if any of the inputs are invalid
     */

    private DurationDiagramStatisticOuter( DurationDiagramStatistic statistic, PoolMetadata metadata )
    {
        //Validate
        if ( Objects.isNull( statistic ) )
        {
            throw new StatisticException( "Specify a non-null output." );
        }
        if ( Objects.isNull( metadata ) )
        {
            throw new StatisticException( "Specify non-null metadata." );
        }

        //Set content
        this.statistic = statistic;
        this.metadata = metadata;
        this.metricName = MetricConstants.valueOf( statistic.getMetric().getName().name() );
    }

}
