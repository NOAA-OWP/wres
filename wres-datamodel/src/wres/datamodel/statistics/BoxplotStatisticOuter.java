package wres.datamodel.statistics;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.statistics.generated.BoxplotStatistic;

/**
 * Immutable store of several box plot statistics.
 * 
 * @author james.brown@hydrosolved.com
 */
public class BoxplotStatisticOuter implements Statistic<BoxplotStatistic>
{

    /**
     * The statistics metadata.
     */

    private final StatisticMetadata metadata;

    /**
     * The statistics.
     */

    private final BoxplotStatistic statistic;

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
                                            StatisticMetadata metadata )
    {
        return new BoxplotStatisticOuter( statistic, metadata );
    }

    @Override
    public StatisticMetadata getMetadata()
    {
        return this.metadata;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof BoxplotStatisticOuter ) )
        {
            return false;
        }

        if ( o == this )
        {
            return true;
        }

        BoxplotStatisticOuter p = (BoxplotStatisticOuter) o;

        if ( !this.getData().equals( p.getData() ) )
        {
            return false;
        }

        return this.getMetadata().equals( p.getMetadata() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getData(), this.getMetadata() );
    }
    
    @Override
    public String toString()
    {
        ToStringBuilder builder = new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE );

        builder.append( "metric name", this.getData().getMetric().getName() );
        builder.append( "linked value type", this.getData().getMetric().getLinkedValueType() );
        builder.append( "quantile value type", this.getData().getMetric().getQuantileValueType() );
        
        this.getData()
            .getStatisticsList()
            .forEach( component -> builder.append( "linked value:", component.getLinkedValue() )
                                          .append( "box:", component.getQuantilesList() ) );

        builder.append( "metadata", this.getMetadata() );
        
        return builder.toString();
    }
    
    @Override
    public BoxplotStatistic getData()
    {
        return this.statistic; // Rendered immutable on construction
    }

    /**
     * Hidden constructor.
     * 
     * @param statistic the box plot data
     * @param metadata the metadata
     * @throws NullPointerException if any input is null
     */

    private BoxplotStatisticOuter( BoxplotStatistic statistic,
                                   StatisticMetadata metadata )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( statistic );

        this.metadata = metadata;
        this.statistic = statistic;
    }

}
