package wres.datamodel.statistics;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable store of several box plot statistics.
 * 
 * @author james.brown@hydrosolved.com
 */
public class BoxPlotStatistics implements Statistic<List<BoxPlotStatistic>>
{

    /**
     * The statistics metadata.
     */

    private final StatisticMetadata metadata;
    
    /**
     * The statistics.
     */

    private final List<BoxPlotStatistic> statistics;
    
    /**
     * Returns an instance from the inputs.
     * 
     * @param statistics the box plot data
     * @param metadata the metadata
     * @throws StatisticException if any of the inputs is invalid
     * @throws NullPointerException if any of the inputs is null
     * @return an instance of the output
     */

    public static BoxPlotStatistics of( List<BoxPlotStatistic> statistics,
                                        StatisticMetadata metadata )
    {
        return new BoxPlotStatistics( statistics, metadata );
    }

    @Override
    public StatisticMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof BoxPlotStatistics ) )
        {
            return false;
        }

        if( o == this )
        {
            return true;
        }

        BoxPlotStatistics p = (BoxPlotStatistics) o;

        if ( !this.statistics.equals( p.statistics ) )
        {
            return false;
        }

        return this.getMetadata().equals( p.getMetadata() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( statistics, metadata );
    }

    /**
     * Hidden constructor.
     * 
     * @param statistics the box plot data
     * @param metadata the metadata
     * @throws NullPointerException if any input is null
     */

    private BoxPlotStatistics( List<BoxPlotStatistic> statistics,
                               StatisticMetadata metadata )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( statistics );

        this.metadata = metadata;
        this.statistics = Collections.unmodifiableList( statistics );
        
        if ( this.statistics.contains( (BoxPlotStatistic) null ) )
        {
            throw new StatisticException( "Cannot build a list of box plot statistics with one or more null entries." );
        }
    }

    @Override
    public List<BoxPlotStatistic> getData()
    {
        return this.statistics; // Rendered immutable on construction
    }

}
