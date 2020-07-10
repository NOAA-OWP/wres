package wres.datamodel.statistics;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable store of several box plot statistics.
 * 
 * @author james.brown@hydrosolved.com
 */
public class BoxplotStatisticOuter implements Statistic<List<BoxplotStatistic>>
{

    /**
     * The statistics metadata.
     */

    private final StatisticMetadata metadata;
    
    /**
     * The statistics.
     */

    private final List<BoxplotStatistic> statistics;
    
    /**
     * Returns an instance from the inputs.
     * 
     * @param statistics the box plot data
     * @param metadata the metadata
     * @throws StatisticException if any of the inputs is invalid
     * @throws NullPointerException if any of the inputs is null
     * @return an instance of the output
     */

    public static BoxplotStatisticOuter of( List<BoxplotStatistic> statistics,
                                        StatisticMetadata metadata )
    {
        return new BoxplotStatisticOuter( statistics, metadata );
    }

    @Override
    public StatisticMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof BoxplotStatisticOuter ) )
        {
            return false;
        }

        if( o == this )
        {
            return true;
        }

        BoxplotStatisticOuter p = (BoxplotStatisticOuter) o;

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

    private BoxplotStatisticOuter( List<BoxplotStatistic> statistics,
                               StatisticMetadata metadata )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( statistics );

        this.metadata = metadata;
        this.statistics = Collections.unmodifiableList( statistics );
        
        if ( this.statistics.contains( (BoxplotStatistic) null ) )
        {
            throw new StatisticException( "Cannot build a list of box plot statistics with one or more null entries." );
        }
    }

    @Override
    public List<BoxplotStatistic> getData()
    {
        return this.statistics; // Rendered immutable on construction
    }

}
