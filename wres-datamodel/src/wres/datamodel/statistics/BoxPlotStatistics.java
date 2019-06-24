package wres.datamodel.statistics;

import java.util.List;
import java.util.Objects;

/**
 * Immutable store of several box plot statistics.
 * 
 * @author james.brown@hydrosolved.com
 */
public class BoxPlotStatistics extends ListOfStatistics<BoxPlotStatistic> implements Statistic<List<BoxPlotStatistic>>
{

    /**
     * The statistics metadata.
     */

    private final StatisticMetadata metadata;

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

        if ( !super.equals( o ) )
        {
            return false;
        }

        BoxPlotStatistics p = (BoxPlotStatistics) o;

        return this.getMetadata().equals( p.getMetadata() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), metadata );
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
        super( statistics );

        Objects.requireNonNull( metadata );

        this.metadata = metadata;
    }

}
