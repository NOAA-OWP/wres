package wres.vis.data;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.jfree.data.xy.AbstractIntervalXYDataset;
import org.jfree.data.xy.XYDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * Creates an {@link XYDataset} for building a box plot.
 * 
 * @author James Brown
 */
class Boxplot extends AbstractIntervalXYDataset
{
    /** serial version identifier. */
    private static final long serialVersionUID = 4254109136599641286L;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Boxplot.class );

    /** The number of boxes. */
    private final int itemCount;

    /** The number of quantiles per box. */
    private final int seriesCount;

    /** The raw boxes. */
    private final List<Box> boxes;

    /**
     * @param statistics the statistics
     * @return an instance
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if there are no statistics
     */

    static Boxplot of( List<BoxplotStatisticOuter> statistics )
    {
        return new Boxplot( statistics );
    }

    @Override
    public int getItemCount( int series )
    {
        return this.itemCount;
    }

    @Override
    public Number getX( int series, final int item )
    {
        return this.boxes.get( item )
                         .getLinkedValue();
    }

    @Override
    public Number getY( int series, final int item )
    {
        double y = this.boxes.get( item )
                             .getQuantiles( series );

        if ( Double.isFinite( y ) )
        {
            return y;
        }
        
        // JFreeChart cannot handle infinite values
        return null;
    }

    @Override
    public Number getEndX( int series, int item )
    {
        return this.getX( series, item );
    }

    @Override
    public Number getEndY( int series, int item )
    {
        return this.getY( series, item );
    }

    @Override
    public Number getStartX( int series, int item )
    {
        return this.getX( series, item );
    }

    @Override
    public Number getStartY( int series, int item )
    {
        return this.getY( series, item );
    }

    @Override
    public int getSeriesCount()
    {
        return this.seriesCount;
    }

    @Override
    public Comparable<String> getSeriesKey( int series )
    {
        return "Series_" + series;
    }

    /**
     * Hidden constructor
     * @param statistics the statistics
     * @throws NullPointerException if the statistics are null
     * @throws IllegalArgumentException if the statistics is empty
     */
    private Boxplot( List<BoxplotStatisticOuter> statistics )
    {
        Objects.requireNonNull( statistics );

        this.boxes = statistics.stream()
                               .map( next -> next.getData().getStatisticsList() )
                               .flatMap( List::stream )
                               .collect( Collectors.toList() );

        this.itemCount = statistics.stream()
                                   .mapToInt( next -> next.getData().getStatisticsCount() )
                                   .sum();

        // Empty?
        if ( this.boxes.isEmpty() )
        {
            this.seriesCount = 0;

            LOGGER.debug( "Found an empty box plot dataset while constructing a box plot." );
        }
        else
        {
            List<Box> innerBoxes = statistics.get( 0 )
                                             .getData()
                                             .getStatisticsList();

            this.seriesCount = innerBoxes.get( 0 )
                                         .getQuantilesCount();
        }
    }

}
