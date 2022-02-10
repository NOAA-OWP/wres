package wres.vis.data;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.jfree.data.xy.AbstractIntervalXYDataset;
import org.jfree.data.xy.XYDataset;

import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * Creates an {@link XYDataset} for building a box plot.
 * 
 * @author James Brown
 */
class BoxPlot extends AbstractIntervalXYDataset
{
    /** serial version identifier. */
    private static final long serialVersionUID = 4254109136599641286L;

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

    static BoxPlot of( List<BoxplotStatisticOuter> statistics )
    {
        return new BoxPlot( statistics );
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
        return this.boxes.get( item )
                         .getQuantiles( series );
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
        return "Probability " + this.boxes.get( 0 )
                                          .getQuantiles( series );
    }

    /**
     * Hidden constructor
     * @param statistics the statistics
     * @throws NullPointerException if the statistics are null
     * @throws IllegalArgumentException if the statistics is empty
     */
    private BoxPlot( final List<BoxplotStatisticOuter> statistics )
    {
        Objects.requireNonNull( statistics );

        this.boxes = statistics.stream()
                               .map( next -> next.getData().getStatisticsList() )
                               .flatMap( List::stream )
                               .collect( Collectors.toList() );

        if ( this.boxes.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot create a box plot dataset with no boxes." );
        }

        this.itemCount = statistics.stream()
                                   .mapToInt( next -> next.getData().getStatisticsCount() )
                                   .sum();

        List<Box> innerBoxes = statistics.get( 0 )
                                         .getData()
                                         .getStatisticsList();

        this.seriesCount = innerBoxes.get( 0 )
                                     .getQuantilesCount();
    }

}
