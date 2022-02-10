package wres.vis.data;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;

import org.jfree.data.xy.AbstractIntervalXYDataset;
import org.jfree.data.xy.XYDataset;

import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.vis.GraphicsUtils;

/**
 * Creates an {@link XYDataset} for building a box plot.
 * 
 * @author James Brown
 */
class BoxPlotByLead extends AbstractIntervalXYDataset
{
    /** serial version identifier. */
    private static final long serialVersionUID = -1990283965174892955L;

    /** The underlying dataset. */
    private final BoxPlot data;

    /** The lead duration associated with each box. */
    private final long[] leadDurations;

    /**
     * @param statistics the statistics
     * @param durationUnits the duration units
     * @return an instance
     * @throws NullPointerException if either input is null
     * @throws IllegalArgumentException if there are no statistics
     */

    static BoxPlotByLead of( List<BoxplotStatisticOuter> statistics, ChronoUnit durationUnits )
    {
        return new BoxPlotByLead( statistics, durationUnits );
    }

    @Override
    public int getItemCount( final int series )
    {
        return this.data.getItemCount( series );
    }

    @Override
    public Number getX( final int series, final int item )
    {
        return this.leadDurations[item];
    }

    @Override
    public Number getY( final int series, final int item )
    {
        return this.data.getY( series, item );
    }

    @Override
    public Number getEndX( int series, int item )
    {
        return this.getX( series, item );
    }

    @Override
    public Number getEndY( int series, int item )
    {
        return this.data.getEndY( series, item );
    }

    @Override
    public Number getStartX( int series, int item )
    {
        return this.getX( series, item );
    }

    @Override
    public Number getStartY( int series, int item )
    {
        return this.data.getStartY( series, item );
    }

    @Override
    public int getSeriesCount()
    {
        return this.data.getSeriesCount();
    }

    @Override
    public Comparable<String> getSeriesKey( int series )
    {
        return this.data.getSeriesKey( series );
    }

    /**
     * Hidden constructor
     * @param statistics the statistics
     * @param durationUnits the duration units
     * @throws NullPointerException if the statistics are null
     * @throws IllegalArgumentException if the statistics is empty
     */
    private BoxPlotByLead( final List<BoxplotStatisticOuter> statistics, ChronoUnit durationUnits )
    {
        Objects.requireNonNull( durationUnits );

        this.data = BoxPlot.of( statistics );
        this.leadDurations = statistics.stream()
                                       .map( next -> next.getMetadata().getTimeWindow().getLatestLeadDuration() )
                                       .mapToLong( next -> GraphicsUtils.durationToLongUnits( next, durationUnits ) )
                                       .toArray();
    }

}
