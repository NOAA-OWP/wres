package wres.vis.data;

import java.time.temporal.ChronoUnit;
import java.util.List;

import org.jfree.data.xy.AbstractIntervalXYDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.metrics.MetricConstants.MetricDimension;
import wres.datamodel.statistics.DiagramStatisticOuter;

/**
 * Creates an XY dataset for plotting a rank histogram.
 * 
 * @author James Brown
 */

class RankHistogram extends AbstractIntervalXYDataset
{
    /** Serial version identifier. */
    private static final long serialVersionUID = 4164482599232111408L;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( RankHistogram.class );

    /** The underlying diagram dataset. **/
    private final Diagram diagram;

    /**
     * Returns a dataset with one series per threshold for a single lead duration
     * @param statistics the statistics
     * @param xDimension the dimension that corresponds to the x axis in the plot
     * @param yDimension the dimension that corresponds to the y axis in the plot
     * @param durationUnits the lead duration units
     * @return an instance
     * @throws NullPointerException if either input is null
     * @throws IllegalArgumentException if the dataset contains more than one lead duration
     */

    static RankHistogram ofLeadThreshold( List<DiagramStatisticOuter> statistics,
                                          MetricDimension xDimension,
                                          MetricDimension yDimension,
                                          ChronoUnit durationUnits )
    {
        return new RankHistogram( statistics, xDimension, yDimension, durationUnits, true );
    }

    /**
     * Returns a dataset with one series per lead duration for a single threshold
     * @param statistics the statistics
     * @param xDimension the dimension that corresponds to the x axis in the plot
     * @param yDimension the dimension that corresponds to the y axis in the plot
     * @param durationUnits the lead duration units
     * @return an instance
     * @throws NullPointerException if either input is null
     * @throws IllegalArgumentException if the dataset contains more than one threshold
     */

    static RankHistogram ofThresholdLead( List<DiagramStatisticOuter> statistics,
                                          MetricDimension xDimension,
                                          MetricDimension yDimension,
                                          ChronoUnit durationUnits )
    {
        return new RankHistogram( statistics, xDimension, yDimension, durationUnits, false );
    }

    @Override
    public Number getEndX( int arg0, int arg1 )
    {
        return this.getX( arg0, arg1 ).doubleValue() + 0.35;
    }

    @Override
    public Number getStartX( int arg0, int arg1 )
    {
        return this.getX( arg0, arg1 ).doubleValue() - 0.35;
    }


    @Override
    public Number getEndY( int series, int item )
    {
        return this.getY( series, item );
    }

    @Override
    public Number getStartY( int series, int item )
    {
        return this.getY( series, item );
    }

    @Override
    public int getItemCount( int series )
    {
        return this.diagram.getItemCount( series );
    }

    @Override
    public Number getX( int series, int item )
    {
        return this.diagram.getX( series, item );
    }

    @Override
    public Number getY( int series, int item )
    {
        return this.diagram.getY( series, item );
    }

    @Override
    public int getSeriesCount()
    {
        return this.diagram.getSeriesCount();
    }

    @Override
    public Comparable<String> getSeriesKey( int series )
    {
        return this.diagram.getSeriesKey( series );
    }

    /**
     * Hidden constructor.
     * @param statistics the statistics
     * @param xDimension the dimension that corresponds to the x axis in the plot
     * @param yDimension the dimension that corresponds to the y axis in the plot 
     * @param durationUnits the lead duration units
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the data has an unexpected shape
     */

    private RankHistogram( List<DiagramStatisticOuter> statistics,
                           MetricDimension xDimension,
                           MetricDimension yDimension,
                           ChronoUnit durationUnits,
                           boolean leadThreshold )
    {
        if ( leadThreshold )
        {
            this.diagram = Diagram.ofLeadThreshold( statistics, xDimension, yDimension, durationUnits );
            LOGGER.debug( "Created a RankHistogram dataset with {} series organized by lead duration and then "
                          + "threshold.",
                          statistics.size() );
        }
        else
        {
            this.diagram = Diagram.ofThresholdLead( statistics, xDimension, yDimension, durationUnits );
            LOGGER.debug( "Created a RankHistogram dataset with {} series organized by threshold and then "
                          + "lead duration.",
                          statistics.size() );
        }
    }

}
