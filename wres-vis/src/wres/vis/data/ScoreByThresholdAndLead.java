package wres.vis.data;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.data.xy.AbstractXYDataset;
import org.jfree.data.xy.AbstractIntervalXYDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.Slicer;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.vis.charts.GraphicsUtils;

/**
 * Creates an XY dataset for plotting a verification score component by threshold (X axis) and score value (Y axis) with 
 * up to N series per dataset, each representing a distinct lead duration.
 * 
 * In order to support displays of intervals, such as confidence intervals, upgrade to 
 * {@link AbstractIntervalXYDataset}.
 * 
 * @author James Brown
 */

class ScoreByThresholdAndLead extends AbstractXYDataset
{

    /** Serial version identifier. */
    private static final long serialVersionUID = -5997279852022884528L;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ScoreByThresholdAndLead.class );

    /** The duration units. */
    private final ChronoUnit durationUnits;

    /** The statistics to plot, arranged by series. Each item in the outer list contains a series, indexed by name. */
    private final List<Pair<String, List<DoubleScoreComponentOuter>>> statistics;

    /**
     * @param statistics the statistics
     * @param durationUnits the lead duration units
     * @return an instance
     * @throws NullPointerException if either input is null
     */

    static ScoreByThresholdAndLead of( List<DoubleScoreComponentOuter> statistics, ChronoUnit durationUnits )
    {
        return new ScoreByThresholdAndLead( statistics, durationUnits );
    }

    @Override
    public int getItemCount( int series )
    {
        return this.statistics.get( series )
                              .getRight()
                              .size();
    }

    @Override
    public Number getX( int series, int item )
    {
        // Cannot allow all data (infinite) threshold. Use lower bound if this is a "BETWEEN" threshold
        final double test = this.statistics.get( series )
                                           .getRight()
                                           .get( item )
                                           .getMetadata()
                                           .getThresholds()
                                           .first()
                                           .getValues()
                                           .first();
        if ( Double.isInfinite( test ) )
        {
            return Double.MIN_VALUE; // JFreeChart missing protocol is to return finite double for X and null for Y
        }

        return test;
    }

    @Override
    public Number getY( int series, int item )
    {
        // Cannot allow all data (infinite) threshold
        Double test = (Double) this.getX( series, item );
        if ( test.equals( Double.MIN_VALUE ) )
        {
            return null;
        }
        return this.statistics.get( series )
                              .getRight()
                              .get( item )
                              .getData()
                              .getValue();
    }

    @Override
    public int getSeriesCount()
    {
        return this.statistics.size();
    }

    @Override
    public Comparable<String> getSeriesKey( int series )
    {
        return this.statistics.get( series )
                              .getLeft();
    }

    /**
     * Hidden constructor.
     * @param statistics the statistics
     * @param durationUnits the lead duration units
     * @throws NullPointerException if either input is null
     */

    private ScoreByThresholdAndLead( List<DoubleScoreComponentOuter> statistics, ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( durationUnits );

        this.durationUnits = durationUnits;

        // Arrange the series by threshold and then set them, ignoring the all data threshold
        List<Pair<String, List<DoubleScoreComponentOuter>>> innerStatistics = new ArrayList<>();
        SortedSet<TimeWindowOuter> timeWindows =
                Slicer.discover( statistics, next -> next.getMetadata().getTimeWindow() );
        for ( TimeWindowOuter key : timeWindows )
        {
            List<DoubleScoreComponentOuter> sliced = Slicer.filter( statistics,
                                                                    next -> next.getMetadata()
                                                                                .getTimeWindow()
                                                                                .equals( key )
                                                                            && !next.getMetadata()
                                                                                    .getThresholds()
                                                                                    .first()
                                                                                    .isAllDataThreshold() );


            long leadDuration = GraphicsUtils.durationToLongUnits( key.getLatestLeadDuration(),
                                                                   this.durationUnits );
            String name = Long.toString( leadDuration );
            Pair<String, List<DoubleScoreComponentOuter>> pair = Pair.of( name, sliced );
            innerStatistics.add( pair );
        }

        this.statistics = Collections.unmodifiableList( innerStatistics );

        LOGGER.debug( "Created a ScoreByThresholdAndLead dataset with {} series representing the time windows: {}.",
                      this.statistics.size(),
                      timeWindows );
    }

}
