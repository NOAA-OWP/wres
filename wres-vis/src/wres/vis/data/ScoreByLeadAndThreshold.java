package wres.vis.data;

import java.io.Serial;
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

import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * Creates an XY dataset for plotting a verification score component by lead duration (X axis) and score value (Y axis) 
 * with up to N series per dataset, each representing a distinct threshold.
 * 
 * In order to support displays of intervals, such as confidence intervals, upgrade to 
 * {@link AbstractIntervalXYDataset}.
 * 
 * @author James Brown
 */

class ScoreByLeadAndThreshold extends AbstractXYDataset
{

    /** Serial version identifier. */
    @Serial
    private static final long serialVersionUID = -6260904713900350909L;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ScoreByLeadAndThreshold.class );

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

    static ScoreByLeadAndThreshold of( List<DoubleScoreComponentOuter> statistics, ChronoUnit durationUnits )
    {
        return new ScoreByLeadAndThreshold( statistics, durationUnits );
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
        return DataUtilities.durationToNumericUnits( this.statistics.get( series )
                                                                    .getRight()
                                                                    .get( item )
                                                                    .getMetadata()
                                                                    .getTimeWindow()
                                                                    .getLatestLeadDuration(),
                                                     this.durationUnits );
    }

    @Override
    public Number getY( int series, int item )
    {
        double test = this.statistics.get( series )
                                     .getRight()
                                     .get( item )
                                     .getData()
                                     .getValue();

        if ( Double.isInfinite( test ) )
        {
            return null; // JFreeChart missing protocol, #103129
        }

        return test;
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

    private ScoreByLeadAndThreshold( List<DoubleScoreComponentOuter> statistics, ChronoUnit durationUnits )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( durationUnits );

        this.durationUnits = durationUnits;

        // Arrange the series by threshold and then set them
        List<Pair<String, List<DoubleScoreComponentOuter>>> innerStatistics = new ArrayList<>();
        SortedSet<OneOrTwoThresholds> thresholds =
                Slicer.discover( statistics, next -> next.getMetadata().getThresholds() );
        for ( OneOrTwoThresholds key : thresholds )
        {
            List<DoubleScoreComponentOuter> sliced = Slicer.filter( statistics,
                                                                    next -> next.getMetadata()
                                                                                .getThresholds()
                                                                                .equals( key ) );


            String name = DataUtilities.toStringWithoutUnits( key );
            Pair<String, List<DoubleScoreComponentOuter>> pair = Pair.of( name, sliced );
            innerStatistics.add( pair );
        }

        this.statistics = Collections.unmodifiableList( innerStatistics );

        LOGGER.debug( "Created a ScoreByLeadAndThreshold dataset with {} series representing the thresholds: {}.",
                      this.statistics.size(),
                      thresholds );
    }

}
