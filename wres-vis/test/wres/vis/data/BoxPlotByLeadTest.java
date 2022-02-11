package wres.vis.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * Tests the {@link BoxPlotByLeadTest}.
 * @author James Brown
 */

class BoxPlotByLeadTest
{
    /** An instance to test. */
    private BoxPlotByLead boxPlotByLead;

    @BeforeEach
    void runBeforeEachTest()
    {
        Box box = Box.newBuilder()
                     .addAllQuantiles( List.of( 0.0, 10.0, 30.0, 75.0, 100.0 ) )
                     .setLinkedValue( 40.0 )
                     .build();

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE )
                                            .setLinkedValueType( LinkedValueType.ENSEMBLE_MEAN )
                                            .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .build();

        BoxplotStatistic twoBoxes = BoxplotStatistic.newBuilder()
                                                    .setMetric( metric )
                                                    .addStatistics( box )
                                                    .addStatistics( box )
                                                    .build();

        // Set metadata with minimum content
        TimeWindow timeWindow = TimeWindow.newBuilder()
                                          .setEarliestLeadDuration( Duration.newBuilder().setSeconds( 0 ) )
                                          .setLatestLeadDuration( Duration.newBuilder().setSeconds( 27 ) )
                                          .setEarliestReferenceTime( Timestamp.getDefaultInstance() )
                                          .setLatestReferenceTime( Timestamp.getDefaultInstance() )
                                          .setEarliestValidTime( Timestamp.getDefaultInstance() )
                                          .setLatestValidTime( Timestamp.getDefaultInstance() )
                                          .build();

        PoolMetadata meta = PoolMetadata.of( PoolMetadata.of(), TimeWindowOuter.of( timeWindow ) );

        BoxplotStatisticOuter outerBoxes = BoxplotStatisticOuter.of( twoBoxes, meta );

        List<BoxplotStatisticOuter> boxes = List.of( outerBoxes );
        this.boxPlotByLead = BoxPlotByLead.of( boxes, ChronoUnit.SECONDS );
    }

    @Test
    void testGetItemCount()
    {
        assertEquals( 2, this.boxPlotByLead.getItemCount( 0 ) );
    }

    @Test
    void testGetSeriesCount()
    {
        assertEquals( 5, this.boxPlotByLead.getSeriesCount() );
    }

    @Test
    void testGetX()
    {
        assertEquals( 27L, this.boxPlotByLead.getX( 0, 0 ) );
    }

    @Test
    void testGetY()
    {
        assertEquals( 30.0, this.boxPlotByLead.getY( 2, 0 ) );
    }

    @Test
    void testGetStartX()
    {
        assertEquals( 27L, this.boxPlotByLead.getStartX( 0, 0 ) );
    }

    @Test
    void testGetStartY()
    {
        assertEquals( 30.0, this.boxPlotByLead.getStartY( 2, 0 ) );
    }

    @Test
    void testGetEndX()
    {
        assertEquals( 27L, this.boxPlotByLead.getEndX( 0, 0 ) );
    }

    @Test
    void testGetEndY()
    {
        assertEquals( 30.0, this.boxPlotByLead.getEndY( 2, 0 ) );
    }

    @Test
    void testGetSeriesKey()
    {
        assertEquals( "Probability 0.75", this.boxPlotByLead.getSeriesKey( 3 ) );
    }
}
