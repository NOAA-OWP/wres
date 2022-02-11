package wres.vis.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * Tests the {@link BoxPlot}.
 * @author James Brown
 */

class BoxPlotTest
{
    /** An instance to test. */
    private BoxPlot boxPlot;

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

        BoxplotStatisticOuter outerBoxes = BoxplotStatisticOuter.of( twoBoxes, PoolMetadata.of() );

        List<BoxplotStatisticOuter> boxes = List.of( outerBoxes );
        this.boxPlot = BoxPlot.of( boxes );
    }

    @Test
    void testGetItemCount()
    {
        assertEquals( 2, this.boxPlot.getItemCount( 0 ) );
    }

    @Test
    void testGetSeriesCount()
    {
        assertEquals( 5, this.boxPlot.getSeriesCount() );
    }

    @Test
    void testGetX()
    {
        assertEquals( 40.0, this.boxPlot.getX( 0, 0 ) );
    }

    @Test
    void testGetY()
    {
        assertEquals( 30.0, this.boxPlot.getY( 2, 0 ) );
    }

    @Test
    void testGetStartX()
    {
        assertEquals( 40.0, this.boxPlot.getStartX( 0, 0 ) );
    }

    @Test
    void testGetStartY()
    {
        assertEquals( 30.0, this.boxPlot.getStartY( 2, 0 ) );
    }

    @Test
    void testGetEndX()
    {
        assertEquals( 40.0, this.boxPlot.getEndX( 0, 0 ) );
    }

    @Test
    void testGetEndY()
    {
        assertEquals( 30.0, this.boxPlot.getEndY( 2, 0 ) );
    }

    @Test
    void testGetSeriesKey()
    {
        assertEquals( "Probability 0.75", this.boxPlot.getSeriesKey( 3 ) );
    }
}
