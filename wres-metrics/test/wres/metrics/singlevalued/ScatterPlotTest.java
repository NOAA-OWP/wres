package wres.metrics.singlevalued;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.MetricName;

/**
 * Tests the {@link ScatterPlot}.
 *
 * @author James Brown
 */
public final class ScatterPlotTest
{

    /** Default instance of a {@link ScatterPlot}. */
    private ScatterPlot scatterPlot;

    /** Observations. */
    private static final DiagramMetric.DiagramMetricComponent OBSERVATIONS =
            DiagramMetric.DiagramMetricComponent.newBuilder()
                                                .setName( MetricName.OBSERVATIONS )
                                                .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_DOMAIN_AXIS )
                                                .setMinimum( MetricConstants.SCATTER_PLOT.getMinimum() )
                                                .setMaximum( MetricConstants.SCATTER_PLOT.getMaximum() )
                                                .build();

    /** Predictions. */
    private static final DiagramMetric.DiagramMetricComponent PREDICTIONS =
            DiagramMetric.DiagramMetricComponent.newBuilder()
                                                .setName( MetricName.PREDICTIONS )
                                                .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_RANGE_AXIS )
                                                .setMinimum( MetricConstants.SCATTER_PLOT.getMinimum() )
                                                .setMaximum( MetricConstants.SCATTER_PLOT.getMaximum() )
                                                .build();

    @Before
    public void setupBeforeEachTest()
    {
        this.scatterPlot = ScatterPlot.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        final List<Pair<Double, Double>> values = new ArrayList<>();
        for ( int i = 1; i < 11; i++ )
        {
            values.add( Pair.of( ( double ) i, ( double ) i ) );
        }

        Pool<Pair<Double, Double>> input = Pool.of( values, PoolMetadata.of() );

        // Check the results
        DiagramStatisticOuter actual = this.scatterPlot.apply( input );

        List<Double> observed = new ArrayList<>();
        List<Double> predicted = new ArrayList<>();

        for ( int i = 1; i < 11; i++ )
        {
            double next = Precision.round( i, 1 );
            observed.add( next );
            predicted.add( next );
        }

        DiagramStatisticComponent obs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( OBSERVATIONS.toBuilder()
                                                                 .setUnits( "DIMENSIONLESS" ) )
                                         .addAllValues( observed )
                                         .build();

        DiagramStatisticComponent pred =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( PREDICTIONS.toBuilder()
                                                                .setUnits( "DIMENSIONLESS" ) )
                                         .addAllValues( predicted )
                                         .build();

        DiagramStatistic expected = DiagramStatistic.newBuilder()
                                                    .addStatistics( obs )
                                                    .addStatistics( pred )
                                                    .setMetric( ScatterPlot.BASIC_METRIC )
                                                    .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( List.of(), PoolMetadata.of() );

        DiagramStatisticOuter actual = this.scatterPlot.apply( input );

        DiagramStatisticComponent obs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( OBSERVATIONS.toBuilder()
                                                                 .setUnits( "DIMENSIONLESS" ) )
                                         .build();

        DiagramStatisticComponent pred =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( PREDICTIONS.toBuilder()
                                                                .setUnits( "DIMENSIONLESS" ) )
                                         .build();

        DiagramStatistic expected = DiagramStatistic.newBuilder()
                                                    .addStatistics( obs )
                                                    .addStatistics( pred )
                                                    .setMetric( ScatterPlot.BASIC_METRIC )
                                                    .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.SCATTER_PLOT.toString(), this.scatterPlot.getMetricNameString() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        NullPointerException expected =
                assertThrows( NullPointerException.class, () -> this.scatterPlot.apply( null ) );

        assertEquals( "Specify non-null input to the 'SCATTER PLOT'.", expected.getMessage() );
    }

}
