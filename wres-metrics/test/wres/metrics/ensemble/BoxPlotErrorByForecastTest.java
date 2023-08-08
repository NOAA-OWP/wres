package wres.metrics.ensemble;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.Ensemble;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.VectorOfDoubles;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricDimension;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.metrics.MetricParameterException;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * Tests the {@link BoxPlotErrorByForecast}.
 * 
 * @author James Brown
 */
public final class BoxPlotErrorByForecastTest
{

    /**
     * Default instance of a {@link BoxPlotErrorByForecast}.
     */

    private BoxPlotErrorByForecast bpe;

    @Before
    public void setupBeforeEachTest()
    {
        this.bpe = BoxPlotErrorByForecast.of();
    }

    @Test
    public void testApplyWithEnsembleMean()
    {
        List<Pair<Double, Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, Ensemble.of( 0.0, 20.0, 30.0, 50.0, 100.0 ) ) );

        Pool<Pair<Double, Ensemble>> input = Pool.of( values, PoolMetadata.of() );

        BoxplotStatisticOuter actual = this.bpe.apply( input );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE )
                                            .setLinkedValueType( LinkedValueType.ENSEMBLE_MEAN )
                                            .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .setUnits( "DIMENSIONLESS" )
                                            .build();

        Box box = Box.newBuilder()
                     .addAllQuantiles( List.of( 0.0, 10.0, 30.0, 75.0, 100.0 ) )
                     .setLinkedValue( 40.0 )
                     .build();

        BoxplotStatistic expectedBox = BoxplotStatistic.newBuilder()
                                                       .setMetric( metric )
                                                       .addStatistics( box )
                                                       .build();

        assertEquals( expectedBox, actual.getStatistic() );
    }

    @Test
    public void testApplyWithEnsembleMedian() throws MetricParameterException
    {
        List<Pair<Double, Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, Ensemble.of( 0.0, 20.0, 30.0, 50.0, 100.0 ) ) );

        Pool<Pair<Double, Ensemble>> input = Pool.of( values, PoolMetadata.of() );

        //Build the metric
        BoxPlotErrorByForecast bpef = BoxPlotErrorByForecast.of( MetricDimension.ENSEMBLE_MEDIAN,
                                                                 VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) );

        BoxplotStatisticOuter actual = bpef.apply( input );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE )
                                            .setLinkedValueType( LinkedValueType.ENSEMBLE_MEDIAN )
                                            .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .setUnits( "DIMENSIONLESS" )
                                            .build();

        Box box = Box.newBuilder()
                     .addAllQuantiles( List.of( 0.0, 10.0, 30.0, 75.0, 100.0 ) )
                     .setLinkedValue( 30.0 )
                     .build();

        BoxplotStatistic expectedBox = BoxplotStatistic.newBuilder()
                                                       .setMetric( metric )
                                                       .addStatistics( box )
                                                       .build();

        assertEquals( expectedBox, actual.getStatistic() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Ensemble>> input =
                Pool.of( List.of(), PoolMetadata.of() );

        BoxplotStatisticOuter actual = this.bpe.apply( input );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE )
                                            .setLinkedValueType( LinkedValueType.ENSEMBLE_MEAN )
                                            .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                            .addAllQuantiles( EnsembleBoxPlot.DEFAULT_PROBABILITIES )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .setUnits( "DIMENSIONLESS" )
                                            .build();

        BoxplotStatistic expected = BoxplotStatistic.newBuilder()
                                                    .setMetric( metric )
                                                    .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE.toString(), this.bpe.getMetricNameString() );
    }

    @Test
    public void testHasRealUnits()
    {
        assertTrue( this.bpe.hasRealUnits() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        PoolException expected = assertThrows( PoolException.class, () -> this.bpe.apply( null ) );

        assertEquals( "Specify non-null input to the 'BOX PLOT OF ERRORS BY FORECAST VALUE'.", expected.getMessage() );
    }

    @Test
    public void testConstructionWithNullDimensionException() throws MetricParameterException
    {
        VectorOfDoubles doubles = VectorOfDoubles.of( 0.0, 1.0 );
        MetricParameterException expected =
                assertThrows( MetricParameterException.class,
                              () -> BoxPlotErrorByForecast.of( null, doubles ) );

        assertEquals( "Cannot build the box plot of forecast errors by forecast value without a dimension "
                      + "for the domain axis.",
                      expected.getMessage() );
    }

    @Test
    public void testConstructionWithWrongDimensionException() throws MetricParameterException
    {
        VectorOfDoubles doubles = VectorOfDoubles.of( 0.0, 1.0 );
        MetricParameterException expected =
                assertThrows( MetricParameterException.class,
                              () -> BoxPlotErrorByForecast.of( MetricDimension.FALSE_NEGATIVES,
                                                               doubles ) );

        assertEquals( "Unsupported dimension for the domain axis of the box plot: 'FALSE NEGATIVES'.",
                      expected.getMessage() );
    }

}
