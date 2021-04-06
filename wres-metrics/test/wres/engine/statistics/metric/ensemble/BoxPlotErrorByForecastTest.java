package wres.engine.statistics.metric.ensemble;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.pools.SampleData;
import wres.datamodel.pools.SampleDataBasic;
import wres.datamodel.pools.SampleDataException;
import wres.datamodel.pools.SampleMetadata;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.engine.statistics.metric.MetricParameterException;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * Tests the {@link BoxPlotErrorByForecast}.
 * 
 * @author james.brown@hydrosolved.com
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

    /**
     * Compares the output from {@link BoxPlotErrorByForecast#apply(EnsemblePairs)} against 
     * expected output for box plots configured to use the ensemble mean as the forecast value.
     */

    @Test
    public void testApplyWithEnsembleMean()
    {
        List<Pair<Double, Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, Ensemble.of( 0.0, 20.0, 30.0, 50.0, 100.0 ) ) );

        SampleData<Pair<Double, Ensemble>> input = SampleDataBasic.of( values, SampleMetadata.of() );

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

        assertEquals( expectedBox, actual.getData() );
    }

    /**
     * Compares the output from {@link BoxPlotErrorByForecast#apply(EnsemblePairs)} against 
     * expected output for box plots configured to use the ensemble median as the forecast value.
     * @throws MetricParameterException if the metric could not be built
     */

    @Test
    public void testApplyWithEnsembleMedian() throws MetricParameterException
    {
        List<Pair<Double, Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, Ensemble.of( 0.0, 20.0, 30.0, 50.0, 100.0 ) ) );

        SampleData<Pair<Double, Ensemble>> input = SampleDataBasic.of( values, SampleMetadata.of() );

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

        assertEquals( expectedBox, actual.getData() );
    }

    /**
     * Validates the output from {@link BoxPlotErrorByForecast#apply(EnsemblePairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleData<Pair<Double, Ensemble>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

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

        assertEquals( expected, actual.getData() );
    }

    /**
     * Checks that the {@link BoxPlotErrorByForecast#getName()} returns 
     * {@link MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE.toString(), this.bpe.getName() );
    }

    /**
     * Checks that the {@link BoxPlotErrorByForecast#hasRealUnits()} returns <code>true</code>.
     */

    @Test
    public void testHasRealUnits()
    {
        assertTrue( this.bpe.hasRealUnits() );
    }

    /**
     * Tests for an expected exception on calling 
     * {@link BoxPlotErrorByForecast#apply(EnsemblePairs)} with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        SampleDataException expected = assertThrows( SampleDataException.class, () -> this.bpe.apply( null ) );

        assertEquals( "Specify non-null input to the 'BOX PLOT OF ERRORS BY FORECAST VALUE'.", expected.getMessage() );
    }

    /**
     * Constructs a {@link BoxPlotErrorByForecast} and checks for an expected exception when the forecast dimension 
     * is null. 
     * @throws MetricParameterException if the metric could not be constructed for reasons other than the 
     *            expected reason
     */

    @Test
    public void testConstructionWithNullDimensionException() throws MetricParameterException
    {
        MetricParameterException expected =
                assertThrows( MetricParameterException.class,
                              () -> BoxPlotErrorByForecast.of( null, VectorOfDoubles.of( 0.0, 1.0 ) ) );

        assertEquals( "Cannot build the box plot of forecast errors by forecast value without a dimension "
                      + "for the domain axis.",
                      expected.getMessage() );
    }

    /**
     * Constructs a {@link BoxPlotErrorByForecast} and checks for an expected exception when the forecast dimension 
     * is wrong. 
     * @throws MetricParameterException if the metric could not be constructed for reasons other than the 
     *            expected reason
     */

    @Test
    public void testConstructionWithWrongDimensionException() throws MetricParameterException
    {
        MetricParameterException expected =
                assertThrows( MetricParameterException.class,
                              () -> BoxPlotErrorByForecast.of( MetricDimension.FALSE_NEGATIVES,
                                                               VectorOfDoubles.of( 0.0, 1.0 ) ) );

        assertEquals( "Unsupported dimension for the domain axis of the box plot: 'FALSE NEGATIVES'.",
                      expected.getMessage() );
    }

}
