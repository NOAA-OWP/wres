package wres.metrics.ensemble;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.Ensemble;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.metrics.MetricParameterException;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * Tests the {@link BoxPlotErrorByObserved}.
 * 
 * @author James Brown
 */
public final class BoxPlotErrorByObservedTest
{

    /**
     * Default instance of a {@link BoxPlotErrorByObserved}.
     */

    private BoxPlotErrorByObserved bpe;

    @Before
    public void setupBeforeEachTest()
    {
        this.bpe = BoxPlotErrorByObserved.of();
    }

    /**
     * Compares the output from {@link BoxPlotErrorByObserved#apply(Pool)} against 
     * expected output.
     */

    @Test
    public void testApply()
    {
        List<Pair<Double, Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 50.0, Ensemble.of( 0.0, 25.0, 50.0, 75.0, 100.0 ) ) );

        Pool<Pair<Double, Ensemble>> input = Pool.of( values, PoolMetadata.of() );

        BoxplotStatisticOuter actual = this.bpe.apply( input );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE )
                                            .setLinkedValueType( LinkedValueType.OBSERVED_VALUE )
                                            .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .setUnits( "DIMENSIONLESS" )
                                            .build();

        Box box = Box.newBuilder()
                     .addAllQuantiles( List.of( -50.0, -37.5, 0.0, 37.5, 50.0 ) )
                     .setLinkedValue( 50.0 )
                     .build();

        BoxplotStatistic expectedBox = BoxplotStatistic.newBuilder()
                                                       .setMetric( metric )
                                                       .addStatistics( box )
                                                       .build();

        assertEquals( expectedBox, actual.getData() );
    }

    /**
     * Validates the output from {@link BoxPlotErrorByObserved#apply(Pool)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Ensemble>> input =
                Pool.of( Arrays.asList(), PoolMetadata.of() );

        BoxplotStatisticOuter actual = bpe.apply( input );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE )
                                            .setLinkedValueType( LinkedValueType.OBSERVED_VALUE )
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
     * Checks that the {@link BoxPlotErrorByObserved#getName()} returns 
     * {@link MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE.toString(), this.bpe.getName() );
    }

    /**
     * Checks that the {@link BoxPlotErrorByObserved#hasRealUnits()} returns <code>true</code>.
     */

    @Test
    public void testHasRealUnits()
    {
        assertTrue( this.bpe.hasRealUnits() );
    }

    /**
     * Tests the construction of a {@link BoxPlotErrorByObserved} with two probabilities.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testConstructionWithTwoProbabilities() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( BoxPlotErrorByObserved.of( VectorOfDoubles.of( 0.0, 1.0 ) ) ) );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        PoolException expected = assertThrows( PoolException.class, () -> this.bpe.apply( null ) );

        assertEquals( "Specify non-null input to the 'BOX PLOT OF ERRORS BY OBSERVED VALUE'.", expected.getMessage() );
    }

    /**
     * Tests for an expected exception on construction with too few probabilities. 
     */

    @Test
    public void testForExceptionOnTooFewProbabilities() throws MetricParameterException
    {
        VectorOfDoubles doubles = VectorOfDoubles.of( 0.1 );
        MetricParameterException expected =
                assertThrows( MetricParameterException.class,
                              () -> BoxPlotErrorByObserved.of( doubles ) );

        assertEquals( "Specify at least two probabilities for the verification box plot.", expected.getMessage() );
    }

    /**
     * Tests for an expected exception on construction with negative probabilities. 
     */

    @Test
    public void testForExceptionOnNegativeProbabilities() throws MetricParameterException
    {
        VectorOfDoubles doubles = VectorOfDoubles.of( -0.1, 0.0, 0.5 );
        MetricParameterException expected =
                assertThrows( MetricParameterException.class,
                              () -> BoxPlotErrorByObserved.of( doubles ) );

        assertEquals( "Specify only valid probabilities within [0,1] from which to construct the box plot.",
                      expected.getMessage() );
    }

    /**
     * Tests for an expected exception on construction with probabilities that are too high. 
     */

    @Test
    public void testForExceptionOnProbabilitiesGreaterThanOne() throws MetricParameterException
    {
        VectorOfDoubles doubles = VectorOfDoubles.of( 0.0, 0.5, 1.5 );
        MetricParameterException expected =
                assertThrows( MetricParameterException.class,
                              () -> BoxPlotErrorByObserved.of( doubles ) );

        assertEquals( "Specify only valid probabilities within [0,1] from which to construct the box plot.",
                      expected.getMessage() );
    }

    /**
     * Tests for an expected exception on construction with duplicate probabilities. 
     */

    @Test
    public void testForExceptionOnDuplicateProbabilities() throws MetricParameterException
    {
        VectorOfDoubles doubles = VectorOfDoubles.of( 0.0, 0.0, 1.0 );
        MetricParameterException expected =
                assertThrows( MetricParameterException.class,
                              () -> BoxPlotErrorByObserved.of( doubles ) );

        assertEquals( "Specify only non-unique probabilities from which to construct the box plot.",
                      expected.getMessage() );
    }

    /**
     * Tests for an expected exception on construction with an even number of probabilities. 
     */

    @Test
    public void testForExceptionOnEvenNumberOfProbabilities() throws MetricParameterException
    {
        VectorOfDoubles doubles = VectorOfDoubles.of( 0.0, 0.25, 0.5, 1.0 );
        MetricParameterException expected =
                assertThrows( MetricParameterException.class,
                              () -> BoxPlotErrorByObserved.of( doubles ) );

        assertEquals( "Specify an odd number of probabilities for the verification box plot.",
                      expected.getMessage() );
    }

}
