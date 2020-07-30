package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link SumOfSquareError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SumOfSquareErrorTest
{

    /**
     * Default instance of a {@link SumOfSquareError}.
     */

    private SumOfSquareError sse;

    @Before
    public void setupBeforeEachTest()
    {
        this.sse = SumOfSquareError.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Check the results
        DoubleScoreStatisticOuter actual = this.sse.apply( input );

        DoubleScoreMetricComponent metricComponent = SumOfSquareError.MAIN.toBuilder()
                                                                          .setUnits( input.getMetadata()
                                                                                          .getMeasurementUnit()
                                                                                          .toString() )
                                                                          .build();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricComponent )
                                                                               .setValue( 4000039.29 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( SumOfSquareError.BASIC_METRIC )
                                                            .addStatistics( component )
                                                            .setSampleSize( 10 )
                                                            .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatisticOuter actual = sse.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertTrue( this.sse.getName().equals( MetricConstants.SUM_OF_SQUARE_ERROR.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.sse.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.sse.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( this.sse.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( this.sse.getCollectionOf().equals( MetricConstants.SUM_OF_SQUARE_ERROR ) );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        SampleDataException actual = assertThrows( SampleDataException.class,
                                                   () -> this.sse.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.sse.getName() + "'.", actual.getMessage() );
    }

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        SampleDataException actual = assertThrows( SampleDataException.class,
                                                   () -> this.sse.aggregate( null ) );

        assertEquals( "Specify non-null input to the '" + this.sse.getName() + "'.", actual.getMessage() );
    }

}
