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
 * Tests the {@link MeanSquareError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MeanSquareErrorTest
{

    /**
     * Default instance of a {@link MeanSquareError}.
     */

    private MeanSquareError mse;

    @Before
    public void setupBeforeEachTest()
    {
        this.mse = MeanSquareError.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.mse.apply( input );

        DoubleScoreMetricComponent metricComponent = MeanSquareError.MAIN.toBuilder()
                                                                         .setUnits( input.getMetadata()
                                                                                         .getMeasurementUnit()
                                                                                         .toString() )
                                                                         .build();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricComponent )
                                                                               .setValue( 400003.929 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( MeanSquareError.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatisticOuter actual = mse.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertTrue( this.mse.getName().equals( MetricConstants.MEAN_SQUARE_ERROR.toString() ) );
    }


    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.mse.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.mse.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( this.mse.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        SampleDataException actual = assertThrows( SampleDataException.class,
                                                   () -> this.mse.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.mse.getName() + "'.", actual.getMessage() );
    }

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        SampleDataException actual = assertThrows( SampleDataException.class,
                                                   () -> mse.aggregate( null ) );

        assertEquals( "Specify non-null input to the '" + this.mse.getName() + "'.", actual.getMessage() );
    }

}
