package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link MeanSquareError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MeanSquareErrorTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

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

        //Metadata for the output
        SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of() );
        
        //Check the results
        DoubleScoreStatisticOuter actual = this.mse.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setName( ComponentName.MAIN )
                                                                               .setValue( 400003.929 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( MeanSquareError.METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );
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
        assertTrue( mse.getName().equals( MetricConstants.MEAN_SQUARE_ERROR.toString() ) );
    }


    @Test
    public void testIsDecomposable()
    {
        assertTrue( mse.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( mse.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( mse.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN SQUARE ERROR'." );

        mse.apply( null );
    }

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN SQUARE ERROR'." );

        mse.aggregate( null );
    }

}
