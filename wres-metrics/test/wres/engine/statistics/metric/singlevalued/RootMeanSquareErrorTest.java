package wres.engine.statistics.metric.singlevalued;

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
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link RootMeanSquareError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class RootMeanSquareErrorTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Default instance of a {@link RootMeanSquareError}.
     */

    private RootMeanSquareError rmse;

    @Before
    public void setupBeforeEachTest()
    {
        this.rmse = RootMeanSquareError.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                     input.getRawData().size(),
                                                     MeasurementUnit.of(),
                                                     MetricConstants.ROOT_MEAN_SQUARE_ERROR,
                                                     MetricConstants.MAIN );
        //Check the results
        DoubleScoreStatistic actual = rmse.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 632.4586381732801, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );
 
        DoubleScoreStatistic actual = rmse.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    @Test
    public void testGetName()
    {
        assertTrue( rmse.getName().equals( MetricConstants.ROOT_MEAN_SQUARE_ERROR.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( rmse.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( rmse.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( rmse.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( rmse.getCollectionOf().equals( MetricConstants.SUM_OF_SQUARE_ERROR ) );
    }   
    
    @Test
    public void testHasRealUnits()
    {
        assertTrue( rmse.hasRealUnits() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'ROOT MEAN SQUARE ERROR'." );
        
        rmse.apply( null );
    }    

}
