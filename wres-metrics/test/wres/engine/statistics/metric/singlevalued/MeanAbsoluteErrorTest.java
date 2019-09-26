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
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.TimeSeriesOfPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link MeanAbsoluteError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MeanAbsoluteErrorTest
{
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Default instance of a {@link MeanAbsoluteError}.
     */

    private MeanAbsoluteError mae;

    @Before
    public void setupBeforeEachTest()
    {
        this.mae = MeanAbsoluteError.of();
    }
    
    @Test
    public void testApply()
    {
        //Generate some data
        TimeSeriesOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                     input.getRawData().size(),
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                     MetricConstants.MAIN );
        //Check the results
        final DoubleScoreStatistic actual = mae.apply( input );
        final DoubleScoreStatistic expected = DoubleScoreStatistic.of( 201.37, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );
 
        DoubleScoreStatistic actual = mae.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    @Test
    public void testGetName()
    {
        assertTrue( mae.getName().equals( MetricConstants.MEAN_ABSOLUTE_ERROR.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( mae.isDecomposable() );
    }
    
    @Test
    public void testIsSkillScore()
    {
        assertFalse( mae.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( mae.getScoreOutputGroup() == ScoreGroup.NONE );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN ABSOLUTE ERROR'." );
        
        mae.apply( null );
    }    

}
