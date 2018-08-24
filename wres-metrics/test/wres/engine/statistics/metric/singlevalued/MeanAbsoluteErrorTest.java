package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.MetricParameterException;
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
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.mae = MeanAbsoluteError.of();
    }

    /**
     * Compares the output from {@link MeanAbsoluteError#apply(SingleValuedPairs)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        StatisticMetadata m1 = StatisticMetadata.of( input.getRawData().size(),
                                                                   MeasurementUnit.of(),
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

    /**
     * Validates the output from {@link MeanAbsoluteError#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                SingleValuedPairs.of( Arrays.asList(), SampleMetadata.of() );
 
        DoubleScoreStatistic actual = mae.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link MeanAbsoluteError#getName()} returns 
     * {@link MetricConstants#MEAN_ABSOLUTE_ERROR.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( mae.getName().equals( MetricConstants.MEAN_ABSOLUTE_ERROR.toString() ) );
    }

    /**
     * Checks that the {@link MeanAbsoluteError#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( mae.isDecomposable() );
    }

    /**
     * Checks that the {@link MeanAbsoluteError#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( mae.isSkillScore() );
    }

    /**
     * Checks that the {@link MeanAbsoluteError#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( mae.getScoreOutputGroup() == ScoreGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link MeanAbsoluteError#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN ABSOLUTE ERROR'." );
        
        mae.apply( null );
    }    

}
