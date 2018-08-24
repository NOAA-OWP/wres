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
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link BiasFraction}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BiasFractionTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Default instance of a {@link BiasFraction}.
     */

    private BiasFraction biasFraction;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.biasFraction = BiasFraction.of();
    }

    /**
     * Compares the output from {@link BiasFraction#apply(SingleValuedPairs)} against expected output.
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
                                                                   MetricConstants.BIAS_FRACTION,
                                                                   MetricConstants.MAIN );
        //Check the results
        DoubleScoreStatistic actual = biasFraction.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.056796297974534414, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link BiasFraction#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                SingleValuedPairs.of( Arrays.asList(), Metadata.of() );
 
        DoubleScoreStatistic actual = biasFraction.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link BiasFraction#getName()} returns {@link MetricConstants#BIAS_FRACTION.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( biasFraction.getName().equals( MetricConstants.BIAS_FRACTION.toString() ) );
    }

    /**
     * Checks that the {@link BiasFraction#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( biasFraction.isDecomposable() );
    }

    /**
     * Checks that the {@link BiasFraction#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( biasFraction.isSkillScore() );
    }

    /**
     * Checks that the {@link BiasFraction#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( biasFraction.getScoreOutputGroup() == ScoreGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link BiasFraction#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'BIAS FRACTION'." );
        
        biasFraction.apply( null );
    }

}
