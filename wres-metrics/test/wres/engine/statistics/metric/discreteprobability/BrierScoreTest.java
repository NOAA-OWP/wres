package wres.engine.statistics.metric.discreteprobability;

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
import wres.datamodel.Probability;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.Boilerplate;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link BrierScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BrierScoreTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link BrierScore}.
     */

    private BrierScore brierScore;

    @Before
    public void setupBeforeEachTest()
    {
        this.brierScore = BrierScore.of();
    }

    /**
     * Compares the output from {@link BrierScore#apply(SampleData)} against expected output.
     */

    @Test
    public void testApply()
    {
        // Generate some data
        SampleData<Pair<Probability,Probability>> input = MetricTestDataFactory.getDiscreteProbabilityPairsOne();

        // Metadata for the output
        StatisticMetadata m1 =
                StatisticMetadata.of( Boilerplate.getSampleMetadata(),
                                      input.getRawData().size(),
                                      MeasurementUnit.of(),
                                      MetricConstants.BRIER_SCORE,
                                      MetricConstants.MAIN );

        // Check the results       
        DoubleScoreStatistic actual = brierScore.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.26, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link BrierScore#apply(SampleData)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleData<Pair<Probability,Probability>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatistic actual = brierScore.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link BrierScore#getName()} returns {@link MetricConstants.BRIER_SCORE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( brierScore.getName().equals( MetricConstants.BRIER_SCORE.toString() ) );
    }

    /**
     * Checks that the {@link BrierScore#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( brierScore.isDecomposable() );
    }

    /**
     * Checks that the {@link BrierScore#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( brierScore.isSkillScore() );
    }

    /**
     * Checks that the {@link BrierScore#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( brierScore.getScoreOutputGroup() == MetricGroup.NONE );
    }

    /**
     * Checks that the {@link BrierScore#isProper()} returns <code>true</code>.
     */

    @Test
    public void testIsProper()
    {
        assertTrue( brierScore.isProper() );
    }

    /**
     * Checks that the {@link BrierScore#isStrictlyProper()} returns <code>true</code>.
     */

    @Test
    public void testIsStrictlyProper()
    {
        assertTrue( brierScore.isStrictlyProper() );
    }

    /**
     * Tests for an expected exception on calling {@link BrierScore#apply(DiscreteProbabilityPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'BRIER SCORE'." );

        brierScore.apply( null );
    }

}
