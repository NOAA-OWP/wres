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
import wres.datamodel.Probability;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link BrierSkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BrierSkillScoreTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link BrierSkillScore}.
     */

    private BrierSkillScore brierSkillScore;

    @Before
    public void setupBeforeEachTest()
    {
        this.brierSkillScore = BrierSkillScore.of();
    }

    /**
     * Compares the output from {@link BrierSkillScore#apply(SampleData)} against expected output for a 
     * dataset with a supplied baseline.
     */

    @Test
    public void testApplyWithSuppliedBaseline()
    {
        // Generate some data
        SampleData<Pair<Probability,Probability>> input = MetricTestDataFactory.getDiscreteProbabilityPairsTwo();

        // Metadata for the output
        StatisticMetadata m1 =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(),
                                                         DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                               "SQIN",
                                                                               "HEFS",
                                                                               "ESP") ),
                                      input.getRawData().size(),
                                      MeasurementUnit.of(),
                                      MetricConstants.BRIER_SKILL_SCORE,
                                      MetricConstants.MAIN );

        // Check the results       
        final DoubleScoreStatistic actual = brierSkillScore.apply( input );
        final DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.11363636363636376, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Compares the output from {@link BrierSkillScore#apply(SampleData)} against expected output for a 
     * dataset with a climatological baseline.
     */

    @Test
    public void testApplyWithClimatologicalBaseline()
    {
        // Generate some data
        SampleData<Pair<Probability,Probability>> input = MetricTestDataFactory.getDiscreteProbabilityPairsOne();

        // Metadata for the output
        StatisticMetadata m1 =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(),
                                                         DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                               "SQIN",
                                                                               "HEFS" ) ),
                                      input.getRawData().size(),
                                      MeasurementUnit.of(),
                                      MetricConstants.BRIER_SKILL_SCORE,
                                      MetricConstants.MAIN );

        // Check the results       
        final DoubleScoreStatistic actual = brierSkillScore.apply( input );
        final DoubleScoreStatistic expected = DoubleScoreStatistic.of( -0.040000000000000036, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }


    /**
     * Validates the output from {@link BrierSkillScore#apply(SampleData)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleData<Pair<Probability,Probability>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatistic actual = brierSkillScore.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link BrierSkillScore#getName()} returns {@link MetricConstants.BRIER_SKILL_SCORE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( brierSkillScore.getName().equals( MetricConstants.BRIER_SKILL_SCORE.toString() ) );
    }

    /**
     * Checks that the {@link BrierSkillScore#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( brierSkillScore.isDecomposable() );
    }

    /**
     * Checks that the {@link BrierSkillScore#isSkillScore()} returns <code>true</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertTrue( brierSkillScore.isSkillScore() );
    }

    /**
     * Checks that the {@link BrierSkillScore#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( brierSkillScore.getScoreOutputGroup() == MetricGroup.NONE );
    }

    /**
     * Checks that the {@link BrierSkillScore#isProper()} returns <code>false</code>.
     */

    @Test
    public void testIsProper()
    {
        assertFalse( brierSkillScore.isProper() );
    }

    /**
     * Checks that the {@link BrierSkillScore#isStrictlyProper()} returns <code>false</code>.
     */

    @Test
    public void testIsStrictlyProper()
    {
        assertFalse( brierSkillScore.isStrictlyProper() );
    }

    /**
     * Tests for an expected exception on calling {@link BrierSkillScore#apply(DiscreteProbabilityPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'BRIER SKILL SCORE'." );

        brierSkillScore.apply( null );
    }

    /**
     * Tests for {@link Double#NaN} output when supplying {@link BrierSkillScore#apply(DiscreteProbabilityPairs)} with 
     * a baseline whose input is {@link Double#NaN}.
     */

    @Test
    public void testApplyNaNOutputWithNaNBaseline()
    {
        assertTrue( "Expected NaN for a forecast with only non-occurrences as the baseline.",
                    Double.isNaN( brierSkillScore.apply( MetricTestDataFactory.getDiscreteProbabilityPairsFour() )
                                                 .getData() ) );
    }
}
