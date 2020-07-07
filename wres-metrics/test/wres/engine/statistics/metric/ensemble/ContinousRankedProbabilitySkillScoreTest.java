package wres.engine.statistics.metric.ensemble;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link ContinuousRankedProbabilitySkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ContinousRankedProbabilitySkillScoreTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link ContinuousRankedProbabilitySkillScore}.
     */

    private ContinuousRankedProbabilitySkillScore crpss;

    @Before
    public void setupBeforeEachTest()
    {
        this.crpss = ContinuousRankedProbabilitySkillScore.of();
    }

    /**
     * Compares the output from {@link ContinuousRankedProbabilitySkillScore#apply(SampleData)} against expected 
     * output for a dataset with a supplied baseline.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        List<Pair<Double, Ensemble>> pairs = new ArrayList<>();
        pairs.add( Pair.of( 25.7, Ensemble.of( 23, 43, 45, 23, 54 ) ) );
        pairs.add( Pair.of( 21.4, Ensemble.of( 19, 16, 57, 23, 9 ) ) );
        pairs.add( Pair.of( 32.1, Ensemble.of( 23, 54, 23, 12, 32 ) ) );
        pairs.add( Pair.of( 47.0, Ensemble.of( 12, 54, 23, 54, 78 ) ) );
        pairs.add( Pair.of( 12.0, Ensemble.of( 9, 8, 5, 6, 12 ) ) );
        pairs.add( Pair.of( 43.0, Ensemble.of( 23, 12, 12, 34, 10 ) ) );
        List<Pair<Double, Ensemble>> basePairs = new ArrayList<>();
        basePairs.add( Pair.of( 25.7, Ensemble.of( 20, 43, 45, 23, 94 ) ) );
        basePairs.add( Pair.of( 21.4, Ensemble.of( 19, 76, 57, 23, 9 ) ) );
        basePairs.add( Pair.of( 32.1, Ensemble.of( 23, 53, 23, 12, 32 ) ) );
        basePairs.add( Pair.of( 47.0, Ensemble.of( 2, 54, 23, 54, 78 ) ) );
        basePairs.add( Pair.of( 12.1, Ensemble.of( 9, 18, 5, 6, 12 ) ) );
        basePairs.add( Pair.of( 43.0, Ensemble.of( 23, 12, 12, 39, 10 ) ) );

        SampleData<Pair<Double, Ensemble>> input = SampleDataBasic.of( pairs,
                                                                       SampleMetadata.of(),
                                                                       basePairs,
                                                                       SampleMetadata.of(),
                                                                       null );

        //Metadata for the output
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                           input.getRawData().size(),
                                                           MeasurementUnit.of(),
                                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                           MetricConstants.MAIN );
        //Check the results       
        DoubleScoreStatistic actual = crpss.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.0779168348809044, m1 );

        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link ContinuousRankedProbabilitySkillScore#apply(SampleData)} when supplied 
     * with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleData<Pair<Double, Ensemble>> input =
                SampleDataBasic.of( Arrays.asList(),
                                    SampleMetadata.of(),
                                    Arrays.asList(),
                                    SampleMetadata.of(),
                                    null );

        DoubleScoreStatistic actual = crpss.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilitySkillScore#getName()} returns 
     * {@link MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( crpss.getName().equals( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE.toString() ) );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilitySkillScore#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( crpss.isDecomposable() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilitySkillScore#isSkillScore()} returns <code>true</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertTrue( crpss.isSkillScore() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilitySkillScore#getScoreOutputGroup()} returns the result 
     * provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( crpss.getScoreOutputGroup() == MetricGroup.NONE );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilitySkillScore#isProper()} returns <code>false</code>.
     */

    @Test
    public void testIsProper()
    {
        assertFalse( crpss.isProper() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilitySkillScore#isStrictlyProper()} returns <code>false</code>.
     */

    @Test
    public void testIsStrictlyProper()
    {
        assertFalse( crpss.isStrictlyProper() );
    }

    /**
     * Checks that the baseline identifier is correctly propagated to the metric output metadata.
     * @throws IOException if the input pairs could not be read
     */

    @Test
    public void testMetadataContainsBaselineIdentifier() throws IOException
    {
        SampleData<Pair<Double, Ensemble>> pairs = MetricTestDataFactory.getEnsemblePairsOne();

        assertTrue( crpss.apply( pairs )
                         .getMetadata()
                         .getSampleMetadata()
                         .getIdentifier()
                         .getScenarioNameForBaseline()
                         .equals( "ESP" ) );
    }

    /**
     * Tests for an expected exception on calling {@link ContinuousRankedProbabilitySkillScore#apply(SampleData)} 
     * with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'CONTINUOUS RANKED PROBABILITY SKILL SCORE'." );

        crpss.apply( null );
    }

    /**
     * Tests for an expected exception on building a {@link ContinuousRankedProbabilitySkillScore} with 
     * an unrecognized decomposition identifier.
     * @throws MetricParameterException if the metric could not be built for an unexpected reason
     */

    @Test
    public void testApplyExceptionOnUnrecognizedDecompositionIdentifier() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Unsupported decomposition identifier 'LBR'." );

        ContinuousRankedProbabilitySkillScore.of( MetricGroup.LBR );
    }

    /**
     * Checks for an expected exception when the input data does not contain an explicit baseline.
     */

    @Test
    public void testExceptionOnInputWithMissingBaseline()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify a non-null baseline for the 'CONTINUOUS RANKED PROBABILITY SKILL SCORE'." );
        List<Pair<Double,Ensemble>> pairs = new ArrayList<>();
        pairs.add( Pair.of( 25.7, Ensemble.of( 23, 43, 45, 23, 54 ) ) );
        SampleData<Pair<Double, Ensemble>> input = SampleDataBasic.of( pairs, SampleMetadata.of() );
        crpss.apply( input );
    }


}
