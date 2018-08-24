package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.sampledata.MetricInputException;
import wres.datamodel.sampledata.pairs.DichotomousPairs;
import wres.datamodel.sampledata.pairs.MulticategoryPairs;
import wres.datamodel.statistics.DoubleScoreOutput;
import wres.datamodel.statistics.MatrixOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.Score;

/**
 * Tests the {@link PeirceSkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class PeirceSkillScoreTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Score used for testing. 
     */

    private PeirceSkillScore<DichotomousPairs> pss;

    /**
     * Metadata used for testing.
     */

    private MetricOutputMetadata meta;

    @Before
    public void setUpBeforeEachTest() throws MetricParameterException
    {
        pss = PeirceSkillScore.of();
        meta = MetricOutputMetadata.of( 365,
                                                  MeasurementUnit.of(),
                                                  MeasurementUnit.of(),
                                                  MetricConstants.PEIRCE_SKILL_SCORE,
                                                  MetricConstants.MAIN,
                                                  DatasetIdentifier.of(
                                                                                        Location.of( "DRRC2" ),
                                                                                        "SQIN",
                                                                                        "HEFS" ) );
    }

    /**
     * Compares the actual output from {@link PeirceSkillScore#apply(MulticategoryPairs)} with {@link DichotomousPairs}
     * to the expected output.
     */

    @Test
    public void testApplyWithDichotomousInput()
    {
        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Check the results
        final DoubleScoreOutput actual = pss.apply( input );
        final DoubleScoreOutput expected = DoubleScoreOutput.of( 0.6347985347985348, meta );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Compares the actual output from {@link PeirceSkillScore#apply(MulticategoryPairs)} with 
     * {@link MulticategoryPairs} to the expected output.
     * @throws MetricParameterException if the multicategory metric could not be constructed
     */

    @Test
    public void testApplyWithMulticategoryInput() throws MetricParameterException
    {
        //Generate some data
        final MulticategoryPairs input = MetricTestDataFactory.getMulticategoryPairsOne();

        PeirceSkillScore<MulticategoryPairs> ps = PeirceSkillScore.of();

        //Check the results
        final DoubleScoreOutput actual = ps.apply( input );
        final DoubleScoreOutput expected =
                DoubleScoreOutput.of( 0.05057466520850963, MetricOutputMetadata.of( meta, input.getRawData().size() ) );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link Metric#apply(DichotomousPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        DichotomousPairs input =
                DichotomousPairs.ofDichotomousPairs( Arrays.asList(), Metadata.of() );

        DoubleScoreOutput actual = pss.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Verifies that {@link Metric#getName()} returns the expected result.
     */

    @Test
    public void testMetricIsNamedCorrectly()
    {
        assertTrue( pss.getName().equals( MetricConstants.PEIRCE_SKILL_SCORE.toString() ) );
    }

    /**
     * Verifies that {@link Score#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testMetricIsNotDecoposable()
    {
        assertFalse( pss.isDecomposable() );
    }

    /**
     * Verifies that {@link Score#isSkillScore()} returns <code>true</code>.
     */

    @Test
    public void testMetricIsASkillScore()
    {
        assertTrue( pss.isSkillScore() );
    }

    /**
     * Verifies that {@link Score#getScoreOutputGroup()} returns {@link ScoreOutputGroup#NONE}.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( pss.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Verifies that {@link Collectable#getCollectionOf()} returns {@link MetricConstants#CONTINGENCY_TABLE}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( pss.getCollectionOf() == MetricConstants.CONTINGENCY_TABLE );
    }

    /**
     * Checks for an exception when calling {@link Collectable#aggregate(wres.datamodel.statistics.MetricOutput)} with 
     * null input.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the '" + pss.getName() + "'." );
        pss.aggregate( (MatrixOutput) null );
    }

    /**
     * Checks for an exception when calling {@link Collectable#aggregate(wres.datamodel.statistics.MetricOutput)} with 
     * input that is not square.
     */

    @Test
    public void testExceptionOnInputThatIsNotSquare()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Expected an intermediate result with a square matrix when computing the "
                                 + "'"
                                 + pss.getName()
                                 + "': [2, 3]." );
        pss.aggregate( MatrixOutput.of( new double[][] { { 0.0, 0.0, 0.0 }, { 0.0, 0.0, 0.0 } }, meta ) );
    }

    /**
     * Checks for an exception when calling {@link Collectable#aggregate(wres.datamodel.statistics.MetricOutput)} with 
     * input that does not have a positive sum product.
     */

    @Test
    public void testExceptionOnInputWithZeroSumProduct()
    {
        exception.expect( MetricCalculationException.class );
        exception.expectMessage( "The sum product of the rows and columns in the contingency table "
                                 + "must exceed zero when computing the '"
                                 + pss.getName()
                                 + "': 0.0" );
        pss.aggregate( MatrixOutput.of( new double[][] { { 0.0, 0.0, 0.0 }, { 0.0, 0.0, 0.0 },
                                                                    { 0.0, 0.0, 0.0 } },
                                                   meta ) );
    }


}
