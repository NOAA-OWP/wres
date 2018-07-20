package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.Score;

/**
 * Tests the {@link ProbabilityOfFalseDetection}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ProbabilityOfFalseDetectionTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Metric factory.
     */

    private MetricFactory metricFactory;

    /**
     * Score used for testing. 
     */

    private ProbabilityOfFalseDetection pofd;

    /**
     * Metadata used for testing.
     */

    private MetricOutputMetadata meta;

    @Before
    public void setUpBeforeEachTest() throws MetricParameterException
    {
        metricFactory = MetricFactory.getInstance();
        pofd = metricFactory.ofProbabilityOfFalseDetection();
        meta = MetadataFactory.getOutputMetadata( 365,
                                                  MetadataFactory.getDimension(),
                                                  MetadataFactory.getDimension(),
                                                  MetricConstants.PROBABILITY_OF_FALSE_DETECTION,
                                                  MetricConstants.MAIN,
                                                  MetadataFactory.getDatasetIdentifier( MetadataFactory.getLocation( "DRRC2" ),
                                                                                        "SQIN",
                                                                                        "HEFS" ) );
    }

    /**
     * Compares the output from {@link Metric#apply(wres.datamodel.inputs.MetricInput)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Check the results
        final DoubleScoreOutput actual = pofd.apply( input );
        final DoubleScoreOutput expected = DataFactory.ofDoubleScoreOutput( 0.14615384615384616, meta );
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
                DataFactory.ofDichotomousPairs( Arrays.asList(), MetadataFactory.getMetadata() );

        DoubleScoreOutput actual = pofd.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Verifies that {@link Metric#getName()} returns the expected result.
     */

    @Test
    public void testMetricIsNamedCorrectly()
    {
        assertTrue( pofd.getName().equals( MetricConstants.PROBABILITY_OF_FALSE_DETECTION.toString() ) );
    }

    /**
     * Verifies that {@link Score#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testMetricIsNotDecoposable()
    {
        assertFalse( pofd.isDecomposable() );
    }

    /**
     * Verifies that {@link Score#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testMetricIsASkillScore()
    {
        assertFalse( pofd.isSkillScore() );
    }

    /**
     * Verifies that {@link Score#getScoreOutputGroup()} returns {@link OutputScoreGroup#NONE}.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( pofd.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Verifies that {@link Collectable#getCollectionOf()} returns {@link MetricConstants#CONTINGENCY_TABLE}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( pofd.getCollectionOf() == MetricConstants.CONTINGENCY_TABLE );
    }

    /**
     * Checks for an exception when calling {@link Collectable#aggregate(wres.datamodel.outputs.MetricOutput)} with 
     * null input.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the '" + pofd.getName() + "'." );
        pofd.aggregate( (MatrixOutput) null );
    }

}
