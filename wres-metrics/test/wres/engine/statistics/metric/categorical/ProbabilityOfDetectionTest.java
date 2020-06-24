package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.Boilerplate;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.Score;

/**
 * Tests the {@link ProbabilityOfDetection}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ProbabilityOfDetectionTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Score used for testing. 
     */

    private ProbabilityOfDetection pod;

    /**
     * Metadata used for testing.
     */

    private StatisticMetadata meta;

    @Before
    public void setUpBeforeEachTest()
    {
        pod = ProbabilityOfDetection.of();
        meta = StatisticMetadata.of( Boilerplate.getSampleMetadata(),
                                     365,
                                     MeasurementUnit.of(),
                                     MetricConstants.PROBABILITY_OF_DETECTION,
                                     MetricConstants.MAIN );
    }

    /**
     * Compares the output from {@link Metric#apply(SampleData)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        final SampleData<Pair<Boolean,Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Check the results
        final DoubleScoreStatistic actual = pod.apply( input );
        final DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.780952380952381, meta );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link Metric#apply(SampleData)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleData<Pair<Boolean,Boolean>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatistic actual = pod.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Verifies that {@link Metric#getName()} returns the expected result.
     */

    @Test
    public void testMetricIsNamedCorrectly()
    {
        assertTrue( pod.getName().equals( MetricConstants.PROBABILITY_OF_DETECTION.toString() ) );
    }

    /**
     * Verifies that {@link Score#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testMetricIsNotDecoposable()
    {
        assertFalse( pod.isDecomposable() );
    }

    /**
     * Verifies that {@link Score#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testMetricIsASkillScore()
    {
        assertFalse( pod.isSkillScore() );
    }

    /**
     * Verifies that {@link Score#getScoreOutputGroup()} returns {@link OutputScoreGroup#NONE}.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( pod.getScoreOutputGroup() == MetricGroup.NONE );
    }

    /**
     * Verifies that {@link Collectable#getCollectionOf()} returns {@link MetricConstants#CONTINGENCY_TABLE}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( pod.getCollectionOf() == MetricConstants.CONTINGENCY_TABLE );
    }

    /**
     * Checks for an exception when calling {@link Collectable#aggregate(wres.datamodel.statistics.MetricOutput)} with 
     * null input.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the '" + pod.getName() + "'." );
        pod.aggregate( (DoubleScoreStatistic) null );
    }

}
