package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.MetricConstants;
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
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.Score;

/**
 * Tests the {@link PeirceSkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class PeirceSkillScoreTest
{

    /**
     * Score used for testing. 
     */

    private PeirceSkillScore pss;

    /**
     * Metadata used for testing.
     */

    private StatisticMetadata meta;

    @Before
    public void setUpBeforeEachTest()
    {
        pss = PeirceSkillScore.of();
        meta = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(),
                                                        DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                              "SQIN",
                                                                              "HEFS" ) ),
                                     365,
                                     MeasurementUnit.of(),
                                     MetricConstants.PEIRCE_SKILL_SCORE,
                                     MetricConstants.MAIN );
    }

    /**
     * Compares the actual output from {@link PeirceSkillScore#apply(SampleData)} to the expected output.
     */

    @Test
    public void testApplyWithDichotomousInput()
    {
        //Generate some data
        final SampleData<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Check the results
        final DoubleScoreStatistic actual = pss.apply( input );
        final DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.6347985347985348, meta );
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
        SampleData<Pair<Boolean, Boolean>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatistic actual = pss.apply( input );

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
     * Verifies that {@link Score#getScoreOutputGroup()} returns {@link MetricGroup#NONE}.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( pss.getScoreOutputGroup() == MetricGroup.NONE );
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
        SampleDataException exception =
                assertThrows( SampleDataException.class,
                              () -> pss.aggregate( pss.aggregate( (DoubleScoreStatistic) null ) ) );

        String expectedMessage = "Specify non-null input to the '" + this.pss.getName() + "'.";

        assertEquals( expectedMessage, exception.getMessage() );
    }

    /**
     * Checks for an exception when calling {@link Collectable#aggregate(wres.datamodel.statistics.MetricOutput)} with 
     * input that is not square.
     */

    @Test
    public void testExceptionOnInputThatIsNotSquare()
    {
        Map<MetricConstants, Double> elements = new HashMap<>();
        elements.put( MetricConstants.TRUE_POSITIVES, 1.0 );
        elements.put( MetricConstants.TRUE_NEGATIVES, 1.0 );
        elements.put( MetricConstants.FALSE_POSITIVES, 1.0 );

        SampleDataException exception =
                assertThrows( SampleDataException.class,
                              () -> pss.aggregate( DoubleScoreStatistic.of( elements, this.meta ) ) );

        String expectedMessage = "Expected an intermediate result with a square number of elements when computing "
                                 + "the '"
                                 + this.pss.getName()
                                 + "': [3].";

        assertEquals( expectedMessage, exception.getMessage() );
    }

}
