package wres.engine.statistics.metric.discreteprobability;

import static org.junit.Assert.assertEquals;
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
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.Boilerplate;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

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
        SampleData<Pair<Probability, Probability>> input = MetricTestDataFactory.getDiscreteProbabilityPairsTwo();

        // Metadata for the output
        SampleMetadata m1 = Boilerplate.getSampleMetadata();

        // Check the results       
        DoubleScoreStatisticOuter actual = this.brierSkillScore.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setName( ComponentName.MAIN )
                                                                               .setValue( 0.11363636363636376 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( BrierSkillScore.METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected.getData(), actual.getData() );
    }

    /**
     * Compares the output from {@link BrierSkillScore#apply(SampleData)} against expected output for a 
     * dataset with a climatological baseline.
     */

    @Test
    public void testApplyWithClimatologicalBaseline()
    {
        // Generate some data
        SampleData<Pair<Probability, Probability>> input = MetricTestDataFactory.getDiscreteProbabilityPairsOne();

        // Metadata for the output
        SampleMetadata m1 = Boilerplate.getSampleMetadata();

        // Check the results
        DoubleScoreStatisticOuter actual = this.brierSkillScore.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setName( ComponentName.MAIN )
                                                                               .setValue( -0.040000000000000036 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( BrierSkillScore.METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );
    }


    /**
     * Validates the output from {@link BrierSkillScore#apply(SampleData)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleData<Pair<Probability, Probability>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatisticOuter actual = brierSkillScore.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
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
        assertEquals( Double.NaN,
                      this.brierSkillScore.apply( MetricTestDataFactory.getDiscreteProbabilityPairsFour() )
                                          .getComponent( MetricConstants.MAIN )
                                          .getData()
                                          .getValue(),
                      0.0 );
    }
}
