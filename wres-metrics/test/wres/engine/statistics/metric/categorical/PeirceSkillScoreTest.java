package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.Score;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

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

    private SampleMetadata meta;

    @Before
    public void setUpBeforeEachTest()
    {
        this.pss = PeirceSkillScore.of();
        this.meta = SampleMetadata.of( MeasurementUnit.of(),
                                                             DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                                   "SQIN",
                                                                                   "HEFS" ) );
    }

    /**
     * Compares the actual output from {@link PeirceSkillScore#apply(SampleData)} to the expected output.
     */

    @Test
    public void testApplyWithDichotomousInput()
    {
        //Generate some data
        SampleData<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.pss.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setName( ComponentName.MAIN )
                                                                               .setValue( 0.6347985347985348 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( PeirceSkillScore.METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, this.meta );

        assertEquals( expected, actual );
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

        DoubleScoreStatisticOuter actual = this.pss.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    /**
     * Verifies that {@link Metric#getName()} returns the expected result.
     */

    @Test
    public void testMetricIsNamedCorrectly()
    {
        assertTrue( this.pss.getName().equals( MetricConstants.PEIRCE_SKILL_SCORE.toString() ) );
    }

    /**
     * Verifies that {@link Score#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testMetricIsNotDecoposable()
    {
        assertFalse( this.pss.isDecomposable() );
    }

    /**
     * Verifies that {@link Score#isSkillScore()} returns <code>true</code>.
     */

    @Test
    public void testMetricIsASkillScore()
    {
        assertTrue( this.pss.isSkillScore() );
    }

    /**
     * Verifies that {@link Score#getScoreOutputGroup()} returns {@link MetricGroup#NONE}.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( this.pss.getScoreOutputGroup() == MetricGroup.NONE );
    }

    /**
     * Verifies that {@link Collectable#getCollectionOf()} returns {@link MetricConstants#CONTINGENCY_TABLE}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( this.pss.getCollectionOf() == MetricConstants.CONTINGENCY_TABLE );
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
                              () -> this.pss.aggregate( this.pss.aggregate( (DoubleScoreStatisticOuter) null ) ) );

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
        DoubleScoreStatistic table = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( ContingencyTable.METRIC )
                                                         .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                                      .setName( DoubleScoreMetricComponent.ComponentName.TRUE_POSITIVES )
                                                                                                      .setValue( 1.0 ) )
                                                         .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                                      .setName( DoubleScoreMetricComponent.ComponentName.TRUE_NEGATIVES )
                                                                                                      .setValue( 1.0 ) )
                                                         .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                                      .setName( DoubleScoreMetricComponent.ComponentName.FALSE_POSITIVES )
                                                                                                      .setValue( 1.0 ) )
                                                         .build();

        SampleDataException exception =
                assertThrows( SampleDataException.class,
                              () -> this.pss.aggregate( DoubleScoreStatisticOuter.of( table, this.meta ) ) );

        String expectedMessage = "Expected an intermediate result with a square number of elements when computing "
                                 + "the '"
                                 + this.pss.getName()
                                 + "': [3].";

        assertEquals( expectedMessage, exception.getMessage() );
    }

}
