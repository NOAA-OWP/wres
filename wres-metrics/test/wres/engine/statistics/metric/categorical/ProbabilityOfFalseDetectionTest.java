package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertEquals;
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
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.Score;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

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
     * Score used for testing. 
     */

    private ProbabilityOfFalseDetection pofd;

    /**
     * Metadata used for testing.
     */

    private StatisticMetadata meta;

    @Before
    public void setUpBeforeEachTest()
    {
        pofd = ProbabilityOfFalseDetection.of();
        meta = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(),
                                                        DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                              "SQIN",
                                                                              "HEFS" ) ),
                                     365,
                                     MeasurementUnit.of(),
                                     MetricConstants.PROBABILITY_OF_FALSE_DETECTION,
                                     MetricConstants.MAIN );
    }

    /**
     * Compares the output from {@link Metric#apply(SampleData)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        SampleData<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.pofd.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setName( ComponentName.MAIN )
                                                                               .setValue( 0.14615384615384616 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( ProbabilityOfFalseDetection.METRIC )
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

        DoubleScoreStatisticOuter actual = this.pofd.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    /**
     * Verifies that {@link Metric#getName()} returns the expected result.
     */

    @Test
    public void testMetricIsNamedCorrectly()
    {
        assertTrue( this.pofd.getName().equals( MetricConstants.PROBABILITY_OF_FALSE_DETECTION.toString() ) );
    }

    /**
     * Verifies that {@link Score#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testMetricIsNotDecoposable()
    {
        assertFalse( this.pofd.isDecomposable() );
    }

    /**
     * Verifies that {@link Score#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testMetricIsASkillScore()
    {
        assertFalse( this.pofd.isSkillScore() );
    }

    /**
     * Verifies that {@link Score#getScoreOutputGroup()} returns {@link OutputScoreGroup#NONE}.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( this.pofd.getScoreOutputGroup() == MetricGroup.NONE );
    }

    /**
     * Verifies that {@link Collectable#getCollectionOf()} returns {@link MetricConstants#CONTINGENCY_TABLE}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( this.pofd.getCollectionOf() == MetricConstants.CONTINGENCY_TABLE );
    }

    /**
     * Checks for an exception when calling {@link Collectable#aggregate(wres.datamodel.statistics.MetricOutput)} with 
     * null input.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the '" + this.pofd.getName() + "'." );
        pofd.aggregate( (DoubleScoreStatisticOuter) null );
    }

}
