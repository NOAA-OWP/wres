package wres.metrics.categorical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.pools.Pool;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Boilerplate;
import wres.metrics.Collectable;
import wres.metrics.Metric;
import wres.metrics.MetricCalculationException;
import wres.metrics.MetricTestDataFactory;
import wres.metrics.Score;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link ProbabilityOfDetection}.
 * 
 * @author James Brown
 */
public final class ProbabilityOfDetectionTest
{

    /**
     * Score used for testing. 
     */

    private ProbabilityOfDetection pod;

    /**
     * Metadata used for testing.
     */

    private PoolMetadata meta;

    @Before
    public void setUpBeforeEachTest()
    {
        this.pod = ProbabilityOfDetection.of();
        this.meta = Boilerplate.getPoolMetadata( false );
    }

    /**
     * Compares the output from {@link Metric#apply(Pool)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.pod.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( ProbabilityOfDetection.MAIN )
                                                                               .setValue( 0.780952380952381 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( ProbabilityOfDetection.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, this.meta );

        assertEquals( expected, actual );
    }

    /**
     * Validates the output from {@link Metric#apply(Pool)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Boolean, Boolean>> input = Pool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.pod.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    /**
     * Verifies that {@link Metric#getName()} returns the expected result.
     */

    @Test
    public void testMetricIsNamedCorrectly()
    {
        assertEquals( MetricConstants.PROBABILITY_OF_DETECTION.toString(), this.pod.getName() );
    }

    /**
     * Verifies that {@link Score#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testMetricIsNotDecoposable()
    {
        assertFalse( this.pod.isDecomposable() );
    }

    /**
     * Verifies that {@link Score#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testMetricIsASkillScore()
    {
        assertFalse( this.pod.isSkillScore() );
    }

    /**
     * Verifies that {@link Score#getScoreOutputGroup()} returns {@link OutputScoreGroup#NONE}.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.pod.getScoreOutputGroup() );
    }

    /**
     * Verifies that {@link Collectable#getCollectionOf()} returns {@link MetricConstants#CONTINGENCY_TABLE}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertSame( MetricConstants.CONTINGENCY_TABLE , this.pod.getCollectionOf() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        MetricCalculationException actual = assertThrows( MetricCalculationException.class,
                                                   () -> this.pod.aggregate( (DoubleScoreStatisticOuter) null, null ) );

        assertEquals( "Specify non-null input to the '" + this.pod.getName() + "'.", actual.getMessage() );
    }

}
