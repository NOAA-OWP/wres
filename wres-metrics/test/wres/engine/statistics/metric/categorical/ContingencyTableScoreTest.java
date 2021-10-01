package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.Boilerplate;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link ContingencyTableScore}.
 * 
 * @author James Brown
 */
public final class ContingencyTableScoreTest
{

    /**
     * Expected warning.
     */

    private static final String SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE =
            "Specify non-null input to the 'THREAT SCORE'.";

    /**
     * Score used for testing. 
     */

    private ThreatScore cs;

    /**
     * Metadata used for testing.
     */

    private PoolMetadata meta;

    /**
     * Contingency table.
     */

    private DoubleScoreStatistic table;

    /**
     * Invalid contingency table.
     */

    private DoubleScoreStatistic invalidTable;

    @Before
    public void setupBeforeEachTest()
    {
        this.cs = ThreatScore.of();
        this.meta = PoolMetadata.of();

        this.table =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                 .setValue( 82.0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_POSITIVES )
                                                                                 .setValue( 38.0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_NEGATIVES )
                                                                                 .setValue( 23.0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_NEGATIVES )
                                                                                 .setValue( 222.0 ) )
                                    .build();

        this.invalidTable = DoubleScoreStatistic.newBuilder()
                                                .setMetric( ContingencyTable.BASIC_METRIC )
                                                .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                             .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                             .setValue( 82.0 ) )
                                                .build();
    }

    /**
     * Checks that a {@link ContingencyTableScore#hasRealUnits()} returns <code>false</code> and that input with the 
     * correct shape is accepted.
     * @throws PoolException if the input is not accepted
     */

    @Test
    public void testHasRealUnits()
    {
        assertFalse( "The Critical Success Index should not have real units.", this.cs.hasRealUnits() );
    }

    /**
     * Checks that {@link ContingencyTableScore#getCollectionOf()} returns {@link MetricConstants#CONTINGENCY_TABLE}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertSame( MetricConstants.CONTINGENCY_TABLE, this.cs.getCollectionOf() );
    }

    /**
     * Compares the output from {@link ContingencyTableScore#getInputForAggregation(Pool)} 
     * against a benchmark.
     */

    @Test
    public void testGetCollectionInput()
    {
        Pool<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        PoolMetadata m1 = Boilerplate.getPoolMetadata();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( this.table, m1 );

        DoubleScoreStatisticOuter actual = this.cs.getInputForAggregation( input );

        assertEquals( "Unexpected result for the contingency table.", expected, actual );
    }

    /**
     * Checks that {@link ContingencyTableScore#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposableReturnsFalse()
    {
        assertFalse( cs.isDecomposable() );
    }

    /**
     * Checks that {@link ContingencyTableScore#getScoreOutputGroup()} returns {@link MetricGroup#NONE}.
     */

    @Test
    public void testGetScoreOutputGroupReturnsNone()
    {
        assertSame( MetricGroup.NONE, cs.getScoreOutputGroup() );
    }

    /**
     * Checks the output from {@link ContingencyTableScore#getMetadata(DoubleScoreStatisticOuter)} against a benchmark.
     */

    @Test
    public void testGetMetadataReturnsExpectedOutput()
    {
        Pool<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        PoolMetadata expected = Boilerplate.getPoolMetadata();

        assertEquals( expected, this.cs.getInputForAggregation( input ).getMetadata() );
    }

    /**
     * Checks for an exception on null input.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException exception =
                assertThrows( PoolException.class,
                              () -> cs.apply( (Pool<Pair<Boolean, Boolean>>) null ) );

        assertEquals( SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE, exception.getMessage() );
    }

    /**
     * Checks for an exception on null input when computing the score from an existing contingency table.
     */

    @Test
    public void testExceptionOnNullInputInternal()
    {
        MetricCalculationException exception =
                assertThrows( MetricCalculationException.class,
                              () -> cs.is2x2ContingencyTable( (DoubleScoreStatisticOuter) null, cs ) );

        assertEquals( SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE, exception.getMessage() );
    }

    /**
     * Checks for an exception on null input when computing the score from an existing contingency table of 
     * arbitray size.
     */

    @Test
    public void testExceptionOnNullInputInternalForLargeTable()
    {
        MetricCalculationException exception =
                assertThrows( MetricCalculationException.class,
                              () -> cs.isContingencyTable( (DoubleScoreStatisticOuter) null, cs ) );

        assertEquals( SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE, exception.getMessage() );
    }

    /**
     * Checks for an exception on receiving an input that is too small.
     */

    @Test
    public void testExceptionOnInputThatIsTooSmall()
    {
        DoubleScoreStatisticOuter statistic = DoubleScoreStatisticOuter.of( this.invalidTable, this.meta );

        MetricCalculationException exception =
                assertThrows( MetricCalculationException.class,
                              () -> cs.is2x2ContingencyTable( statistic, this.cs ) );

        Set<MetricConstants> expected = Set.of( MetricConstants.TRUE_POSITIVES,
                                                MetricConstants.TRUE_NEGATIVES,
                                                MetricConstants.FALSE_POSITIVES,
                                                MetricConstants.FALSE_NEGATIVES );

        String expectedMessage = "Expected an intermediate result with elements "
                                 + expected
                                 + " but found elements [TRUE POSITIVES].";

        assertEquals( expectedMessage, exception.getMessage() );
    }


    /**
     * Checks for an exception on receiving a null metric.
     */

    @Test
    public void testExceptionOnNullMetric()
    {
        DoubleScoreStatisticOuter statistic = DoubleScoreStatisticOuter.of( this.invalidTable, this.meta );

        MetricCalculationException exception =
                assertThrows( MetricCalculationException.class,
                              () -> this.cs.is2x2ContingencyTable( statistic, null ) );

        assertEquals( SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE, exception.getMessage() );
    }

    /**
     * Checks for an exception on receiving a null metric for a contingency table of arbitrary size.
     */

    @Test
    public void testExceptionOnNullMetricForLargeTable()
    {
        DoubleScoreStatisticOuter statistic = DoubleScoreStatisticOuter.of( this.invalidTable, this.meta );

        MetricCalculationException exception =
                assertThrows( MetricCalculationException.class,
                              () -> this.cs.isContingencyTable( statistic, null ) );

        assertEquals( SPECIFY_NON_NULL_INPUT_TO_THE_THREAT_SCORE, exception.getMessage() );
    }

    @Test
    public void testExceptionOnInputThatIsNotSquare()
    {
        DoubleScoreStatisticOuter statistic = DoubleScoreStatisticOuter.of( this.invalidTable, this.meta );
        ProbabilityOfDetection metric = ProbabilityOfDetection.of();

        MetricCalculationException exception =
                assertThrows( MetricCalculationException.class,
                              () -> this.cs.isContingencyTable( statistic, metric ) );

        assertEquals( "Expected an intermediate result with a square number of elements when computing the "
                      + "'PROBABILITY OF DETECTION': [1].",
                      exception.getMessage() );
    }


}
