package wres.metrics.categorical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Boilerplate;
import wres.metrics.Metric;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link ContingencyTable}.
 * 
 * @author James Brown
 */
public final class ContingencyTableTest
{

    /**
     * The metric to test.
     */

    private ContingencyTable table;

    @Before
    public void setupBeforeEachTest()
    {
        this.table = ContingencyTable.of();
    }

    /**
     * Compares the output from {@link Metric#apply(wres.datamodel.sampledata.MetricInput)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        PoolMetadata meta = Boilerplate.getPoolMetadata( false );

        DoubleScoreStatistic result =
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

        final DoubleScoreStatisticOuter actual = this.table.apply( input );
        final DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( result, meta );
        assertEquals( expected, actual );

        //Check the parameters
        assertEquals( MetricConstants.CONTINGENCY_TABLE.toString(), this.table.getName() );
    }

    /**
     * Checks that the {@link ContingencyTable#getName()} returns {@link MetricConstants.CONTINGENCY_TABLE.toString()} 
     */

    @Test
    public void testContingencyTableIsNamedCorrectly()
    {
        assertEquals( MetricConstants.CONTINGENCY_TABLE.toString(),  this.table.getName() );
    }

    /**
     * Checks for an expected exception on null input to {@link ContingencyTable#apply(wres.datamodel.sampledata.MetricInput)}.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException exception =
                assertThrows( PoolException.class,
                              () -> this.table.apply( (Pool<Pair<Boolean, Boolean>>) null ) );

        String expectedMessage = "Specify non-null input to the 'CONTINGENCY TABLE'.";

        assertEquals( expectedMessage, exception.getMessage() );
    }

}
