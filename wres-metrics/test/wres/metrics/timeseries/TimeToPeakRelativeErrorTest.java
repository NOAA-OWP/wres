package wres.metrics.timeseries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import com.google.protobuf.Timestamp;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.time.TimeSeries;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link TimeToPeakRelativeError}.
 * 
 * @author James Brown
 */
public final class TimeToPeakRelativeErrorTest
{

    /**
     * Instance for testing.
     */

    private TimeToPeakRelativeError ttp;

    @Before
    public void runBeforeEachTest()
    {
        this.ttp = TimeToPeakRelativeError.of();
    }

    @Test
    public void testTimeToPeakRelativeError()
    {
        // Generate some data
        Pool<TimeSeries<Pair<Double, Double>>> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        DurationDiagramStatisticOuter actual = this.ttp.apply( input );

        Instant firstInstant = Instant.parse( "1985-01-01T00:00:00Z" );
        Instant secondInstant = Instant.parse( "1985-01-02T00:00:00Z" );

        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( firstInstant.getEpochSecond() )
                                                                                  .setNanos( firstInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -1200 ) )
                                                               .setReferenceTimeType( ReferenceTimeType.T0 )
                                                               .build();

        PairOfInstantAndDuration two = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( secondInstant.getEpochSecond() )
                                                                                  .setNanos( secondInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( 7200 ) )
                                                               .setReferenceTimeType( ReferenceTimeType.T0 )
                                                               .build();

        DurationDiagramStatistic expected = DurationDiagramStatistic.newBuilder()
                                                                    .setMetric( TimeToPeakRelativeError.METRIC )
                                                                    .addStatistics( one )
                                                                    .addStatistics( two )
                                                                    .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testHasRealUnitsReturnsTrue()
    {
        assertTrue( this.ttp.hasRealUnits() );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR.toString(), this.ttp.getName() );
    }

    @Test
    public void testApplyThrowsExceptionOnNullInput()
    {
        assertThrows( PoolException.class, () -> this.ttp.apply( null ) );
    }

}
