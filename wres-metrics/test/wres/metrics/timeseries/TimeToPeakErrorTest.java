package wres.metrics.timeseries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.Timestamp;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.time.TimeSeries;
import wres.metrics.MetricTestDataFactory;
import wres.metrics.singlevalued.SumOfSquareError;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link TimeToPeakError}.
 * 
 * @author James Brown
 */
public final class TimeToPeakErrorTest
{

    /**
     * Default instance of a {@link SumOfSquareError}.
     */

    private TimeToPeakError ttp;

    @Before
    public void setupBeforeEachTest()
    {
        this.ttp = TimeToPeakError.of( new Random( 123456789 ) );
    }

    @Test
    public void testTimeToPeakError()
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
                                                                                                         .setSeconds( -21600 ) )
                                                               .build();

        PairOfInstantAndDuration two = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( secondInstant.getEpochSecond() )
                                                                                  .setNanos( secondInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( 43200 ) )
                                                               .build();

        DurationDiagramStatistic expected = DurationDiagramStatistic.newBuilder()
                                                                    .setMetric( TimeToPeakError.METRIC )
                                                                    .addStatistics( one )
                                                                    .addStatistics( two )
                                                                    .setReferenceTimeType( ReferenceTimeType.T0 )
                                                                    .build();


        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testHasRealUnitsReturnsTrue()
    {
        assertTrue( this.ttp.hasRealUnits() );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.TIME_TO_PEAK_ERROR.toString(), this.ttp.getMetricNameString() );
    }

    @Test
    public void testApplyThrowsExceptionOnNullInput()
    {
        assertThrows( PoolException.class, () -> this.ttp.apply( null ) );
    }

}
