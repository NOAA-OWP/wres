package wres.engine.statistics.metric.timeseries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;

import org.junit.Before;
import org.junit.Test;
import com.google.protobuf.Timestamp;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.Builder;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;

/**
 * Tests the {@link TimeToPeakRelativeError}.
 * 
 * @author james.brown@hydrosolved.com
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
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output
        TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "1985-01-02T00:00:00Z" ),
                                                     Duration.ofHours( 6 ),
                                                     Duration.ofHours( 18 ) );

        SampleMetadata m1 =
                new SampleMetadata.Builder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                            .setIdentifier( DatasetIdentifier.of( Location.of( "A" ),
                                                                                  "Streamflow" ) )
                                            .setTimeWindow( window )
                                            .build();

        DurationDiagramStatisticOuter actual = ttp.apply( input );

        Instant firstInstant = Instant.parse( "1985-01-01T00:00:00Z" );
        Instant secondInstant = Instant.parse( "1985-01-02T00:00:00Z" );

        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( firstInstant.getEpochSecond() )
                                                                                  .setNanos( firstInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -1200 ) )
                                                               .build();

        PairOfInstantAndDuration two = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( secondInstant.getEpochSecond() )
                                                                                  .setNanos( secondInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( 7200 ) )
                                                               .build();

        DurationDiagramStatistic expectedSource = DurationDiagramStatistic.newBuilder()
                                                                          .setMetric( TimeToPeakRelativeError.METRIC )
                                                                          .addStatistics( one )
                                                                          .addStatistics( two )
                                                                          .build();

        DurationDiagramStatisticOuter expected = DurationDiagramStatisticOuter.of( expectedSource, m1 );

        assertEquals( expected, actual );
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
        assertThrows( SampleDataException.class, () -> this.ttp.apply( null ) );
    }

}
