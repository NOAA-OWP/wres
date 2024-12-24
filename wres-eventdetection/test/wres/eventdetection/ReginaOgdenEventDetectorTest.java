package wres.eventdetection;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.yaml.components.EventDetectionParameters;
import wres.config.yaml.components.EventDetectionParametersBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.MessageFactory;
import wres.statistics.generated.TimeWindow;

/**
 * Tests the {@link ReginaOgdenEventDetector}.
 *
 * @author James Brown
 */

class ReginaOgdenEventDetectorTest
{
    @Test
    void testDetectWithZeroTrendAndTwoEvents()
    {
        EventDetectionParameters parameters = EventDetectionParametersBuilder.builder()
                                                                             .windowSize( Duration.ofHours( 6 ) )
                                                                             .minimumEventDuration( Duration.ZERO )
                                                                             .halfLife( Duration.ofHours( 2 ) )
                                                                             .build();

        ReginaOgdenEventDetector detector = ReginaOgdenEventDetector.of( parameters );

        Event<Double> one = Event.of( Instant.parse( "2079-12-03T00:00:00Z" ), 5.0 );
        Event<Double> two = Event.of( Instant.parse( "2079-12-03T01:00:00Z" ), 5.0 );
        Event<Double> three = Event.of( Instant.parse( "2079-12-03T02:00:00Z" ), 24.0 );
        Event<Double> four = Event.of( Instant.parse( "2079-12-03T03:00:00Z" ), 25.0 );
        Event<Double> five = Event.of( Instant.parse( "2079-12-03T04:00:00Z" ), 5.0 );
        Event<Double> six = Event.of( Instant.parse( "2079-12-03T05:00:00Z" ), 5.0 );
        Event<Double> seven = Event.of( Instant.parse( "2079-12-03T06:00:00Z" ), 5.0 );
        Event<Double> eight = Event.of( Instant.parse( "2079-12-03T07:00:00Z" ), 84.0 );
        Event<Double> nine = Event.of( Instant.parse( "2079-12-03T08:00:00Z" ), 85.0 );
        Event<Double> ten = Event.of( Instant.parse( "2079-12-03T09:00:00Z" ), 87.0 );
        Event<Double> eleven = Event.of( Instant.parse( "2079-12-03T10:00:00Z" ), 5.0 );
        Event<Double> twelve = Event.of( Instant.parse( "2079-12-03T11:00:00Z" ), 5.0 );

        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                             TimeScaleOuter.of(),
                                                             "foo",
                                                             Feature.of( MessageFactory.getGeometry( "bar" ) ),
                                                             "baz" );
        TimeSeries<Double> timeSeries =
                new TimeSeries.Builder<Double>()
                        .addEvent( one )
                        .addEvent( two )
                        .addEvent( three )
                        .addEvent( four )
                        .addEvent( five )
                        .addEvent( six )
                        .addEvent( seven )
                        .addEvent( eight )
                        .addEvent( nine )
                        .addEvent( ten )
                        .addEvent( eleven )
                        .addEvent( twelve )
                        .setMetadata( metadata )
                        .build();

        Set<TimeWindowOuter> actual = detector.detect( timeSeries );

        Instant startOne = Instant.parse( "2079-12-03T03:00:00Z" );
        Instant endOne = Instant.parse( "2079-12-03T03:00:00Z" );

        TimeWindow expectedInnerOne = MessageFactory.getTimeWindow()
                                                    .toBuilder()
                                                    .setEarliestValidTime( MessageFactory.getTimestamp( startOne ) )
                                                    .setLatestValidTime( MessageFactory.getTimestamp( endOne ) )
                                                    .build();

        Instant startTwo = Instant.parse( "2079-12-03T08:00:00Z" );
        Instant endTwo = Instant.parse( "2079-12-03T10:00:00Z" );

        TimeWindow expectedInnerTwo = MessageFactory.getTimeWindow()
                                                    .toBuilder()
                                                    .setEarliestValidTime( MessageFactory.getTimestamp( startTwo ) )
                                                    .setLatestValidTime( MessageFactory.getTimestamp( endTwo ) )
                                                    .build();

        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( expectedInnerOne ),
                                                TimeWindowOuter.of( expectedInnerTwo ) );


        assertEquals( expected, actual );
    }

    @Test
    void testDetectWithTrendAndOneEvent()
    {
        TimeSeries<Double> testSeries = this.createTestSeries( false );

        EventDetectionParameters parameters = EventDetectionParametersBuilder.builder()
                                                                             .windowSize( Duration.ofDays( 7 ) )
                                                                             .minimumEventDuration( Duration.ZERO )
                                                                             .halfLife( Duration.ofHours( 6 ) )
                                                                             .build();

        ReginaOgdenEventDetector detector = ReginaOgdenEventDetector.of( parameters );

        Set<TimeWindowOuter> actual = detector.detect( testSeries );

        Instant start = Instant.parse( "2018-01-22T02:00:00Z" );
        Instant end = Instant.parse( "2018-01-29T00:00:00Z" );

        TimeWindow expectedInner = MessageFactory.getTimeWindow()
                                                 .toBuilder()
                                                 .setEarliestValidTime( MessageFactory.getTimestamp( start ) )
                                                 .setLatestValidTime( MessageFactory.getTimestamp( end ) )
                                                 .build();

        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( expectedInner ) );

        assertEquals( expected, actual );
    }

    @Test
    void testDetectWithTrendAndThreeEvents()
    {
        TimeSeries<Double> testSeries = this.createTestSeries( false );

        // Duplicate the series three times
        Instant start = testSeries.getEvents()
                                  .first()
                                  .getTime();
        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<Double>().setMetadata( testSeries.getMetadata() );
        for ( int i = 0; i < 3; i++ )
        {
            for ( Event<Double> nextEvent : testSeries.getEvents() )
            {
                Event<Double> adjusted = Event.of( start, nextEvent.getValue() );
                builder.addEvent( adjusted );
                start = start.plus( Duration.ofHours( 1 ) );
            }
        }

        testSeries = builder.build();

        EventDetectionParameters parameters = EventDetectionParametersBuilder.builder()
                                                                             .windowSize( Duration.ofDays( 7 ) )
                                                                             .minimumEventDuration( Duration.ZERO )
                                                                             .halfLife( Duration.ofHours( 6 ) )
                                                                             .build();

        ReginaOgdenEventDetector detector = ReginaOgdenEventDetector.of( parameters );

        Set<TimeWindowOuter> actual = detector.detect( testSeries );

        Instant startOne = Instant.parse( "2018-01-22T02:00:00Z" );
        Instant endOne = Instant.parse( "2018-01-29T00:00:00Z" );

        TimeWindow expectedInnerOne = MessageFactory.getTimeWindow()
                                                 .toBuilder()
                                                 .setEarliestValidTime( MessageFactory.getTimestamp( startOne ) )
                                                 .setLatestValidTime( MessageFactory.getTimestamp( endOne ) )
                                                 .build();

        Instant startTwo = Instant.parse( "2018-03-04T18:00:00Z" );
        Instant endTwo = Instant.parse( "2018-03-11T16:00:00Z" );

        TimeWindow expectedInnerTwo = MessageFactory.getTimeWindow()
                                                    .toBuilder()
                                                    .setEarliestValidTime( MessageFactory.getTimestamp( startTwo ) )
                                                    .setLatestValidTime( MessageFactory.getTimestamp( endTwo ) )
                                                    .build();

        Instant startThree = Instant.parse( "2018-04-15T10:00:00Z" );
        Instant endThree = Instant.parse( "2018-04-22T08:00:00Z" );

        TimeWindow expectedInnerThree = MessageFactory.getTimeWindow()
                                                    .toBuilder()
                                                    .setEarliestValidTime( MessageFactory.getTimestamp( startThree ) )
                                                    .setLatestValidTime( MessageFactory.getTimestamp( endThree ) )
                                                    .build();

        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( expectedInnerOne),
                                                TimeWindowOuter.of( expectedInnerTwo ),
                                                TimeWindowOuter.of( expectedInnerThree ) );

        assertEquals( expected, actual );
    }

    @Test
    void testDetectWithTrendAndOneEventAndNoise()
    {
        TimeSeries<Double> testSeries = this.createTestSeries( true );

        EventDetectionParameters parameters = EventDetectionParametersBuilder.builder()
                                                                             .halfLife( Duration.ofHours( 6 ) )
                                                                             .windowSize( Duration.ofDays( 7 ) )
                                                                             .minimumEventDuration( Duration.ofHours( 6 ) )
                                                                             .startRadius( Duration.ofHours( 7 ) )
                                                                             .build();

        ReginaOgdenEventDetector detector = ReginaOgdenEventDetector.of( parameters );

        Set<TimeWindowOuter> actual = detector.detect( testSeries );

        Instant start = Instant.parse( "2018-01-21T19:00:00Z" );
        Instant end = Instant.parse( "2018-01-29T00:00:00Z" );

        TimeWindow expectedInner = MessageFactory.getTimeWindow()
                                                 .toBuilder()
                                                 .setEarliestValidTime( MessageFactory.getTimestamp( start ) )
                                                 .setLatestValidTime( MessageFactory.getTimestamp( end ) )
                                                 .build();

        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( expectedInner ) );

        assertEquals( expected, actual );
    }

    /**
     * @param addNoise is true to add noise, false otherwise
     * @return the test series
     */
    private TimeSeries<Double> createTestSeries( boolean addNoise )
    {
        // Create a trend array
        double[] trend = new double[1000];
        for ( int i = 0; i < 1000; i++ )
        {
            trend[i] = 100.0 * Math.exp( -0.002 * ( i + 1 ) * 1.0 );
        }

        // Create the event flows, which span 500 times
        double[] eventFlows = new double[500];
        for ( int i = 0; i < 500; i++ )
        {
            eventFlows[i] = 1000.0 * Math.exp( -0.004 * ( i + 1 ) * 1.0 );
        }

        // Concatenate the event to zero flows, which span the first 500 times
        double[] flow = new double[1000];
        System.arraycopy( eventFlows, 0, flow, 500, 500 );      // Second part: event flows

        // Add a noise component if needed
        double[] noise = new double[1000];
        if ( addNoise )
        {
            for ( int i = 0; i < 1000; i++ )
            {
                noise[i] = i * 334 * Math.PI / 1000; // Simulating np.linspace(0.0, 334*np.pi, 1000)
                noise[i] = 5.0 * Math.sin( noise[i] ); // noise
            }
        }

        // Sum the trend, flow and noise arrays to get total flow
        double[] totalFlow = new double[1000];
        for ( int i = 0; i < 1000; i++ )
        {
            totalFlow[i] = trend[i] + flow[i] + noise[i];
        }

        // Create a time-series
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                             TimeScaleOuter.of(),
                                                             "foo",
                                                             Feature.of( MessageFactory.getGeometry( "bar" ) ),
                                                             "baz" );
        TimeSeries.Builder<Double> timeSeries = new TimeSeries.Builder<Double>().setMetadata( metadata );

        Instant startDate = Instant.parse( "2018-01-01T00:00:00Z" );
        for ( int i = 0; i < totalFlow.length; i++ )
        {
            Instant nextDate = startDate.plus( Duration.ofHours( i ) );
            Event<Double> nextEvent = Event.of( nextDate, totalFlow[i] );
            timeSeries.addEvent( nextEvent );
        }

        return timeSeries.build();
    }
}