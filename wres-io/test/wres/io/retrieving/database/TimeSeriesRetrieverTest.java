package wres.io.retrieving.database;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import wres.config.components.DatasetOrientation;
import wres.config.components.Variable;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.DataProvider;
import wres.io.database.caching.Features;
import wres.io.database.caching.MeasurementUnits;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.statistics.generated.Geometry;

import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link TimeSeriesRetriever}.
 *
 * @author James Brown
 */

class TimeSeriesRetrieverTest
{
    private static final String UNKNOWN = "unknown";
    private static final String SCALE_PERIOD = "scale_period";
    private static final String SCALE_FUNCTION = "scale_function";
    private static final String REFERENCE_TIME = "reference_time";
    private static final String FEATURE_ID = "feature_id";
    private static final String OCCURRENCES = "occurrences";
    private static final String A_UNIT = "a unit";
    private static final String VARIABLE_NAME = "a variable";
    private static final Variable VARIABLE = new Variable( VARIABLE_NAME, null, null );
    private static final String A_FEATURE = "a feature";
    private static final String T1985_12_02T00_00_00Z = "1985-12-02T00:00:00Z";
    private static final String T1985_12_01T00_00_00Z = "1985-12-01T00:00:00Z";

    @Test
    void testGetTimeSeriesFromScriptIncludesOneTimeSeries() throws SQLException
    {
        @SuppressWarnings( "unchecked" )
        TimeSeriesRetriever<Double> retriever = Mockito.mock( TimeSeriesRetriever.class );

        Mockito.when( retriever.getTimeSeriesFromScript( Mockito.any(), Mockito.any() ) )
               .thenCallRealMethod();

        Mockito.when( retriever.checkAndGetLatestTimeScale( Mockito.any(),
                                                            Mockito.any(),
                                                            Mockito.any(),
                                                            Mockito.any() ) )
               .thenCallRealMethod();

        Mockito.when( retriever.getDatasetOrientation() )
               .thenReturn( DatasetOrientation.LEFT );

        Database database = Mockito.mock( Database.class );
        Connection connection = Mockito.mock( Connection.class );

        Mockito.when( retriever.getDatabase() )
               .thenReturn( database );

        Mockito.when( database.getConnection() )
               .thenReturn( connection );

        DataScripter scripter = Mockito.mock( DataScripter.class );
        DataProvider provider = Mockito.mock( DataProvider.class );

        // This requires knowledge of an implementation detail that there are two calls to next for every value because
        // each iteration looks ahead for the next value and then calls back. Hence there are twice as many true as 
        // values, terminated by a false.
        Mockito.when( provider.next() )
               .thenReturn( true, true, true, true, true, false );

        Mockito.when( provider.getLong( "series_id" ) )
               .thenReturn( 1L, 1L, 1L, 1L, 1L );

        Mockito.when( provider.getInstant( "valid_time" ) )
               .thenReturn( Instant.parse( "1985-12-01T01:00:00Z" ),
                            Instant.parse( "1985-12-01T02:00:00Z" ),
                            Instant.parse( "1985-12-01T03:00:00Z" ),
                            Instant.parse( "1985-12-01T04:00:00Z" ),
                            Instant.parse( "1985-12-01T05:00:00Z" ) );

        Mockito.when( provider.getInstant( REFERENCE_TIME ) )
               .thenReturn( Instant.parse( T1985_12_01T00_00_00Z ) );

        Mockito.when( provider.getString( SCALE_FUNCTION ) )
               .thenReturn( UNKNOWN );

        Mockito.when( provider.getLong( SCALE_PERIOD ) )
               .thenReturn( 1000L );

        Mockito.when( provider.hasColumn( REFERENCE_TIME ) )
               .thenReturn( true );

        Mockito.when( provider.getLong( FEATURE_ID ) )
               .thenReturn( 1L );

        Mockito.when( provider.hasColumn( OCCURRENCES ) )
               .thenReturn( true );

        Mockito.when( provider.getInt( OCCURRENCES ) )
               .thenReturn( 1, 1, 1, 1, 1 );

        Feature featureKey = Feature.of( Geometry.newBuilder()
                                                 .setName( A_FEATURE )
                                                 .build() );

        Features features = Mockito.mock( Features.class );
        Mockito.when( features.getFeatureKey( 1L ) )
               .thenReturn( featureKey );

        Mockito.when( retriever.getFeaturesCache() )
               .thenReturn( features );

        Mockito.when( retriever.getVariable() )
               .thenReturn( VARIABLE );

        Mockito.when( scripter.buffer( Mockito.any() ) )
               .thenReturn( provider );

        MeasurementUnits units = Mockito.mock( MeasurementUnits.class );
        Mockito.when( units.getUnit( Mockito.anyLong() ) )
               .thenReturn( A_UNIT );
        Mockito.when( retriever.getMeasurementUnitsCache() )
               .thenReturn( units );

        AtomicInteger next = new AtomicInteger();
        List<Event<Double>> timeSeriesValues = List.of( Event.of( Instant.parse( "1985-12-01T01:00:00Z" ), 1.0 ),
                                                        Event.of( Instant.parse( "1985-12-01T02:00:00Z" ), 2.0 ),
                                                        Event.of( Instant.parse( "1985-12-01T03:00:00Z" ), 3.0 ),
                                                        Event.of( Instant.parse( "1985-12-01T04:00:00Z" ), 4.0 ),
                                                        Event.of( Instant.parse( "1985-12-01T05:00:00Z" ), 5.0 ) );
        Function<DataProvider, Event<Double>> mapper = dataProvider -> timeSeriesValues.get( next.getAndIncrement() );

        Stream<TimeSeries<Double>> series = retriever.getTimeSeriesFromScript( scripter, mapper );

        List<TimeSeries<Double>> actual = series.toList();

        TimeSeries<Double> expectedSeries =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1985-12-01T01:00:00Z" ), 1.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1985-12-01T02:00:00Z" ), 2.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1985-12-01T03:00:00Z" ), 3.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1985-12-01T04:00:00Z" ), 4.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1985-12-01T05:00:00Z" ), 5.0 ) )
                                                .setMetadata( TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                                             Instant.parse(
                                                                                                     T1985_12_01T00_00_00Z ) ),
                                                                                     TimeScaleOuter.of( Duration.ofMillis(
                                                                                             1000 ) ),
                                                                                     VARIABLE_NAME,
                                                                                     featureKey,
                                                                                     A_UNIT ) )
                                                .build();

        List<TimeSeries<Double>> expected = List.of( expectedSeries );

        assertEquals( expected, actual );
    }

    @Test
    void testGetTimeSeriesFromScriptIncludesTwoTimeSeriesAndOneDuplicateTimeSeries() throws SQLException
    {
        @SuppressWarnings( "unchecked" )
        TimeSeriesRetriever<Double> retriever = Mockito.mock( TimeSeriesRetriever.class );

        Mockito.when( retriever.getTimeSeriesFromScript( Mockito.any(), Mockito.any() ) )
               .thenCallRealMethod();

        Mockito.when( retriever.checkAndGetLatestTimeScale( Mockito.any(),
                                                            Mockito.any(),
                                                            Mockito.any(),
                                                            Mockito.any() ) )
               .thenCallRealMethod();

        Mockito.when( retriever.getDatasetOrientation() )
               .thenReturn( DatasetOrientation.LEFT );

        Database database = Mockito.mock( Database.class );
        Connection connection = Mockito.mock( Connection.class );

        Mockito.when( retriever.getDatabase() )
               .thenReturn( database );

        Mockito.when( database.getConnection() )
               .thenReturn( connection );

        DataScripter scripter = Mockito.mock( DataScripter.class );
        DataProvider provider = Mockito.mock( DataProvider.class );

        // This requires knowledge of an implementation detail that there are two calls to next for every value because
        // each iteration looks ahead for the next value and then calls back. Hence there are twice as many true as 
        // values, terminated by a false.
        Mockito.when( provider.next() )
               .thenReturn( true,
                            true,
                            true,
                            true,
                            true,
                            true,
                            true,
                            true,
                            true,
                            true,
                            false );

        Mockito.when( provider.getLong( "series_id" ) )
               .thenReturn( 1L, 1L, 1L, 1L, 1L, 2L, 2L, 2L, 2L, 2L );

        Mockito.when( provider.getInstant( "valid_time" ) )
               .thenReturn( Instant.parse( "1985-12-01T01:00:00Z" ),
                            Instant.parse( "1985-12-01T02:00:00Z" ),
                            Instant.parse( "1985-12-01T03:00:00Z" ),
                            Instant.parse( "1985-12-01T04:00:00Z" ),
                            Instant.parse( "1985-12-01T05:00:00Z" ),
                            Instant.parse( "1985-12-02T01:00:00Z" ),
                            Instant.parse( "1985-12-02T02:00:00Z" ),
                            Instant.parse( "1985-12-02T03:00:00Z" ),
                            Instant.parse( "1985-12-02T04:00:00Z" ),
                            Instant.parse( "1985-12-02T05:00:00Z" ) );
        Mockito.when( provider.getInstant( REFERENCE_TIME ) )
               .thenReturn( Instant.parse( T1985_12_01T00_00_00Z ) )
               .thenReturn( Instant.parse( T1985_12_01T00_00_00Z ) )
               .thenReturn( Instant.parse( T1985_12_01T00_00_00Z ) )
               .thenReturn( Instant.parse( T1985_12_01T00_00_00Z ) )
               .thenReturn( Instant.parse( T1985_12_01T00_00_00Z ) )
               .thenReturn( Instant.parse( T1985_12_02T00_00_00Z ) )
               .thenReturn( Instant.parse( T1985_12_02T00_00_00Z ) )
               .thenReturn( Instant.parse( T1985_12_02T00_00_00Z ) )
               .thenReturn( Instant.parse( T1985_12_02T00_00_00Z ) )
               .thenReturn( Instant.parse( T1985_12_02T00_00_00Z ) );

        Mockito.when( provider.getString( SCALE_FUNCTION ) )
               .thenReturn( UNKNOWN );

        Mockito.when( provider.getLong( SCALE_PERIOD ) )
               .thenReturn( 1000L );

        Mockito.when( provider.hasColumn( REFERENCE_TIME ) )
               .thenReturn( true );

        Mockito.when( provider.getLong( FEATURE_ID ) )
               .thenReturn( 1L );

        Mockito.when( provider.hasColumn( OCCURRENCES ) )
               .thenReturn( true );

        Mockito.when( provider.getInt( OCCURRENCES ) )
               .thenReturn( 1, 1, 1, 1, 1, 2, 2, 2, 2, 2 );

        Feature featureKey = Feature.of( Geometry.newBuilder()
                                                 .setName( A_FEATURE )
                                                 .build() );

        Features features = Mockito.mock( Features.class );
        Mockito.when( features.getFeatureKey( Mockito.anyLong() ) )
               .thenReturn( featureKey );

        Mockito.when( retriever.getFeaturesCache() )
               .thenReturn( features );

        Mockito.when( retriever.getVariable() )
               .thenReturn( VARIABLE );

        Mockito.when( scripter.buffer( Mockito.any() ) )
               .thenReturn( provider );

        MeasurementUnits units = Mockito.mock( MeasurementUnits.class );
        Mockito.when( units.getUnit( Mockito.anyLong() ) )
               .thenReturn( A_UNIT );
        Mockito.when( retriever.getMeasurementUnitsCache() )
               .thenReturn( units );

        AtomicInteger next = new AtomicInteger();

        List<Event<Double>> timeSeriesValues = List.of( Event.of( Instant.parse( "1985-12-01T01:00:00Z" ), 1.0 ),
                                                        Event.of( Instant.parse( "1985-12-01T02:00:00Z" ), 2.0 ),
                                                        Event.of( Instant.parse( "1985-12-01T03:00:00Z" ), 3.0 ),
                                                        Event.of( Instant.parse( "1985-12-01T04:00:00Z" ), 4.0 ),
                                                        Event.of( Instant.parse( "1985-12-01T05:00:00Z" ), 5.0 ),
                                                        Event.of( Instant.parse( "1985-12-02T01:00:00Z" ), 6.0 ),
                                                        Event.of( Instant.parse( "1985-12-02T02:00:00Z" ), 7.0 ),
                                                        Event.of( Instant.parse( "1985-12-02T03:00:00Z" ), 8.0 ),
                                                        Event.of( Instant.parse( "1985-12-02T04:00:00Z" ), 9.0 ),
                                                        Event.of( Instant.parse( "1985-12-02T05:00:00Z" ), 10.0 ) );
        Function<DataProvider, Event<Double>> mapper = dataProvider -> timeSeriesValues.get( next.getAndIncrement() );

        Stream<TimeSeries<Double>> series = retriever.getTimeSeriesFromScript( scripter, mapper );

        List<TimeSeries<Double>> actual = series.toList();

        TimeSeries<Double> expectedSeriesOne =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1985-12-01T01:00:00Z" ), 1.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1985-12-01T02:00:00Z" ), 2.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1985-12-01T03:00:00Z" ), 3.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1985-12-01T04:00:00Z" ), 4.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1985-12-01T05:00:00Z" ), 5.0 ) )
                                                .setMetadata( TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                                             Instant.parse(
                                                                                                     T1985_12_01T00_00_00Z ) ),
                                                                                     TimeScaleOuter.of( Duration.ofMillis(
                                                                                             1000 ) ),
                                                                                     VARIABLE_NAME,
                                                                                     featureKey,
                                                                                     A_UNIT ) )
                                                .build();

        TimeSeries<Double> expectedSeriesTwo =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1985-12-02T01:00:00Z" ), 6.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1985-12-02T02:00:00Z" ), 7.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1985-12-02T03:00:00Z" ), 8.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1985-12-02T04:00:00Z" ), 9.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1985-12-02T05:00:00Z" ), 10.0 ) )
                                                .setMetadata( TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                                             Instant.parse(
                                                                                                     T1985_12_02T00_00_00Z ) ),
                                                                                     TimeScaleOuter.of( Duration.ofMillis(
                                                                                             1000 ) ),
                                                                                     VARIABLE_NAME,
                                                                                     featureKey,
                                                                                     A_UNIT ) )
                                                .build();

        List<TimeSeries<Double>> expected = List.of( expectedSeriesOne, expectedSeriesTwo, expectedSeriesTwo );

        assertEquals( expected, actual );
    }

    @Test
    void testGetTimeSeriesFromScriptIncludesThreeTimeSeriesAndTwoDuplicateTimeSeries() throws SQLException
    {
        @SuppressWarnings( "unchecked" )
        TimeSeriesRetriever<Double> retriever = Mockito.mock( TimeSeriesRetriever.class );

        Mockito.when( retriever.getTimeSeriesFromScript( Mockito.any(), Mockito.any() ) )
               .thenCallRealMethod();

        Mockito.when( retriever.checkAndGetLatestTimeScale( Mockito.any(),
                                                            Mockito.any(),
                                                            Mockito.any(),
                                                            Mockito.any() ) )
               .thenCallRealMethod();

        Mockito.when( retriever.getDatasetOrientation() )
               .thenReturn( DatasetOrientation.LEFT );

        Database database = Mockito.mock( Database.class );
        Connection connection = Mockito.mock( Connection.class );

        Mockito.when( retriever.getDatabase() )
               .thenReturn( database );

        Mockito.when( database.getConnection() )
               .thenReturn( connection );

        DataScripter scripter = Mockito.mock( DataScripter.class );
        DataProvider provider = Mockito.mock( DataProvider.class );

        // This requires knowledge of an implementation detail that there are two calls to next for every value because
        // each iteration looks ahead for the next value and then calls back. Hence there are twice as many true as 
        // values, terminated by a false.
        Mockito.when( provider.next() )
               .thenReturn( true,
                            true,
                            true,
                            false );

        Mockito.when( provider.getLong( "series_id" ) )
               .thenReturn( 1L, 2L, 3L );

        Mockito.when( provider.getInstant( "valid_time" ) )
               .thenReturn( Instant.parse( "1985-12-01T01:00:00Z" ),
                            Instant.parse( "1985-12-02T01:00:00Z" ),
                            Instant.parse( "1985-12-03T01:00:00Z" ) );
        Mockito.when( provider.getInstant( REFERENCE_TIME ) )
               .thenReturn( Instant.parse( T1985_12_01T00_00_00Z ) )
               .thenReturn( Instant.parse( T1985_12_02T00_00_00Z ) )
               .thenReturn( Instant.parse( "1985-12-03T00:00:00Z" ) );

        Mockito.when( provider.getString( SCALE_FUNCTION ) )
               .thenReturn( UNKNOWN );

        Mockito.when( provider.getLong( SCALE_PERIOD ) )
               .thenReturn( 1000L );

        Mockito.when( provider.hasColumn( REFERENCE_TIME ) )
               .thenReturn( true );

        Mockito.when( provider.getLong( FEATURE_ID ) )
               .thenReturn( 1L );

        Mockito.when( provider.hasColumn( OCCURRENCES ) )
               .thenReturn( true );

        Mockito.when( provider.getInt( OCCURRENCES ) )
               .thenReturn( 1, 3, 2 );

        Feature featureKey = Feature.of( Geometry.newBuilder()
                                                 .setName( A_FEATURE )
                                                 .build() );

        Features features = Mockito.mock( Features.class );
        Mockito.when( features.getFeatureKey( Mockito.anyLong() ) )
               .thenReturn( featureKey );

        Mockito.when( retriever.getFeaturesCache() )
               .thenReturn( features );

        Mockito.when( retriever.getVariable() )
               .thenReturn( VARIABLE );

        Mockito.when( scripter.buffer( Mockito.any() ) )
               .thenReturn( provider );

        MeasurementUnits units = Mockito.mock( MeasurementUnits.class );
        Mockito.when( units.getUnit( Mockito.anyLong() ) )
               .thenReturn( A_UNIT );
        Mockito.when( retriever.getMeasurementUnitsCache() )
               .thenReturn( units );

        AtomicInteger next = new AtomicInteger();

        List<Event<Double>> timeSeriesValues = List.of( Event.of( Instant.parse( "1985-12-01T01:00:00Z" ), 1.0 ),
                                                        Event.of( Instant.parse( "1985-12-02T01:00:00Z" ), 2.0 ),
                                                        Event.of( Instant.parse( "1985-12-03T01:00:00Z" ), 3.0 ) );
        Function<DataProvider, Event<Double>> mapper = dataProvider -> timeSeriesValues.get( next.getAndIncrement() );

        Stream<TimeSeries<Double>> series = retriever.getTimeSeriesFromScript( scripter, mapper );

        List<TimeSeries<Double>> actual = series.toList();

        TimeSeries<Double> expectedSeriesOne =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1985-12-01T01:00:00Z" ), 1.0 ) )
                                                .setMetadata( TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                                             Instant.parse(
                                                                                                     T1985_12_01T00_00_00Z ) ),
                                                                                     TimeScaleOuter.of( Duration.ofMillis(
                                                                                             1000 ) ),
                                                                                     VARIABLE_NAME,
                                                                                     featureKey,
                                                                                     A_UNIT ) )
                                                .build();

        TimeSeries<Double> expectedSeriesTwo =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1985-12-02T01:00:00Z" ), 2.0 ) )
                                                .setMetadata( TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                                             Instant.parse(
                                                                                                     T1985_12_02T00_00_00Z ) ),
                                                                                     TimeScaleOuter.of( Duration.ofMillis(
                                                                                             1000 ) ),
                                                                                     VARIABLE_NAME,
                                                                                     featureKey,
                                                                                     A_UNIT ) )
                                                .build();

        TimeSeries<Double> expectedSeriesThree =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1985-12-03T01:00:00Z" ), 3.0 ) )
                                                .setMetadata( TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                                             Instant.parse(
                                                                                                     "1985-12-03T00:00:00Z" ) ),
                                                                                     TimeScaleOuter.of( Duration.ofMillis(
                                                                                             1000 ) ),
                                                                                     VARIABLE_NAME,
                                                                                     featureKey,
                                                                                     A_UNIT ) )
                                                .build();

        List<TimeSeries<Double>> expected = List.of( expectedSeriesOne,
                                                     expectedSeriesTwo,
                                                     expectedSeriesTwo,
                                                     expectedSeriesTwo,
                                                     expectedSeriesThree,
                                                     expectedSeriesThree );

        assertEquals( expected, actual );
    }


}
