package wres.datamodel;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link DefaultDataProvider}.
 */

class DefaultDataProviderTest
{
    private static final String BYTE = "byte";
    private static final String SHORT = "short";
    private static final String INT = "int";
    private static final String LONG = "long";
    private static final String FLOAT = "float";
    private static final String DOUBLE = "double";
    private static final String STRING = "string";

    /** Default data provider. */
    private DataProvider data;

    @BeforeEach
    void runBeforeEach()
    {

        String[] columns = {
                BYTE,
                SHORT,
                INT,
                LONG,
                FLOAT,
                DOUBLE,
                STRING,
                "bad_string",
                "bad_object"
        };

        Object[] values = {
                ( byte ) 0,
                ( short ) 1,
                2,
                ( long ) 3,
                4.0F,
                5.0,
                "6",
                "tokyo",
                new Object()
        };
        this.data = DefaultDataProvider.with( columns )
                                       .addRow( values )
                                       .build();
    }

    @Test
    void typeRetrieveTests()
    {
        String testString = "testString";
        Short testShort = Short.MAX_VALUE;
        Integer testInt = Integer.MAX_VALUE;
        Long testLong = Long.MAX_VALUE;
        float testFloat = Float.MAX_VALUE;
        double testDouble = Double.MAX_VALUE;
        Double[] testDoubles = { 1.0, 2.0, 3.0 };
        BigDecimal testDecimal = BigDecimal.TEN;
        boolean testBoolean = true;
        LocalDate testDate = LocalDate.now();
        LocalTime testTime = LocalTime.now();
        LocalDateTime testLocalDateTime = LocalDateTime.now();
        OffsetDateTime testOffsetDateTime = OffsetDateTime.now();
        Instant testInstant = Instant.now();
        URI testUri = URI.create( "here/is/an/example" );
        Duration testDuration = Duration.ofHours( 10 );

        try ( DataProvider provider = DefaultDataProvider.with( STRING,
                                                                SHORT,
                                                                INT,
                                                                LONG,
                                                                FLOAT,
                                                                DOUBLE,
                                                                "double[]",
                                                                "bigdecimal",
                                                                "boolean",
                                                                "localdate",
                                                                "localtime",
                                                                "localdatetime",
                                                                "offsetdatetime",
                                                                "instant",
                                                                "uri",
                                                                "duration" )
                                                         .set( STRING, testString )
                                                         .set( SHORT, testShort )
                                                         .set( INT, testInt )
                                                         .set( LONG, testLong )
                                                         .set( FLOAT, testFloat )
                                                         .set( DOUBLE, testDouble )
                                                         .set( "double[]", testDoubles )
                                                         .set( "bigdecimal", testDecimal )
                                                         .set( "boolean", testBoolean )
                                                         .set( "localdate", testDate )
                                                         .set( "localtime", testTime )
                                                         .set( "localdatetime", testLocalDateTime )
                                                         .set( "offsetdatetime", testOffsetDateTime )
                                                         .set( "instant", testInstant )
                                                         .set( "uri", testUri )
                                                         .set( "duration", testDuration )
                                                         .build() )
        {
            assertEquals( provider.getString( STRING ), testString );
            assertEquals( provider.getShort( SHORT ), testShort );
            assertEquals( provider.getInt( INT ), testInt );
            assertEquals( provider.getLong( LONG ), testLong );
            assertEquals( provider.getFloat( FLOAT ), testFloat, 0.00001F );
            assertEquals( provider.getDouble( DOUBLE ), testDouble, 0.000001 );
            assertArrayEquals( provider.getDoubleArray( "double[]" ), testDoubles );
            assertEquals( provider.getBigDecimal( "bigdecimal" ), testDecimal );
            assertEquals( provider.getBoolean( "boolean" ), testBoolean );
            assertEquals( provider.getDate( "localdate" ), testDate );
            assertEquals( provider.getTime( "localtime" ), testTime );
            assertEquals( provider.getLocalDateTime( "localdatetime" ), testLocalDateTime );
            assertEquals( provider.getOffsetDateTime( "offsetdatetime" ), testOffsetDateTime );
            assertEquals( provider.getInstant( "instant" ), testInstant );
            assertEquals( provider.getURI( "uri" ), testUri );
            assertEquals( provider.getDuration( "duration" ), testDuration );
        }
    }

    @Test
    void valueReadTests()
    {
        String[] columnNames = new String[] {
                "integer",
                STRING,
                DOUBLE,
                FLOAT,
                "localdatetime",
                "date_string",
                "instant",
                LONG,
                "boolean",
                "local_time",
                "doubles",
                "date",
                SHORT,
                "object",
                "big",
                "offset_datetime"
        };

        List<Object[]> initialValues = new ArrayList<>();
        initialValues.add( new Object[] {
                0,
                "1",
                2.0,
                3.0F,
                LocalDateTime.now(),
                "2018-05-06 16:48:00",
                Instant.now(),
                546843545486L,
                false,
                LocalTime.now(),
                new Double[] { 1.0, 2.0, 3.0 },
                LocalDate.now(),
                ( short ) 1,
                new Object(),
                BigDecimal.ZERO,
                OffsetDateTime.now()
        } );
        initialValues.add( new Object[] {
                4,
                "five",
                6.0,
                7.0F,
                LocalDateTime.of( 1970, 1, 1, 0, 0 ),
                "1970-01-01T00:00:00",
                Instant.ofEpochSecond( 54868486 ),
                999999999L,
                true,
                LocalTime.of( 0, 0, 0 ),
                new Double[] {},
                LocalDate.of( 1970, 1, 1 ),
                ( short ) 45,
                new Object(),
                BigDecimal.ONE,
                OffsetDateTime.ofInstant( Instant.now(), ZoneId.of( "UTC" ) )
        } );
        initialValues.add( new Object[] {
                8,
                "nine",
                10.0,
                11.0F,
                LocalDateTime.of( 1987, 10, 29, 12, 0 ),
                "2008-06-26T16:00:00",
                Instant.ofEpochSecond( 544156868486L ),
                -9999999L,
                0,
                LocalTime.of( 15, 17, 18 ),
                new Double[] { -1.5, 1.58489687, 1568.54684476 },
                LocalDate.of( 1987, 10, 29 ),
                Short.MAX_VALUE,
                new Object(),
                new BigDecimal( "28.4" ),
                OffsetDateTime.of(
                        1987,
                        10,
                        19,
                        12,
                        11,
                        15,
                        1868,
                        ZoneOffset.UTC )
        } );

        DefaultDataProvider builder = DefaultDataProvider.with( columnNames );

        for ( Object[] row : initialValues )
        {
            builder.addRow( row );
        }


        try ( DataProvider provider = builder.build() )
        {
            int rowNumber = 0;
            while ( provider.next() )
            {
                int columnIndex = 0;
                for ( String columnName : columnNames )
                {
                    Object control = initialValues.get( rowNumber )[columnIndex];
                    Object test = provider.getValue( columnName );
                    assertEquals( control,
                                  test,
                                  "The values of the '" +
                                  columnName
                                  +
                                  "' column were not equivalent on row "
                                  +
                                  rowNumber
                                  +
                                  ". The values were: "
                                  +
                                  control
                                  +
                                  " and "
                                  +
                                  test );
                    columnIndex++;
                }

                rowNumber++;
            }
        }
    }

    @Test
    void testByteConversion()
    {
        // "byte" column conversions
        assertAll( () -> assertEquals( ( Byte ) ( byte ) 0, this.data.getByte( BYTE ) ),
                   () -> assertEquals( ( Short ) ( short ) 0, this.data.getShort( BYTE ) ),
                   () -> assertEquals( ( Integer ) 0, this.data.getInt( BYTE ) ),
                   () -> assertEquals( ( Long ) ( long ) 0, this.data.getLong( BYTE ) ),
                   () -> assertEquals( ( float ) 0, this.data.getFloat( BYTE ), 0.00001 ),
                   () -> assertEquals( 0, this.data.getDouble( BYTE ), 0.00001 ),
                   () -> assertEquals( new BigDecimal( 0 ), this.data.getBigDecimal( BYTE ) ),
                   () -> assertEquals( "0", this.data.getString( BYTE ) ) );
    }

    @Test
    void testShortConversion()
    {
        // "short" column conversions
        assertAll( () -> assertEquals( ( Byte ) ( byte ) 1, this.data.getByte( SHORT ) ),
                   () -> assertEquals( ( Short ) ( short ) 1, this.data.getShort( SHORT ) ),
                   () -> assertEquals( ( Integer ) 1, this.data.getInt( SHORT ) ),
                   () -> assertEquals( ( Long ) ( long ) 1, this.data.getLong( SHORT ) ),
                   () -> assertEquals( ( float ) 1, this.data.getFloat( SHORT ), 0.00001 ),
                   () -> assertEquals( 1, this.data.getDouble( SHORT ), 0.00001 ),
                   () -> assertEquals( new BigDecimal( 1 ), this.data.getBigDecimal( SHORT ) ),
                   () -> assertEquals( "1", this.data.getString( SHORT ) ) );
    }

    @Test
    void testIntConversion()
    {
        // "int" column conversions
        assertAll( () -> assertEquals( ( Byte ) ( byte ) 2, this.data.getByte( INT ) ),
                   () -> assertEquals( ( Short ) ( short ) 2, this.data.getShort( INT ) ),
                   () -> assertEquals( ( Integer ) 2, this.data.getInt( INT ) ),
                   () -> assertEquals( ( Long ) ( long ) 2, this.data.getLong( INT ) ),
                   () -> assertEquals( ( float ) 2, this.data.getFloat( INT ), 0.00001 ),
                   () -> assertEquals( 2, this.data.getDouble( INT ), 0.00001 ),
                   () -> assertEquals( new BigDecimal( 2 ), this.data.getBigDecimal( INT ) ),
                   () -> assertEquals( "2", this.data.getString( INT ) ) );
    }

    @Test
    void testLongConversion()
    {
        // "long" column conversions
        assertAll( () -> assertEquals( ( Byte ) ( byte ) 3, this.data.getByte( LONG ) ),
                   () -> assertEquals( ( Short ) ( short ) 3, this.data.getShort( LONG ) ),
                   () -> assertEquals( ( Integer ) 3, this.data.getInt( LONG ) ),
                   () -> assertEquals( ( Long ) ( long ) 3, this.data.getLong( LONG ) ),
                   () -> assertEquals( ( float ) 3, this.data.getFloat( LONG ), 0.00001 ),
                   () -> assertEquals( 3, this.data.getDouble( LONG ), 0.00001 ),
                   () -> assertEquals( new BigDecimal( 3 ), this.data.getBigDecimal( LONG ) ),
                   () -> assertEquals( "3", this.data.getString( LONG ) ) );
    }

    @Test
    void testFloatConversion()
    {
        // "float" column conversions
        assertAll( () -> assertEquals( ( Byte ) ( byte ) 4, this.data.getByte( FLOAT ) ),
                   () -> assertEquals( ( Short ) ( short ) 4, this.data.getShort( FLOAT ) ),
                   () -> assertEquals( ( Integer ) 4, this.data.getInt( FLOAT ) ),
                   () -> assertEquals( ( Long ) ( long ) 4, this.data.getLong( FLOAT ) ),
                   () -> assertEquals( ( float ) 4, this.data.getFloat( FLOAT ), 0.00001 ),
                   () -> assertEquals( 4.0, this.data.getDouble( FLOAT ), 0.00001 ),
                   () -> assertEquals( BigDecimal.valueOf( 4F ), this.data.getBigDecimal( FLOAT ) ),
                   () -> assertEquals( "4.0", this.data.getString( FLOAT ) ) );
    }

    @Test
    void testDoubleConversion()
    {
        // "double" column conversions
        assertAll( () -> assertEquals( ( Byte ) ( byte ) 5, this.data.getByte( DOUBLE ) ),
                   () -> assertEquals( ( Short ) ( short ) 5, this.data.getShort( DOUBLE ) ),
                   () -> assertEquals( ( Integer ) 5, this.data.getInt( DOUBLE ) ),
                   () -> assertEquals( ( Long ) ( long ) 5, this.data.getLong( DOUBLE ) ),
                   () -> assertEquals( ( float ) 5, this.data.getFloat( DOUBLE ), 0.00001 ),
                   () -> assertEquals( 5, this.data.getDouble( DOUBLE ), 0.00001 ),
                   () -> assertEquals( BigDecimal.valueOf( 5.0 ), this.data.getBigDecimal( DOUBLE ) ),
                   () -> assertEquals( "5.0", this.data.getString( DOUBLE ) ) );
    }

    @Test
    void testStringConversion()
    {
        // "String" column conversions
        assertAll( () -> assertEquals( ( Byte ) ( byte ) 6, this.data.getByte( STRING ) ),
                   () -> assertEquals( ( Short ) ( short ) 6, this.data.getShort( STRING ) ),
                   () -> assertEquals( ( Integer ) 6, this.data.getInt( STRING ) ),
                   () -> assertEquals( ( Long ) ( long ) 6, this.data.getLong( STRING ) ),
                   () -> assertEquals( 6.0F, this.data.getFloat( STRING ), 0.00001 ),
                   () -> assertEquals( 6.0, this.data.getDouble( STRING ), 0.00001 ),
                   () -> assertEquals( new BigDecimal( 6 ), this.data.getBigDecimal( STRING ) ),
                   () -> assertEquals( "6", this.data.getString( STRING ) ) );
    }

    @Test
    void testBadConversionsThrowClassCastException()
    {
        assertAll( () -> failsOnNumericConversion( this.data, "bad_string" ),
                   () -> failsOnNumericConversion( this.data, "bad_object" ) );
    }

    /**
     * Helper to assert bad conversions.
     * @param data the data
     * @param column the column
     */
    private void failsOnNumericConversion( final DataProvider data, final String column )
    {
        Stream.Builder<Function<String, ?>> builder = Stream.builder();
        Stream<Function<String, ?>> conversions = builder.add( data::getByte )
                                                         .add( data::getShort )
                                                         .add( data::getInt )
                                                         .add( data::getLong )
                                                         .add( data::getFloat )
                                                         .add( data::getDouble )
                                                         .add( data::getBigDecimal )
                                                         .build();

        conversions.forEach( function -> assertThrows( ClassCastException.class, () -> function.apply( column ) ) );
    }
}