package wres.io.data;

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

import org.junit.Assert;
import org.junit.Test;

import wres.io.data.DataBuilder;
import wres.io.data.DataProvider;

public class DataProviderTest
{
    // TODO: Add test for URL -> URI conversion tests
    // TODO: Add test for String -> URI conversion tests
    // TODO: Add number -> Duration conversion tests
    // TODO: Add string -> Duration conversion tests
    // TODO: Add date conversion tests
    // TODO: Add tests for the SQLDataProvider

    @Test
    /*
    Tests typed retrieval
     */
    public void typeRetrieveTests()
    {
        String testString = "testString";
        Short testShort = Short.MAX_VALUE;
        Integer testInt = Integer.MAX_VALUE;
        Long testLong = Long.MAX_VALUE;
        Float testFloat = Float.MAX_VALUE;
        Double testDouble = Double.MAX_VALUE;
        Double[] testDoubles = {1.0, 2.0, 3.0};
        BigDecimal testDecimal = BigDecimal.TEN;
        boolean testBoolean = true;
        LocalDate testDate = LocalDate.now();
        LocalTime testTime = LocalTime.now();
        LocalDateTime testLocalDateTime = LocalDateTime.now();
        OffsetDateTime testOffsetDateTime = OffsetDateTime.now();
        Instant testInstant = Instant.now();
        URI testUri = URI.create( "here/is/an/example" );
        Duration testDuration = Duration.ofHours( 10 );

        DataProvider provider = DataBuilder
                .with(
                        "string",
                        "short",
                        "int",
                        "long",
                        "float",
                        "double",
                        "double[]",
                        "bigdecimal",
                        "boolean",
                        "localdate",
                        "localtime",
                        "localdatetime",
                        "offsetdatetime",
                        "instant",
                        "uri",
                        "duration")
                .set( "string", testString )
                .set( "short", testShort )
                .set("int", testInt)
                .set("long", testLong)
                .set("float", testFloat)
                .set("double", testDouble)
                .set("double[]", testDoubles)
                .set("bigdecimal", testDecimal)
                .set("boolean", testBoolean)
                .set("localdate", testDate)
                .set("localtime", testTime)
                .set("localdatetime", testLocalDateTime)
                .set("offsetdatetime", testOffsetDateTime)
                .set("instant", testInstant)
                .set("uri", testUri)
                .set("duration", testDuration)
                .build();
        Assert.assertEquals( provider.getString("string"), testString );
        Assert.assertEquals( provider.getShort( "short" ), testShort );
        Assert.assertEquals( provider.getInt("int"), testInt );
        Assert.assertEquals( provider.getLong( "long" ), testLong);
        Assert.assertEquals( provider.getFloat( "float" ), testFloat, 0.00001F );
        Assert.assertEquals( provider.getDouble( "double" ), testDouble, 0.000001 );
        Assert.assertArrayEquals( provider.getDoubleArray( "double[]" ), testDoubles );
        Assert.assertEquals( provider.getBigDecimal( "bigdecimal" ), testDecimal);
        Assert.assertEquals( provider.getBoolean( "boolean" ), testBoolean);
        Assert.assertEquals( provider.getDate( "localdate" ), testDate);
        Assert.assertEquals( provider.getTime("localtime"), testTime);
        Assert.assertEquals( provider.getLocalDateTime( "localdatetime" ), testLocalDateTime);
        Assert.assertEquals( provider.getOffsetDateTime( "offsetdatetime" ), testOffsetDateTime);
        Assert.assertEquals( provider.getInstant( "instant" ), testInstant );
        Assert.assertEquals( provider.getURI( "uri" ), testUri );
        Assert.assertEquals( provider.getDuration( "duration" ), testDuration );
    }

    @Test
    /*
      Tests whether or not retrieved values are correct.
     */
    public void valueReadTests()
    {
        String[] columnNames = new String[] {
                "integer",
                "string",
                "double",
                "float",
                "localdatetime",
                "date_string",
                "instant",
                "long",
                "boolean",
                "local_time",
                "doubles",
                "date",
                "short",
                "object",
                "big",
                "offset_datetime"
        };

        List<Object[]> initialValues = new ArrayList<>();
        initialValues.add(
                new Object[] {
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
                        new Double[] {1.0,2.0,3.0},
                        LocalDate.now(),
                        (short)1,
                        new Object(),
                        BigDecimal.ZERO,
                        OffsetDateTime.now()
                }
        );
        initialValues.add(
                new Object[] {
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
                        new Double[]{},
                        LocalDate.of( 1970, 1, 1 ),
                        (short)45,
                        new Object(),
                        BigDecimal.ONE,
                        OffsetDateTime.ofInstant( Instant.now(), ZoneId.of( "UTC" ) )
                }
        );
        initialValues.add(
                new Object[] {
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
                        new Double[]{-1.5, 1.58489687, 1568.54684476},
                        LocalDate.of( 1987, 10, 29 ),
                        Short.MAX_VALUE,
                        new Object(),
                        new BigDecimal( 28.4 ),
                        OffsetDateTime.of(
                                1987,
                                10,
                                19,
                                12,
                                11,
                                15,
                                1868,
                                ZoneOffset.UTC
                        )
                }
        );

        DataBuilder builder = DataBuilder.with( columnNames );

        for (Object[] row : initialValues)
        {
            builder.addRow( row );
        }

        DataProvider provider = builder.build();
        int rowNumber = 0;
        while (provider.next())
        {
            int columnIndex = 0;
            for (String columnName : columnNames)
            {
                Object control = initialValues.get( rowNumber )[columnIndex];
                Object test = provider.getValue( columnName );
                Assert.assertEquals(
                        "The values of the '" +
                        columnName +
                        "' column were not equivalent on row " +
                        rowNumber +
                        ". The values were: " +
                        String.valueOf( control ) +
                        " and " +
                        String.valueOf( test ),
                        control, test);
                columnIndex++;
            }

            rowNumber++;
        }
    }

    @Test
    /*
     * Tests if numerical values may be converted to one another
     */
    public void numericalConversionTests()
    {
        String[] columns = {
                "byte",
                "short",
                "int",
                "long",
                "float",
                "double",
                "string",
                "bad_string",
                "bad_object"
        };

        Object[] values = {
                (byte)0,
                (short)1,
                2,
                (long)3,
                4.0F,
                5.0,
                "6",
                "tokyo",
                new Object()
        };
        DataProvider data = DataBuilder.with(columns )
                                       .addRow( values )
                                       .build();

        // "byte" column conversions
        String column = "byte";
        Assert.assertEquals( (Byte)(byte)0, data.getByte( column ) );
        Assert.assertEquals( (Short)(short)0, data.getShort( column ) );
        Assert.assertEquals( (Integer)0, data.getInt( column ) );
        Assert.assertEquals( (Long)(long)0, data.getLong( column ) );
        Assert.assertEquals( (float)0, data.getFloat( column ), 0.00001 );
        Assert.assertEquals( (double)0, data.getDouble( column ), 0.00001 );
        Assert.assertEquals( new BigDecimal( 0 ), data.getBigDecimal( column ) );
        Assert.assertEquals( "0", data.getString( column ) );

        // "short" column conversions
        column = "short";
        Assert.assertEquals( (Byte)(byte)1, data.getByte( column ) );
        Assert.assertEquals( (Short)(short)1, data.getShort( column ) );
        Assert.assertEquals( (Integer)1, data.getInt( column ) );
        Assert.assertEquals( (Long)(long)1, data.getLong( column ) );
        Assert.assertEquals( (float)1, data.getFloat( column ), 0.00001 );
        Assert.assertEquals( (double)1, data.getDouble( column ), 0.00001 );
        Assert.assertEquals( new BigDecimal( 1 ), data.getBigDecimal( column ) );
        Assert.assertEquals( "1", data.getString( column ) );

        // "int" column conversions
        column = "int";
        Assert.assertEquals( (Byte)(byte)2, data.getByte( column ) );
        Assert.assertEquals( (Short)(short)2, data.getShort( column ) );
        Assert.assertEquals( (Integer)2, data.getInt( column ) );
        Assert.assertEquals( (Long)(long)2, data.getLong( column ) );
        Assert.assertEquals( (float)2, data.getFloat( column ), 0.00001 );
        Assert.assertEquals( (double)2, data.getDouble( column ), 0.00001 );
        Assert.assertEquals( new BigDecimal( 2 ), data.getBigDecimal( column ) );
        Assert.assertEquals( "2", data.getString( column ) );

        // "long" column conversions
        column = "long";
        Assert.assertEquals( (Byte)(byte)3, data.getByte( column ) );
        Assert.assertEquals( (Short)(short)3, data.getShort( column ) );
        Assert.assertEquals( (Integer)3, data.getInt( column ) );
        Assert.assertEquals( (Long)(long)3, data.getLong( column ) );
        Assert.assertEquals( (float)3, data.getFloat( column ), 0.00001 );
        Assert.assertEquals( (double)3, data.getDouble( column ), 0.00001 );
        Assert.assertEquals( new BigDecimal( 3 ), data.getBigDecimal( column ) );
        Assert.assertEquals( "3", data.getString( column ) );

        // "float" column conversions
        column = "float";
        Assert.assertEquals( (Byte)(byte)4, data.getByte( column ) );
        Assert.assertEquals( (Short)(short)4, data.getShort( column ) );
        Assert.assertEquals( (Integer)4, data.getInt( column ) );
        Assert.assertEquals( (Long)(long)4, data.getLong( column ) );
        Assert.assertEquals( (float)4, data.getFloat( column ), 0.00001 );
        Assert.assertEquals( (double)4, data.getDouble( column ), 0.00001 );
        Assert.assertEquals( new BigDecimal( 4 ), data.getBigDecimal( column ) );
        Assert.assertEquals( "4.0", data.getString( column ) );

        // "double" column conversions
        column = "double";
        Assert.assertEquals( (Byte)(byte)5, data.getByte( column ) );
        Assert.assertEquals( (Short)(short)5, data.getShort( column ) );
        Assert.assertEquals( (Integer)5, data.getInt( column ) );
        Assert.assertEquals( (Long)(long)5, data.getLong( column ) );
        Assert.assertEquals( (float)5, data.getFloat( column ), 0.00001 );
        Assert.assertEquals( (double)5, data.getDouble( column ), 0.00001 );
        Assert.assertEquals( new BigDecimal( 5 ), data.getBigDecimal( column ) );
        Assert.assertEquals( "5.0", data.getString( column ) );

        // "String" column conversions
        column = "string";
        Assert.assertEquals( (Byte)(byte)6, data.getByte( column ) );
        Assert.assertEquals( (Short)(short)6, data.getShort( column ) );
        Assert.assertEquals( (Integer)6, data.getInt( column ) );
        Assert.assertEquals( (Long)(long)6, data.getLong( column ) );
        Assert.assertEquals( 6.0F, data.getFloat( column ), 0.00001 );
        Assert.assertEquals( 6.0, data.getDouble( column ), 0.00001 );
        Assert.assertEquals( new BigDecimal( 6 ), data.getBigDecimal( column ) );
        Assert.assertEquals( "6", data.getString( column ) );

        // "bad_string" column conversions
        failsOnNumericConversion( data, "bad_string" );

        // "bad_object" column conversions
        failsOnNumericConversion( data, "bad_object" );
    }

    private void failsOnNumericConversion(final DataProvider data, final String column)
    {
        Stream.Builder<Function<String, ?>> builder = Stream.builder();
        Stream<Function<String, ?>> conversions = builder.add(data::getByte)
                                                         .add(data::getShort)
                                                         .add(data::getInt)
                                                         .add(data::getLong)
                                                         .add(data::getFloat)
                                                         .add(data::getDouble)
                                                         .add(data::getBigDecimal)
                                                         .build();

        conversions.forEach( function -> {
            boolean thrown = false;
            try
            {
                function.apply( column );
            }
            catch (ClassCastException c)
            {
                thrown = true;
            }

            Assert.assertTrue( "'" + function.toString() +
                               "' should not have been able to convert the value in '" +
                               column,
                               thrown );
        } );
    }
}
