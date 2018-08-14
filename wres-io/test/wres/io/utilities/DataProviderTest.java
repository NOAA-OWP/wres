package wres.io.utilities;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*") // thanks https://stackoverflow.com/questions/16520699/mockito-powermock-linkageerror-while-mocking-system-class#21268013
public class DataProviderTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DataProviderTest.class);

    @Test
    /*
    Tests typed retrieval
     */
    public void typeRetrieveTests()
    {
        String testString = "testString";
        short testShort = Short.MAX_VALUE;
        int testInt = Integer.MAX_VALUE;
        long testLong = Long.MAX_VALUE;
        float testFloat = Float.MAX_VALUE;
        double testDouble = Double.MAX_VALUE;
        Double[] testDoubles = {1.0, 2.0, 3.0};
        BigDecimal testDecimal = BigDecimal.TEN;
        boolean testBoolean = true;
        LocalDate testDate = LocalDate.now();
        LocalTime testTime = LocalTime.now();
        LocalDateTime testLocalDateTime = LocalDateTime.now();
        OffsetDateTime testOffsetDateTime = OffsetDateTime.now();
        Instant testInstant = Instant.now();

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
                        "instant" )
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
}
