package wres.reading.wrds.nwm;

import org.junit.Assert;
import org.junit.Test;

import tools.jackson.databind.ObjectMapper;

import wres.datamodel.MissingValues;

public class NwmDataPointDeserializerTest
{
    private static final String GOOD_TEST_CASE = """
            {\r
            "time": "20210523T02:00:00Z",\r
            "value": 0.04\r
            }""";

    private static final String NULL_TEST_CASE = """
            {\r
            "time": "20210523T02:00:00Z",\r
            "value": null\r
            }""";

    @Test
    public void readGoodTestCase()
    {
        NwmDataPoint dataPoint = new ObjectMapper().readValue( GOOD_TEST_CASE.getBytes(), NwmDataPoint.class );
        Assert.assertEquals( "2021-05-23T02:00:00Z", dataPoint.time().toString() );
        Assert.assertEquals( 0.04d, dataPoint.value(), 0.0d );
    }

    @Test
    public void readNullTestCase()
    {
        NwmDataPoint dataPoint = new ObjectMapper().readValue( NULL_TEST_CASE.getBytes(), NwmDataPoint.class );
        Assert.assertEquals( "2021-05-23T02:00:00Z", dataPoint.time().toString() );
        Assert.assertEquals( MissingValues.DOUBLE, dataPoint.value(), 0.0d );
    }
}
