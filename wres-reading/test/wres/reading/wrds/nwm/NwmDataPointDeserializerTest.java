package wres.reading.wrds.nwm;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import wres.datamodel.MissingValues;

public class NwmDataPointDeserializerTest
{

    private static final String GOOD_TEST_CASE = "{\r\n"
                                                 + "\"time\": \"20210523T02:00:00Z\",\r\n"
                                                 + "\"value\": 0.04\r\n"
                                                 + "}";
    private static final String NULL_TEST_CASE = "{\r\n"
            + "\"time\": \"20210523T02:00:00Z\",\r\n"
            + "\"value\": null\r\n"
            + "}";
    
    @Test
    public void readGoodTestCase() throws JsonParseException, JsonMappingException, IOException
    {
            NwmDataPoint dataPoint = new ObjectMapper().readValue(GOOD_TEST_CASE.getBytes(), NwmDataPoint.class);
            Assert.assertEquals("2021-05-23T02:00:00Z", dataPoint.time().toString());
            Assert.assertEquals( 0.04d, dataPoint.value(), 0.0d);
    }

    @Test
    public void readNullTestCase() throws JsonParseException, JsonMappingException, IOException
    {
            NwmDataPoint dataPoint = new ObjectMapper().readValue(NULL_TEST_CASE.getBytes(), NwmDataPoint.class);
            Assert.assertEquals("2021-05-23T02:00:00Z", dataPoint.time().toString());
            Assert.assertEquals( MissingValues.DOUBLE, dataPoint.value(), 0.0d);
    }
}
