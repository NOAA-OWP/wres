package wres.io.reading.wrds.nwm;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import wres.datamodel.MissingValues;

public class NwmDataPointDeserializerTest
{

    private static final String GOOD_TEST_CASE = "{\r\n"
                                                 + "\"time\": \"20210523T02Z\",\r\n"
                                                 + "\"value\": 0.04\r\n"
                                                 + "}";
    private static final String NULL_TEST_CASE = "{\r\n"
            + "\"time\": \"20210523T02Z\",\r\n"
            + "\"value\": null\r\n"
            + "}";
    
    @Test
    public void readGoodTestCase() throws JsonParseException, JsonMappingException, IOException
    {
            NwmDataPoint dataPoint = new ObjectMapper().readValue(GOOD_TEST_CASE.getBytes(), NwmDataPoint.class);
            Assert.assertEquals("2021-05-23T02:00:00Z", dataPoint.getTime().toString());
            Assert.assertEquals(0.04d, dataPoint.getValue(), 0.0d);
    }

    @Test
    public void readNullTestCase() throws JsonParseException, JsonMappingException, IOException
    {
            NwmDataPoint dataPoint = new ObjectMapper().readValue(NULL_TEST_CASE.getBytes(), NwmDataPoint.class);
            Assert.assertEquals("2021-05-23T02:00:00Z", dataPoint.getTime().toString());
            Assert.assertEquals(MissingValues.DOUBLE, dataPoint.getValue(), 0.0d);
    }
}
