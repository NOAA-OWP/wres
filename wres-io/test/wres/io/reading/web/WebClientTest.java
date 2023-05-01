package wres.io.reading.web;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link WebClient}.
 * 
 * @author James Brown
 */

class WebClientTest
{

    /**
     * #77007
     */
    
    @Test
    void testGetTimingInformationDoesNotThrowArrayIndexOutOfBoundsExceptionWhenNothingRead()
    {
        WebClient client = new WebClient( true );
        
        String timingInformation = client.getTimingInformation();
        String firstPart = StringUtils.substringBefore( timingInformation, "," );
        
        assertEquals( "Out of request/response count 0", firstPart );
    }
}
