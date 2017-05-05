package wres.datamodel;

import java.time.LocalDateTime;

import org.junit.Test;

public class TimeConversionTest
{
    @Test
    public void testConvertTimeToAndFromInternal()
    {
        // pick a datetime
        LocalDateTime someTime = LocalDateTime.of(2017, 4, 21, 10, 50);
        // convert it to internal representation
        int convertedTime = TimeConversion.internalTimeOf(someTime);
        // convert it back
        LocalDateTime hopefullySameTime = TimeConversion.localDateTimeOf(convertedTime);
        // Should be the same
        assert(someTime.equals(hopefullySameTime));
    }
}
