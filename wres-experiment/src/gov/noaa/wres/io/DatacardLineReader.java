package gov.noaa.wres.io;

import java.util.List;
import java.util.ArrayList;

import java.util.function.Function;

import java.time.LocalDateTime;
import java.time.Duration;
import java.time.Year;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.format.FormatStyle;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import gov.noaa.wres.datamodel.Event;
import gov.noaa.wres.datamodel.SimpleEvent;

public class DatacardLineReader implements Function<String,List<Event>>
{
    private final ZoneOffset offset;

    private DatacardLineReader(ZoneOffset offset)
    {
        this.offset = offset;
    }

    public static DatacardLineReader of(ZoneOffset offset)
    {
        return new DatacardLineReader(offset);
    }

    public ZoneOffset getOffset()
    {
        return this.offset;
    }

    public List<Event> apply(String datacardLine)
    {
        // Sample header here (note this is from Ross' output)
        // $  IDENTIFIER=BLKO2          DESCRIPTION=BLKO2 
        // $  PERIOD OF RECORD=01/1951 THRU 12/1999 
        // $  SYMBOL FOR MISSING DATA=-999.00   SYMBOL FOR ACCUMULATED DATA=-998.00 
        // $  TYPE=MAP    UNITS=IN     DIMENSIONS=L      DATA TIME INTERVAL= 6 HOURS 
        // $  OUTPUT FORMAT=(3A4,2I2,I4,6F9.3) 
        // DATACARD      MAP  L    IN    6   BLKO2          BLKO2 
        //  1  1985 12   1999  4   F9.3    

        if (datacardLine.startsWith("$")
            || datacardLine.startsWith("DATACARD")
            || datacardLine.startsWith(" "))
        {
            System.out.println("Datacard Header: " + datacardLine);
            return null;
        }

        // Sample line here:
        // BLKO2       0185  09    0.000    0.257    0.026    0.121

        String locationId =   datacardLine.subSequence(0,12).toString();
        String month =        datacardLine.subSequence(12,14).toString();
        String twoDigitYear = datacardLine.subSequence(14,16).toString();
        String twoDigitDay =  datacardLine.subSequence(18,20).toString();
        // String monthYearDay = datacardLine.subSequence(12,20).toString();
        String fieldOne =     datacardLine.subSequence(20,29).toString();
        String fieldTwo =     datacardLine.subSequence(29,38).toString();
        String fieldThree =   datacardLine.subSequence(38,47).toString();
        String fieldFour =    datacardLine.subSequence(47,56).toString();

        // never would have gotten this from API.
        // https://stackoverflow.com/questions/29490893/parsing-string-to-local-date-doesnt-use-desired-century
        // still isn't working though
        // DateTimeFormatter formatter = new DateTimeFormatterBuilder()
        //     .appendValueReduced(ChronoField.YEAR, 2, 2, 1900)
        //     .appendPattern("MMyy  dd")
        //     .toFormatter();
        // LocalDateTime dateTime = LocalDateTime.parse(monthYearDay, formatter);
        LocalDateTime dateTime = 
            LocalDateTime.of(Integer.valueOf(twoDigitYear) + 1900,
                             Integer.valueOf(month),
                             Integer.valueOf(twoDigitDay),
                             0, 0, 0);

        LocalDateTime firstDate =  dateTime.plusHours(6)
            .minusSeconds(getOffset().getLong(ChronoField.OFFSET_SECONDS));
        LocalDateTime secondDate = dateTime.plusHours(12)
            .minusSeconds(getOffset().getLong(ChronoField.OFFSET_SECONDS));
        LocalDateTime thirdDate =  dateTime.plusHours(18)
            .minusSeconds(getOffset().getLong(ChronoField.OFFSET_SECONDS));
        LocalDateTime fourthDate = dateTime.plusHours(24)
            .minusSeconds(getOffset().getLong(ChronoField.OFFSET_SECONDS));

        List<Event> result = new ArrayList<>();

        result.add(SimpleEvent.of(firstDate, Double.valueOf(fieldOne)));
        result.add(SimpleEvent.of(secondDate, Double.valueOf(fieldTwo)));
        result.add(SimpleEvent.of(thirdDate, Double.valueOf(fieldThree)));
        result.add(SimpleEvent.of(fourthDate, Double.valueOf(fieldFour)));

        return result;
    }
}
