package gov.noaa.wres.io;

import java.util.Arrays;

import java.util.function.Function;

import java.time.LocalDateTime;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.format.DateTimeFormatter;

//import gov.noaa.wres.datamodel.Event;
//import gov.noaa.wres.datamodel.ForecastEvent;
import gov.noaa.wres.datamodel.EnsembleForecastEvent;

/**
 * Tricky: how do we do error handling while implementing Function?
 */
public class AscLineReader implements Function<String,EnsembleForecastEvent>
{
    private final ZoneOffset offset;

    private AscLineReader(ZoneOffset offset)
    {
        this.offset = offset;
    }

    public static AscLineReader of(ZoneOffset offset)
    {
        return new AscLineReader(offset);
    }

    public ZoneOffset getOffset()
    {
        return this.offset;
    }

    public EnsembleForecastEvent apply(String ascLine)
    {
        // Sample Line here:
        // 01/02/1985 18 30.0 0.013 0.016 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.067 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0
        // field 0: date in old US format
        // field 1: time HH
        // field 2: lead time in hours
        // field 3..n: members 0..n-3
        String[] ascLineParts = ascLine.split(" ");

        if (ascLineParts.length < 4)
        {
            System.err.println("AscLineReader failed to parse this line "
                               + "due to lack of data:");
            System.err.println(ascLine);
            return null;
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy'T'HH");
        LocalDateTime dateTime = LocalDateTime.parse(ascLineParts[0] + "T"
                                                     + ascLineParts[1],
                                                     formatter)
            .minusSeconds(getOffset().get(ChronoField.OFFSET_SECONDS));

        // We assume that field 2 is lead time in hours
        // But hours cannot have fractions, so we convert to seconds first.
        double leadTimeInSeconds = 3600.0 * Double.parseDouble(ascLineParts[2]);
        Duration leadTime = Duration.parse("PT" + leadTimeInSeconds + "S");

/*
        return Arrays.stream(ascLineParts)
            .skip(3) // skip date, hour, and lead time
            .map(v -> ForecastEvent.of(dateTime, leadTime, Double.valueOf(v)))
            .collect(toList());
*/
        // instead of collecting to a list of ForecastEvents,
        // create a single EnsembleForecastEvent holding an array of values.
        return EnsembleForecastEvent.of(dateTime,
                                        leadTime,
                                        Arrays.stream(ascLineParts)
                                        .skip(3) // skip date,hour,leadtime
                                        .mapToDouble(Double::valueOf)
                                        .toArray());
    }
}
