package gov.noaa.wres.io;

import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.stream.Stream;
import java.util.function.Predicate;

import java.time.ZoneId;
import java.time.ZoneOffset;


import gov.noaa.wres.datamodel.Event;
import gov.noaa.wres.datamodel.TimeSeries;
import gov.noaa.wres.datamodel.SeriesInfo;


public class DatacardReader
{
    public static List<TimeSeries> read(Path source,
                                        ZoneOffset offset,
                                        List<Predicate<Event>> tests)
    {
        List<Event> events = new ArrayList<>();
        List<TimeSeries> result = new ArrayList<>();

        try(Stream<String> fileLines = Files.lines(source,
                                                   StandardCharsets.UTF_8))
        {
            DatacardLineReader reader = DatacardLineReader.of(offset);

            fileLines.map(reader)
                .filter(e -> e != null)
                // filter out based on tests
                .flatMap(List::stream)
                .filter(tests.stream()
                        .reduce(Predicate::and).orElse(p->true))
                .forEach(e -> events.add(e));
        }
        catch (IOException ioe)
        {
            System.err.println("While reading " + source + ": " + ioe);
            return result;
        }

        // now mash the events into a timeseries. hopefully it's just one!
        Event last = events.get(events.size() - 1); // to get the issued time

        // convert to UTC time on the way in above, call it UTC
        TimeSeries series = TimeSeries.of(SeriesInfo.of(last.getDateTime(),
                                                        ZoneId.of("UTC",
                                                            ZoneId.SHORT_IDS),
                                                        new HashMap<String,Object>()),
                                          events);
        result.add(series);
        return result;
    }
}
