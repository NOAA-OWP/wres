package gov.noaa.wres.io;

// import java.io.IOException;

// import java.nio.file.Files;
import java.nio.file.Path;
// import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.ArrayList;
// import java.util.ArrayList;
import java.util.List;
// import java.util.stream.Stream;
import java.util.function.Predicate;

import gov.noaa.wres.datamodel.Event;
import gov.noaa.wres.datamodel.TimeSeries;
// import gov.noaa.wres.datamodel.EnsembleForecastEvent;
// import gov.noaa.wres.datamodel.SeriesInfo;

// import gov.noaa.wres.io.AscLineReader;

// import static java.util.stream.Collectors.*;

public class AscReader
{

    //Commented by JBr @ 9 July 2017: issues with compilation    
    public static List<TimeSeries> readAsc(final Path source, final ZoneOffset offset, final List<Predicate<Event>> tests)
    {
//        // moving to Ensemble events
//        //List<List<Event>> events = new ArrayList<>();
//        List<EnsembleForecastEvent> events = new ArrayList<>();
//        List<TimeSeries> result = new ArrayList<>();
//
//        try(Stream<String> fileLines = Files.lines(source,
//                                                   StandardCharsets.UTF_8))
//        {
//            AscLineReader reader = AscLineReader.of(offset);
//            // for each line, get a list of ForecastEvents, preserving order
//            events = fileLines.map(reader)
//                .collect(toList());
//        }
//        catch (IOException ioe)
//        {
//            System.err.println("While reading " + source + ": " + ioe);
//            return result;
//        }
//        // There are multiple forecasts put in here. Each forecast/ensembleid
//        // combination should be a separate timeseries. Group by forecast
//        // datetime.
//
//        // have to read the valid datetime AND the duration, calculate forecast date
//
//        // shape of data now:
//        // ((forecast1validTime1, (ensembleMem1, ensembleMem2...ensembleMemN)),
//        //  (forecast1validTime2, (ensembleMem1, ensembleMem2...ensembleMemN)),
//        //  ...
//        //  (forecast1validTimeN, (ensembleMem1, ensembleMem2...ensembleMemN)),
//        //  (forecast2validTime1, (ensembleMem1, ensembleMem2...ensembleMemN)),
//        //  (forecast2validTime2, (ensembleMem1, ensembleMem2...ensembleMemN)),
//        //  ...
//        // )
//
//        // What we REALLY want:
//        // {forecast1issueTime:((ensembleMem1, ensembleMem1, ensembleMem1...),
//        //                      (ensembleMem2, ensembleMem2, ensembleMem2...),
//        // ...
//        // }
//           
///*
//        // <ORIGINAL WAY>
//
//        // Transpose from rows based on time to rows based on ensemble id
//        // This is stateful, avoid streams?
//
//        // Integer here is the ensemble id, LocalDateTime is the forecast date.
//        Map<LocalDateTime,Map<Integer,List<Event>>> byEnsembleIdAndForecast =
//            new HashMap<>();
//
//        // Maybe there's a way to do this transposition with streams, idk.
//        for (List<Event> byTimeByEnsemble : events)
//        {
//            Map<Integer,List<Event>> byEnsembleId = null;
//            LocalDateTime forecastDateTime = LocalDateTime.MIN;
//            if (!byTimeByEnsemble.isEmpty())
//            {
//                forecastDateTime = byTimeByEnsemble.get(0).getIssuedDateTime();
//            }
//            else
//            {
//                continue;
//            }
//
//            if (byEnsembleIdAndForecast.containsKey(forecastDateTime))
//            {
//                byEnsembleId = byEnsembleIdAndForecast.get(forecastDateTime);
//            }
//            else
//            {
//                byEnsembleId = new HashMap<>();
//                byEnsembleIdAndForecast.put(forecastDateTime, byEnsembleId);
//            }
//
//            // To get the index, that is, ensemble id, a traditional "for" helps
//            for (int i = 0; i < byTimeByEnsemble.size(); i++)
//            {
//                if (byEnsembleId.containsKey(i))
//                {
//                    // note, mutating collection held by outer collection
//                    byEnsembleId.get(i).add(byTimeByEnsemble.get(i));
//                }
//                else
//                {
//                    List<Event> newEnsemble = new ArrayList<>();
//                    newEnsemble.add(byTimeByEnsemble.get(i));
//                    // note, mutating collection held by outer collection
//                    byEnsembleId.put(i, newEnsemble);
//                }
//            }
//        }
//
//        // wait, why not just go straight to a list of timeseries?
//        // because each timeseries would be mutated along the way, that's why.
//        // we are adding to each timeseries with each row processed.
//
//
//        // Create the resulting TimeSerieses (and conditioning)
//        // Unfortunate we can't condition while doing file io
//        // because this format has data inferred by position... or can we?
//
//
//        // Finally create the TimeSeries List.
//        byEnsembleIdAndForecast.entrySet()
//            .stream()
//            .forEach(outer -> outer.getValue()
//                     .entrySet()
//                     .stream()
//                     .map(inner -> TimeSeries.of(SeriesInfo.of(outer.getKey(),
//                                                               inner.getKey()),
//                                                 inner.getValue()
//                                                 .stream() // filter while creating.
//                                                 .filter(tests.stream()
//                                                         .reduce(Predicate::and).orElse(p->true))
//                                                 .collect(toList())))
//                     .forEach(result::add));
//        return result;
//        // </ORIGINAL WAY>
//*/    
//
//
//
//
//
//        // <10 MARCH 2017 WAY>
//
//
//        // Ensemble id is implicit now per event.
//
//        // wait, why not just go straight to a list of timeseries?
//
//        // Create the resulting TimeSerieses (and conditioning)
//        // Unfortunate we can't condition while doing file io
//        // because this format has data inferred by position... or can we?
//
//
//        // Finally create the TimeSeries List.
//        return events.stream()
//            .collect(groupingBy(EnsembleForecastEvent::getIssuedDateTime)) // map by forecast
//            .entrySet()
//            .stream()
////            .peek(System.out::println)
//            .map(byForecast -> TimeSeries.of(SeriesInfo.of(byForecast.getKey()),
//                                             byForecast.getValue()
//                                             .stream() // filter while creating.
//                                             .filter(tests.stream()
//                                                     .reduce(Predicate::and).orElse(p->true))
//                                             .collect(toList())))
//            .collect(toList());
//
//        // </10 MARCH 2017 WAY>

        //JB to allow compilation
        return new ArrayList<>();
    }
}
