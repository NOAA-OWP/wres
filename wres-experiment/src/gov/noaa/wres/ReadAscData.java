package gov.noaa.wres;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.time.Duration;
import java.time.LocalDateTime;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.stream.Stream;
import static java.util.stream.Collectors.*;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;

import java.time.ZoneOffset;

import gov.noaa.wres.datamodel.Event;
import gov.noaa.wres.datamodel.ForecastEvent;
import gov.noaa.wres.datamodel.PairEvent;
import gov.noaa.wres.datamodel.SimpleEvent;
import gov.noaa.wres.datamodel.TimeSeries;

import gov.noaa.wres.io.AscReader;
import gov.noaa.wres.io.DatacardReader;

import gov.noaa.wres.metric.SimpleError;


public class ReadAscData
{
    public static void main(String[] args) throws InterruptedException
    {

        List<Predicate<Event>> stuffToTest = new ArrayList<>();
        stuffToTest.add(new Predicate<Event>()
                        {
                            public boolean test(Event t)
                            {
                                return t.getValue() != -999.0;
                            }
                        });
/*        stuffToTest.add(new Predicate<Event>()
                        {
                            public boolean test(Event t)
                            {
                                return t.getDateTime() != null 
                                    && t.getDateTime().getDayOfYear() > 2;
                            }
                        });

        stuffToTest.add(new Predicate<Event>()
                        {
                            public boolean test(Event t)
                            {
                                return t.getDateTime() != null 
                                    && t.getDateTime().getYear() > 1945;
                            }
                        });

*/

        long millis = System.currentTimeMillis();
        
        System.out.println("Reading observations...");

        Path blkFile = Paths.get("../data/BLKO2.txt2");
        ZoneOffset offsetCst = ZoneOffset.ofHours(-6);
        List<TimeSeries> obsResults = DatacardReader.read(blkFile,
                                                          offsetCst,
                                                          stuffToTest);
        TimeSeries observations = obsResults.get(0);

        System.out.println("Time to read observations: "
                           + (System.currentTimeMillis() - millis)
                           + " ms");
        System.out.println("Observation count: "
                           + observations.getEvents().size());

        millis = System.currentTimeMillis();
        
        Path hefsFile = Paths.get("../data/BLKO2_GEFSMAP.ASC");

        System.out.println("Reading HEFS forecasts...");

        ZoneOffset offsetGmt = ZoneOffset.ofHours(0);
        List<TimeSeries> myResults = AscReader.readAsc(hefsFile,
                                                       offsetGmt,
                                                       stuffToTest);
        System.out.println("Time to read forecasts: "
                           + (System.currentTimeMillis() - millis)
                           + " ms");
        
        System.out.println("Count of Ensemble TimeSerieses: " + myResults.size());

        System.out.println("Pairing then grouping by lead time...");


        millis = System.currentTimeMillis();
        Map<Duration,List<PairEvent>> byLeadTime =
            myResults.parallelStream()
//            .skip(1) // everything but first list.
            .map(l -> Pair.of(observations, l)) // pair em
//            .peek(System.out::println)   // now we have list of pairs
            .flatMap(Collection::stream)      // flatten to single list of pairs
            .collect(toList())                // flatten to single list of pairs
//            .peek(System.out::println)     // single list of pairs?
            .stream()
            .collect(groupingBy(PairEvent::getLeadTime)); // by lead time
            // we now have a Map<Duration,List<PairEvent>>

        System.out.println("Total pairing/grouping time: " +
                           (System.currentTimeMillis() - millis)
                           + " ms");

        System.out.println("Count of lead times: " + byLeadTime.size());
        // thanks https://stackoverflow.com/questions/27448266/java-8-streams-iterate-over-map-of-lists?rq=1

        millis = System.currentTimeMillis();

        ToDoubleFunction<PairEvent> errorFunc = new SimpleError();
        System.out.println(
            byLeadTime.entrySet()
            .stream()
            .collect(toMap(
                         Map.Entry::getKey,    // same key (lead time)
                         e -> e.getValue()     
                         .stream()             // work with stream on PairEvents
                         .mapToDouble(errorFunc) // calculate error
                         .average()))         // mean error
            );
        System.out.println("MeanError calculation time: " 
                           + (System.currentTimeMillis() - millis)
                           + " ms");

        Thread.sleep(30000);
    }
}
