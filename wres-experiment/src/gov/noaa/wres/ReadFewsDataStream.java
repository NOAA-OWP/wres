package gov.noaa.wres;

import javax.xml.bind.*;
import javax.xml.stream.*;
import javax.xml.transform.stream.StreamSource;
import javax.xml.stream.events.*;

import java.time.Duration;
import java.time.LocalDateTime;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Stream;
import static java.util.stream.Collectors.*;
import java.util.function.Predicate;
import java.util.function.BiFunction;

import gov.noaa.wres.io.XmlParser;
import gov.noaa.wres.pixml.Header;
import gov.noaa.wres.pixml.DelftEvent;
import gov.noaa.wres.pixml.ReadPiIntoEvent;
import gov.noaa.wres.pixml.ReadPiIntoHeader;
import gov.noaa.wres.datamodel.Event;
import gov.noaa.wres.datamodel.ForecastEvent;
import gov.noaa.wres.datamodel.PairEvent;
import gov.noaa.wres.datamodel.SimpleEvent;
import gov.noaa.wres.datamodel.TimeSeries;

import gov.noaa.wres.metric.SimpleError;


public class ReadFewsDataStream
{
    public static void main(String[] args) throws JAXBException, InterruptedException
    {
/* from 2017-02-17... was working to print out stuff
        // create streaming parser emitting objects?
        XMLInputFactory xif = XMLInputFactory.newFactory();
        StreamSource xml = new StreamSource("../data/CBNK1.qin.xml");

        // advance to element desired?

        XMLStreamReader xsr = null;

        long count = 0;
        try
        {
            xsr = xif.createXMLStreamReader(xml);
            while (xsr.hasNext()) // && !xsr.getLocalName().equals("Event"))
            {
                System.out.println("On count " + count);

                int type = xsr.next();
                switch(type)
                {
                case XMLEvent.START_DOCUMENT:
                    System.out.println("Start of doc");
                    break;
                case XMLEvent.START_ELEMENT:
                    System.out.println("Start element: " + xsr.getName());
                    break;
                case XMLEvent.CHARACTERS:
                    System.out.println("Characters: " + xsr.getText());
                    break;
                case XMLEvent.END_ELEMENT:
                    System.out.println("End element: " + xsr.getName());
                    break;
                case XMLEvent.END_DOCUMENT:
                    System.out.println("End of document");
                    break;
                default:
                    System.out.println("Event type: " + type);
                    break;
                }
                count++;
            }
*/
            // look for Event

/* from before 2017-02-17... was kind of maybe working? no.
            JAXBContext jaxbContext = JAXBContext.newInstance(DelftEvent.class);
            System.out.println("Here are jaxb classes the context knows: " + jaxbContext);

            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();

            List<JAXBElement<DelftEvent>> jaxbElements = new ArrayList<>();

            while (xsr.getLocalName().equals("Event") && xsr.hasNext())
            {
                jaxbElements.add(jaxbUnmarshaller.unmarshal(xsr, DelftEvent.class));
            }
            // create stream?

            Stream.of(jaxbElements)
                .forEach(System.out::println);
*/

/* (continuing part from 2017-02-17)
        }
        catch (XMLStreamException e)//|JAXBException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (xsr != null)
            {
                try
                {
                    xsr.close();
                }
                catch (XMLStreamException e)
                {
                    System.err.println(e + " sigh we give up.");
                }
            }
        }
*/
        
        /*
        // run filter on resulting stream of objects?
        myParser.getObjects()
            .stream()
            .filter(instanceof DelftEvent)
            .map(System.out::println)
            .collect();
        */

        // can I zip this with another stream?


        /* maybe I want a function that has this sig
           getStreamFromXml(ByteStream bs, JaxbObject.class, int which, JaxbCounterObject.class)
           where the "which" is a kind of index into a list of collections
           so suppose we have multiple series, we can get one series
           of events out of it by saying 5, which is the 6th series.
           The JaxbCounterObject is if we run into this element, we start the counter at 0. Each of those we run into will increment the counter.
           if last arg is null, we treat it as flat list? Or supply second method sig with first two args only.

           So what was different about myParser.getObjects()?
        */

        /* Another issue: if we simply return a collection, what about
           all the metadata, such as units, etc? Is that a separate call?
           That information is in the xml, I think.
        */

        /* Also, another issue with attempting to create a data
           pipeline is this:

           https://docs.oracle.com/javase/8/docs/api/java/util/Spliterator.html

           For mutable sources, arbitrary and non-deterministic behavior may 
           occur if the source is structurally interfered with (elements added,
           replaced, or removed) between the time that the Spliterator binds to
           its data source and the end of traversal. For example, such
           interference will produce arbitrary, non-deterministic results when
           using the java.util.stream framework. 

           So really, all the reading has to be done at once. But wait.
           Earlier it said a generator. 

           public interface Spliterator<T>

           An object for traversing and partitioning elements of a source. The
           source of elements covered by a Spliterator could be, for example, an
           array, a Collection, an IO channel, or a generator function.
          
        */

        /* immutability -> no side effects -> lazy -> infinite streams */
        /* So can we really read from a file lazily? Reading is side-effect?
           Seems like we don't use the stream API, but just pass
           functions into the xml reading and apply them while reading.
         */

        /* lazy or eager? Look at return type.
           Returning a stream from a stream means lazy.
           
           terminal: sum, reduce, collect...
        */

        /* So what do we want?
           function that accepts
           a function that accepts an xml event.

           Inner function:
           ActualData parseData(XMLEvent x)
           {
               // deserialize a single event into ActualData object
           }

           Xml parsing function:
           Collection<R> parseXml(ByteArrayInputStream, Function<T,R>)
           {
               // apply parseData to each xml event from input stream;
               // return the resulting collection of ActualDatas.
           }

           Do we want another function that accepts two streams, two datas?
           Aka pairing? See page 52ish of Design Doc, 3.4.1.
           But no, this is talking about application of a function to 
           a whole DATASET that already exists, not applying the function
           DURING READING of the dataset.

           Should we also emit a sample size of pairs during pairing?
           Accept a random number generator and a threshold?
           For example, 0.05 to get around 5% of the paired values?

           The parseData is CONDITIONING. How do we tell it conditions
           when it only accepts one argument?
           
           Perhaps use an instance method? The object contains
           the conditions?

           Conditions c = new Conditions(new HashMap<String,Object>());
           List<Event> = parseXml(stream, c.parseData); //?

           How much should we do during reading? the whole kit-n-kaboodle?
           or just conditioning and pairing?
         
           What would it look like to do it all? N readers at once, gated?

           I think the right balance is conditioning on read, then
           create the immutable collection. From there: lazy streams.
           
           See 3.4.2.5 discussion in design.


           A Conditions object would therefore contain a list of conditions.
           It would also have the method that knows how to unmarshal
           an xml object. Except those are going to be separate as well.

           The filename itself is a kind of condition?
           The type of data is a condition?
           Could a list of conditions be everything needed to ID a dataset?

           Do we need to pair in lockstep? Read a little from this, read a little from that? If it's going to fail to pair, need to fail right away, right?

           Some set of conditions need to be identical, namely timesteps.

           Do we need to aggregate during pairing then?
           List of conditions, list of aggregations. DURING READING.

           Some data gets skipped, some data gets added, sometimes gets agged.
           But aggregation may be required for pairing.
           So we can't pair before aggregating.
           But aggregation requires state. So we can't aggregate per-event.

           If aggregation happens before pairing, only conditioning on read.

           But we can tell if an aggregation will cause trouble BEFORE we do
           ANYTHING.

           No need to read until we verify everything is good to go.

           (end comments from 2017-02-17)

           (begin comments from 2017-02-21)
           Seems simplest to read full data before any aggregation.
           Some conditioning can be applied to data while being read.


           // going back to a possibility:
           Conditions c = new Conditions(new HashMap<String,Object>());
           List<Event> = parseXml(stream, c.parseData); //?

           This is similar to how EVS does it, it seems.
           Except conditions are more heavily typed/expressed.
           Conditions are really a list of predicates, right?
           
           More like this:
           Conditions c = new Conditions(new List<Predicate>);

           Do we really even need a Conditions object then?
           Conditions object is configuration?

           Let's roll with it for now.

           Seems really silly, but oh well.
           Oh yeah, need to have a parseXml method? or does that even make sense.
           We would pass the list of conditions to the parse method.

           Where is the parse method?
        */

        List<Predicate<Event>> stuffToTest = new ArrayList<>();
// forget predicates for now, this is known working 20170302
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


// single set of events (one timeseries):
//        StreamSource xml = new StreamSource("../data/CBNK1.qin.xml");
// multiple series in one file:
        StreamSource xml = new StreamSource("../data/198601011200_JAKT2_hefsraw.xml");
// multiple series in subset of big file:
//        StreamSource xml = new StreamSource("../data/198601011200_JAKT2_hefsraw_subset.xml");
// multiple series with few events in tiny subset of big file:
//        StreamSource xml = new StreamSource("../data/198601011200_JAKT2_hefsraw_tiny_subset.xml");

        XmlParser p = new XmlParser(xml,             // Source file
                                    "header",        // Name of header tag
                                    Header.class,    // JAXB to parse header
                                    "event",         // name of event tag
                                    DelftEvent.class,// JAXB to parse event
                                    stuffToTest);    // Conditioning

        List<TimeSeries> myResults = p.parseXml(new ReadPiIntoHeader(),
                                                new ReadPiIntoEvent());
        
        System.out.println("Count of myResults: " + myResults.size());

//        System.out.println("Sleeping for 30 seconds");
//        Thread.sleep(30000);

        System.out.println("Here is the average: "
                           + myResults.get(1)
                           .getEvents()
                           .stream()
                           .filter(e -> e.getDateTime().getYear() < 2001)
//                           .mapToDouble(e -> e.getValue())
                           .map(e -> e.getValues())
                           .flatMapToDouble(Arrays::stream) // flatten arrays
                           .average()
                           .getAsDouble());


//        System.out.println("Sleeping for 30 seconds");
//        Thread.sleep(30000);
        System.out.println("Waking up to do pairing");

        // need to add some fake leadtimes to data?
//        Duration d = Duration.ofHours(5);
        // Collections.unmodifiableList

//        List<Event> fakeForecasts =
        TimeSeries fakeForecasts = 
            myResults.get(1);
//                     .getEvents();
//                     .stream()
//                     .map(e -> ForecastEvent.with(e,d))
//                     .collect(collectingAndThen(toList(),
//                                                Collections::unmodifiableList));

        List<PairEvent> myPairs = Pair.of(fakeForecasts,
                                          myResults.get(0));

        System.out.println("Done pairing");
//        System.out.println("Sleeping again for 30 seconds");
//        Thread.sleep(30000);
        System.out.println("Calculating mean error:");
        SimpleError errorFunc = new SimpleError();
        System.out.println(myPairs.stream()
                                  .map(errorFunc)
                                  .flatMapToDouble(Arrays::stream)
                                  .average());

//        Thread.sleep(30000);

        // suppose now I want only for a particular lead time.
        // let's use the first ensemble member as the observation,
        // the rest as forecasts
        TimeSeries observations = myResults.get(0);

        Map<Duration,List<PairEvent>> byLeadTime =
            myResults.stream()
            .skip(1) // everything but first list.
            .map(l -> Pair.of(observations, l)) // pair em
//            .forEach(System.out::println)   // now we have list of pairs
            .flatMap(Collection::stream)      // flatten to single list of pairs
            .collect(toList())                // flatten to single list of pairs
//            .forEach(System.out::println)     // single list of pairs?
            .stream()
            .collect(groupingBy(PairEvent::getLeadTime)); // by lead time
            // we now have a Map<Duration,List<PairEvent>>

        // thanks https://stackoverflow.com/questions/27448266/java-8-streams-iterate-over-map-of-lists?rq=1
        System.out.println(
            byLeadTime.entrySet()
            .stream()
            .collect(toMap(
                         Map.Entry::getKey,    // same key (lead time)
                         e -> e.getValue()     
                         .stream()             // work with stream on PairEvents
                         .map(errorFunc) // calculate error
                         .flatMapToDouble(Arrays::stream)
                         .average()))         // mean error
            );
    }



}
