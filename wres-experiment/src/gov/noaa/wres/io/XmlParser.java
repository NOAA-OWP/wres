package gov.noaa.wres.io;

import java.util.List;
import java.util.ArrayList;

import java.util.function.Predicate;
import java.util.function.Function;
import java.util.function.BiFunction;

import javax.xml.bind.Unmarshaller;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

import javax.xml.transform.stream.StreamSource;

import gov.noaa.wres.datamodel.Event;
import gov.noaa.wres.datamodel.TimeSeries;
import gov.noaa.wres.datamodel.SeriesInfo;

public class XmlParser
{
    private final StreamSource xmlSource;
    private final List<Predicate<Event>> predicates;

    private final XMLInputFactory xmlInputFactory;
    private final JAXBContext jaxbEventContext;
    private final JAXBContext jaxbHeaderContext;
    private final Unmarshaller jaxbEventUnmarshaller;
    private final Unmarshaller jaxbHeaderUnmarshaller;
    private final String headerLowerName;
    private final String eventLowerName;

    public XmlParser(StreamSource xmlSource,
                     String headerLowerName,
                     Class jaxbHeaderClass,
                     String eventLowerName,
                     Class jaxbEventClass,
                     List<Predicate<Event>> predicates) throws JAXBException
    {
        this.xmlSource = xmlSource;
        this.predicates = predicates;
        this.xmlInputFactory = XMLInputFactory.newFactory();
        this.jaxbEventContext = JAXBContext.newInstance(jaxbEventClass);
        this.jaxbEventUnmarshaller = this.jaxbEventContext.createUnmarshaller();
        this.jaxbHeaderContext = JAXBContext.newInstance(jaxbHeaderClass);
        this.jaxbHeaderUnmarshaller = this.jaxbHeaderContext.createUnmarshaller();
        this.headerLowerName = headerLowerName;
        this.eventLowerName = eventLowerName;
    }

    private XMLInputFactory getXMLInputFactory()
    {
        return this.xmlInputFactory;
    }

    private Unmarshaller getJaxbEventUnmarshaller()
    {
        return this.jaxbEventUnmarshaller;
    }

    private Unmarshaller getJaxbHeaderUnmarshaller()
    {
        return this.jaxbHeaderUnmarshaller;
    }

    public String getHeaderLowerName()
    {
        return this.headerLowerName;
    }

    public String getEventLowerName()
    {
        return this.eventLowerName;
    }

    /**
     * warning: leaking state
     */
    public List<Predicate<Event>> getPredicates()
    {
        return this.predicates;
    }

    /**
     * xmlSource - where to read from
     * xmlFunc - how to read each event
     * Warning: side effecting galore, but with internal objects.
     */
    public List<TimeSeries> parseXml(Function<XmlReaderInfo,SeriesInfo> xmlMetadataFunc,
                                     BiFunction<XmlReaderInfo,SeriesInfo,Event> xmlFunc)
    {
        // Deserialize xmlSource using function f.
        // Apply the conditions required to each event, adding to
        // the resulting collection only if the condition is met.

        List<TimeSeries> result = new ArrayList<>();

        XMLStreamReader xsr = null;

        SeriesInfo header = null;
        List<Event> events = new ArrayList<>();

        try
        {
            xsr = getXMLInputFactory().createXMLStreamReader(xmlSource);
            XmlReaderInfo headerReaderInfo = XmlReaderInfo.of(xsr,
                                                              getJaxbHeaderUnmarshaller());
            XmlReaderInfo eventReaderInfo = XmlReaderInfo.of(xsr,
                                                             getJaxbEventUnmarshaller());
            while (xsr.hasNext())
            {
                int type = xsr.next();
                if (type == XMLEvent.START_ELEMENT)
                {
                    if (xsr.getLocalName().toLowerCase() == getHeaderLowerName())
                    {
                        // first, try to unmarshal using header/metadata.
                        // Every time this is non-null, start a new TimeSeries
                        SeriesInfo h = xmlMetadataFunc.apply(headerReaderInfo);
                        // only after we have gone through once.
                        if (h != null)
                        {
                            // only after we hit the second header start adding.
                            if (header != null)
                            {
                                result.add(TimeSeries.of(header, events));
                            }
                            events = new ArrayList<>();
                            header = h;
                        }
                    }
                    else if (xsr.getLocalName().toLowerCase() == getEventLowerName())
                    {
                        Event e = xmlFunc.apply(eventReaderInfo, header);
                        boolean success = true;

                        // test each condition against this event.
                        // Seems really non-functional-ish for now.
                        // But then again we're inside a non-functional interface?
                        for (Predicate<Event> p : getPredicates())
                        {
                            if (e == null)
                            {
                                System.out.println("e was null");
                                success = false;
                                break;
                            }
                            else if (!p.test(e))
                            {
                                System.out.println("e failed a test");
                                success = false;
                                break;
                            }
                        }

                        // If none of the tests failed, success!
                        if (success)
                        {
                            System.out.println("Found an event passing tests");
                            events.add(e);
                        }
                    }
                    else
                    {
                        System.out.println("Skipping this start element: "
                                           + xsr.getLocalName());
                    }
                }
            }
            
            // if we never ran into another header, we're at the end.
            result.add(TimeSeries.of(header, events));

        }
        catch (XMLStreamException e)
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

        return result;
    }



// old
    /**
     * xmlSource - where to read from
     * xmlFunc - how to read each event
     * Warning: side effecting galore, but with internal objects.
     */
/*    public List<Event> parseXml(BiFunction<XMLStreamReader,Unmarshaller,Event> xmlFunc)
    {
        // Deserialize xmlSource using function f.
        // Apply the conditions required to each event, adding to
        // the resulting collection only if the condition is met.

        List<Event> result = new ArrayList<>();

        XMLStreamReader xsr = null;

        try
        {
            xsr = getXMLInputFactory().createXMLStreamReader(xmlSource);
            while (xsr.hasNext())
            {
                // first, try to unmarshal using header/metadata.
                // Every time this is non-null, add a new list of events.
                
                // ok ugly... UGLY! I have no XMLEvents! I have an XSR!
                Event e = xmlFunc.apply(xsr,getJaxbUnmarshaller());
                boolean success = true;

                // test each condition against this event.
                // Seems really non-functional-ish for now.
                // But then again we're inside a non-functional interface?
                for (Predicate<Event> p : getPredicates())
                {
                    if (e == null)
                    {
                        System.out.println("e was null");
                        success = false;
                        break;
                    }
                    else if (!p.test(e))
                    {
                        System.out.println("e failed a test");
                        success = false;
                        break;
                    }
                }

                // If none of the tests failed, success!
                if (success)
                {
                    System.out.println("Found an event passing tests");
                    result.add(e);
                }
            }
        }
        catch (XMLStreamException e)
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

        return result;
    }
*/    
}
