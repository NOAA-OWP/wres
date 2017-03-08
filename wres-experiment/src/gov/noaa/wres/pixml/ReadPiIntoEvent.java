package gov.noaa.wres.pixml;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.JAXBException;

import java.util.function.BiFunction;
import java.time.Duration;
import java.time.LocalDateTime;

import gov.noaa.wres.io.XmlReaderInfo;

import gov.noaa.wres.datamodel.Event;
import gov.noaa.wres.datamodel.ForecastEvent;
import gov.noaa.wres.datamodel.SimpleEvent;
import gov.noaa.wres.datamodel.SeriesInfo;

import gov.noaa.wres.pixml.DelftEvent;

public class ReadPiIntoEvent implements BiFunction<XmlReaderInfo,SeriesInfo,Event>
{
    public Event apply(XmlReaderInfo xri, SeriesInfo seriesInfo)
    {
        XMLStreamReader reader = xri.getXMLStreamReader();
        Unmarshaller unmarsh = xri.getUnmarshaller();
        try
        {
            DelftEvent xmlEvent = (DelftEvent) unmarsh.unmarshal(reader);
            // make an immutable event.
            Event actualEvent = null;
            if (xmlEvent != null)
            {
                LocalDateTime eventDateTime = LocalDateTime.of(xmlEvent.date,
                                                               xmlEvent.time);
                // When there is a start time, this is a forecast
                if (seriesInfo.getForecastDateTime() != null)
                {
                    actualEvent =
                        ForecastEvent.of(eventDateTime,
                                         Duration.between(seriesInfo.getForecastDateTime(),
                                                          eventDateTime),
                                         xmlEvent.value);
                }
                else
                {
                    // merely an observation
                    actualEvent = 
                        SimpleEvent.of(eventDateTime,
                                       xmlEvent.value);
                }
            }
            return actualEvent;
        }
        catch (JAXBException e)
        {
            System.err.println("Stinking jaxb exception" + e);
            e.printStackTrace();
        }
        return null;
    }            
}


/*
    public Event apply(XMLStreamReader t, Unmarshaller u)
    {
        try
        {
            int type = t.next();
            switch(type)
            {
            case XMLEvent.START_DOCUMENT:
                System.out.println("Start of doc");
                break;
            case XMLEvent.START_ELEMENT:
                //System.out.println("Start element: " + t.getName());
                if (t.getLocalName().toLowerCase() == "event")
                {
                    DelftEvent xmlEvent = (DelftEvent) u.unmarshal(t);
                    // make an immutable event.
                    Event actualEvent = null;
                    if (xmlEvent != null)
                    {
                        actualEvent = 
                            new SimpleEvent(LocalDateTime.of(xmlEvent.date,
                                                             xmlEvent.time),
                                            xmlEvent.value);
                    }
                    return actualEvent;
                }
                break;
            case XMLEvent.CHARACTERS:
                System.out.println("Characters: " + t.getText());
                break;
            case XMLEvent.END_ELEMENT:
                System.out.println("End element: " + t.getName());
                break;
            case XMLEvent.END_DOCUMENT:
                System.out.println("End of document");
                break;
            default:
                System.out.println("Event type: " + type);
                break;
            }
        }
        catch (XMLStreamException|JAXBException e)
        {
            System.err.println("Stinking jaxb exception" + e);
            e.printStackTrace();
        }
        return null;
    }
*/

/* 20170302 old:

    public Event apply(XMLStreamReader t, Unmarshaller u)
    {
        try
        {
            DelftEvent xmlEvent = (DelftEvent) u.unmarshal(t);
            // make an immutable event.
            Event actualEvent = null;
            if (xmlEvent != null)
            {
                actualEvent = 
                    SimpleEvent.of(LocalDateTime.of(xmlEvent.date,
                                                    xmlEvent.time),
                                   xmlEvent.value);
            }
            return actualEvent;
        }
        catch (JAXBException e)
        {
            System.err.println("Stinking jaxb exception" + e);
            e.printStackTrace();
        }
        return null;
    }

*/