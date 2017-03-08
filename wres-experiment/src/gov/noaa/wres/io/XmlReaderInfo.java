package gov.noaa.wres.io;

import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamReader;

public class XmlReaderInfo
{
    private final XMLStreamReader xmlStreamReader;
    private final Unmarshaller unmarshaller;

    private XmlReaderInfo(XMLStreamReader xmlStreamReader,
                          Unmarshaller unmarshaller)
    {
        this.xmlStreamReader = xmlStreamReader;
        this.unmarshaller = unmarshaller;
    }

    public static XmlReaderInfo of(XMLStreamReader xmlStreamReader,
                                   Unmarshaller unmarshaller)
    {
        return new XmlReaderInfo(xmlStreamReader, unmarshaller);
    }

    public XMLStreamReader getXMLStreamReader()
    {
        return this.xmlStreamReader;
    }

    public Unmarshaller getUnmarshaller()
    {
        return this.unmarshaller;
    }
}
