package gov.noaa.wres.pixml;

import javax.xml.bind.annotation.*;
import java.util.List;

public class Series
{
    @XmlElement
    public Header header;

    @XmlElement(name="event")
    public List<DelftEvent> events;
}
