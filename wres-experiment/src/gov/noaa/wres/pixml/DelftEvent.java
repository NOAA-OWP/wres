package gov.noaa.wres.pixml;

import javax.xml.bind.annotation.*;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name="event")
public class DelftEvent
{
    @XmlAttribute
    @XmlJavaTypeAdapter(LocalDateAdapter.class)
    public LocalDate date;

    @XmlAttribute
    @XmlJavaTypeAdapter(LocalTimeAdapter.class)
    public LocalTime time;

    @XmlAttribute
    public double value;

    @XmlAttribute
    public String flag;
}
