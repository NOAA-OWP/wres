package gov.noaa.wres.pixml;

import javax.xml.bind.annotation.*;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.time.LocalDate;
import java.time.LocalTime;

@XmlAccessorType(XmlAccessType.FIELD)
public class HeaderDate
{
    @XmlAttribute
    @XmlJavaTypeAdapter(LocalDateAdapter.class)
    public LocalDate date;
    
    @XmlAttribute
    @XmlJavaTypeAdapter(LocalTimeAdapter.class)
    public LocalTime time;
}
