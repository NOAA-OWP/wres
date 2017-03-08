package gov.noaa.wres.pixml;

import javax.xml.bind.annotation.*;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.List;
import java.time.LocalDate;
import java.time.LocalTime;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name="header")
public class Header
{
    @XmlElement(required = false)
    public String type;
    @XmlElement(required = false)
    public String locationId;
    @XmlElement(required = false)
    public String parameterId;
    @XmlElement(required = false)
    public String ensembleId;
    @XmlElement(required = false)
    public String ensembleMemberIndex;
    @XmlElement(required = false)
    public TimeStep timeStep;
    @XmlElement(required = false)
    public HeaderDate startDate;
    @XmlElement(required = false)
    public HeaderDate endDate;
    @XmlElement(required = false)
    public HeaderDate forecastDate;
    @XmlElement(required = false)
    public double missVal;
    @XmlElement(required = false)
    public String stationName;
    @XmlElement(required = false)
    public String units;
    @XmlElement(required = false)
    @XmlJavaTypeAdapter(LocalDateAdapter.class)
    public LocalDate creationDate;
    @XmlElement(required = false)
    @XmlJavaTypeAdapter(LocalTimeAdapter.class)
    public LocalTime creationTime;
}
