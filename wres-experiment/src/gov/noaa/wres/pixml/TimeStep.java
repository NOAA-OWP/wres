package gov.noaa.wres.pixml;

import javax.xml.bind.annotation.*;

public class TimeStep
{
    @XmlElement
    public String unit;
    @XmlElement
    public double multiplier;

}
