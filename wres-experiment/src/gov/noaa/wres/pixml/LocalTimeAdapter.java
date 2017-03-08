package gov.noaa.wres.pixml;

import java.time.LocalTime;
import javax.xml.bind.annotation.adapters.XmlAdapter;

public final class LocalTimeAdapter extends XmlAdapter<String,LocalTime>
{
    public String marshal(LocalTime lt)
    {
        return lt.toString();
    }
    public LocalTime unmarshal(String s)
    {
        return LocalTime.parse(s);
    }
}