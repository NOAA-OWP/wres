package gov.noaa.wres.pixml;

import java.time.LocalDate;
import javax.xml.bind.annotation.adapters.XmlAdapter;

public final class LocalDateAdapter extends XmlAdapter<String,LocalDate>
{
    public String marshal(LocalDate ld)
    {
        return ld.toString();
    }
    public LocalDate unmarshal(String s)
    {
        return LocalDate.parse(s);
    }
}