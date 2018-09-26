package wres.io.reading.waterml.timeseries;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class TimeSeriesValue
{
    Double value;

    public Double getValue()
    {
        return value;
    }

    public void setValue( Double value )
    {
        this.value = value;
    }

    public String[] getQualifiers()
    {
        return qualifiers;
    }

    public void setQualifiers( String[] qualifiers )
    {
        this.qualifiers = qualifiers;
    }

    public Instant getDateTime()
    {
        if (this.dateTime != null)
        {
            return OffsetDateTime.parse( this.dateTime)
                                 .withOffsetSameInstant( ZoneOffset.UTC )
                                 .toInstant();
        }

        return null;
    }

    public void setDateTime( String dateTime )
    {
        this.dateTime = dateTime;
    }

    String[] qualifiers;
    String dateTime;
}
