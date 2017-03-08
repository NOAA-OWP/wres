package gov.noaa.wres.datamodel;

import java.time.Duration;
import java.time.LocalDateTime;
import java.lang.Short;

public class SimpleEvent implements Event
{
    private final LocalDateTime dateTime;
    private final double value;

    private SimpleEvent(LocalDateTime dateTime, double value)
    {
        this.dateTime = dateTime;
        this.value = value;
    }

    public static SimpleEvent of(LocalDateTime dateTime,
                                 double value)
    {
        return new SimpleEvent(dateTime, value);
    }

    public LocalDateTime getDateTime()
    {
        return this.dateTime;
    }

    public double getValue()
    {
        return this.value;
    }

    /* A simple event has no lead time, return 0 lead time */
    public Duration getLeadTime()
    {
        return Duration.ofMinutes(0);
    }

    /* A simple event has no forecast time, return datetime */
    public LocalDateTime getIssuedDateTime()
    {
        return this.dateTime;
    }
}
