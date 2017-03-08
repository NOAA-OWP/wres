package gov.noaa.wres.datamodel;

import java.time.Duration;
import java.time.LocalDateTime;

public class ForecastEvent implements Event
{
    private final LocalDateTime dateTime;
    private final double value;
    private final Duration leadTime;

    private ForecastEvent(LocalDateTime dateTime,
                          Duration leadTime,
                          double value)
    {
        this.dateTime = dateTime;
        this.leadTime = leadTime;
        this.value = value;
    }

    public static ForecastEvent of(LocalDateTime dateTime,
                                   Duration leadTime,
                                   double value)
    {
        return new ForecastEvent(dateTime, leadTime, value);
    }

    public ForecastEvent with(Duration leadTime)
    {
        return new ForecastEvent(getDateTime(),
                                 leadTime,
                                 getValue());
    }

    public ForecastEvent with(double value)
    {
        return new ForecastEvent(getDateTime(),
                                 getLeadTime(),
                                 value);
    }

    public static ForecastEvent with(Event event,
                                     Duration leadTime)
    {
        return new ForecastEvent(event.getDateTime(),
                                 leadTime,
                                 event.getValue());
    }

    public LocalDateTime getDateTime()
    {
        return this.dateTime;
    }

    public double getValue()
    {
        return this.value;
    }

    public Duration getLeadTime()
    {
        return this.leadTime;
    }

    public LocalDateTime getIssuedDateTime()
    {
        return this.dateTime.minus(this.leadTime);
    }
}
