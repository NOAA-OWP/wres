package gov.noaa.wres.datamodel;

import java.time.LocalDateTime;
import java.time.Duration;

/**
 * Immutable event representing a pair of double data
 */
public class PairEvent implements Event
{
    private final LocalDateTime dateTime;
    private final Duration forecastLeadTime;
    private final double forecast;
    private final double observation;

    // Use .of to construct?
    private PairEvent(LocalDateTime dateTime,
                      Duration forecastLeadTime,
                      double forecast,
                      double observation)
    {
        this.dateTime = dateTime;
        this.forecastLeadTime = forecastLeadTime;
        this.forecast = forecast;
        this.observation = observation;
    }

    public static PairEvent of(LocalDateTime dateTime,
                               Duration forecastLeadTime,
                               double forecast,
                               double observation)
    {
        return new PairEvent(dateTime, forecastLeadTime, forecast, observation);
    }

    public LocalDateTime getDateTime()
    {
        return this.dateTime;
    }

    public double getForecast()
    {
        return this.forecast;
    }

    public Duration getLeadTime()
    {
        return this.forecastLeadTime;
    }

    public double getObservation()
    {
        return this.observation;
    }

    public double getValue()
    {
        return getObservation();
    }

    public LocalDateTime getIssuedDateTime()
    {
        return this.dateTime.minus(this.forecastLeadTime);
    }
}
