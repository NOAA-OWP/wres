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
    private final double[] forecasts;
    private final double observation;

    // Use .of to construct?
    private PairEvent(LocalDateTime dateTime,
                      Duration forecastLeadTime,
                      double[] forecasts,
                      double observation)
    {
        this.dateTime = dateTime;
        this.forecastLeadTime = forecastLeadTime;
        this.forecasts = forecasts;
        this.observation = observation;
    }

    public static PairEvent of(LocalDateTime dateTime,
                               Duration forecastLeadTime,
                               double[] forecasts,
                               double observation)
    {
        return new PairEvent(dateTime,
                             forecastLeadTime,
                             forecasts,
                             observation);
    }

    public LocalDateTime getDateTime()
    {
        return this.dateTime;
    }

    public double[] getForecasts()
    {
        return this.forecasts;
    }

    public Duration getLeadTime()
    {
        return this.forecastLeadTime;
    }

    public double getObservation()
    {
        return this.observation;
    }

    /** Since the interface assumes array,
     *  go for the forecast side.
     */
    public double getValue(int index) throws IndexOutOfBoundsException
    {
        return this.forecasts[index];
    }

    /**
     * go for the forecasts again
     */
    public double[] getValues()
    {
        return this.getForecasts();
    }

    public int getLength()
    {
        return this.getForecasts().length;
    }

    public LocalDateTime getIssuedDateTime()
    {
        return this.dateTime.minus(this.forecastLeadTime);
    }
}
