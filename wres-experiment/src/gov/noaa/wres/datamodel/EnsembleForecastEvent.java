package gov.noaa.wres.datamodel;

import java.time.Duration;
import java.time.LocalDateTime;

import java.lang.IndexOutOfBoundsException;

public class EnsembleForecastEvent implements Event
{
    /** the valid datetime */
    private final LocalDateTime dateTime;
    private final double[] values;
    /** the lead time past the issued time */
    private final Duration leadTime;

    private EnsembleForecastEvent(LocalDateTime dateTime,
                                  Duration leadTime,
                                  double[] values)
    {
        this.dateTime = dateTime;
        this.leadTime = leadTime;
        this.values = values;
    }

    public static EnsembleForecastEvent of(LocalDateTime dateTime,
                                           Duration leadTime,
                                           double[] values)
    {
        return new EnsembleForecastEvent(dateTime, leadTime, values);
    }

    public EnsembleForecastEvent with(Duration leadTime)
    {
        return new EnsembleForecastEvent(getDateTime(),
                                         leadTime,
                                         getValues());
    }

    public EnsembleForecastEvent with(double[] values)
    {
        return new EnsembleForecastEvent(getDateTime(),
                                         getLeadTime(),
                                         values);
    }

    /** returns the VALID datetime */
    public LocalDateTime getDateTime()
    {
        return this.dateTime;
    }

    public double[] getValues()
    {
        return this.values;
    }

    public double getValue(int index) throws IndexOutOfBoundsException
    {
        return this.values[index];
    }
    /** returns the first value or null if none present */
    public double getValue()
    {
        try
        {
            return this.getValue(0);
        }
        catch (IndexOutOfBoundsException ioobe)
        {
            return -999.0;
        }
    }

    /** returns the length of values array or -1 if null */
    public int getLength()
    {
        if (this.values != null)
        {
            return this.values.length;
        }
        else
        {
            return -1;
        }
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
