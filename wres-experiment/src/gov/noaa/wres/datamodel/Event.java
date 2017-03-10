package gov.noaa.wres.datamodel;

import java.time.Duration;
import java.time.LocalDateTime;

import java.lang.IndexOutOfBoundsException;

public interface Event
{
    public LocalDateTime getDateTime();
    public Duration getLeadTime();
    // count of ensemble members if multiple are present
    public int getLength();
    public double[] getValues();
    public double getValue(int index) throws IndexOutOfBoundsException;
    public LocalDateTime getIssuedDateTime();
}
