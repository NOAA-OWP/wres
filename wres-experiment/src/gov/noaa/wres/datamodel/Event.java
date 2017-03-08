package gov.noaa.wres.datamodel;

import java.time.Duration;
import java.time.LocalDateTime;

public interface Event
{
    public LocalDateTime getDateTime();
    public Duration getLeadTime();
    public double getValue();
    public LocalDateTime getIssuedDateTime();
}
