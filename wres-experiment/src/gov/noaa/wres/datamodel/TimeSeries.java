package gov.noaa.wres.datamodel;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static java.util.stream.Collectors.*;

import gov.noaa.wres.datamodel.Event;

public class TimeSeries
{
    private final SeriesInfo seriesInfo;
    private final List<Event> events;

    private Map<LocalDateTime,Event> eventsByDateTime = null;

    private TimeSeries(SeriesInfo seriesInfo,
                       List<Event> events)
    {                       
        this.events = events;
        this.seriesInfo = seriesInfo;
    }

    public static TimeSeries of(SeriesInfo seriesInfo,
                                List<Event> events)
    {
        return new TimeSeries(seriesInfo, events);
    }

    public List<Event> getEvents()
    {
        return this.events;
    }

    /**
     * Lazily get a Map of this TimeSeries' events keyed on DateTime
     * Make it unmodifiable once created.
     */
    public Map<LocalDateTime,Event> getEventsByDateTime()
    {
        if (this.eventsByDateTime == null)
        {
            Map<LocalDateTime,Event> byDateTime =
                this.events.stream()
                .collect(collectingAndThen(toMap(Event::getDateTime,
                                                 e -> e),
                                           Collections::unmodifiableMap));
            this.eventsByDateTime = byDateTime;
            return byDateTime;
        }
        return this.eventsByDateTime;
    }

    public boolean isEmpty()
    {
        return this.events.isEmpty();
    }
}
