package gov.noaa.wres;

import java.lang.IllegalArgumentException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import java.time.LocalDateTime;

import gov.noaa.wres.datamodel.Event;
import gov.noaa.wres.datamodel.PairEvent;
import gov.noaa.wres.datamodel.TimeSeries;

public class Pair
{
    public static List<PairEvent> of(TimeSeries observed, TimeSeries forecast)
        throws IllegalArgumentException
    {
        if (observed.isEmpty() || forecast.isEmpty())
        {
            throw new IllegalArgumentException("Lists of Events must have data!"
                                               + " observed.isEmpty() = " 
                                               + observed.isEmpty()
                                               + " forecast.isEmpty() = "
                                               + forecast.isEmpty());
        }
        
        List<PairEvent> pairedData = new ArrayList<>();

        // Convert the forecast to a map, so we can key on datetime.
        Map<LocalDateTime,Event> forecastMap = forecast.getEventsByDateTime();

        for (Event obs : observed.getEvents())
        {
            // if we have a matching datetime, add to paired result.
            Event fc = forecastMap.get(obs.getDateTime());
            if (fc != null)
            {
                pairedData.add(PairEvent.of(obs.getDateTime(),
                                            fc.getLeadTime(),
                                            fc.getValues(),
                                            obs.getValue(0)));
            }
        }
        return pairedData;
    }
}
