package gov.noaa.wres.datamodel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class SeriesInfo
{
    private final LocalDateTime forecastDateTime;
    private final ZoneId forecastTimeZone;
    private final Map<String,Object> metadata;

    private SeriesInfo(LocalDateTime forecastDateTime,
                       ZoneId forecastTimeZone,
                       Map<String,Object> metadata)
    {
        this.forecastTimeZone = forecastTimeZone;
        this.forecastDateTime = forecastDateTime;
        // could shallow copy or deep copy this, I suppose:
        this.metadata = metadata;
    }

    public static SeriesInfo of(LocalDateTime forecastDateTime,
                                ZoneId forecastTimeZone,
                                Map<String,Object> metadata)
        
    {
        return new SeriesInfo(forecastDateTime,
                              forecastTimeZone,
                              metadata);
    }

    /**
     * Factory to create SeriesInfo with Zulu ZoneId, ensembleId in metadata
     * for when this is all that is known.
     */
    public static SeriesInfo of(LocalDateTime forecastDateTime,
                                String ensembleId)
    {
        Map<String,Object> metadata = new ConcurrentHashMap<>();
        metadata.put("ensembleId", ensembleId);
        return SeriesInfo.of(forecastDateTime, ZoneId.of("Z"), metadata);
    }

    public static SeriesInfo of(LocalDateTime forecastDateTime,
                                Integer ensembleId)
    {
        return SeriesInfo.of(forecastDateTime, ensembleId.toString());
    }

    public LocalDateTime getForecastDateTime()
    {
        return this.forecastDateTime;
    }

    public ZoneId getZoneId()
    {
        return this.forecastTimeZone;
    }

    public Object getMetadata(String key)
    {
        return this.metadata.get(key);
    }

    public boolean isEnsemble()
    {
        return this.metadata.containsKey("ensembleId")
            && this.metadata.get("ensembleId") != null;
    }

    /*probably shouldn't have this available, but then should we
      expose all the map-reading methods?
      Option: we could expose an unmodifiable view of the object...
    public Map<String,Object> getMetadata()
    {
        return this.metadata;
    }
    */

}
