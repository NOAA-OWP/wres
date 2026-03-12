package wres.reading.nwis.ogc.response;

import wres.reading.TimeSeriesTuple;

/**
 * A small value class the stores a time-series identifier associated with a single-valued time-series contained in a
 * {@link TimeSeriesTuple}. This is used to preserve the time-series identity of each time-series in a response from
 * a USGS OGC time-series web service, thereby allowing paginated time-series with the same identity to be consolidated
 * when each page is read separately by a {@link UsgsOgcResponseReader}. This value class could be eliminated if a
 * {@link wres.datamodel.time.TimeSeries} contained more flexible {@link wres.datamodel.time.TimeSeriesMetadata}.
 *
 * @param id the time-series identity
 * @param tuple the tuple
 */

public record TimeSeriesTuplePlusId( String id, TimeSeriesTuple tuple )
{
}
