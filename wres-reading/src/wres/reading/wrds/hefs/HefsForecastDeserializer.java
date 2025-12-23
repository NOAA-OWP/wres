package wres.reading.wrds.hefs;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.types.Ensemble;

/**
 * <p>Deserializes an ensemble forecast contained in a response from the Water Resources Data Service (WRDS) for the
 * Hydrologic Ensemble Forecast Service. An example document:
 * <p>
 * [
 *   [
 *     {
 *       "creation_datetime": "2025-10-05T14:51:00Z",
 *       "end_datetime": "2025-11-04T12:00:00Z",
 *       "ensemble_id": "MEFP",
 *       "ensemble_member_index": 1992,
 *       "forecast_datetime": "2025-10-05T12:00:00Z",
 *       "lat": 32.02,
 *       "location_id": "RDBN5",
 *       "lon": -104.05,
 *       "parameter_id": "QINE",
 *       "start_datetime": "2025-10-05T12:00:00Z",
 *       "station_name": "RDBN5 - Red Bluff NM - Delaware River",
 *       "time_step_multiplier": "21600",
 *       "time_step_unit": "second",
 *       "type": "instantaneous",
 *       "units": "CFS",
 *       "x": -104.05,
 *       "y": 32.02,
 *       "z": 883.92,
 *       "events": [
 *         {
 *           "flag": "2",
 *           "value": 0.7000038,
 *           "valid_datetime": "2025-10-05T12:00:00Z"
 *         },
 *         {
 *           "flag": "2",
 *           "value": 0.7000038,
 *           "valid_datetime": "2025-10-05T18:00:00Z"
 *         }
 *       ]
 *     },
 *     {
 *       "creation_datetime": "2025-10-05T14:51:00Z",
 *       "end_datetime": "2025-11-04T12:00:00Z",
 *       "ensemble_id": "MEFP",
 *       "ensemble_member_index": 1993,
 *       "forecast_datetime": "2025-10-05T12:00:00Z",
 *       "lat": 32.02,
 *       "location_id": "RDBN5",
 *       "lon": -104.05,
 *       "parameter_id": "QINE",
 *       "start_datetime": "2025-10-05T12:00:00Z",
 *       "station_name": "RDBN5 - Red Bluff NM - Delaware River",
 *       "time_step_multiplier": "21600",
 *       "time_step_unit": "second",
 *       "type": "instantaneous",
 *       "units": "CFS",
 *       "x": -104.05,
 *       "y": 32.02,
 *       "z": 883.92,
 *       "events": [
 *         {
 *           "flag": "2",
 *           "value": 0.7000038,
 *           "valid_datetime": "2025-10-05T12:00:00Z"
 *         },
 *         {
 *           "flag": "2",
 *           "value": 0.7000038,
 *           "valid_datetime": "2025-10-05T18:00:00Z"
 *         }
 *       ]
 *     },
 *   ]
 * ]
 *
 * @author James Brown
 */
class HefsForecastDeserializer extends JsonDeserializer<HefsForecast>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( HefsForecastDeserializer.class );

    @Override
    public HefsForecast deserialize( JsonParser jsonParser,
                                     DeserializationContext deserializationContext ) throws IOException
    {
        ObjectMapper mapper = ( ObjectMapper ) jsonParser.getCodec();
        HefsTrace[] traces = mapper.readValue( jsonParser, HefsTrace[].class );

        Map<String, TimeSeries<Double>> traceSeries = new TreeMap<>();

        for ( HefsTrace t : traces )
        {
            String label = t.header()
                            .ensembleMemberIndex();
            TimeSeries<Double> nextSeries = t.timeSeries();
            traceSeries.put( label, nextSeries );
        }

        LOGGER.debug( "Read a HEFS forecast containing {} traces with the following labels: {}.",
                      traceSeries.size(),
                      traceSeries.keySet() );

        TimeSeries<Ensemble> forecast = TimeSeriesSlicer.compose( traceSeries.values()
                                                                             .stream()
                                                                             .toList(),
                                                                  new TreeSet<>( traceSeries.keySet() ) );
        return new HefsForecast( forecast );
    }
}