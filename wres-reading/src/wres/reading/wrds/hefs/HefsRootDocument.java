package wres.reading.wrds.hefs;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.lang3.builder.ToStringBuilder;

import wres.datamodel.time.TimeSeries;
import wres.datamodel.types.Ensemble;

/**
 * <p>Reads a document from the Water Resources Evaluation Service that contains one or more forecasts from the
 * Hydrologic Ensemble Forecast Service (HEFS). An example document:
 * {
 *               "count": 99,
 *               "next": "foo.bar",
 *               "previous": "foo.bar",
 *               "results": [
 *                 {
 *                   "events": [
 *                     {
 *                       "date": "2025-04-21",
 *                       "flag": "0",
 *                       "time": "00:00:00",
 *                       "value": 55.797176
 *                     },
 *                     {
 *                       "date": "2025-04-21",
 *                       "flag": "0",
 *                       "time": "06:00:00",
 *                       "value": 55.090878
 *                     }
 *                   ],
 *                   "tracking_id": "972df95b-c2b7-4e0d-84af-a60e63a50e9d",
 *                   "type": "instantaneous",
 *                   "location_id": "PGRC2",
 *                   "parameter_id": "QINE",
 *                   "ensemble_id": "MEFP",
 *                   "ensemble_member_index": 1982,
 *                   "time_step_unit": "second",
 *                   "time_step_multiplier": "21600",
 *                   "start_date_date": "2025-04-21",
 *                   "start_date_time": "00:00:00",
 *                   "end_date_date": "2025-05-21",
 *                   "end_date_time": "00:00:00",
 *                   "forecast_date_date": "2025-04-21",
 *                   "forecast_date_time": "00:00:00",
 *                   "miss_val": null,
 *                   "station_name": "PGRC2",
 *                   "lat": 37.1438888889,
 *                   "lon": -104.547222222,
 *                   "x": -104.547222222,
 *                   "y": 37.1438888889,
 *                   "units": "CFS",
 *                   "creation_date": "2025-04-21",
 *                   "creation_time": "01:15:24",
 *                   "module_instance_id": null,
 *                   "qualifier_id": null,
 *                   "approved_date": null,
 *                   "long_name": null,
 *                   "z": 1851.3552,
 *                   "source_organisation": null,
 *                   "source_system": null,
 *                   "file_description": null,
 *                   "region": null
 *                 },
 *                 {
 *                   "events": [
 *                     {
 *                       "date": "2025-04-21",
 *                       "flag": "0",
 *                       "time": "00:00:00",
 *                       "value": 149.38104
 *                     },
 *                     {
 *                       "date": "2025-04-21",
 *                       "flag": "0",
 *                       "time": "06:00:00",
 *                       "value": 146.90901
 *                     }
 *                   ],
 *                   "tracking_id": "370f07bf-412d-486c-bbd9-8db504065fd0",
 *                   "type": "instantaneous",
 *                   "location_id": "PGRC2",
 *                   "parameter_id": "QINE",
 *                   "ensemble_id": "MEFP",
 *                   "ensemble_member_index": 1983,
 *                   "time_step_unit": "second",
 *                   "time_step_multiplier": "21600",
 *                   "start_date_date": "2025-04-21",
 *                   "start_date_time": "00:00:00",
 *                   "end_date_date": "2025-05-21",
 *                   "end_date_time": "00:00:00",
 *                   "forecast_date_date": "2025-04-21",
 *                   "forecast_date_time": "00:00:00",
 *                   "miss_val": null,
 *                   "station_name": "PGRC2",
 *                   "lat": 37.1438888889,
 *                   "lon": -104.547222222,
 *                   "x": -104.547222222,
 *                   "y": 37.1438888889,
 *                   "units": "CFS",
 *                   "creation_date": "2025-04-21",
 *                   "creation_time": "01:15:24",
 *                   "module_instance_id": null,
 *                   "qualifier_id": null,
 *                   "approved_date": null,
 *                   "long_name": null,
 *                   "z": 1851.3552,
 *                   "source_organisation": null,
 *                   "source_system": null,
 *                   "file_description": null,
 *                   "region": null
 *                 }
 *               ]
 *             }
 *             """
 *
 * @author James Brown
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public record HefsRootDocument( @JsonProperty( "count" ) int count,
                                @JsonDeserialize( using = HefsTimeSeriesDeserializer.class )
                                @JsonProperty( "results" ) List<TimeSeries<Ensemble>> results )
{
    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "count", this.count() )
                .append( "results", this.results() )
                .toString();
    }
}
