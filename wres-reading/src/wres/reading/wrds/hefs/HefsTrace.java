package wres.reading.wrds.hefs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.lang3.builder.ToStringBuilder;

import wres.datamodel.time.TimeSeries;
import wres.reading.TimeSeriesHeader;

/**
 * <p>Reads an ensemble trace from a document supplied by the Water Resources Data Service. There are up to many traces
 * in each document. An example document:
 * [
 *    {
 *      "events": [
 *        {
 *          "date": "2025-04-21",
 *          "flag": "0",
 *          "time": "00:00:00",
 *          "value": 55.797176
 *        },
 *        {
 *          "date": "2025-04-21",
 *          "flag": "0",
 *          "time": "06:00:00",
 *          "value": 55.090878
 *        }
 *      ],
 *      "tracking_id": "972df95b-c2b7-4e0d-84af-a60e63a50e9d",
 *      "type": "instantaneous",
 *      "location_id": "PGRC2",
 *      "parameter_id": "QINE",
 *      "ensemble_id": "MEFP",
 *      "ensemble_member_index": 1982,
 *      "time_step_unit": "second",
 *      "time_step_multiplier": "21600",
 *      "start_date_date": "2025-04-21",
 *      "start_date_time": "00:00:00",
 *      "end_date_date": "2025-05-21",
 *      "end_date_time": "00:00:00",
 *      "forecast_date_date": "2025-04-21",
 *      "forecast_date_time": "00:00:00",
 *      "miss_val": null,
 *      "station_name": "PGRC2",
 *      "lat": 37.1438888889,
 *      "lon": -104.547222222,
 *      "x": -104.547222222,
 *      "y": 37.1438888889,
 *      "units": "CFS",
 *      "creation_date": "2025-04-21",
 *      "creation_time": "01:15:24",
 *      "module_instance_id": null,
 *      "qualifier_id": null,
 *      "approved_date": null,
 *      "long_name": null,
 *      "z": 1851.3552,
 *      "source_organisation": null,
 *      "source_system": null,
 *      "file_description": null,
 *      "region": null
 *    },
 *    {
 *      "events": [
 *        {
 *          "date": "2025-04-21",
 *          "flag": "0",
 *          "time": "00:00:00",
 *          "value": 149.38104
 *        },
 *        {
 *          "date": "2025-04-21",
 *          "flag": "0",
 *          "time": "06:00:00",
 *          "value": 146.90901
 *        }
 *      ],
 *      "tracking_id": "370f07bf-412d-486c-bbd9-8db504065fd0",
 *      "type": "instantaneous",
 *      "location_id": "PGRC2",
 *      "parameter_id": "QINE",
 *      "ensemble_id": "MEFP",
 *      "ensemble_member_index": 1983,
 *      "time_step_unit": "second",
 *      "time_step_multiplier": "21600",
 *      "start_date_date": "2025-04-21",
 *      "start_date_time": "00:00:00",
 *      "end_date_date": "2025-05-21",
 *      "end_date_time": "00:00:00",
 *      "forecast_date_date": "2025-04-21",
 *      "forecast_date_time": "00:00:00",
 *      "miss_val": null,
 *      "station_name": "PGRC2",
 *      "lat": 37.1438888889,
 *      "lon": -104.547222222,
 *      "x": -104.547222222,
 *      "y": 37.1438888889,
 *      "units": "CFS",
 *      "creation_date": "2025-04-21",
 *      "creation_time": "01:15:24",
 *      "module_instance_id": null,
 *      "qualifier_id": null,
 *      "approved_date": null,
 *      "long_name": null,
 *      "z": 1851.3552,
 *      "source_organisation": null,
 *      "source_system": null,
 *      "file_description": null,
 *      "region": null
 *    }
 * ]
 *
 * @author James Brown
 */

@JsonIgnoreProperties( ignoreUnknown = true )
@JsonDeserialize( using = HefsTraceDeserializer.class )
public record HefsTrace( TimeSeriesHeader header, TimeSeries<Double> timeSeries )
{
    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "header", this.header() )
                .append( "timeSeries", this.timeSeries() )
                .toString();
    }
}
