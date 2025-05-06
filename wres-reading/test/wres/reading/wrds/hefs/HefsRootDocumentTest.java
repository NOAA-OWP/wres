package wres.reading.wrds.hefs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.types.Ensemble;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.ReferenceTime;

/**
 * Tests the {@link HefsRootDocument}.
 *
 * @author James Brown
 */
class HefsRootDocumentTest
{

    /** Test document. */
    private static final String TEST_DOCUMENT = """
            {
              "count": 99,
              "next": "https://api.water.noaa.gov/hefs/v1/ensembles/?limit=10&location_id=PGRC2&offset=12&parameter_id=QINE&start_date_date=2025-04-21",
              "previous": "https://api.water.noaa.gov/hefs/v1/ensembles/?limit=10&location_id=PGRC2&parameter_id=QINE&start_date_date=2025-04-21",
              "results": [
                {
                  "events": [
                    {
                      "date": "2025-04-21",
                      "flag": "0",
                      "time": "00:00:00",
                      "value": 55.797176
                    },
                    {
                      "date": "2025-04-21",
                      "flag": "0",
                      "time": "06:00:00",
                      "value": 55.090878
                    }
                  ],
                  "tracking_id": "972df95b-c2b7-4e0d-84af-a60e63a50e9d",
                  "type": "instantaneous",
                  "location_id": "PGRC2",
                  "parameter_id": "QINE",
                  "ensemble_id": "MEFP",
                  "ensemble_member_index": 1982,
                  "time_step_unit": "second",
                  "time_step_multiplier": "21600",
                  "start_date_date": "2025-04-21",
                  "start_date_time": "00:00:00",
                  "end_date_date": "2025-05-21",
                  "end_date_time": "00:00:00",
                  "forecast_date_date": "2025-04-21",
                  "forecast_date_time": "00:00:00",
                  "miss_val": null,
                  "station_name": "PGRC2",
                  "lat": 37.1438888889,
                  "lon": -104.547222222,
                  "x": -104.547222222,
                  "y": 37.1438888889,
                  "units": "CFS",
                  "creation_date": "2025-04-21",
                  "creation_time": "01:15:24",
                  "module_instance_id": null,
                  "qualifier_id": null,
                  "approved_date": null,
                  "long_name": null,
                  "z": 1851.3552,
                  "source_organisation": null,
                  "source_system": null,
                  "file_description": null,
                  "region": null
                },
                {
                  "events": [
                    {
                      "date": "2025-04-21",
                      "flag": "0",
                      "time": "00:00:00",
                      "value": 149.38104
                    },
                    {
                      "date": "2025-04-21",
                      "flag": "0",
                      "time": "06:00:00",
                      "value": 146.90901
                    }
                  ],
                  "tracking_id": "370f07bf-412d-486c-bbd9-8db504065fd0",
                  "type": "instantaneous",
                  "location_id": "PGRC2",
                  "parameter_id": "QINE",
                  "ensemble_id": "MEFP",
                  "ensemble_member_index": 1983,
                  "time_step_unit": "second",
                  "time_step_multiplier": "21600",
                  "start_date_date": "2025-04-21",
                  "start_date_time": "00:00:00",
                  "end_date_date": "2025-05-21",
                  "end_date_time": "00:00:00",
                  "forecast_date_date": "2025-04-21",
                  "forecast_date_time": "00:00:00",
                  "miss_val": null,
                  "station_name": "PGRC2",
                  "lat": 37.1438888889,
                  "lon": -104.547222222,
                  "x": -104.547222222,
                  "y": 37.1438888889,
                  "units": "CFS",
                  "creation_date": "2025-04-21",
                  "creation_time": "01:15:24",
                  "module_instance_id": null,
                  "qualifier_id": null,
                  "approved_date": null,
                  "long_name": null,
                  "z": 1851.3552,
                  "source_organisation": null,
                  "source_system": null,
                  "file_description": null,
                  "region": null
                }
              ]
            }
            """;

    @Test
    void testRead() throws IOException
    {
        ObjectMapper mapper =
                new ObjectMapper()
                        .enable( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES );

        try ( InputStream stream = new ByteArrayInputStream( TEST_DOCUMENT.getBytes() ) )
        {
            HefsRootDocument rootDocument = mapper.readValue( stream, HefsRootDocument.class );

            Geometry geometry = MessageUtilities.getGeometry( "PGRC2",
                                                              null,
                                                              null,
                                                              "POINT ( -104.547222222 37.1438888889 1851.3552 )" );
            TimeSeriesMetadata metadata = new TimeSeriesMetadata.Builder()
                    .setVariableName( "QINE" )
                    .setUnit( "CFS" )
                    .setFeature( Feature.of( geometry ) )
                    .setTimeScale( TimeScaleOuter.of() )
                    .setReferenceTimes( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                                Instant.parse( "2025-04-21T00:00:00Z" ) ) )
                    .build();

            Instant instant = Instant.parse( "2025-04-21T00:00:00Z" );
            Ensemble ensemble = Ensemble.of( new double[] { 55.797176, 149.38104 },
                                             Ensemble.Labels.of( "1982", "1983" ) );
            Event<Ensemble> event = Event.of( instant, ensemble );

            Instant anotherInstant = Instant.parse( "2025-04-21T06:00:00Z" );
            Ensemble anotherEnsemble = Ensemble.of( new double[] { 55.090878, 146.90901 },
                                                    Ensemble.Labels.of( "1982", "1983" ) );
            Event<Ensemble> anotherEvent = Event.of( anotherInstant, anotherEnsemble );

            SortedSet<Event<Ensemble>> events = new TreeSet<>();
            events.add( event );
            events.add( anotherEvent );
            List<TimeSeries<Ensemble>> expected = List.of( TimeSeries.of( metadata, events ) );

            assertEquals( expected, rootDocument.results() );
        }
    }
}
