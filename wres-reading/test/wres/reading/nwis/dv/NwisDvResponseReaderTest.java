package wres.reading.nwis.dv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.components.Dataset;
import wres.config.components.DatasetBuilder;
import wres.config.components.DatasetOrientation;
import wres.config.components.Source;
import wres.config.components.SourceBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.TimeScale;

/**
 * Tests the {@link NwisDvResponseReader}.
 *
 * @author James Brown
 */
class NwisDvResponseReaderTest
{
    /** An example response. */
    private static final String RESPONSE = """
            {
              "type": "FeatureCollection",
              "features": [
                {
                  "type": "Feature",
                  "properties": {
                    "monitoring_location_id": "USGS-02238500",
                    "parameter_code": "00060",
                    "statistic_id": "00003",
                    "time_series_id": "0ab995b8bbf44a609562fd1b939dbb35",
                    "time": "2024-02-18",
                    "unit_of_measure": "ft^3/s",
                    "last_modified": "2025-03-11T00:13:29.795987+00:00",
                    "value": "108",
                    "approval_status": "Approved",
                    "qualifier": null
                  },
                  "id": "047d796d-dd50-4492-9230-7edc7cc5b693",
                  "geometry": {
                    "type": "Point",
                    "coordinates": [
                      -81.8808333333333,
                      29.0811111111111
                    ]
                  }
                },
                {
                  "type": "Feature",
                  "properties": {
                    "monitoring_location_id": "USGS-02238500",
                    "parameter_code": "00060",
                    "statistic_id": "00003",
                    "time_series_id": "0ab995b8bbf44a609562fd1b939dbb35",
                    "time": "2024-02-16",
                    "unit_of_measure": "ft^3/s",
                    "last_modified": "2025-03-11T00:13:29.795987+00:00",
                    "value": "10.2",
                    "approval_status": "Approved",
                    "qualifier": null
                  },
                  "id": "078fb4b1-5332-453b-b702-b6ad34426b72",
                  "geometry": {
                    "type": "Point",
                    "coordinates": [
                      -81.8808333333333,
                      29.0811111111111
                    ]
                  }
                },
                {
                  "type": "Feature",
                  "properties": {
                    "monitoring_location_id": "USGS-02238500",
                    "parameter_code": "00060",
                    "statistic_id": "00003",
                    "time_series_id": "0ab995b8bbf44a609562fd1b939dbb35",
                    "time": "2024-02-15",
                    "unit_of_measure": "ft^3/s",
                    "last_modified": "2025-03-11T00:13:29.795987+00:00",
                    "value": "10.2",
                    "approval_status": "Approved",
                    "qualifier": null
                  },
                  "id": "0ddf5160-43bd-4e65-af71-ebbd109c4183",
                  "geometry": {
                    "type": "Point",
                    "coordinates": [
                      -81.8808333333333,
                      29.0811111111111
                    ]
                  }
                },
                {
                  "type": "Feature",
                  "properties": {
                    "monitoring_location_id": "USGS-02238500",
                    "parameter_code": "00060",
                    "statistic_id": "00003",
                    "time_series_id": "0ab995b8bbf44a609562fd1b939dbb35",
                    "time": "2024-02-20",
                    "unit_of_measure": "ft^3/s",
                    "last_modified": "2025-03-11T00:13:29.795987+00:00",
                    "value": "600",
                    "approval_status": "Approved",
                    "qualifier": null
                  },
                  "id": "12367d6a-2c95-486e-85a6-bf688c7189cf",
                  "geometry": {
                    "type": "Point",
                    "coordinates": [
                      -81.8808333333333,
                      29.0811111111111
                    ]
                  }
                },
                {
                  "type": "Feature",
                  "properties": {
                    "monitoring_location_id": "USGS-02238500",
                    "parameter_code": "00060",
                    "statistic_id": "00003",
                    "time_series_id": "0ab995b8bbf44a609562fd1b939dbb35",
                    "time": "2024-03-16",
                    "unit_of_measure": "ft^3/s",
                    "last_modified": "2025-03-11T00:13:29.795987+00:00",
                    "value": "70.7",
                    "approval_status": "Approved",
                    "qualifier": null
                  },
                  "id": "1e4dbd3e-fe77-4d2c-a2c6-a24cc7b66a50",
                  "geometry": {
                    "type": "Point",
                    "coordinates": [
                      -81.8808333333333,
                      29.0811111111111
                    ]
                  }
                },
                {
                  "type": "Feature",
                  "properties": {
                    "monitoring_location_id": "USGS-02238500",
                    "parameter_code": "00060",
                    "statistic_id": "00003",
                    "time_series_id": "0ab995b8bbf44a609562fd1b939dbb35",
                    "time": "2024-03-15",
                    "unit_of_measure": "ft^3/s",
                    "last_modified": "2025-03-11T00:13:29.795987+00:00",
                    "value": "179",
                    "approval_status": "Approved",
                    "qualifier": null
                  },
                  "id": "222fa449-705e-49eb-a788-7d1edf7491b7",
                  "geometry": {
                    "type": "Point",
                    "coordinates": [
                      -81.8808333333333,
                      29.0811111111111
                    ]
                  }
                },
                {
                  "type": "Feature",
                  "properties": {
                    "monitoring_location_id": "USGS-02238500",
                    "parameter_code": "00060",
                    "statistic_id": "00003",
                    "time_series_id": "0ab995b8bbf44a609562fd1b939dbb35",
                    "time": "2024-03-13",
                    "unit_of_measure": "ft^3/s",
                    "last_modified": "2025-03-11T00:13:29.795987+00:00",
                    "value": "284",
                    "approval_status": "Approved",
                    "qualifier": null
                  },
                  "id": "266e53ee-be24-4e92-ba1f-8493f46bee4d",
                  "geometry": {
                    "type": "Point",
                    "coordinates": [
                      -81.8808333333333,
                      29.0811111111111
                    ]
                  }
                },
                {
                  "type": "Feature",
                  "properties": {
                    "monitoring_location_id": "USGS-02238500",
                    "parameter_code": "00060",
                    "statistic_id": "00003",
                    "time_series_id": "0ab995b8bbf44a609562fd1b939dbb35",
                    "time": "2024-03-14",
                    "unit_of_measure": "ft^3/s",
                    "last_modified": "2025-03-11T00:13:29.795987+00:00",
                    "value": "262",
                    "approval_status": "Approved",
                    "qualifier": null
                  },
                  "id": "27195ef2-2587-4bd1-a981-b6f8f7992ec5",
                  "geometry": {
                    "type": "Point",
                    "coordinates": [
                      -81.8808333333333,
                      29.0811111111111
                    ]
                  }
                },
                {
                  "type": "Feature",
                  "properties": {
                    "monitoring_location_id": "USGS-02238500",
                    "parameter_code": "00060",
                    "statistic_id": "00003",
                    "time_series_id": "0ab995b8bbf44a609562fd1b939dbb35",
                    "time": "2024-02-14",
                    "unit_of_measure": "ft^3/s",
                    "last_modified": "2025-03-11T00:13:29.795987+00:00",
                    "value": "10.2",
                    "approval_status": "Approved",
                    "qualifier": null
                  },
                  "id": "364197d9-4c4a-4b57-8db7-b2333531f564",
                  "geometry": {
                    "type": "Point",
                    "coordinates": [
                      -81.8808333333333,
                      29.0811111111111
                    ]
                  }
                },
                {
                  "type": "Feature",
                  "properties": {
                    "monitoring_location_id": "USGS-02238500",
                    "parameter_code": "00060",
                    "statistic_id": "00003",
                    "time_series_id": "0ab995b8bbf44a609562fd1b939dbb35",
                    "time": "2024-03-06",
                    "unit_of_measure": "ft^3/s",
                    "last_modified": "2025-03-11T00:13:29.795987+00:00",
                    "value": "410",
                    "approval_status": "Approved",
                    "qualifier": null
                  },
                  "id": "38020de1-e0e7-43a0-8748-1795d1941535",
                  "geometry": {
                    "type": "Point",
                    "coordinates": [
                      -81.8808333333333,
                      29.0811111111111
                    ]
                  }
                }
              ],
              "numberReturned": 10,
              "links": [
                {
                  "rel": "next",
                  "href": "https://foo/next.link",
                  "type": "application/geo+json",
                  "title": "Items (next)"
                },
                {
                  "type": "application/geo+json",
                  "rel": "self",
                  "title": "This document as GeoJSON",
                  "href": "https://foo/self.link"
                },
                {
                  "rel": "alternate",
                  "type": "application/ld+json",
                  "title": "This document as RDF (JSON-LD)",
                  "href": "https://foo/alternate.link"
                },
                {
                  "type": "text/html",
                  "rel": "alternate",
                  "title": "This document as HTML",
                  "href": "foo/alternate_html.link"
                },
                {
                  "type": "application/json",
                  "title": "Daily values",
                  "rel": "collection",
                  "href": "foo/collection.link"
                }
              ],
              "timeStamp": "2025-11-18T11:53:21.880557Z"
            }
            """;

    @Test
    void testRead() throws IOException
    {
        try ( InputStream inputStream = new ByteArrayInputStream( RESPONSE.getBytes() ) )
        {

            URI fakeUri = URI.create( "http://foo" );

            Source fakeDeclarationSource = SourceBuilder.builder()
                                                        .uri( fakeUri )
                                                        .timeZoneOffset( ZoneOffset.UTC )
                                                        .build();

            Dataset dataset = DatasetBuilder.builder()
                                            .sources( List.of( fakeDeclarationSource ) )
                                            .build();

            DataSource dataSource = DataSource.builder()
                                              .disposition( DataSource.DataDisposition.GEOJSON )
                                              .source( fakeDeclarationSource )
                                              .context( dataset )
                                              .links( Collections.emptyList() )
                                              .uri( fakeUri )
                                              .datasetOrientation( DatasetOrientation.LEFT )
                                              .build();

            NwisDvResponseReader reader = NwisDvResponseReader.of();

            List<TimeSeriesTuple> series = reader.read( dataSource, inputStream )
                                                 .toList();

            assertEquals( 1, series.size() );

            TimeSeries<Double> actual = series.get( 0 )
                                              .getSingleValuedTimeSeries();

            Geometry expectedGeometry = Geometry.newBuilder()
                                                .setName( "02238500" )
                                                .setWkt( "POINT (-81.8808333333333 29.0811111111111)" )
                                                .build();
            Feature expectedFeature = Feature.of( expectedGeometry );

            TimeScale expectedTimeScale = TimeScale.newBuilder()
                                                   .setPeriod( MessageUtilities.getDuration( Duration.ofDays( 1 ) ) )
                                                   .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                                   .build();
            TimeScaleOuter expectedTimeScaleOuter = TimeScaleOuter.of( expectedTimeScale );

            TimeSeriesMetadata expectedMetadata =
                    new TimeSeriesMetadata.Builder().setReferenceTimes( Map.of() )
                                                    .setUnit( "ft^3/s" )
                                                    .setVariableName( "00060" )
                                                    .setTimeScale( expectedTimeScaleOuter )
                                                    .setFeature( expectedFeature )
                                                    .build();
            TimeSeries<Double> expected =
                    new TimeSeries.Builder<Double>().setMetadata( expectedMetadata )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-14T00:00:00Z" ),
                                                                         10.2 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-15T00:00:00Z" ),
                                                                         10.2 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-16T00:00:00Z" ),
                                                                         10.2 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-18T00:00:00Z" ),
                                                                         108.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-20T00:00:00Z" ),
                                                                         600.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-03-06T00:00:00Z" ),
                                                                         410.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-03-13T00:00:00Z" ),
                                                                         284.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-03-14T00:00:00Z" ),
                                                                         262.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-03-15T00:00:00Z" ),
                                                                         179.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-03-16T00:00:00Z" ),
                                                                         70.7 ) )
                                                    .build();

            // Make assertions about the data source and the series content
            assertAll( () -> assertEquals( "https://foo/next.link",
                                           series.get( 0 )
                                                 .getDataSource()
                                                 .getNextPage()
                                                 .toString() ),
                       () -> assertEquals( expected, actual ) );
        }
    }

}
