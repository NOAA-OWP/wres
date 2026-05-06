package wres.reading.usgs.ogc;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.components.Dataset;
import wres.config.components.DatasetBuilder;
import wres.config.components.DatasetOrientation;
import wres.config.components.EvaluationDeclaration;
import wres.config.components.EvaluationDeclarationBuilder;
import wres.config.components.Features;
import wres.config.components.FeaturesBuilder;
import wres.config.components.LeadTimeInterval;
import wres.config.components.LeadTimeIntervalBuilder;
import wres.config.components.Source;
import wres.config.components.SourceBuilder;
import wres.config.components.TimeInterval;
import wres.config.components.TimeIntervalBuilder;
import wres.config.components.TimePools;
import wres.config.components.TimePoolsBuilder;
import wres.config.components.VariableBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.DataSource.DataDisposition;
import wres.reading.ReaderUtilities;
import wres.reading.TimeChunker;
import wres.reading.TimeSeriesTuple;
import wres.reading.usgs.ogc.response.UsgsOgcResponseReader;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeScale;
import wres.system.SystemSettings;

/**
 * Tests the {@link UsgsOgcReader}.
 *
 * @author James Brown
 */

class UsgsOgcReaderTest
{
    @RegisterExtension
    private static final WireMockExtension WIREMOCK = WireMockExtension.newInstance()
                                                                       .options( WireMockConfiguration.wireMockConfig()
                                                                                                      .dynamicPort()
                                                                                                      .dynamicHttpsPort() )
                                                                       .build();
    /** Re-used string. */
    private static final String CONTENT_TYPE = "Content-Type";

    /** Re-used string. */
    private static final String APPLICATION_JSON = "application/json";

    /** Path used to GET daily values. */
    private static final String DAILY_VALUES_PATH = "/collections/daily/items";

    /** Parameters used to GET daily values. */
    private static final String DAILY_VALUES_PARAMS =
            "?f=json&lang=en-US&limit=10&properties=id,time_series_id,monitoring_location_id,parameter_code,statistic_id,time,value,unit_of_measure&skipGeometry=false&monitoring_location_id=USGS-02238500&parameter_code=00060&time=2024-02-12T00%3A00%3A00Z%2F2024-03-18T12%3A31%3A12Z";

    /** Path used to GET continuous values. */
    private static final String CONTINUOUS_VALUES_PATH = "/collections/continuous/items";

    /** Parameters used to GET continuous values. */
    private static final String CONTINUOUS_VALUES_PARAMS =
            "?f=json&lang=en-US&limit=50000&monitoring_location_id=USGS-01593450&parameter_code=00060&properties=id%2Ctime_series_id%2Cmonitoring_location_id%2Cstatistic_id%2Ctime%2Cvalue%2Cunit_of_measure&skipGeometry=true&time=2026-03-09T07%3A00%3A01Z%2F2026-03-09T07%3A58%3A53Z";

    /** Response from GET for daily values. */
    private static final String DAILY_VALUES_RESPONSE_ONE_PAGE = """
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

    /** Response from GET for daily values without geometry. */
    private static final String DAILY_VALUES_RESPONSE_ONE_PAGE_NO_GEOMETRY = """
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
                  "geometry": null
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
                  "geometry": null
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
                  "geometry": null
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
                  "geometry": null
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
                  "geometry": null
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
                  "geometry": null
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
                  "geometry": null
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
                  "geometry": null
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
                  "geometry": null
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
                  "geometry": null
                }
              ],
              "numberReturned": 10,
              "links": [
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

    /** Response from GET for daily values, first page. */
    private static final String DAILY_VALUES_RESPONSE_PAGE_ONE_OF_THREE = """
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
                    "time_series_id": "0ab995b8bbf44a609562fd1b939dbb36",
                    "time": "2024-02-18",
                    "unit_of_measure": "ft^3/s",
                    "last_modified": "2025-03-11T00:13:29.795987+00:00",
                    "value": "1.0",
                    "approval_status": "Approved",
                    "qualifier": null
                  },
                  "id": "047d796d-dd50-4492-9230-7edc7cc5b698",
                  "geometry": {
                    "type": "Point",
                    "coordinates": [
                      -81.8808333333333,
                      29.0811111111111
                    ]
                  }
                }
              ],
              "numberReturned": 6,
              "links": [
                {
                  "type": "application/geo+json",
                  "rel": "next",
                  "title": "Next document as GeoJSON",
                  "href": "NEXT_PAGE_PLACEHOLDER"
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

    /** Response from GET for daily values, second page. */
    private static final String DAILY_VALUES_RESPONSE_PAGE_TWO_OF_THREE = """
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
                    "time_series_id": "0ab995b8bbf44a609562fd1b939dbb36",
                    "time": "2024-02-19",
                    "unit_of_measure": "ft^3/s",
                    "last_modified": "2025-03-11T00:13:29.795987+00:00",
                    "value": "2.0",
                    "approval_status": "Approved",
                    "qualifier": null
                  },
                  "id": "222fa449-705e-49eb-a788-7d1edf7491b8",
                  "geometry": {
                    "type": "Point",
                    "coordinates": [
                      -81.8808333333333,
                      29.0811111111111
                    ]
                  }
                }
              ],
              "numberReturned": 5,
              "links": [
                {
                  "type": "application/geo+json",
                  "rel": "next",
                  "title": "Next document as GeoJSON",
                  "href": "NEXT_PAGE_PLACEHOLDER"
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

    /** Response from GET for daily values, third page. */
    private static final String DAILY_VALUES_RESPONSE_PAGE_THREE_OF_THREE = """
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
                },
                {
                  "type": "Feature",
                  "properties": {
                    "monitoring_location_id": "USGS-02238500",
                    "parameter_code": "00060",
                    "statistic_id": "00003",
                    "time_series_id": "0ab995b8bbf44a609562fd1b939dbb36",
                    "time": "2024-02-20",
                    "unit_of_measure": "ft^3/s",
                    "last_modified": "2025-03-11T00:13:29.795987+00:00",
                    "value": "3.0",
                    "approval_status": "Approved",
                    "qualifier": null
                  },
                  "id": "38020de1-e0e7-43a0-8748-1795d1941539",
                  "geometry": {
                    "type": "Point",
                    "coordinates": [
                      -81.8808333333333,
                      29.0811111111111
                    ]
                  }
                }
              ],
              "numberReturned": 2,
              "links": [
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

    /** Response from GET for continuous values. */
    private static final String CONTINUOUS_VALUES_RESPONSE_ONE_PAGE = """
            {
                "type":"FeatureCollection",
                "features":[
                    {
                        "type":"Feature",
                        "properties":{
                            "id":"2d5a5f06-1110-48bb-836f-925fb2c1b79c",
                            "time_series_id":"ccc31222b3d64ca49c62d7f0476fd423",
                            "monitoring_location_id":"USGS-01593450",
                            "statistic_id":"00011",
                            "time":"2026-03-09T07:05:00+00:00",
                            "value":"1.31",
                            "unit_of_measure":"ft^3/s"
                        },
                        "id":"2d5a5f06-1110-48bb-836f-925fb2c1b79c",
                        "geometry":null
                    },
                    {
                        "type":"Feature",
                        "properties":{
                            "id":"625b2720-d3bf-492b-807e-8b3c77a2c832",
                            "time_series_id":"ccc31222b3d64ca49c62d7f0476fd423",
                            "monitoring_location_id":"USGS-01593450",
                            "statistic_id":"00011",
                            "time":"2026-03-09T07:10:00+00:00",
                            "value":"1.40",
                            "unit_of_measure":"ft^3/s"
                        },
                        "id":"625b2720-d3bf-492b-807e-8b3c77a2c832",
                        "geometry":null
                    },
                    {
                        "type":"Feature",
                        "properties":{
                            "id":"88b08c4c-341d-4846-8865-713f60a53be7",
                            "time_series_id":"ccc31222b3d64ca49c62d7f0476fd423",
                            "monitoring_location_id":"USGS-01593450",
                            "statistic_id":"00011",
                            "time":"2026-03-09T07:15:00+00:00",
                            "value":"1.31",
                            "unit_of_measure":"ft^3/s"
                        },
                        "id":"88b08c4c-341d-4846-8865-713f60a53be7",
                        "geometry":null
                    },
                    {
                        "type":"Feature",
                        "properties":{
                            "id":"1cafbd0e-e991-4ea5-9a45-0c919cf14264",
                            "time_series_id":"ccc31222b3d64ca49c62d7f0476fd423",
                            "monitoring_location_id":"USGS-01593450",
                            "statistic_id":"00011",
                            "time":"2026-03-09T07:20:00+00:00",
                            "value":"1.31",
                            "unit_of_measure":"ft^3/s"
                        },
                        "id":"1cafbd0e-e991-4ea5-9a45-0c919cf14264",
                        "geometry":null
                    },
                    {
                        "type":"Feature",
                        "properties":{
                            "id":"06b08f42-eea8-4b80-9cbc-6ac5d91fec9f",
                            "time_series_id":"ccc31222b3d64ca49c62d7f0476fd423",
                            "monitoring_location_id":"USGS-01593450",
                            "statistic_id":"00011",
                            "time":"2026-03-09T07:25:00+00:00",
                            "value":"1.31",
                            "unit_of_measure":"ft^3/s"
                        },
                        "id":"06b08f42-eea8-4b80-9cbc-6ac5d91fec9f",
                        "geometry":null
                    },
                    {
                        "type":"Feature",
                        "properties":{
                            "id":"8821089a-16f2-45d5-9697-b84faab8001e",
                            "time_series_id":"ccc31222b3d64ca49c62d7f0476fd423",
                            "monitoring_location_id":"USGS-01593450",
                            "statistic_id":"00011",
                            "time":"2026-03-09T07:30:00+00:00",
                            "value":"1.31",
                            "unit_of_measure":"ft^3/s"
                        },
                        "id":"8821089a-16f2-45d5-9697-b84faab8001e",
                        "geometry":null
                    },
                    {
                        "type":"Feature",
                        "properties":{
                            "id":"cb6f8c17-8c9d-4b40-9369-13c193decedc",
                            "time_series_id":"ccc31222b3d64ca49c62d7f0476fd423",
                            "monitoring_location_id":"USGS-01593450",
                            "statistic_id":"00011",
                            "time":"2026-03-09T07:35:00+00:00",
                            "value":"1.31",
                            "unit_of_measure":"ft^3/s"
                        },
                        "id":"cb6f8c17-8c9d-4b40-9369-13c193decedc",
                        "geometry":null
                    },
                    {
                        "type":"Feature",
                        "properties":{
                            "id":"1b8e0dd7-2e4a-48bd-ba48-da149cd3b3bb",
                            "time_series_id":"ccc31222b3d64ca49c62d7f0476fd423",
                            "monitoring_location_id":"USGS-01593450",
                            "statistic_id":"00011",
                            "time":"2026-03-09T07:40:00+00:00",
                            "value":"1.40",
                            "unit_of_measure":"ft^3/s"
                        },
                        "id":"1b8e0dd7-2e4a-48bd-ba48-da149cd3b3bb",
                        "geometry":null
                    },
                    {
                        "type":"Feature",
                        "properties":{
                            "id":"1d728096-5032-4070-bf5d-36dc700a4052",
                            "time_series_id":"ccc31222b3d64ca49c62d7f0476fd423",
                            "monitoring_location_id":"USGS-01593450",
                            "statistic_id":"00011",
                            "time":"2026-03-09T07:45:00+00:00",
                            "value":"1.31",
                            "unit_of_measure":"ft^3/s"
                        },
                        "id":"1d728096-5032-4070-bf5d-36dc700a4052",
                        "geometry":null
                    },
                    {
                        "type":"Feature",
                        "properties":{
                            "id":"773db5ed-0847-456f-ab93-61f72e50814d",
                            "time_series_id":"ccc31222b3d64ca49c62d7f0476fd423",
                            "monitoring_location_id":"USGS-01593450",
                            "statistic_id":"00011",
                            "time":"2026-03-09T07:50:00+00:00",
                            "value":"1.31",
                            "unit_of_measure":"ft^3/s"
                        },
                        "id":"773db5ed-0847-456f-ab93-61f72e50814d",
                        "geometry":null
                    },
                    {
                        "type":"Feature",
                        "properties":{
                            "id":"8e61cb75-8b40-428e-bf15-c4312b43d5bc",
                            "time_series_id":"ccc31222b3d64ca49c62d7f0476fd423",
                            "monitoring_location_id":"USGS-01593450",
                            "statistic_id":"00011",
                            "time":"2026-03-09T07:55:00+00:00",
                            "value":"1.31",
                            "unit_of_measure":"ft^3/s"
                        },
                        "id":"8e61cb75-8b40-428e-bf15-c4312b43d5bc",
                        "geometry":null
                    }
                ],
                "numberReturned":11,
                "links":[
                    {
                        "type":"application/geo+json",
                        "rel":"self",
                        "title":"This document as GeoJSON",
                        "href":"https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items?f=json&lang=en-US&limit=50000&monitoring_location_id=USGS-01593450&parameter_code=00060&properties=id,time_series_id,monitoring_location_id,statistic_id,time,value,unit_of_measure&skipGeometry=true&time=2026-03-09T07%3A00%3A01Z%2F2026-03-09T07%3A58%3A53Z"
                    },
                    {
                        "rel":"alternate",
                        "type":"application/ld+json",
                        "title":"This document as RDF (JSON-LD)",
                        "href":"https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items?f=jsonld&lang=en-US&limit=50000&monitoring_location_id=USGS-01593450&parameter_code=00060&properties=id,time_series_id,monitoring_location_id,statistic_id,time,value,unit_of_measure&skipGeometry=true&time=2026-03-09T07%3A00%3A01Z%2F2026-03-09T07%3A58%3A53Z"
                    },
                    {
                        "type":"text/html",
                        "rel":"alternate",
                        "title":"This document as HTML",
                        "href":"https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items?f=html&lang=en-US&limit=50000&monitoring_location_id=USGS-01593450&parameter_code=00060&properties=id,time_series_id,monitoring_location_id,statistic_id,time,value,unit_of_measure&skipGeometry=true&time=2026-03-09T07%3A00%3A01Z%2F2026-03-09T07%3A58%3A53Z"
                    },
                    {
                        "type":"text/csv; charset=utf-8",
                        "rel":"alternate",
                        "title":"This document as CSV",
                        "href":"https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items?f=csv&lang=en-US&limit=50000&monitoring_location_id=USGS-01593450&parameter_code=00060&properties=id,time_series_id,monitoring_location_id,statistic_id,time,value,unit_of_measure&skipGeometry=true&time=2026-03-09T07%3A00%3A01Z%2F2026-03-09T07%3A58%3A53Z"
                    },
                    {
                        "type":"application/json",
                        "title":"Continuous values",
                        "rel":"collection",
                        "href":"https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous"
                    }
                ],
                "timeStamp":"2026-04-09T16:43:19.026117Z"
            }
            """;

    /** Response from GET for the monitoring-locations endpoint. */
    private static final String FEATURE_RESPONSE = """
            {
              "type": "Feature",
              "properties": {
                "monitoring_location_name": "OCKLAWAHA RIVER AT MOSS BLUFF, FL",
                "minor_civil_division_code": null,
                "altitude_method_name": null,
                "original_horizontal_datum": "NAD83",
                "aquifer_type_code": null,
                "district_code": "125",
                "site_type_code": "ST",
                "vertical_datum": null,
                "original_horizontal_datum_name": "North American Datum of 1983",
                "well_constructed_depth": null,
                "country_code": "US",
                "site_type": "Stream",
                "vertical_datum_name": null,
                "drainage_area": 879,
                "hole_constructed_depth": null,
                "country_name": "United States of America",
                "hydrologic_unit_code": "030801020501",
                "horizontal_positional_accuracy_code": "1",
                "contributing_drainage_area": null,
                "depth_source_code": null,
                "agency_code": "USGS",
                "state_code": "12",
                "basin_code": null,
                "horizontal_positional_accuracy": "Accurate to + or - .1 sec (Differentially-Corrected GPS).",
                "time_zone_abbreviation": "EST",
                "state_name": "Florida",
                "altitude": null,
                "uses_daylight_savings": "Y",
                "construction_date": null,
                "agency_name": "U.S. Geological Survey",
                "county_code": "083",
                "altitude_accuracy": null,
                "horizontal_position_method_code": "M",
                "aquifer_code": null,
                "monitoring_location_number": "02238500",
                "county_name": "Marion County",
                "altitude_method_code": null,
                "horizontal_position_method_name": "Interpolated from MAP.",
                "national_aquifer_code": null
              },
              "id": "USGS-02238500",
              "geometry": {
                "type": "Point",
                "coordinates": [
                  -81.8808333333333,
                  29.0811111111111
                ]
              },
              "prev": "USGS-02238499",
              "next": "USGS-02238800",
              "links": [
                {
                  "type": "application/json",
                  "rel": "root",
                  "title": "The landing page of this server as JSON",
                  "href": "https://api.waterdata.usgs.gov/ogcapi/v0?f=json"
                },
                {
                  "type": "text/html",
                  "rel": "root",
                  "title": "The landing page of this server as HTML",
                  "href": "https://api.waterdata.usgs.gov/ogcapi/v0?f=html"
                },
                {
                  "rel": "self",
                  "type": "application/geo+json",
                  "title": "This document as JSON",
                  "href": "https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items/USGS-02238500?f=json"
                },
                {
                  "rel": "alternate",
                  "type": "application/ld+json",
                  "title": "This document as RDF (JSON-LD)",
                  "href": "https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items/USGS-02238500?f=jsonld"
                },
                {
                  "rel": "alternate",
                  "type": "text/html",
                  "title": "This document as HTML",
                  "href": "https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items/USGS-02238500?f=html"
                },
                {
                  "rel": "collection",
                  "type": "application/json",
                  "title": "Monitoring locations",
                  "href": "https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations"
                },
                {
                  "rel": "prev",
                  "type": "application/json",
                  "href": "https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items/USGS-02238499?f=json"
                },
                {
                  "rel": "next",
                  "type": "application/json",
                  "href": "https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items/USGS-02238800?f=json"
                }
              ]
            }
            """;

    @Test
    void testReadDailyValuesReturnsOneTimeSeries()
    {
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( DAILY_VALUES_PATH ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withHeader( CONTENT_TYPE, APPLICATION_JSON )
                                                       .withBody( DAILY_VALUES_RESPONSE_ONE_PAGE ) ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + WIREMOCK.getPort()
                                  + DAILY_VALUES_PATH
                                  + DAILY_VALUES_PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .timeZoneOffset( ZoneOffset.UTC )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder().name( "00060" ).build() )
                                        .build();

        DataSource fakeSource = DataSource.builder()
                                          .disposition( DataDisposition.GEOJSON )
                                          .source( fakeDeclarationSource )
                                          .context( dataset )
                                          .links( Collections.emptyList() )
                                          .uri( fakeUri )
                                          .datasetOrientation( DatasetOrientation.LEFT )
                                          .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder().setName( "02238500" ) )
                                                             .build() );
        Features features = FeaturesBuilder.builder().geometries( geometries ).build();
        TimeInterval interval = TimeIntervalBuilder.builder()
                                                   .minimum( Instant.parse( "2025-01-01T00:00:00Z" ) )
                                                   .maximum( Instant.parse( "2025-11-01T00:00:00Z" ) )
                                                   .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .features( features )
                                                                        .validDates( interval )
                                                                        .build();

        TimeChunker timeChunker = ReaderUtilities.getTimeChunker(
                TimeChunker.ChunkingStrategy.YEAR_RANGES, declaration, fakeSource );

        UsgsOgcReader reader = UsgsOgcReader.of( declaration, systemSettings, timeChunker, UsgsOgcResponseReader.of() );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            Geometry expectedGeometry = Geometry.newBuilder()
                                                .setName( "02238500" )
                                                .setWkt( "POINT (-81.8808333333333 29.0811111111111)" )
                                                .setSrid( 4326 )
                                                .build();

            TimeSeriesMetadata expectedMetadata = new TimeSeriesMetadata.Builder()
                    .setReferenceTimes( Map.of() )
                    .setUnit( "ft^3/s" )
                    .setVariableName( "00060" )
                    .setFeature( Feature.of( expectedGeometry ) )
                    .setTimeScale( TimeScaleOuter.of( TimeScale.newBuilder()
                                                               .setPeriod( MessageUtilities.getDuration( Duration.ofDays(
                                                                       1 ) ) )
                                                               .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                                               .build() ) )
                    .build();

            TimeSeries<Double> expectedSeries = new TimeSeries.Builder<Double>()
                    .setMetadata( expectedMetadata )
                    .addEvent( Event.of( Instant.parse( "2024-02-14T00:00:00Z" ), 10.2 ) )
                    .addEvent( Event.of( Instant.parse( "2024-02-15T00:00:00Z" ), 10.2 ) )
                    .addEvent( Event.of( Instant.parse( "2024-02-16T00:00:00Z" ), 10.2 ) )
                    .addEvent( Event.of( Instant.parse( "2024-02-18T00:00:00Z" ), 108.0 ) )
                    .addEvent( Event.of( Instant.parse( "2024-02-20T00:00:00Z" ), 600.0 ) )
                    .addEvent( Event.of( Instant.parse( "2024-03-06T00:00:00Z" ), 410.0 ) )
                    .addEvent( Event.of( Instant.parse( "2024-03-13T00:00:00Z" ), 284.0 ) )
                    .addEvent( Event.of( Instant.parse( "2024-03-14T00:00:00Z" ), 262.0 ) )
                    .addEvent( Event.of( Instant.parse( "2024-03-15T00:00:00Z" ), 179.0 ) )
                    .addEvent( Event.of( Instant.parse( "2024-03-16T00:00:00Z" ), 70.7 ) )
                    .build();

            Assertions.assertEquals( List.of( expectedSeries ), actual );
        }

        WIREMOCK.verify( WireMock.getRequestedFor( WireMock.urlPathEqualTo( DAILY_VALUES_PATH ) ) );
    }

    @Test
    void testReadDailyValuesReturnsOneTimeSeriesWithGeospatialMetadataAndAppliesZoneOffset()
    {
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( DAILY_VALUES_PATH ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withHeader( CONTENT_TYPE, APPLICATION_JSON )
                                                       .withBody( DAILY_VALUES_RESPONSE_ONE_PAGE_NO_GEOMETRY ) ) );

        // Supply the feature metadata on demand
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( "/collections/monitoring-locations/items/USGS-02238500" ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withHeader( CONTENT_TYPE, APPLICATION_JSON )
                                                       .withBody( FEATURE_RESPONSE ) ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + WIREMOCK.getPort()
                                  + DAILY_VALUES_PATH
                                  + DAILY_VALUES_PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder()
                                                                  .name( "00060" )
                                                                  .build() )
                                        .build();

        DataSource fakeSource = DataSource.builder()
                                          .disposition( DataDisposition.GEOJSON )
                                          .source( fakeDeclarationSource )
                                          .context( dataset )
                                          .links( Collections.emptyList() )
                                          .uri( fakeUri )
                                          .datasetOrientation( DatasetOrientation.LEFT )
                                          .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder()
                                                                               .setName( "02238500" ) )
                                                             .build() );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();
        TimeInterval interval = TimeIntervalBuilder.builder()
                                                   .minimum( Instant.parse( "2025-01-01T00:00:00Z" ) )
                                                   .maximum( Instant.parse( "2025-11-01T00:00:00Z" ) )
                                                   .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .features( features )
                                                                        .validDates( interval )
                                                                        .build();

        TimeChunker timeChunker = ReaderUtilities.getTimeChunker( TimeChunker.ChunkingStrategy.YEAR_RANGES,
                                                                  declaration,
                                                                  fakeSource );

        UsgsOgcReader reader = UsgsOgcReader.of( declaration, systemSettings, timeChunker, UsgsOgcResponseReader.of() );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            Geometry expectedGeometry = Geometry.newBuilder()
                                                .setName( "02238500" )
                                                .setWkt( "POINT (-81.8808333333333 29.0811111111111)" )
                                                .setDescription( "OCKLAWAHA RIVER AT MOSS BLUFF, FL" )
                                                .setSrid( 4326 )
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
            TimeSeries<Double> expectedSeries =
                    new TimeSeries.Builder<Double>().setMetadata( expectedMetadata )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-14T05:00:00Z" ),
                                                                         10.2 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-15T05:00:00Z" ),
                                                                         10.2 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-16T05:00:00Z" ),
                                                                         10.2 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-18T05:00:00Z" ),
                                                                         108.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-20T05:00:00Z" ),
                                                                         600.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-03-06T05:00:00Z" ),
                                                                         410.0 ) )
                                                    // Daylight savings begins on 10 March, note the hour difference
                                                    .addEvent( Event.of( Instant.parse( "2024-03-13T04:00:00Z" ),
                                                                         284.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-03-14T04:00:00Z" ),
                                                                         262.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-03-15T04:00:00Z" ),
                                                                         179.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-03-16T04:00:00Z" ),
                                                                         70.7 ) )
                                                    .build();

            List<TimeSeries<Double>> expected = List.of( expectedSeries );

            assertEquals( expected, actual );
        }
    }

    @Test
    void testReadDailyValuesReturnsOneTimeSeriesAndIgnoresDaylightSavings()
    {
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( DAILY_VALUES_PATH ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withHeader( CONTENT_TYPE, APPLICATION_JSON )
                                                       .withBody( DAILY_VALUES_RESPONSE_ONE_PAGE ) ) );

        // Supply the feature metadata on demand
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( "/collections/monitoring-locations/items/USGS-02238500" ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withHeader( CONTENT_TYPE, APPLICATION_JSON )
                                                       .withBody( FEATURE_RESPONSE ) ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + WIREMOCK.getPort()
                                  + DAILY_VALUES_PATH
                                  + DAILY_VALUES_PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .daylightSavings( false )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder()
                                                                  .name( "00060" )
                                                                  .build() )
                                        .build();

        DataSource fakeSource = DataSource.builder()
                                          .disposition( DataDisposition.GEOJSON )
                                          .source( fakeDeclarationSource )
                                          .context( dataset )
                                          .links( Collections.emptyList() )
                                          .uri( fakeUri )
                                          .datasetOrientation( DatasetOrientation.LEFT )
                                          .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder()
                                                                               .setName( "02238500" ) )
                                                             .build() );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();
        TimeInterval interval = TimeIntervalBuilder.builder()
                                                   .minimum( Instant.parse( "2025-01-01T00:00:00Z" ) )
                                                   .maximum( Instant.parse( "2025-11-01T00:00:00Z" ) )
                                                   .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .features( features )
                                                                        .validDates( interval )
                                                                        .build();

        TimeChunker timeChunker = ReaderUtilities.getTimeChunker( TimeChunker.ChunkingStrategy.YEAR_RANGES,
                                                                  declaration,
                                                                  fakeSource );

        UsgsOgcReader reader = UsgsOgcReader.of( declaration, systemSettings, timeChunker, UsgsOgcResponseReader.of() );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            Geometry expectedGeometry = Geometry.newBuilder()
                                                .setName( "02238500" )
                                                .setWkt( "POINT (-81.8808333333333 29.0811111111111)" )
                                                .setDescription( "OCKLAWAHA RIVER AT MOSS BLUFF, FL" )
                                                .setSrid( 4326 )
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
            TimeSeries<Double> expectedSeries =
                    new TimeSeries.Builder<Double>().setMetadata( expectedMetadata )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-14T05:00:00Z" ),
                                                                         10.2 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-15T05:00:00Z" ),
                                                                         10.2 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-16T05:00:00Z" ),
                                                                         10.2 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-18T05:00:00Z" ),
                                                                         108.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-20T05:00:00Z" ),
                                                                         600.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-03-06T05:00:00Z" ),
                                                                         410.0 ) )
                                                    // Daylight savings begins on 10 March, but this is ignored
                                                    .addEvent( Event.of( Instant.parse( "2024-03-13T05:00:00Z" ),
                                                                         284.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-03-14T05:00:00Z" ),
                                                                         262.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-03-15T05:00:00Z" ),
                                                                         179.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-03-16T05:00:00Z" ),
                                                                         70.7 ) )
                                                    .build();

            List<TimeSeries<Double>> expected = List.of( expectedSeries );

            assertEquals( expected, actual );
        }
    }

    @Test
    void testReadDailyValuesReturnsTwoConsolidatedTimeSeriesAcrossThreePages()
    {
        String secondPagePath = "/next/path";

        // Add the link to the next page with respect to the mock server
        String adjustedResponse = DAILY_VALUES_RESPONSE_PAGE_ONE_OF_THREE.replace( "NEXT_PAGE_PLACEHOLDER",
                                                                                   "http://localhost:"
                                                                                   + WIREMOCK.getPort()
                                                                                   + secondPagePath );
        String thirdPagePath = "/another/path";

        String adjustedResponseTwo = DAILY_VALUES_RESPONSE_PAGE_TWO_OF_THREE.replace( "NEXT_PAGE_PLACEHOLDER",
                                                                                      "http://localhost:"
                                                                                      + WIREMOCK.getPort()
                                                                                      + thirdPagePath );

        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( DAILY_VALUES_PATH ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withHeader( CONTENT_TYPE, APPLICATION_JSON )
                                                       .withBody( adjustedResponse ) ) );

        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( secondPagePath ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withHeader( CONTENT_TYPE, APPLICATION_JSON )
                                                       .withBody( adjustedResponseTwo ) ) );

        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( thirdPagePath ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withHeader( CONTENT_TYPE, APPLICATION_JSON )
                                                       .withBody( DAILY_VALUES_RESPONSE_PAGE_THREE_OF_THREE ) ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + WIREMOCK.getPort()
                                  + DAILY_VALUES_PATH
                                  + DAILY_VALUES_PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .timeZoneOffset( ZoneOffset.UTC )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder()
                                                                  .name( "00060" )
                                                                  .build() )
                                        .build();

        DataSource fakeSource = DataSource.builder()
                                          .disposition( DataDisposition.GEOJSON )
                                          .source( fakeDeclarationSource )
                                          .context( dataset )
                                          .links( Collections.emptyList() )
                                          .uri( fakeUri )
                                          .datasetOrientation( DatasetOrientation.LEFT )
                                          .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder()
                                                                               .setName( "02238500" ) )
                                                             .build() );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();
        TimeInterval interval = TimeIntervalBuilder.builder()
                                                   .minimum( Instant.parse( "2025-01-01T00:00:00Z" ) )
                                                   .maximum( Instant.parse( "2025-11-01T00:00:00Z" ) )
                                                   .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .features( features )
                                                                        .validDates( interval )
                                                                        .build();

        TimeChunker timeChunker = ReaderUtilities.getTimeChunker( TimeChunker.ChunkingStrategy.YEAR_RANGES,
                                                                  declaration,
                                                                  fakeSource );

        UsgsOgcReader reader = UsgsOgcReader.of( declaration, systemSettings, timeChunker, UsgsOgcResponseReader.of() );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            Geometry expectedGeometry = Geometry.newBuilder()
                                                .setName( "02238500" )
                                                .setWkt( "POINT (-81.8808333333333 29.0811111111111)" )
                                                .setSrid( 4326 )
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
            TimeSeries<Double> expectedSeriesOne =
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

            TimeSeries<Double> expectedSeriesTwo =
                    new TimeSeries.Builder<Double>().setMetadata( expectedMetadata )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-18T00:00:00Z" ),
                                                                         1.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-19T00:00:00Z" ),
                                                                         2.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2024-02-20T00:00:00Z" ),
                                                                         3.0 ) )
                                                    .build();

            List<TimeSeries<Double>> expected = List.of( expectedSeriesOne, expectedSeriesTwo );

            assertEquals( expected, actual );
        }
    }

    @Test
    void testReadDailyValuesReturnsThreeChunkedTimeSeriesRequests()
    {
        // First chunk
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( DAILY_VALUES_PATH ) )
                                  .withQueryParam( "time",
                                                   WireMock.equalTo( "2018-01-01T00:00:01Z/2019-01-01T00:00:00Z" ) )
                                  .withQueryParam( "f", WireMock.equalTo( "json" ) )
                                  .withQueryParam( "parameter_code", WireMock.equalTo( "00060" ) )
                                  .withQueryParam( "monitoring_location_id", WireMock.equalTo( "USGS-02238500" ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withHeader( CONTENT_TYPE, APPLICATION_JSON )
                                                       .withBody( DAILY_VALUES_RESPONSE_ONE_PAGE ) ) );

        // Second chunk
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( DAILY_VALUES_PATH ) )
                                  .withQueryParam( "time",
                                                   WireMock.equalTo( "2019-01-01T00:00:01Z/2020-01-01T00:00:00Z" ) )
                                  .withQueryParam( "f", WireMock.equalTo( "json" ) )
                                  .withQueryParam( "parameter_code", WireMock.equalTo( "00060" ) )
                                  .withQueryParam( "monitoring_location_id", WireMock.equalTo( "USGS-02238500" ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withHeader( CONTENT_TYPE, APPLICATION_JSON )
                                                       .withBody( DAILY_VALUES_RESPONSE_ONE_PAGE ) ) );

        // Third chunk
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( DAILY_VALUES_PATH ) )
                                  .withQueryParam( "time",
                                                   WireMock.equalTo( "2020-01-01T00:00:01Z/2021-01-01T00:00:00Z" ) )
                                  .withQueryParam( "f", WireMock.equalTo( "json" ) )
                                  .withQueryParam( "parameter_code", WireMock.equalTo( "00060" ) )
                                  .withQueryParam( "monitoring_location_id", WireMock.equalTo( "USGS-02238500" ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withHeader( CONTENT_TYPE, APPLICATION_JSON )
                                                       .withBody( DAILY_VALUES_RESPONSE_ONE_PAGE ) ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + WIREMOCK.getPort()
                                  + DAILY_VALUES_PATH
                                  + DAILY_VALUES_PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .timeZoneOffset( ZoneOffset.UTC )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder()
                                                                  .name( "00060" )
                                                                  .build() )
                                        .build();

        DataSource fakeSource = DataSource.builder()
                                          .disposition( DataDisposition.GEOJSON )
                                          .source( fakeDeclarationSource )
                                          .context( dataset )
                                          .links( Collections.emptyList() )
                                          .uri( fakeUri )
                                          .datasetOrientation( DatasetOrientation.LEFT )
                                          .build();

        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 18 ) )
                                                            .build();
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( "2018-01-01T00:00:00Z" ) )
                                                     .maximum( Instant.parse( "2021-01-01T00:00:00Z" ) )
                                                     .build();
        Set<TimePools> referenceTimePools = Collections.singleton( TimePoolsBuilder.builder()
                                                                                   .period( Duration.ofHours( 13 ) )
                                                                                   .frequency( Duration.ofHours( 7 ) )
                                                                                   .build() );
        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder()
                                                                               .setName( "02238500" ) )
                                                             .build() );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .validDates( validDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .features( features )
                                                                        .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        TimeChunker timeChunker = ReaderUtilities.getTimeChunker( TimeChunker.ChunkingStrategy.YEAR_RANGES,
                                                                  declaration,
                                                                  fakeSource );

        UsgsOgcReader reader = UsgsOgcReader.of( declaration, systemSettings, timeChunker, UsgsOgcResponseReader.of() );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            // Three chunks expected
            assertEquals( 3, actual.size() );
        }

        // Three requests made
        WIREMOCK.verify( WireMock.exactly( 3 ),
                         WireMock.getRequestedFor( WireMock.urlPathEqualTo( DAILY_VALUES_PATH ) ) );

        WIREMOCK.verify( WireMock.exactly( 1 ),
                         WireMock.getRequestedFor( WireMock.urlPathEqualTo( DAILY_VALUES_PATH ) )
                                 .withQueryParam( "time",
                                                  WireMock.equalTo( "2018-01-01T00:00:01Z/2019-01-01T00:00:00Z" ) )
                                 .withQueryParam( "f", WireMock.equalTo( "json" ) )
                                 .withQueryParam( "parameter_code", WireMock.equalTo( "00060" ) )
                                 .withQueryParam( "monitoring_location_id", WireMock.equalTo( "USGS-02238500" ) ) );

        WIREMOCK.verify( WireMock.exactly( 1 ),
                         WireMock.getRequestedFor( WireMock.urlPathEqualTo( DAILY_VALUES_PATH ) )
                                 .withQueryParam( "time",
                                                  WireMock.equalTo( "2019-01-01T00:00:01Z/2020-01-01T00:00:00Z" ) )
                                 .withQueryParam( "f", WireMock.equalTo( "json" ) )
                                 .withQueryParam( "parameter_code", WireMock.equalTo( "00060" ) )
                                 .withQueryParam( "monitoring_location_id", WireMock.equalTo( "USGS-02238500" ) ) );

        WIREMOCK.verify( WireMock.exactly( 1 ),
                         WireMock.getRequestedFor( WireMock.urlPathEqualTo( DAILY_VALUES_PATH ) )
                                 .withQueryParam( "time",
                                                  WireMock.equalTo( "2020-01-01T00:00:01Z/2021-01-01T00:00:00Z" ) )
                                 .withQueryParam( "f", WireMock.equalTo( "json" ) )
                                 .withQueryParam( "parameter_code", WireMock.equalTo( "00060" ) )
                                 .withQueryParam( "monitoring_location_id", WireMock.equalTo( "USGS-02238500" ) ) );
    }

    @Test
    void testReadContinuousValuesReturnsOneTimeSeries()
    {
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( CONTINUOUS_VALUES_PATH ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withHeader( CONTENT_TYPE, APPLICATION_JSON )
                                                       .withBody( CONTINUOUS_VALUES_RESPONSE_ONE_PAGE ) ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + WIREMOCK.getPort()
                                  + CONTINUOUS_VALUES_PATH
                                  + CONTINUOUS_VALUES_PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder().name( "00060" )
                                                                  .build() )
                                        .build();

        DataSource fakeSource = DataSource.builder()
                                          .disposition( DataDisposition.GEOJSON )
                                          .source( fakeDeclarationSource )
                                          .context( dataset )
                                          .links( Collections.emptyList() )
                                          .uri( fakeUri )
                                          .datasetOrientation( DatasetOrientation.LEFT )
                                          .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder()
                                                                               .setName( "01593450" ) )
                                                             .build() );
        Features features = FeaturesBuilder.builder().geometries( geometries ).build();
        TimeInterval interval = TimeIntervalBuilder.builder()
                                                   .minimum( Instant.parse( "2026-03-09T07:00:00Z" ) )
                                                   .maximum( Instant.parse( "2026-03-09T08:00:00Z" ) )
                                                   .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .features( features )
                                                                        .validDates( interval )
                                                                        .build();

        TimeChunker timeChunker = ReaderUtilities.getTimeChunker(
                TimeChunker.ChunkingStrategy.YEAR_RANGES, declaration, fakeSource );

        UsgsOgcReader reader = UsgsOgcReader.of( declaration, systemSettings, timeChunker, UsgsOgcResponseReader.of() );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            Geometry expectedGeometry = Geometry.newBuilder()
                                                .setName( "01593450" )
                                                .setSrid( 4326 )
                                                .build();

            TimeSeriesMetadata expectedMetadata = new TimeSeriesMetadata.Builder()
                    .setReferenceTimes( Map.of() )
                    .setUnit( "ft^3/s" )
                    .setVariableName( "00060" )
                    .setFeature( Feature.of( expectedGeometry ) )
                    .setTimeScale( TimeScaleOuter.of() )
                    .build();

            TimeSeries<Double> expectedSeries = new TimeSeries.Builder<Double>()
                    .setMetadata( expectedMetadata )
                    .addEvent( Event.of( Instant.parse( "2026-03-09T07:05:00Z" ), 1.31 ) )
                    .addEvent( Event.of( Instant.parse( "2026-03-09T07:10:00Z" ), 1.4 ) )
                    .addEvent( Event.of( Instant.parse( "2026-03-09T07:15:00Z" ), 1.31 ) )
                    .addEvent( Event.of( Instant.parse( "2026-03-09T07:20:00Z" ), 1.31 ) )
                    .addEvent( Event.of( Instant.parse( "2026-03-09T07:25:00Z" ), 1.31 ) )
                    .addEvent( Event.of( Instant.parse( "2026-03-09T07:30:00Z" ), 1.31 ) )
                    .addEvent( Event.of( Instant.parse( "2026-03-09T07:35:00Z" ), 1.31 ) )
                    .addEvent( Event.of( Instant.parse( "2026-03-09T07:40:00Z" ), 1.4 ) )
                    .addEvent( Event.of( Instant.parse( "2026-03-09T07:45:00Z" ), 1.31 ) )
                    .addEvent( Event.of( Instant.parse( "2026-03-09T07:50:00Z" ), 1.31 ) )
                    .addEvent( Event.of( Instant.parse( "2026-03-09T07:55:00Z" ), 1.31 ) )
                    .build();

            Assertions.assertEquals( List.of( expectedSeries ), actual );
        }

        WIREMOCK.verify( WireMock.getRequestedFor( WireMock.urlPathEqualTo( CONTINUOUS_VALUES_PATH ) ) );
    }
}
