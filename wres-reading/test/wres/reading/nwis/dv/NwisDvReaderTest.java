package wres.reading.nwis.dv;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;
import org.mockserver.model.Parameters;
import org.mockserver.verify.VerificationTimes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockserver.model.HttpRequest.request;

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
import wres.reading.TimeSeriesTuple;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeScale;
import wres.system.SystemSettings;

/**
 * Tests the {@link NwisDvReader}.
 *
 * @author James Brown
 */

class NwisDvReaderTest
{
    /** Mocker server instance. */
    private ClientAndServer mockServer;

    /** Path used by GET. */
    private static final String PATH = "/collections/daily/items";


    /** Parameters used by GET. */
    private static final String PARAMS =
            "?f=json&lang=en-US&limit=10&properties=id,time_series_id,monitoring_location_id,parameter_code,statistic_id,time,value,unit_of_measure&skipGeometry=false&monitoring_location_id=USGS-02238500&parameter_code=00060&time=2024-02-12T00%3A00%3A00Z%2F2024-03-18T12%3A31%3A12Z";

    /** Response from GET. */
    private static final String RESPONSE_ONE_PAGE = """
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

    /** Response from GET, first page. */
    private static final String RESPONSE_PAGE_ONE_OF_THREE = """
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

    /** Response from GET, second page. */
    private static final String RESPONSE_PAGE_TWO_OF_THREE = """
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
                }
              ],
              "numberReturned": 4,
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

    /** Response from GET, second page. */
    private static final String RESPONSE_PAGE_THREE_OF_THREE = """
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
                }
              ],
              "numberReturned": 1,
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

    private static final String GET = "GET";

    @BeforeEach
    void startServer()
    {
        this.mockServer = ClientAndServer.startClientAndServer( 0 );
    }

    @AfterEach
    void stopServer()
    {
        this.mockServer.stop();
    }

    @Test
    void testReadReturnsOneTimeSeries()
    {
        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE_ONE_PAGE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + PATH
                                  + PARAMS );

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

        NwisDvReader reader = NwisDvReader.of( declaration, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

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
            TimeSeries<Double> expectedSeries =
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

            List<TimeSeries<Double>> expected = List.of( expectedSeries );

            assertEquals( expected, actual );
        }
    }

    @Test
    void testReadReturnsOneTimeSeriesAndAcquiresTimingInformation()
    {
        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE_ONE_PAGE ) );

        // Supply the feature metadata on demand
        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/collections/monitoring_locations/items/USGS-02238500" )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( FEATURE_RESPONSE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + PATH
                                  + PARAMS );

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

        NwisDvReader reader = NwisDvReader.of( declaration, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

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
    void testReadReturnsOneTimeSeriesAcrossThreePages()
    {
        String secondPagePath = "/next/path";

        // Add the link to the next page with respect to the mock server
        String adjustedResponse = RESPONSE_PAGE_ONE_OF_THREE.replace( "NEXT_PAGE_PLACEHOLDER",
                                                                      "http://localhost:"
                                                                      + this.mockServer.getLocalPort()
                                                                      + secondPagePath );
        String thirdPagePath = "/another/path";

        String adjustedResponseTwo = RESPONSE_PAGE_TWO_OF_THREE.replace( "NEXT_PAGE_PLACEHOLDER",
                                                                         "http://localhost:"
                                                                         + this.mockServer.getLocalPort()
                                                                         + thirdPagePath );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( adjustedResponse ) );
        this.mockServer.when( HttpRequest.request()
                                         .withPath( secondPagePath )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( adjustedResponseTwo ) );
        this.mockServer.when( HttpRequest.request()
                                         .withPath( thirdPagePath )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE_PAGE_THREE_OF_THREE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + PATH
                                  + PARAMS );

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

        NwisDvReader reader = NwisDvReader.of( declaration, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

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
            TimeSeries<Double> expectedSeries =
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

            List<TimeSeries<Double>> expected = List.of( expectedSeries );

            assertEquals( expected, actual );
        }
    }

    @Test
    void testReadReturnsThreeChunkedTimeSeriesRequests()
    {
        // Create the chunk parameters
        Parameters parametersOne =
                new Parameters( new Parameter( "time", "2018-01-01T00:00:01Z/2019-01-01T00:00:00Z" ),
                                new Parameter( "f", "json" ),
                                new Parameter( "parameter_code", "00060" ),
                                new Parameter( "monitoring_location_id", "USGS-02238500" ) );

        Parameters parametersTwo =
                new Parameters( new Parameter( "time", "2019-01-01T00:00:01Z/2020-01-01T00:00:00Z" ),
                                new Parameter( "f", "json" ),
                                new Parameter( "parameter_code", "00060" ),
                                new Parameter( "monitoring_location_id", "USGS-02238500" ) );

        Parameters parametersThree =
                new Parameters( new Parameter( "time", "2020-01-01T00:00:01Z/2021-01-01T00:00:00Z" ),
                                new Parameter( "f", "json" ),
                                new Parameter( "parameter_code", "00060" ),
                                new Parameter( "monitoring_location_id", "USGS-02238500" ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersOne )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE_ONE_PAGE ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersTwo )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE_ONE_PAGE ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersThree )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE_ONE_PAGE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + PATH
                                  + PARAMS );

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

        NwisDvReader reader = NwisDvReader.of( declaration, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            // Three chunks expected
            assertEquals( 3, actual.size() );
        }

        // Three requests made
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH ),
                                VerificationTimes.exactly( 3 ) );

        // One request made with parameters one
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersOne ),
                                VerificationTimes.exactly( 1 ) );

        // One request made with parameters two
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersTwo ),
                                VerificationTimes.exactly( 1 ) );

        // One request made with parameters three
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersThree ),
                                VerificationTimes.exactly( 1 ) );

    }
}
