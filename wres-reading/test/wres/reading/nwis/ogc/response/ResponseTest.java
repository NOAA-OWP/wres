package wres.reading.nwis.ogc.response;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the {@link Response}.
 *
 * @author James Brown
 */
class ResponseTest
{
    /** An object mapper. */
    private static final ObjectMapper OBJECT_MAPPER =
            JsonMapper.builder()
                      .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true )
                      .build();

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
                  "href": "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?cursor=MzgwMjBkZTEtZTBlNy00M2EwLTg3NDgtMTc5NWQxOTQxNTM1&lang=en-US&limit=10&properties=id,time_series_id,monitoring_location_id,parameter_code,statistic_id,time,value,unit_of_measure,approval_status,qualifier,last_modified&skipGeometry=false&monitoring_location_id=USGS-02238500&parameter_code=00060&time=2024-02-12T00%3A00%3A00Z%2F2024-03-18T12%3A31%3A12Z",
                  "type": "application/geo+json",
                  "title": "Items (next)"
                },
                {
                  "type": "application/geo+json",
                  "rel": "self",
                  "title": "This document as GeoJSON",
                  "href": "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&lang=en-US&limit=10&properties=id,time_series_id,monitoring_location_id,parameter_code,statistic_id,time,value,unit_of_measure,approval_status,qualifier,last_modified&skipGeometry=false&monitoring_location_id=USGS-02238500&parameter_code=00060&time=2024-02-12T00%3A00%3A00Z%2F2024-03-18T12%3A31%3A12Z"
                },
                {
                  "rel": "alternate",
                  "type": "application/ld+json",
                  "title": "This document as RDF (JSON-LD)",
                  "href": "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=jsonld&lang=en-US&limit=10&properties=id,time_series_id,monitoring_location_id,parameter_code,statistic_id,time,value,unit_of_measure,approval_status,qualifier,last_modified&skipGeometry=false&monitoring_location_id=USGS-02238500&parameter_code=00060&time=2024-02-12T00%3A00%3A00Z%2F2024-03-18T12%3A31%3A12Z"
                },
                {
                  "type": "text/html",
                  "rel": "alternate",
                  "title": "This document as HTML",
                  "href": "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=html&lang=en-US&limit=10&properties=id,time_series_id,monitoring_location_id,parameter_code,statistic_id,time,value,unit_of_measure,approval_status,qualifier,last_modified&skipGeometry=false&monitoring_location_id=USGS-02238500&parameter_code=00060&time=2024-02-12T00%3A00%3A00Z%2F2024-03-18T12%3A31%3A12Z"
                },
                {
                  "type": "application/json",
                  "title": "Daily values",
                  "rel": "collection",
                  "href": "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily"
                }
              ],
              "timeStamp": "2025-11-18T11:53:21.880557Z"
            }
            """;

    @Test
    void testDeserialize()
    {
        Response response = OBJECT_MAPPER.readValue( RESPONSE.getBytes(), Response.class );

        // Make some basic assertions about the shape of the response, alongside the implicit assertion that
        // deserialization was not exceptional
        assertAll( () -> assertEquals( 10, response.getNumberReturned() ),
                   () -> assertEquals( 10, response.getFeatures().length ) );
    }

}
