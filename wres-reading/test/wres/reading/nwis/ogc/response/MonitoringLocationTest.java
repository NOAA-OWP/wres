package wres.reading.nwis.ogc.response;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import org.locationtech.jts.geom.Geometry;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the {@link MonitoringLocation}.
 *
 * @author James Brown
 */

class MonitoringLocationTest
{
    /** An object mapper. */
    private static final ObjectMapper OBJECT_MAPPER =
            JsonMapper.builder()
                      .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true )
                      .build();

    /** A response. **/
    private static final String RESPONSE = """
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
    void testDeserialize()
    {
        MonitoringLocation actual = OBJECT_MAPPER.readValue( RESPONSE.getBytes(), MonitoringLocation.class );

        MonitoringLocation expected = new MonitoringLocation();
        expected.setId( "USGS-02238500" );
        MonitoringLocationProperties properties = new MonitoringLocationProperties();
        properties.setTimeZoneAbbreviation( "EST" );
        properties.setUsesDaylightSavings( "Y" );
        properties.setMonitoringLocationName( "OCKLAWAHA RIVER AT MOSS BLUFF, FL" );
        expected.setProperties( properties );

        GeometryFactory factory = new GeometryFactory();
        Geometry geometry = factory.createPoint( new Coordinate(-81.8808333333333, 29.0811111111111) );
        expected.setGeometry( geometry );

        assertEquals( expected, actual );
    }
}
