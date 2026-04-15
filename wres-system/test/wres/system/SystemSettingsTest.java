package wres.system;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests the {@link SystemSettings}.
 *
 * @author James Brown
 */

class SystemSettingsTest
{

    @Test
    void testRedacted()
    {
        SystemSettings unredacted = SystemSettings.builder()
                                                  .databaseConfiguration( DatabaseSettings.builder()
                                                                                          .password( "fooIgnoreMe" )
                                                                                          .build() )
                                                  .build();

        SystemSettings redacted = unredacted.redacted();

        assertAll( () -> assertEquals( "fooIgnoreMe", unredacted.getDatabaseConfiguration()
                                                                .getPassword() ),
                   () -> assertEquals( "[REDACTED]", redacted.getDatabaseConfiguration()
                                                             .getPassword() ) );
    }

    @Test
    void testRedactedSystemProperties()
    {
        System.setProperty( "wres.usgsApiKey", "foo" );

        Map<String, String> properties = SystemSettings.builder()
                                                       .build()
                                                       .redactedSystemProperties();

        Set<String> propertyNames = properties.keySet();

        assertAll( () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.contains( "pass" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.contains( "class.path" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.contains( "separator" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.startsWith( "sun" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.contains( "user.country" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.startsWith( "java.vendor" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.startsWith( "java.e" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.startsWith( "java.vm.specification" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.startsWith( "java.specification" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.contains( "printer" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.contains( "key" ) ) ),
                   () -> assertEquals( "[REDACTED]", properties.get( "wres.usgsApiKey" ) ) );
    }

    @Test
    void testRedactBadWords()
    {
        String unclean =
                "https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items?api_key=foo&f=json&lang=en-US&limit=50000&monitoring_location_id=USGS-01593450&parameter_code=00060&properties=id%2Ctime_series_id%2Cmonitoring_location_id%2Cstatistic_id%2Ctime%2Cvalue%2Cunit_of_measure&skipGeometry=true&time=2026-01-01T00%3A00%3A01Z%2F2026-03-09T07%3A58%3A53Z";

        String expected =
                "https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items?api_key=[REDACTED]&f=json&lang=en-US&limit=50000&monitoring_location_id=USGS-01593450&parameter_code=00060&properties=id%2Ctime_series_id%2Cmonitoring_location_id%2Cstatistic_id%2Ctime%2Cvalue%2Cunit_of_measure&skipGeometry=true&time=2026-01-01T00%3A00%3A01Z%2F2026-03-09T07%3A58%3A53Z";

        String actual = SystemSettings.redactBadWords( unclean );

        assertEquals( expected, actual );
    }
}
