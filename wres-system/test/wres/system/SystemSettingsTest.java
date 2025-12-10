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
                   () -> assertEquals( "[REDACTED]", redacted.getDatabaseConfiguration().getPassword() ) );
    }

    @Test
    void testRedactedSystemProperties()
    {
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
                                                   .anyMatch( s -> s.contains( "sun" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.contains( "user.country" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.contains( "java.vendor" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.contains( "java.e" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.contains( "java.vm.specification" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.contains( "java.specification" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.contains( "printer" ) ) ),
                   () -> assertFalse( propertyNames.stream()
                                                   .anyMatch( s -> s.contains( "key" ) ) ) );
    }
}
