package wres.datamodel;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.measure.Unit;
import javax.measure.format.MeasurementParseException;
import javax.measure.quantity.Length;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.units.indriya.AbstractUnit;

import static systems.uom.common.USCustomary.FOOT;
import static tech.units.indriya.unit.Units.METRE;

/**
 * Build units using javax.measure. See also wres.io.retrieval.UnitMapper class.
 */

public class Units
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Units.class );
    private static final String OFFICIAL_CUBIC_METERS_PER_SECOND = "m^3/s";
    private static final String OFFICIAL_CUBIC_FEET_PER_SECOND = "ft^3/s";
    private static final String OFFICIAL_KILO_CUBIC_FEET_PER_SECOND = "ft^3*1000/s";
    private static final String OFFICIAL_DEGREES_CELSIUS = "\u2103";
    private static final String OFFICIAL_DEGREES_FAHRENHEIT = "\u00b0F";

    /**
     * For backward compatibility, a map from weird unit names to official ones,
     * "official ones" being those supported by the indriya implementation.
     * Here was the table as of WRES 5.14. Here we omit "NONE", "-", "fraction"
     * because they aren't units, and omit "%" because it's already supported.
     * The units were case-insensitive which is why you will see upper and lower
     * cased versions of each previously supported unit. It seemed too far to do
     * mixed case for each and every unit. This is already too far anyway.
     *
     * wres8=> select * from wres.measurementunit;
     *  measurementunit_id | unit_name
     * --------------------+-----------
     *                   1 | NONE
     *                   2 | CMS
     *                   3 | CFS
     *                   4 | FT
     *                   5 | F
     *                   6 | C
     *                   7 | IN
     *                   8 | M
     *                   9 | MS
     *                  10 | HR
     *                  11 | H
     *                  12 | S
     *                  13 | MM
     *                  14 | CM
     *                  15 | m3 s-1
     *                  16 | kg m{-2}
     *                  17 | %
     *                  18 | ft/sec
     *                  19 | gal/min
     *                  20 | mgd
     *                  21 | m/sec
     *                  22 | ft3/day
     *                  23 | ac-ft
     *                  24 | mph
     *                  25 | l/sec
     *                  26 | ft3/s
     *                  27 | m3/sec
     *                  28 | mm/s
     *                  29 | mm s^-1
     *                  30 | mm s{-1}
     *                  31 | mm s-1
     *                  32 | mm h^-1
     *                  33 | mm/h
     *                  34 | mm h-1
     *                  35 | mm h{-1}
     *                  36 | kg/m^2
     *                  37 | kg/m^2h
     *                  38 | kg/m^2s
     *                  39 | kg/m^2/s
     *                  40 | kg/m^2/h
     *                  41 | Pa
     *                  42 | W/m^2
     *                  43 | W m{-2}
     *                  44 | W m-2
     *                  45 | m s-1
     *                  46 | m/s
     *                  47 | m s{-1}
     *                  48 | kg kg-1
     *                  49 | kg kg{-1}
     *                  50 | kg m-2
     *                  51 | fraction
     *                  52 | K
     *                  53 | -
     *                  54 | KCFS
     *                  55 | in/hr
     *                  56 | in hr^-1
     *                  57 | in hr-1
     *                  58 | in hr{-1}
     *                  59 | in/h
     *                  60 | in h-1
     *                  61 | in h{-1}
     *                  62 | CFSD
     *                  63 | CMSD
     *                  64 | m3/s
     *                  65 | degc
     *                  66 | degf
     * (66 rows)
     */

    private static final Map<String,String> CONVENIENCE_ALIASES = new HashMap<>( 108 );

    static
    {
        // These are for backward compatibility with pre-5.14 units that were
        // previously pre-loaded into the wres.measurementunit table.
        CONVENIENCE_ALIASES.put( "CFS", OFFICIAL_CUBIC_FEET_PER_SECOND );
        CONVENIENCE_ALIASES.put( "cfs", OFFICIAL_CUBIC_FEET_PER_SECOND );
        CONVENIENCE_ALIASES.put( "CFSD", OFFICIAL_CUBIC_FEET_PER_SECOND );
        CONVENIENCE_ALIASES.put( "cfsd", OFFICIAL_CUBIC_FEET_PER_SECOND );
        CONVENIENCE_ALIASES.put( "ft3/s", OFFICIAL_CUBIC_FEET_PER_SECOND );
        CONVENIENCE_ALIASES.put( "FT3/S", OFFICIAL_CUBIC_FEET_PER_SECOND );
        CONVENIENCE_ALIASES.put( "CMS", OFFICIAL_CUBIC_METERS_PER_SECOND );
        CONVENIENCE_ALIASES.put( "cms", OFFICIAL_CUBIC_METERS_PER_SECOND );
        CONVENIENCE_ALIASES.put( "CMSD", OFFICIAL_CUBIC_METERS_PER_SECOND );
        CONVENIENCE_ALIASES.put( "cmsd", OFFICIAL_CUBIC_METERS_PER_SECOND );
        CONVENIENCE_ALIASES.put( "m3 s-1", OFFICIAL_CUBIC_METERS_PER_SECOND );
        CONVENIENCE_ALIASES.put( "M3 S-1", OFFICIAL_CUBIC_METERS_PER_SECOND );
        CONVENIENCE_ALIASES.put( "m3/s", OFFICIAL_CUBIC_METERS_PER_SECOND );
        CONVENIENCE_ALIASES.put( "M3/S", OFFICIAL_CUBIC_METERS_PER_SECOND );
        CONVENIENCE_ALIASES.put( "m3/sec", OFFICIAL_CUBIC_METERS_PER_SECOND );
        CONVENIENCE_ALIASES.put( "M3/SEC", OFFICIAL_CUBIC_METERS_PER_SECOND );
        CONVENIENCE_ALIASES.put( "KCFS", OFFICIAL_KILO_CUBIC_FEET_PER_SECOND );
        CONVENIENCE_ALIASES.put( "kcfs", OFFICIAL_KILO_CUBIC_FEET_PER_SECOND );
        CONVENIENCE_ALIASES.put( "C", OFFICIAL_DEGREES_CELSIUS );
        CONVENIENCE_ALIASES.put( "c", OFFICIAL_DEGREES_CELSIUS );
        CONVENIENCE_ALIASES.put( "DEGC", OFFICIAL_DEGREES_CELSIUS );
        CONVENIENCE_ALIASES.put( "degc", OFFICIAL_DEGREES_CELSIUS );
        CONVENIENCE_ALIASES.put( "F", OFFICIAL_DEGREES_FAHRENHEIT );
        CONVENIENCE_ALIASES.put( "f", OFFICIAL_DEGREES_FAHRENHEIT );
        CONVENIENCE_ALIASES.put( "DEGF", OFFICIAL_DEGREES_FAHRENHEIT );
        CONVENIENCE_ALIASES.put( "degf", OFFICIAL_DEGREES_FAHRENHEIT );
        CONVENIENCE_ALIASES.put( "FT", "ft" );
        CONVENIENCE_ALIASES.put( "IN", "in" );
        CONVENIENCE_ALIASES.put( "M", "m" );
        CONVENIENCE_ALIASES.put( "MS", "ms" );
        CONVENIENCE_ALIASES.put( "HR", "h" );
        CONVENIENCE_ALIASES.put( "hr", "h" );
        CONVENIENCE_ALIASES.put( "S", "s" );
        CONVENIENCE_ALIASES.put( "MM", "mm" );
        CONVENIENCE_ALIASES.put( "CM", "cm" );
        CONVENIENCE_ALIASES.put( "kg m{-2}", "kg/m^2" );
        CONVENIENCE_ALIASES.put( "KG M{-2}", "kg/m^2" );
        CONVENIENCE_ALIASES.put( "ft/sec", "ft/s" );
        CONVENIENCE_ALIASES.put( "FT/SEC", "ft/s" );
        CONVENIENCE_ALIASES.put( "GAL/MIN", "gal/min" );
        CONVENIENCE_ALIASES.put( "mgd", "gal*1000000/d" );
        CONVENIENCE_ALIASES.put( "MGD", "gal*1000000/d" );
        CONVENIENCE_ALIASES.put( "m/sec", "m/s" );
        CONVENIENCE_ALIASES.put( "M/SEC", "m/s" );
        CONVENIENCE_ALIASES.put( "ft3/day", "ft^3/d" );
        CONVENIENCE_ALIASES.put( "FT3/DAY", "ft^3/d" );
        CONVENIENCE_ALIASES.put( "ac-ft", "ac ft" );
        CONVENIENCE_ALIASES.put( "AC-FT", "ac ft" );
        CONVENIENCE_ALIASES.put( "MPH", "mph" );
        CONVENIENCE_ALIASES.put( "l/sec", "l/s");
        CONVENIENCE_ALIASES.put( "L/SEC", "l/s");
        CONVENIENCE_ALIASES.put( "MM/S", "mm/s");
        CONVENIENCE_ALIASES.put( "mm s^-1", "mm/s");
        CONVENIENCE_ALIASES.put( "MM S^-1", "mm/s");
        CONVENIENCE_ALIASES.put( "mm s{-1}", "mm/s");
        CONVENIENCE_ALIASES.put( "MM S{-1}", "mm/s");
        CONVENIENCE_ALIASES.put( "mm s-1", "mm/s");
        CONVENIENCE_ALIASES.put( "MM S-1", "mm/s");
        CONVENIENCE_ALIASES.put( "mm h^-1", "mm/h");
        CONVENIENCE_ALIASES.put( "MM H^-1", "mm/h");
        CONVENIENCE_ALIASES.put( "MM/H", "mm/h");
        CONVENIENCE_ALIASES.put( "mm h-1", "mm/h");
        CONVENIENCE_ALIASES.put( "MM H-1", "mm/h");
        CONVENIENCE_ALIASES.put( "mm h{-1}", "mm/h");
        CONVENIENCE_ALIASES.put( "MM H{-1}", "mm/h");
        CONVENIENCE_ALIASES.put( "KG/M^2", "kg/m^2" );
        CONVENIENCE_ALIASES.put( "kg/m^2h", "kg/m^2*h" );
        CONVENIENCE_ALIASES.put( "KG/M^2H", "kg/m^2*h" );
        CONVENIENCE_ALIASES.put( "kg/m^2s", "kg/m^2*s" );
        CONVENIENCE_ALIASES.put( "KG/M^2S", "kg/m^2*s" );
        CONVENIENCE_ALIASES.put( "kg/m^2/s", "kg/m^2*s" );
        CONVENIENCE_ALIASES.put( "KG/M^2/S", "kg/m^2*s" );
        CONVENIENCE_ALIASES.put( "kg/m^2/h", "kg/m^2*h" );
        CONVENIENCE_ALIASES.put( "KG/M^2/H", "kg/m^2*h" );
        CONVENIENCE_ALIASES.put( "PA", "Pa" );
        CONVENIENCE_ALIASES.put( "pa", "Pa" );
        CONVENIENCE_ALIASES.put( "w/m^2", "W/m^2" );
        CONVENIENCE_ALIASES.put( "W/M^2", "W/m^2" );
        CONVENIENCE_ALIASES.put( "W m{-2}", "W/m^2" );
        CONVENIENCE_ALIASES.put( "w m{-2}", "W/m^2" );
        CONVENIENCE_ALIASES.put( "W M{-2}", "W/m^2" );
        CONVENIENCE_ALIASES.put( "W m-2", "W/m^2" );
        CONVENIENCE_ALIASES.put( "W M-2", "W/m^2" );
        CONVENIENCE_ALIASES.put( "w m-2", "W/m^2" );
        CONVENIENCE_ALIASES.put( "m s-1", "m/s" );
        CONVENIENCE_ALIASES.put( "M S-1", "m/s" );
        CONVENIENCE_ALIASES.put( "M/S", "m/s" );
        CONVENIENCE_ALIASES.put( "M S{-1}", "m/s" );
        CONVENIENCE_ALIASES.put( "m s{-1}", "m/s" );
        CONVENIENCE_ALIASES.put( "kg kg-1", "kg/kg" );
        CONVENIENCE_ALIASES.put( "KG KG-1", "kg/kg" );
        CONVENIENCE_ALIASES.put( "kg kg{-1}", "kg/kg" );
        CONVENIENCE_ALIASES.put( "KG KG{-1}", "kg/kg" );
        CONVENIENCE_ALIASES.put( "kg m-2", "kg/m^2" );
        CONVENIENCE_ALIASES.put( "KG M-2", "kg/m^2" );
        CONVENIENCE_ALIASES.put( "k", "K" );
        CONVENIENCE_ALIASES.put( "IN/HR", "in/h" );
        CONVENIENCE_ALIASES.put( "in hr^-1", "in/h" );
        CONVENIENCE_ALIASES.put( "IN HR^-1", "in/h" );
        CONVENIENCE_ALIASES.put( "in hr-1", "in/h" );
        CONVENIENCE_ALIASES.put( "IN HR-1", "in/h" );
        CONVENIENCE_ALIASES.put( "in hr{-1}", "in/h" );
        CONVENIENCE_ALIASES.put( "IN HR{-1}", "in/h" );
        CONVENIENCE_ALIASES.put( "IN/H", "in/h" );
        CONVENIENCE_ALIASES.put( "in h-1", "in/h" );
        CONVENIENCE_ALIASES.put( "IN H-1", "in/h" );
        CONVENIENCE_ALIASES.put( "in h{-1}", "in/h" );
        CONVENIENCE_ALIASES.put( "IN H{-1}", "in/h" );
    }

    /**
     * Get the full set of known convenience alias names. Mostly for testing.
     * @return The set of convenient alias names.
     */

    static Set<String> getAllConvenienceAliases()
    {
        return CONVENIENCE_ALIASES.keySet();
    }

    /**
     * Get the full set of known convenience units. Mostly for testing.
     * @return The set of convenience indriya unit strings.
     */
    static Set<String> getAllConvenienceUnits()
    {
        return new HashSet<>( CONVENIENCE_ALIASES.values() );
    }

    /**
     * Given a unit name, return the formal javax.measure Unit of Measure.
     *
     * Look in overrides and use the value found (when found).
     * If not found in overrides, look in convenient defaults.
     * If not found in convenient defaults, parse straightaway.
     *
     * @param unitName The name String
     * @param overrideAliases Unit name aliases to official unit names.
     * @return the javax.measure if known, null otherwise.
     * @throws UnsupportedUnitException when unable to find the unit.
     */

    public static Unit<?> getUnit( String unitName,
                                   Map<String,String> overrideAliases )
    {
        Objects.requireNonNull( unitName );
        Objects.requireNonNull( overrideAliases );

        // The next two lines are an attempt to ensure US Customary is loaded.
        Unit<Length> foot = FOOT;
        Unit<Length> meter = METRE;
        String officialName = CONVENIENCE_ALIASES.getOrDefault( unitName, unitName );

        // Priority is given to override aliases.
        officialName = overrideAliases.getOrDefault( unitName, officialName );
        Unit<?> unit;

        try
        {
            LOGGER.debug( "getUnit parsing a unit {}, given {}", officialName, unitName );
            unit = AbstractUnit.parse( officialName );
        }
        catch ( MeasurementParseException mpe )
        {
            SortedSet<String> aliases = new TreeSet<>();
            aliases.addAll( Units.getAllConvenienceAliases() );
            aliases.addAll( overrideAliases.keySet() );
            SortedSet<String> actualUnits = new TreeSet<>();
            actualUnits.addAll( Units.getAllConvenienceUnits() );
            actualUnits.addAll( overrideAliases.values() );
            throw new UnsupportedUnitException( unitName,
                                                aliases,
                                                actualUnits,
                                                mpe );
        }

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "Treating measurement unit name '{}' as formal "
                         + "unit '{}' along dimension '{}'",
                         unitName, officialName, unit.getDimension() );
        }

        return unit;
    }


    /**
     * Given a unit name, return the formal javax.measure Unit of Measure.
     *
     * Look in convenient defaults.
     * If not found in convenient defaults, parse straightaway.
     *
     * @param unitName The name String
     * @return the javax.measure if known, null otherwise.
     * @throws UnsupportedUnitException when unable to find the unit.
     */

    public static Unit<?> getUnit( String unitName )
    {
        return Units.getUnit( unitName, Collections.emptyMap() );
    }


    public static final class UnsupportedUnitException extends RuntimeException
    {
        private static final long serialVersionUID = -6873574285493867322L;

        UnsupportedUnitException( String unit,
                                  Set<String> unitAliases,
                                  Set<String> actualUnits,
                                  Throwable cause )
        {
            super( "Unable to find the measurement unit " + unit
                   + " among the unit aliases "
                   + unitAliases
                   + ", nor was it able to be parsed as an indriya unit,"
                   + " such as these units that can be parsed "
                   + actualUnits + ". You may need to add a unit alias to the "
                   + "project declaration to tell WRES what '" + unit
                   + "' should be interpreted as. For example, if it were cubic"
                   + " meters per second: <unitAlias><alias>" + unit + "</alias>"
                   + "<unit>m^3/s</unit></unitAlias>",
                   cause );
        }
    }
}
