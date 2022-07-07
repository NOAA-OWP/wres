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
import javax.measure.format.UnitFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import systems.uom.ucum.format.UCUMFormat;
import systems.uom.ucum.internal.format.TokenException;
import systems.uom.ucum.internal.format.TokenMgrError;


import static systems.uom.ucum.format.UCUMFormat.Variant.CASE_SENSITIVE;

/**
 * Build units using javax.measure. See also wres.io.retrieval.UnitMapper class.
 */

public class Units
{
    public static final String OFFICIAL_CUBIC_METERS_PER_SECOND = "m3/s";
    public static final String OFFICIAL_CUBIC_FEET_PER_SECOND = "[ft_i]3/s";
    public static final String OFFICIAL_KILO_CUBIC_FEET_PER_SECOND = "1000.[ft_i]3/s";
    public static final String OFFICIAL_DEGREES_CELSIUS = "Cel";
    public static final String OFFICIAL_DEGREES_FAHRENHEIT = "[degF]";
    public static final String OFFICIAL_INCHES = "[in_i]";
    public static final String OFFICIAL_INCHES_PER_HOUR = OFFICIAL_INCHES + "/h";
    public static final String OFFICIAL_MILLIMETERS = "mm";
    private static final Logger LOGGER = LoggerFactory.getLogger( Units.class );
    private static final UnitFormat UNIT_FORMAT = UCUMFormat.getInstance( CASE_SENSITIVE );

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

    private static final Map<String, String> CONVENIENCE_ALIASES = new HashMap<>( 116 );

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
        CONVENIENCE_ALIASES.put( "meter^3 / sec", OFFICIAL_CUBIC_METERS_PER_SECOND );
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
        CONVENIENCE_ALIASES.put( "FT", "[ft_i]" );
        CONVENIENCE_ALIASES.put( "ft", "[ft_i]" );
        CONVENIENCE_ALIASES.put( "IN", OFFICIAL_INCHES );
        CONVENIENCE_ALIASES.put( "in", OFFICIAL_INCHES );
        CONVENIENCE_ALIASES.put( "M", "m" );
        CONVENIENCE_ALIASES.put( "MS", "ms" );
        CONVENIENCE_ALIASES.put( "HR", "h" );
        CONVENIENCE_ALIASES.put( "hr", "h" );
        CONVENIENCE_ALIASES.put( "S", "s" );
        CONVENIENCE_ALIASES.put( "MM", OFFICIAL_MILLIMETERS );
        CONVENIENCE_ALIASES.put( "CM", "cm" );
        CONVENIENCE_ALIASES.put( "kg m{-2}", "kg/m2" );
        CONVENIENCE_ALIASES.put( "KG M{-2}", "kg/m2" );
        CONVENIENCE_ALIASES.put( "ft/sec", "[ft_i]/s" );
        CONVENIENCE_ALIASES.put( "FT/SEC", "[ft_i]/s" );
        CONVENIENCE_ALIASES.put( "gal/min", "[gal_us]/min" );
        CONVENIENCE_ALIASES.put( "GAL/MIN", "[gal_us]/min" );
        CONVENIENCE_ALIASES.put( "mgd", "1000000.[gal_us]/d" );
        CONVENIENCE_ALIASES.put( "MGD", "1000000.[gal_us]/d" );
        CONVENIENCE_ALIASES.put( "m/sec", "m/s" );
        CONVENIENCE_ALIASES.put( "M/SEC", "m/s" );
        CONVENIENCE_ALIASES.put( "ft3/day", "[ft_i]3/d" );
        CONVENIENCE_ALIASES.put( "FT3/DAY", "[ft_i]3/d" );
        CONVENIENCE_ALIASES.put( "ac-ft", "[acr_us].[ft_i]" );
        CONVENIENCE_ALIASES.put( "AC-FT", "[acr_us].[ft_i]" );
        CONVENIENCE_ALIASES.put( "MPH", "[mi_i]/h" );
        CONVENIENCE_ALIASES.put( "mph", "[mi_i]/h" );
        CONVENIENCE_ALIASES.put( "l/sec", "l/s" );
        CONVENIENCE_ALIASES.put( "L/SEC", "l/s" );
        CONVENIENCE_ALIASES.put( "MM/S", "mm/s" );
        CONVENIENCE_ALIASES.put( "mm s^-1", "mm/s" );
        CONVENIENCE_ALIASES.put( "MM S^-1", "mm/s" );
        CONVENIENCE_ALIASES.put( "mm s{-1}", "mm/s" );
        CONVENIENCE_ALIASES.put( "MM S{-1}", "mm/s" );
        CONVENIENCE_ALIASES.put( "mm s-1", "mm/s" );
        CONVENIENCE_ALIASES.put( "MM S-1", "mm/s" );
        CONVENIENCE_ALIASES.put( "mm h^-1", "mm/h" );
        CONVENIENCE_ALIASES.put( "MM H^-1", "mm/h" );
        CONVENIENCE_ALIASES.put( "MM/H", "mm/h" );
        CONVENIENCE_ALIASES.put( "mm h-1", "mm/h" );
        CONVENIENCE_ALIASES.put( "MM H-1", "mm/h" );
        CONVENIENCE_ALIASES.put( "mm h{-1}", "mm/h" );
        CONVENIENCE_ALIASES.put( "MM H{-1}", "mm/h" );
        CONVENIENCE_ALIASES.put( "KG/M^2", "kg/m2" );
        CONVENIENCE_ALIASES.put( "kg/m^2h", "kg/m2.h" );
        CONVENIENCE_ALIASES.put( "KG/M^2H", "kg/m2.h" );
        CONVENIENCE_ALIASES.put( "kg/m^2s", "kg/m2.s" );
        CONVENIENCE_ALIASES.put( "KG/M^2S", "kg/m2.s" );
        CONVENIENCE_ALIASES.put( "kg/m^2/s", "kg/m2.s" );
        CONVENIENCE_ALIASES.put( "KG/M^2/S", "kg/m2.s" );
        CONVENIENCE_ALIASES.put( "kg/m^2/h", "kg/m2.h" );
        CONVENIENCE_ALIASES.put( "KG/M^2/H", "kg/m2.h" );
        CONVENIENCE_ALIASES.put( "PA", "Pa" );
        CONVENIENCE_ALIASES.put( "pa", "Pa" );
        CONVENIENCE_ALIASES.put( "w/m^2", "W/m2" );
        CONVENIENCE_ALIASES.put( "W/M^2", "W/m2" );
        CONVENIENCE_ALIASES.put( "W m{-2}", "W/m2" );
        CONVENIENCE_ALIASES.put( "w m{-2}", "W/m2" );
        CONVENIENCE_ALIASES.put( "W M{-2}", "W/m2" );
        CONVENIENCE_ALIASES.put( "W m-2", "W/m2" );
        CONVENIENCE_ALIASES.put( "W M-2", "W/m2" );
        CONVENIENCE_ALIASES.put( "w m-2", "W/m2" );
        CONVENIENCE_ALIASES.put( "m s-1", "m/s" );
        CONVENIENCE_ALIASES.put( "M S-1", "m/s" );
        CONVENIENCE_ALIASES.put( "M/S", "m/s" );
        CONVENIENCE_ALIASES.put( "M S{-1}", "m/s" );
        CONVENIENCE_ALIASES.put( "m s{-1}", "m/s" );
        CONVENIENCE_ALIASES.put( "kg kg-1", "kg/kg" );
        CONVENIENCE_ALIASES.put( "KG KG-1", "kg/kg" );
        CONVENIENCE_ALIASES.put( "kg kg{-1}", "kg/kg" );
        CONVENIENCE_ALIASES.put( "KG KG{-1}", "kg/kg" );
        CONVENIENCE_ALIASES.put( "kg m-2", "kg/m2" );
        CONVENIENCE_ALIASES.put( "KG M-2", "kg/m2" );
        CONVENIENCE_ALIASES.put( "k", "K" );
        CONVENIENCE_ALIASES.put( "IN/HR", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "in/hr", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "in hr^-1", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "IN HR^-1", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "in hr-1", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "IN HR-1", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "in hr{-1}", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "IN HR{-1}", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "IN/H", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "in/h", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "in h-1", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "IN H-1", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "in h{-1}", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "IN H{-1}", OFFICIAL_INCHES_PER_HOUR );
        CONVENIENCE_ALIASES.put( "M3", "m3" );
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
     * Given a unit name, return the official UCUM unit name.
     * 
     * @param unitName the unit name
     * @param overrideAliases a map of override aliases, possibly empty
     */

    public static String getOfficialUnitName( String unitName, Map<String, String> overrideAliases )
    {
        Objects.requireNonNull( unitName );
        Objects.requireNonNull( overrideAliases );

        String officialName = CONVENIENCE_ALIASES.getOrDefault( unitName, unitName );

        return overrideAliases.getOrDefault( unitName, officialName );
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
     * @throws UnrecognizedUnitException when unable to find the unit.
     */

    public static Unit<?> getUnit( String unitName,
                                   Map<String, String> overrideAliases )
    {
        Objects.requireNonNull( unitName );
        Objects.requireNonNull( overrideAliases );

        String officialName = Units.getOfficialUnitName( unitName, overrideAliases );

        Unit<?> unit;

        try
        {
            LOGGER.debug( "getUnit parsing a unit {}, given {}", officialName, unitName );
            unit = UNIT_FORMAT.parse( officialName );
        }
        // TODO: remove TokenMgrError and TokenException when the libraries
        //  change to not throw these internal exceptions.
        catch ( MeasurementParseException | TokenMgrError | TokenException e )
        {
            SortedSet<String> aliases = new TreeSet<>();
            aliases.addAll( Units.getAllConvenienceAliases() );
            aliases.addAll( overrideAliases.keySet() );
            SortedSet<String> actualUnits = new TreeSet<>( Units.getAllConvenienceUnits() );
            // Omit the overrrideAliases values because they may not parse.
            throw new UnrecognizedUnitException( unitName,
                                                 officialName,
                                                 aliases,
                                                 actualUnits,
                                                 e );
        }

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "Treating measurement unit name '{}' as UCUM "
                         + "unit '{}' along dimension '{}'",
                         unitName,
                         officialName,
                         unit.getDimension() );
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
     * @throws UnrecognizedUnitException when unable to find the unit.
     */

    public static Unit<?> getUnit( String unitName )
    {
        return Units.getUnit( unitName, Collections.emptyMap() );
    }


    public static final class UnrecognizedUnitException extends RuntimeException
    {
        private static final long serialVersionUID = -6873574285493867322L;

        UnrecognizedUnitException( String unitNameGiven,
                                   String failedToParseUnitName,
                                   Set<String> unitAliases,
                                   Set<String> actualUnits,
                                   Throwable cause )
        {
            super( "Unable to find the measurement unit '" + unitNameGiven
                   + "' among the default unit aliases and/or '"
                   + failedToParseUnitName
                   + "' was not recognized as a UCUM "
                   + "unit and/or the UCUM unit declared for '"
                   + unitNameGiven
                   + "' in a unitAlias declaration was not recognized (look "
                   + "for an earlier WARN message). You may need to add a unit "
                   + "alias to the project declaration (or replace an existing "
                   + "one) to tell WRES which UCUM unit '"
                   + unitNameGiven
                   + "'"
                   + " represents. For example, if '"
                   + unitNameGiven
                   + "' "
                   + "represents cubic meters per second then use this: "
                   + "<unitAlias><alias>"
                   + unitNameGiven
                   + "</alias><unit>"
                   + "m3/s</unit></unitAlias>. WRES expects UCUM case-sensitive"
                   + " unit format. To learn the UCUM format for your "
                   + "particular unit(s), try the demo at "
                   + "https://ucum.nlm.nih.gov/ucum-lhc/demo.html and/or review"
                   + " the UCUM documentation at https://ucum.org/ucum.html"
                   + " and/or look at the following UCUM string examples that "
                   + "can be successfully recognized by WRES: "
                   + actualUnits
                   + ". Here are the default unit aliases in this version of "
                   + "WRES that have educated guesses for UCUM units already "
                   + "assigned for convenience (the following are not UCUM "
                   + "units, but these will be automatically mapped to one of "
                   + "the example UCUM units listed above unless overridden by "
                   + "an explicit unit alias declaration): "
                   + unitAliases,
                   cause );
        }
    }
}
