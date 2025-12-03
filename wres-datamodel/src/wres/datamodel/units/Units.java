package wres.datamodel.units;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serial;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.UnaryOperator;

import javax.measure.Dimension;
import javax.measure.Quantity;
import javax.measure.Unit;
import javax.measure.format.MeasurementParseException;
import javax.measure.format.UnitFormat;
import javax.measure.quantity.Length;
import javax.measure.quantity.Mass;
import javax.measure.quantity.Speed;
import javax.measure.quantity.Time;
import javax.measure.quantity.Volume;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import com.networknt.schema.serialization.JsonNodeReader;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import si.uom.quantity.MassFlowRate;
import si.uom.quantity.VolumetricFlowRate;
import systems.uom.ucum.format.UCUMFormat;
import systems.uom.ucum.internal.format.TokenException;
import systems.uom.ucum.internal.format.TokenMgrError;
import tech.units.indriya.quantity.Quantities;
import tech.units.indriya.quantity.time.TemporalQuantity;
import tech.units.indriya.unit.UnitDimension;

import wres.config.DeclarationFactory;
import wres.datamodel.MissingValues;
import wres.datamodel.scale.TimeScaleOuter;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

import static systems.uom.ucum.format.UCUMFormat.Variant.CASE_SENSITIVE;

/**
 * Build units using {@link javax.measure}.
 */
public class Units
{
    /** m3/s. */
    public static final String OFFICIAL_CUBIC_METERS_PER_SECOND = "m3/s";
    /** [ft_i]3/s */
    public static final String OFFICIAL_CUBIC_FEET_PER_SECOND = "[ft_i]3/s";
    /** Cel */
    public static final String OFFICIAL_DEGREES_CELSIUS = "Cel";
    /** [degF] */
    public static final String OFFICIAL_DEGREES_FAHRENHEIT = "[degF]";
    /** [in_i] */
    public static final String OFFICIAL_INCHES = "[in_i]";
    /** mm */
    public static final String OFFICIAL_MILLIMETERS = "mm";
    /** Count. */
    public static final String COUNT = "COUNT";

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Units.class );

    /** Unit format. */
    private static final UnitFormat UNIT_FORMAT = UCUMFormat.getInstance( CASE_SENSITIVE );

    /** Volume dimension. */
    private static final Dimension VOLUME = UnitDimension.of( Volume.class );

    /** Volumetric flow rate dimension. */
    private static final Dimension VOLUMETRIC_FLOW_RATE = VOLUME.divide( UnitDimension.TIME );

    /** Length dimension. */
    private static final Dimension DISTANCE = UnitDimension.of( Length.class );

    /** Length flow rate dimension, aka speed. */
    private static final Dimension SPEED = DISTANCE.divide( UnitDimension.TIME );

    /** Mass. */
    private static final Dimension MASS = UnitDimension.of( Mass.class );

    /** Mass flow rate dimension. */
    private static final Dimension MASS_FLOW_RATE = MASS.divide( UnitDimension.TIME );

    /** A small cache of formal UCUM units against official UCUM unit name strings to avoid repeated parsing from unit 
     * strings. */
    private static final Cache<@NonNull String, Unit<?>> UNIT_CACHE = Caffeine.newBuilder()
                                                                              .maximumSize( 10 )
                                                                              .build();
    /** The units file name on the classpath. */
    private static final String UNITS_FILE_NAME = "units.yml";

    /** The units schema file name on the classpath. */
    private static final String SCHEMA_FILE_NAME = "units_schema.yml";

    /**
     * <p>For backward compatibility and convenience, a map from weird unit names to official ones, "official ones"
     * being those supported by the indriya implementation. Here was the table as of WRES 5.14. We omit "NONE",
     * "-", "fraction" because they aren't units, and omit "%" because it's already supported. The units were
     * case-insensitive which is why you will see upper and lower cased versions of each previously supported unit.
     * It seemed too far to do mixed case for each and every unit. This is already too far anyway.
     *
     * <p>select * from wres.measurementunit;
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
    private static final Map<String, String> CONVENIENCE_ALIASES = new TreeMap<>();

    // Read the convenience aliases from a class path resource
    static
    {
        ObjectMapper mapper =
                new ObjectMapper( new YAMLFactory()
                                          .disable( YAMLGenerator.Feature.WRITE_DOC_START_MARKER )
                                          .disable( YAMLGenerator.Feature.SPLIT_LINES )
                                          .enable( YAMLGenerator.Feature.MINIMIZE_QUOTES )
                                          .enable( YAMLGenerator.Feature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS ) );

        // Get the units from the classpath
        URL unitsUrl = DeclarationFactory.class.getClassLoader()
                                               .getResource( UNITS_FILE_NAME );

        // Get the schema from the classpath
        URL schemaUrl = DeclarationFactory.class.getClassLoader()
                                                .getResource( SCHEMA_FILE_NAME );

        if ( Objects.nonNull( unitsUrl ) )
        {
            // Read the units
            try ( InputStream unitStream = unitsUrl.openStream() )
            {
                String unitsString = new String( unitStream.readAllBytes(), StandardCharsets.UTF_8 );
                JsonNode unitsNode = mapper.readTree( unitsString );

                // Validate against the schema
                if ( Objects.nonNull( schemaUrl ) )
                {
                    Units.validateUnitsAgainstSchema( schemaUrl, mapper, unitsNode );
                }
                else
                {
                    LOGGER.warn( "Encountered an error while attempting to validate the default measurement "
                                 + "units in '{}' against the schema in '{}'. The schema could not be found on the "
                                 + "class path as '{}'. If the units are read successfully, they will be available for "
                                 + "evaluation, but they have not been validated against the schema. It is recommended "
                                 + "to fix this error.",
                                 UNITS_FILE_NAME,
                                 SCHEMA_FILE_NAME,
                                 SCHEMA_FILE_NAME );
                }

                // Validate the UCUM units against UCUM
                Map<String, String> unitsMapped = mapper.readValue( unitsNode.traverse(),
                                                                    new TypeReference<>() {} );

                Map<String, String> unitsFailed = new TreeMap<>();

                LOGGER.debug( "Read the following default measurement units from {}: {}",
                              UNITS_FILE_NAME,
                              unitsMapped );

                for ( Map.Entry<String, String> unitPair : unitsMapped.entrySet() )
                {
                    String informal = unitPair.getKey();
                    String formal = unitPair.getValue();
                    Units.addUnit( informal, formal, unitsFailed );
                }

                LOGGER.debug( "Read and validated the following pairs of informal and formal (UCUM) units: {}.",
                              CONVENIENCE_ALIASES );

                if ( !unitsFailed.isEmpty() )
                {
                    LOGGER.warn( "While reading the default measurement units from '{}', encountered {} measurement "
                                 + "units that do not appear within the Unified Code for Units of Measure (UCUM). "
                                 + "These units are (informal unit, invalid UCUM unit): '{}'. The informal units will "
                                 + "be ignored and will not be available as default units at evaluation time. It is "
                                 + "recommended to fix this error by finding a valid UCUM unit for each informal unit "
                                 + "using the UCUM standard as a guide: https://ucum.org/ucum",
                                 UNITS_FILE_NAME,
                                 unitsFailed.size(),
                                 unitsFailed );
                }
            }
            catch ( IOException e )
            {
                LOGGER.warn( "Encountered an error while attempting to read the default measurement units from '{}'. "
                             + "The default units will not be available for evaluation.", UNITS_FILE_NAME );
            }
        }
        else
        {
            LOGGER.warn( "Encountered an error while attempting to read the default measurement units in '{}'. The "
                         + "units could not be found on the class path as '{}'. The default units will not be "
                         + "available for evaluation.",
                         UNITS_FILE_NAME,
                         UNITS_FILE_NAME );
        }
    }

    /**
     * Given a unit name, return the official UCUM unit name.
     *
     * @param unitName the unit name
     * @param overrideAliases a map of override aliases, possibly empty
     * @return the official unit name
     */

    public static String getOfficialUnitName( String unitName, Map<String, String> overrideAliases )
    {
        Objects.requireNonNull( unitName );
        Objects.requireNonNull( overrideAliases );

        String officialName = CONVENIENCE_ALIASES.getOrDefault( unitName, unitName );

        return overrideAliases.getOrDefault( unitName, officialName );
    }

    /**
     * <p>Given a unit name, return the formal {@link javax.measure} Unit of Measure.
     *
     * <p>Look in overrides and use the value found (when found).
     * If not found in overrides, look in convenient defaults.
     * If not found in convenient defaults, parse straightaway.
     *
     * @param unitName The name String
     * @param overrideAliases Unit name aliases to official unit names.
     * @return the {@link javax.measure} if known, null otherwise.
     * @throws UnrecognizedUnitException when unable to find the unit.
     */

    public static Unit<?> getUnit( String unitName,
                                   Map<String, String> overrideAliases )
    {
        Objects.requireNonNull( unitName );
        Objects.requireNonNull( overrideAliases );

        String officialName = Units.getOfficialUnitName( unitName, overrideAliases );

        // Look in the cache
        Unit<?> unit = UNIT_CACHE.getIfPresent( officialName );

        // Cached, so return it
        if ( Objects.nonNull( unit ) )
        {
            return unit;
        }

        // In reality, this may be repeated across several threads before the cache is seen, but this does not affect
        // accuracy. Could lock to avoid repetition of object creation and logging across multiple pooling threads, but 
        // that adds complexity. The cache itself is thread-safe.
        try
        {
            LOGGER.debug( "getUnit parsing a unit {}, given {}", officialName, unitName );
            unit = UNIT_FORMAT.parse( officialName );

            // Cache
            UNIT_CACHE.put( officialName, unit );
        }
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
            LOGGER.debug( "Treating measurement unit name '{}' as UCUM "
                          + "unit '{}' along dimension '{}'. "
                          + "This message may be repeated (up to) as many times as pooling threads.",
                          unitName,
                          officialName,
                          unit.getDimension() );
        }

        return unit;
    }

    /**
     * <p>Given a unit name, return the formal {@link javax.measure} Unit of Measure.
     *
     * <p>Look in convenient defaults.
     * If not found in convenient defaults, parse straightaway.
     *
     * @param unitName The name String
     * @return the {@link javax.measure} unit if known, null otherwise.
     * @throws UnrecognizedUnitException when unable to find the unit.
     */

    public static Unit<?> getUnit( String unitName )
    {
        return Units.getUnit( unitName, Collections.emptyMap() );
    }

    /**
     * Creates a converter that integrates the existing unit over time to form the desired unit.
     *
     * @see #isSupportedTimeIntegralConversion(Unit, Unit)
     * @param timeScale the timescale
     * @param existingUnit the existing measurement unit
     * @param desiredUnit the desired measurement unit
     * @return a converter from volumetric flow rate to volume
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the timescale does not represent an average over the scale period
     * @throws UnsupportedOperationException if the conversion is not a supported time integration
     */

    public static UnaryOperator<Double> getTimeIntegralConverter( TimeScaleOuter timeScale,
                                                                  Unit<?> existingUnit,
                                                                  Unit<?> desiredUnit )
    {
        Objects.requireNonNull( timeScale );
        Objects.requireNonNull( existingUnit );
        Objects.requireNonNull( desiredUnit );

        // Volumetric flow to volume
        if ( Units.isConvertingFromVolumetricFlowToVolume( existingUnit, desiredUnit ) )
        {
            // These casts are awkward but safe because they are guarded by the check above
            @SuppressWarnings( "unchecked" )
            Unit<VolumetricFlowRate> existingFlowUnit = ( Unit<VolumetricFlowRate> ) existingUnit;
            @SuppressWarnings( "unchecked" )
            Unit<Volume> desiredVolumeUnit = ( Unit<Volume> ) desiredUnit;

            return Units.getTimeIntegralConverterInner( timeScale, existingFlowUnit, desiredVolumeUnit );
        }
        // Speed to distance
        else if ( Units.isConvertingFromSpeedToDistance( existingUnit, desiredUnit ) )
        {
            // These casts are awkward but safe because they are guarded by the check above
            @SuppressWarnings( "unchecked" )
            Unit<Speed> existingFlowUnit = ( Unit<Speed> ) existingUnit;
            @SuppressWarnings( "unchecked" )
            Unit<Length> desiredVolumeUnit = ( Unit<Length> ) desiredUnit;

            return Units.getTimeIntegralConverterInner( timeScale, existingFlowUnit, desiredVolumeUnit );
        }
        // Mass flow to mass
        else if ( Units.isConvertingFromMassFlowToMass( existingUnit, desiredUnit ) )
        {
            // These casts are awkward but safe because they are guarded by the check above
            @SuppressWarnings( "unchecked" )
            Unit<MassFlowRate> existingFlowUnit = ( Unit<MassFlowRate> ) existingUnit;
            @SuppressWarnings( "unchecked" )
            Unit<Mass> desiredVolumeUnit = ( Unit<Mass> ) desiredUnit;

            return Units.getTimeIntegralConverterInner( timeScale, existingFlowUnit, desiredVolumeUnit );
        }

        throw new UnsupportedOperationException( "Cannot perform a time integration of "
                                                 + existingUnit
                                                 + " to form "
                                                 + desiredUnit
                                                 + "." );
    }

    /**
     * @param existingUnit the existing measurement unit.
     * @param desiredUnit the desired measurement unit
     * @return whether the existing unit can be time integrated to form the desired unit
     * @throws NullPointerException if either input is null
     */

    public static boolean isSupportedTimeIntegralConversion( Unit<?> existingUnit,
                                                             Unit<?> desiredUnit )
    {
        return Units.isConvertingFromVolumetricFlowToVolume( existingUnit, desiredUnit )
               || Units.isConvertingFromSpeedToDistance( existingUnit, desiredUnit )
               || Units.isConvertingFromMassFlowToMass( existingUnit, desiredUnit );
    }

    /**
     * Get the full set of known convenience units. Supports testing.
     * @return The set of convenience indriya unit strings.
     */
    static Set<String> getAllConvenienceUnits()
    {
        return Set.copyOf( CONVENIENCE_ALIASES.values() );
    }

    /**
     * Get the full set of known convenience alias names.
     * @return The set of convenient alias names.
     */

    private static Set<String> getAllConvenienceAliases()
    {
        return Collections.unmodifiableSet( CONVENIENCE_ALIASES.keySet() );
    }

    /**
     * @param existingUnit the existing measurement unit.
     * @param desiredUnit the desired measurement unit
     * @return whether the existing unit is a volumetric flow and the desired unit is a volume
     * @throws NullPointerException if either input is null
     */

    private static boolean isConvertingFromVolumetricFlowToVolume( Unit<?> existingUnit,
                                                                   Unit<?> desiredUnit )
    {
        Objects.requireNonNull( existingUnit );
        Objects.requireNonNull( desiredUnit );

        Dimension existingDimension = existingUnit.getDimension();
        Dimension desiredDimension = desiredUnit.getDimension();

        return VOLUMETRIC_FLOW_RATE.equals( existingDimension ) && VOLUME.equals( desiredDimension );
    }

    /**
     * @param existingUnit the existing measurement unit.
     * @param desiredUnit the desired measurement unit
     * @return whether the existing unit is a speed and the desired unit is a distance
     * @throws NullPointerException if either input is null
     */

    private static boolean isConvertingFromSpeedToDistance( Unit<?> existingUnit,
                                                            Unit<?> desiredUnit )
    {
        Objects.requireNonNull( existingUnit );
        Objects.requireNonNull( desiredUnit );

        Dimension existingDimension = existingUnit.getDimension();
        Dimension desiredDimension = desiredUnit.getDimension();

        return SPEED.equals( existingDimension ) && DISTANCE.equals( desiredDimension );
    }

    /**
     * @param existingUnit the existing measurement unit.
     * @param desiredUnit the desired measurement unit
     * @return whether the existing unit is a mass flow rate and the desired unit is a mass
     * @throws NullPointerException if either input is null
     */

    private static boolean isConvertingFromMassFlowToMass( Unit<?> existingUnit,
                                                           Unit<?> desiredUnit )
    {
        Objects.requireNonNull( existingUnit );
        Objects.requireNonNull( desiredUnit );

        Dimension existingDimension = existingUnit.getDimension();
        Dimension desiredDimension = desiredUnit.getDimension();

        return MASS_FLOW_RATE.equals( existingDimension ) && MASS.equals( desiredDimension );
    }

    /**
     * Creates a converter that performs a time integration of the input. The time scale must represent a mean average
     * over the scale period, i.e., a {@link TimeScaleFunction#MEAN} and cannot be an instantaneous time scale.
     *
     * @param timeScale the time scale
     * @param existingUnit the existing measurement unit
     * @param desiredUnit the desired measurement unit
     * @return a converter that performs a time integration
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the time scale does not represent an average over the scale period
     */

    private static <S extends Quantity<S>, T extends Quantity<T>> UnaryOperator<Double>
    getTimeIntegralConverterInner( TimeScaleOuter timeScale,
                                   Unit<S> existingUnit,
                                   Unit<T> desiredUnit )
    {
        Objects.requireNonNull( timeScale );
        Objects.requireNonNull( existingUnit );
        Objects.requireNonNull( desiredUnit );

        if ( timeScale.isInstantaneous() || timeScale.getFunction() != TimeScaleFunction.MEAN )
        {
            throw new IllegalArgumentException( "Cannot create a unit converter from " + existingUnit
                                                + " flow to "
                                                + desiredUnit
                                                + " because the desired time scale of "
                                                + timeScale
                                                + " does not represent a mean average over the scale period." );
        }

        Duration scalePeriod = timeScale.getPeriod();

        // Scale period in millisecond precision
        Quantity<Time> scalePeriodMillis = TemporalQuantity.of( scalePeriod.toMillis(), ChronoUnit.MILLIS );

        // Create a conversion function
        return flowInFlowUnits -> {

            // Missing?
            if ( MissingValues.isMissingValue( flowInFlowUnits ) )
            {
                return MissingValues.DOUBLE;
            }

            Quantity<S> someFlowQuantity = Quantities.getQuantity( flowInFlowUnits, existingUnit );
            @SuppressWarnings( "unchecked" )
            Quantity<T> someVolume = ( Quantity<T> ) someFlowQuantity.multiply( scalePeriodMillis );
            Quantity<T> someVolumeInDesiredUnits = someVolume.to( desiredUnit );

            return someVolumeInDesiredUnits.getValue()
                                           .doubleValue();
        };
    }

    /**
     * Exception for unrecognized units.
     */

    public static final class UnrecognizedUnitException extends RuntimeException
    {
        @Serial
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
                   + "' in a unit_aliases declaration was not recognized (look "
                   + "for an earlier WARN message). You may need to add a unit "
                   + "alias to the project declaration (or replace an existing "
                   + "one) to tell WRES which UCUM unit '"
                   + unitNameGiven
                   + "' represents. For example, if '"
                   + unitNameGiven
                   + "' represents cubic meters per second then use this: "
                   + "unit_aliases: [{alias: "
                   + unitNameGiven
                   + ",unit: m3/s}]"
                   + ". WRES expects UCUM case-sensitive"
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

    /**
     * Adds an informal to formal (UCUM) unit pairing to the map of convenience aliases.
     * @param informal the informal unit
     * @param formal the formal unit
     * @param unitsFailed a rolling map of units that failed to validate
     */
    private static void addUnit( String informal, String formal, Map<String, String> unitsFailed )
    {
        try
        {
            Units.getUnit( formal );
            CONVENIENCE_ALIASES.put( informal, formal );
            LOGGER.debug( "Discovered a valid UCUM unit, '{}', for informal unit '{}'.", formal, informal );
        }
        catch ( Units.UnrecognizedUnitException e )
        {
            LOGGER.debug( "While reading the default measurement units from '{}', encountered a measurement "
                          + "unit that does not appear within the Unified Code for Units of Measure (UCUM): "
                          + "'{}'. The corresponding informal unit, '{}', will be ignored and will not be "
                          + "available as a default unit. It is recommended to fix this error by finding "
                          + "the valid UCUM unit corresponding to the informal unit, '{}', using the UCUM "
                          + "standard as a guide: https://ucum.org/ucum",
                          UNITS_FILE_NAME,
                          formal,
                          informal,
                          informal );
            unitsFailed.put( informal, formal );
        }
    }

    /**
     * Validates the units against the schema.
     *
     * @param schemaUrl the schema URL
     * @param mapper the object mapper
     * @param unitsNode the units node to validate against the schema
     */

    private static void validateUnitsAgainstSchema( URL schemaUrl, ObjectMapper mapper, JsonNode unitsNode )
    {
        try ( InputStream schemaStream = schemaUrl.openStream() )
        {
            String schemaString = new String( schemaStream.readAllBytes(), StandardCharsets.UTF_8 );
            JsonNode schemaNode = mapper.readTree( schemaString );
            JsonNodeReader nodeReader = JsonNodeReader.builder()
                                                      .yamlMapper( mapper )
                                                      .build();
            JsonSchemaFactory factory =
                    JsonSchemaFactory.builder( JsonSchemaFactory.getInstance( SpecVersion.VersionFlag.V201909 ) )
                                     .jsonNodeReader( nodeReader )
                                     .build();
            JsonSchema schema = factory.getSchema( schemaNode );

            Set<ValidationMessage> errors = schema.validate( unitsNode );

            if ( !errors.isEmpty() )
            {
                LOGGER.warn( "Encountered an error while attempting to validate the default measurement "
                             + "units in '{}' against the schema in '{}'. The default units are not valid "
                             + "against the schema. It is recommended to fix these errors. The errors "
                             + "are: {}.",
                             UNITS_FILE_NAME,
                             SCHEMA_FILE_NAME,
                             errors );
            }
        }
        catch ( IOException e )
        {
            LOGGER.warn( "Encountered an error while attempting to validate the default measurement "
                         + "units in '{}' against the schema in '{}'. The default units are not valid "
                         + "against the schema. The default units will be available for evaluation, but "
                         + "the units have not been validated against the schema.",
                         UNITS_FILE_NAME,
                         SCHEMA_FILE_NAME );
        }
    }

    /**
     * Do not construct.
     */
    private Units()
    {
    }
}
