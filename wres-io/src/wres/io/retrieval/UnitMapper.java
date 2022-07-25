package wres.io.retrieval;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.DoubleUnaryOperator;
import javax.measure.IncommensurableException;
import javax.measure.UnconvertibleException;
import javax.measure.Unit;
import javax.measure.UnitConverter;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.ProjectConfig;
import wres.config.generated.UnitAlias;
import wres.datamodel.Units;
import wres.datamodel.Units.UnrecognizedUnitException;
import wres.io.data.caching.MeasurementUnits;

/**
 * Replaced UnitMapper, uses javax.measure/indriya instead of db unitconversion.
 */
public class UnitMapper
{
    private static final Logger LOGGER = LoggerFactory.getLogger( UnitMapper.class );
    private final MeasurementUnits measurementUnitsCache;
    private final String desiredMeasurementUnitName;
    private final ConcurrentMap<String, Unit<?>> indriyaUnits;
    private final Object lock = new Object(); // To avoid calculating the indriya unit N times across N threads
    private final Map<String, String> aliases;
    private final Map<Pair<String, String>, DoubleUnaryOperator> internalMappers;

    private UnitMapper( MeasurementUnits measurementUnitsCache,
                        String desiredMeasurementUnitName,
                        List<UnitAlias> aliases )
    {
        Objects.requireNonNull( measurementUnitsCache );
        Objects.requireNonNull( desiredMeasurementUnitName );
        // Version 2 doesn't need direct db access, but does need units cache.
        this.measurementUnitsCache = measurementUnitsCache;

        if ( desiredMeasurementUnitName.isBlank() )
        {
            throw new NoSuchUnitConversionException( "Unit must not be blank." );
        }

        // Keep the original name supplied for the sake of retaining the public
        // method getDesiredMeasurementUnitName in original UnitMapper api.
        this.desiredMeasurementUnitName = desiredMeasurementUnitName;
        this.indriyaUnits = new ConcurrentHashMap<>( 4 );

        Map<String, String> aliasToUnitStrings = new HashMap<>( aliases.size() );

        for ( UnitAlias alias : aliases )
        {
            String existing = aliasToUnitStrings.put( alias.getAlias(),
                                                      alias.getUnit() );

            if ( existing != null )
            {
                throw new ProjectConfigException( alias,
                                                  "Multiple declarations for a "
                                                         + "single unit alias are not "
                                                         + "supported. Found repeated "
                                                         + "'"
                                                         + alias.getAlias()
                                                         + "' alias. Remove all but "
                                                         + "one declaration for alias "
                                                         + "'"
                                                         + alias.getAlias()
                                                         + "'." );
            }
        }

        // Immediately attempt to get a javax.measure.Unit. But only warn.
        try
        {
            Unit<?> desiredUnit = Units.getUnit( desiredMeasurementUnitName,
                                                 aliasToUnitStrings );
            this.indriyaUnits.put( desiredMeasurementUnitName, desiredUnit );
        }
        catch ( UnrecognizedUnitException uue )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                // Here we have either an alias name or a UCUM unit name where
                // the UCUM value for the alias has failed to be recognized or
                // the unit name itself has failed to be recognized. So it would
                // be awkward to say either "unit alias" or "UCUM unit" here.
                LOGGER.warn( "Unrecognized unit '{}' may cause unit "
                             + "conversion failure if this evaluation requires"
                             + " a unit conversion.",
                             desiredMeasurementUnitName );
            }
        }

        // Immediately attempt to parse the given javax.measure.Unit, only warn.
        for ( UnitAlias unitAlias : aliases )
        {
            String aliasName = unitAlias.getAlias();
            String unitName = unitAlias.getUnit();

            // In the case of the desiredMeasurementUnitName, it's already been
            // added above, so skip here to avoid duplication.
            if ( !aliasName.equals( desiredMeasurementUnitName ) )
            {
                try
                {
                    Unit<?> indriyaUnit = Units.getUnit( aliasName,
                                                         aliasToUnitStrings );
                    this.indriyaUnits.put( aliasName, indriyaUnit );
                }
                catch ( UnrecognizedUnitException uue )
                {
                    // Here we definitely have an alias name and UCUM unit at
                    // hand because we are iterating over declared unit aliases.
                    LOGGER.warn( "Unit alias declaration with unrecognized UCUM"
                                 + " unit '{}' (having alias '{}') may cause "
                                 + "unit conversion failure if this evaluation "
                                 + "requires a unit conversion.",
                                 unitName,
                                 aliasName );
                }
            }
        }

        this.aliases = aliasToUnitStrings;
        this.internalMappers = this.getInternalMappers();
    }

    public static UnitMapper of( MeasurementUnits measurementUnitsCache,
                                 String desiredMeasurementUnit )
    {
        return new UnitMapper( measurementUnitsCache,
                               desiredMeasurementUnit,
                               Collections.emptyList() );
    }

    public static UnitMapper of( MeasurementUnits measurementUnitsCache,
                                 String desiredMeasurementUnit,
                                 ProjectConfig projectConfig )
    {
        List<UnitAlias> aliases = projectConfig.getPair()
                                               .getUnitAlias();
        if ( aliases == null )
        {
            aliases = Collections.emptyList();
        }

        return new UnitMapper( measurementUnitsCache,
                               desiredMeasurementUnit,
                               aliases );
    }

    public String getDesiredMeasurementUnitName()
    {
        return this.desiredMeasurementUnitName;
    }

    /**
     * Returns a unit mapper to this UnitMapper's unit from the given unit name.
     * @param unitName The name of an existing measurement unit.
     * @return A unit mapper for the prescribed existing units to this unit.
     * @throws NoSuchUnitConversionException When unable to create a converter.
     * @throws Units.UnrecognizedUnitException When unable to support given unitName.
     */

    public DoubleUnaryOperator getUnitMapper( String unitName )
    {
        // Identity
        if ( unitName.equals( this.getDesiredMeasurementUnitName() ) )
        {
            return identity -> identity;
        }

        // Return one of the in-built converters or default to an Indriya converter. As of Indriya 2.1.3, some 
        // converters are not performant, so in-built converters are used instead. See #105929.

        String desiredUnits = Units.getOfficialUnitName( this.getDesiredMeasurementUnitName(), this.aliases );
        String currentUnits = Units.getOfficialUnitName( unitName, this.aliases );

        Pair<String, String> key = Pair.of( currentUnits, desiredUnits );

        if ( LOGGER.isTraceEnabled() && this.internalMappers.containsKey( key ) )
        {
            LOGGER.trace( "Discovered an internal unit mapper to convert between {} and {}. Using this preferentially.",
                          currentUnits,
                          desiredUnits );
        }

        return this.internalMappers.getOrDefault( key, this.getUnitMapperInner( unitName ) );
    }

    /**
     * Returns a unit mapper to this UnitMapper's unit from the given unit name.
     * @param unitName The name of an existing measurement unit.
     * @return A unit mapper for the prescribed existing units to this unit.
     * @throws NoSuchUnitConversionException When unable to create a converter.
     * @throws Units.UnrecognizedUnitException When unable to support given unitName.
     */

    private DoubleUnaryOperator getUnitMapperInner( String unitName )
    {
        // Return one of the in-built converters or default to an Indriya converter. As of Indriya 2.1.3, some 
        // converters are not performant, so in-built converters are used instead. See #105929.

        // Avoid redundant unit parsing with local map of found units.
        Unit<?> desiredUnit = this.indriyaUnits.get( this.getDesiredMeasurementUnitName() );

        if ( desiredUnit == null )
        {
            synchronized ( this.lock )
            {
                if ( !this.indriyaUnits.containsKey( this.getDesiredMeasurementUnitName() ) )
                {
                    desiredUnit = Units.getUnit( this.getDesiredMeasurementUnitName() );
                    this.indriyaUnits.put( this.getDesiredMeasurementUnitName(), desiredUnit );
                }
                else
                {
                    desiredUnit = this.indriyaUnits.get( this.getDesiredMeasurementUnitName() );
                }
            }
        }

        Unit<?> existingUnit = this.indriyaUnits.get( unitName );

        if ( existingUnit == null )
        {
            synchronized ( this.lock )
            {
                if ( !this.indriyaUnits.containsKey( unitName ) )
                {
                    existingUnit = Units.getUnit( unitName );
                    this.indriyaUnits.put( unitName, existingUnit );
                }
                else
                {
                    existingUnit = this.indriyaUnits.get( unitName );
                }
            }
        }

        UnitConverter converter;

        try
        {
            converter = existingUnit.getConverterToAny( desiredUnit );
        }
        catch ( IncommensurableException | UnconvertibleException e )
        {
            throw new NoSuchUnitConversionException( "Could not create a conversion from "
                                                     + unitName
                                                     + " to "
                                                     + this.getDesiredMeasurementUnitName(),
                                                     e );
        }

        return UnitMapper.getNonFiniteFriendlyUnitMapper( converter::convert,
                                                          unitName,
                                                          this.getDesiredMeasurementUnitName() );
    }

    /**
     * Returns a unit mapper to this UnitMapper's unit from the given unit db id
     * @param measurementUnitId The surrogate key for the measurement unit.
     * @return A unit mapper for the prescribed existing units to this unit.
     * @throws NoSuchUnitConversionException When unable to create a converter.
     * @throws UnrecognizedUnitException When unable to support given unitName.
     */

    public DoubleUnaryOperator getUnitMapper( long measurementUnitId )
    {
        String unitName = this.measurementUnitsCache.getUnit( measurementUnitId );

        DoubleUnaryOperator unitMapper = this.getUnitMapper( unitName );
        return UnitMapper.getNonFiniteFriendlyUnitMapper( unitMapper,
                                                          unitName,
                                                          this.getDesiredMeasurementUnitName() );
    }

    /**
     * Returns a converter that applies a conversion to finite values and leaves non-finite values as-is.
     * @param converter the converter
     * @param unitName the unit name to help with logging
     * @param desiredUnitName the desired unit name to help with logging
     * @return a converter that accepts non-finite values
     */

    private static DoubleUnaryOperator getNonFiniteFriendlyUnitMapper( DoubleUnaryOperator converter,
                                                                       String unitName,
                                                                       String desiredUnitName )
    {
        return input -> {

            // If finite, convert, otherwise return the input
            if ( Double.isFinite( input ) )
            {
                return converter.applyAsDouble( input );
            }

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Discovered a non-finite value of {} in units of {}, which required conversion to units"
                              + "of {}. Skipping the conversion and returning the raw value instead.",
                              input,
                              unitName,
                              desiredUnitName );
            }

            return input;
        };
    }

    /**
     * Returns a map of internal unit converters to use before library converters for improved performance. See #105929.
     * 
     * @return internal converters
     */

    private Map<Pair<String, String>, DoubleUnaryOperator> getInternalMappers()
    {
        Map<Pair<String, String>, DoubleUnaryOperator> returnMe = new HashMap<>( 6 );

        returnMe.put( Pair.of( Units.OFFICIAL_DEGREES_FAHRENHEIT, Units.OFFICIAL_DEGREES_CELSIUS ),
                      f -> ( f - 32.0 ) / 1.8 );
        returnMe.put( Pair.of( Units.OFFICIAL_DEGREES_CELSIUS, Units.OFFICIAL_DEGREES_FAHRENHEIT ),
                      c -> c * 1.8 + 32.0 );
        returnMe.put( Pair.of( Units.OFFICIAL_CUBIC_FEET_PER_SECOND, Units.OFFICIAL_CUBIC_METERS_PER_SECOND ),
                      cfs -> cfs * 0.028316846592 );
        returnMe.put( Pair.of( Units.OFFICIAL_CUBIC_METERS_PER_SECOND, Units.OFFICIAL_CUBIC_FEET_PER_SECOND ),
                      cms -> cms / 0.028316846592 );
        returnMe.put( Pair.of( Units.OFFICIAL_INCHES, Units.OFFICIAL_MILLIMETERS ),
                      inch -> inch * 25.4 );
        returnMe.put( Pair.of( Units.OFFICIAL_MILLIMETERS, Units.OFFICIAL_INCHES ),
                      mm -> mm / 25.4 );

        return Collections.unmodifiableMap( returnMe );
    }

}
