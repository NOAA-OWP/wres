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
    private final ConcurrentMap<String,Unit<?>> indriyaUnits;

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

        Map<String,String> aliasToUnitStrings = new HashMap<>( aliases.size() );

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
                                                  + "'" + alias.getAlias()
                                                  + "' alias. Remove all but "
                                                  + "one declaration for alias "
                                                  + "'" + alias.getAlias()
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
                LOGGER.warn( "Unrecognized unit '"
                             + desiredMeasurementUnitName + "' may cause unit "
                             + "conversion failure if this evaluation requires"
                             + " a unit conversion." );
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
                                 + " unit '" + unitName + "' (having alias '"
                                 + aliasName + "') may cause unit "
                                 + "conversion failure if this evaluation "
                                 + "requires a unit conversion." );
                }
            }
        }
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

        // Avoid redundant unit parsing with local map of found units.
        Unit<?> desiredUnit = this.indriyaUnits.get( this.getDesiredMeasurementUnitName() );

        if ( desiredUnit == null )
        {
            desiredUnit = Units.getUnit( this.getDesiredMeasurementUnitName() );
            this.indriyaUnits.put( this.getDesiredMeasurementUnitName(), desiredUnit );
        }

        Unit<?> existingUnit = this.indriyaUnits.get( unitName );

        if ( existingUnit == null )
        {
            existingUnit = Units.getUnit( unitName );
            this.indriyaUnits.put( unitName, existingUnit );
        }

        UnitConverter converter;

        try
        {
            converter = existingUnit.getConverterToAny( desiredUnit );
        }
        catch( IncommensurableException | UnconvertibleException e )
        {
            throw new NoSuchUnitConversionException( "Could not create a conversion from "
                                                     + unitName + " to "
                                                     + this.getDesiredMeasurementUnitName(),
                                                     e );
        }

        return converter::convert;
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
        return this.getUnitMapper( unitName );
    }
}
