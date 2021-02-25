package wres.datamodel;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.measure.MetricPrefix;
import javax.measure.Quantity;
import javax.measure.Unit;
import javax.measure.UnitConverter;
import javax.measure.format.MeasurementParseException;
import javax.measure.quantity.Length;

import org.apache.commons.lang3.NotImplementedException;
import si.uom.quantity.VolumetricFlowRate;
import tech.units.indriya.AbstractUnit;
import tech.units.indriya.unit.ProductUnit;

import static systems.uom.common.USCustomary.FOOT;
import static systems.uom.common.USCustomary.GALLON_LIQUID;
import static systems.uom.common.USCustomary.CUBIC_FOOT;
import static systems.uom.common.USCustomary.INCH;
import static tech.units.indriya.unit.Units.CUBIC_METRE;
import static tech.units.indriya.unit.Units.DAY;
import static tech.units.indriya.unit.Units.LITRE;
import static tech.units.indriya.unit.Units.METRE;
import static tech.units.indriya.unit.Units.MINUTE;
import static tech.units.indriya.unit.Units.SECOND;

/**
 * Experimental class to build units using javax.measure. When it is used,
 * visibility will likely need to be public and the library declarations updated
 * in the build.gradle to be "api" instead of "implementation."
 */

public class Units
{
    static final Unit<VolumetricFlowRate> CUBIC_METRE_PER_SECOND =
            new ProductUnit<>( CUBIC_METRE.divide( SECOND ) );
    static final Unit<VolumetricFlowRate> CUBIC_FOOT_PER_SECOND =
            new ProductUnit<>( CUBIC_FOOT.divide( SECOND ) );
    static final Unit<VolumetricFlowRate> CUBIC_FOOT_PER_DAY =
            new ProductUnit<>( CUBIC_FOOT.divide( DAY ) );
    static final Unit<VolumetricFlowRate> KILO_CUBIC_FOOT_PER_SECOND =
            MetricPrefix.KILO( CUBIC_FOOT_PER_SECOND );
    static final Unit<VolumetricFlowRate> GALLON_LIQUID_PER_MINUTE =
            new ProductUnit<>( GALLON_LIQUID.divide( MINUTE ) );
    static final Unit<VolumetricFlowRate> LITRE_PER_SECOND =
            new ProductUnit<>( LITRE.divide( SECOND ) );

    /** Should there be multiple of these, one per supported dimension? */
    private static final Map<String,Unit<?>> KNOWN_UNITS = new HashMap<>( 18 );

    static
    {
        // Populate some known units. Is this even needed? Sadly, it seems so.
        // See wres.datamodel.UnitsTest for examples of attempts to parse.
        KNOWN_UNITS.put( "CFS", CUBIC_FOOT_PER_SECOND );
        KNOWN_UNITS.put( "CFSD", CUBIC_FOOT_PER_SECOND );
        KNOWN_UNITS.put( "ft3/s", CUBIC_FOOT_PER_SECOND );
        KNOWN_UNITS.put( "ft3/day", CUBIC_FOOT_PER_DAY );
        KNOWN_UNITS.put( "CMS", CUBIC_METRE_PER_SECOND );
        KNOWN_UNITS.put( "CMSD", CUBIC_METRE_PER_SECOND );
        KNOWN_UNITS.put( "m3 s-1", CUBIC_METRE_PER_SECOND );
        KNOWN_UNITS.put( "m3/s", CUBIC_METRE_PER_SECOND );
        KNOWN_UNITS.put( "m3/sec", CUBIC_METRE_PER_SECOND );
        KNOWN_UNITS.put( "gal/min", GALLON_LIQUID_PER_MINUTE );
        KNOWN_UNITS.put( "KCFS", KILO_CUBIC_FOOT_PER_SECOND );
        KNOWN_UNITS.put( "l/s", LITRE_PER_SECOND );
        KNOWN_UNITS.put( "FT", FOOT );
        KNOWN_UNITS.put( "IN", INCH );
        KNOWN_UNITS.put( "M", METRE );
        KNOWN_UNITS.put( "CM", MetricPrefix.CENTI( METRE ) );
        KNOWN_UNITS.put( "MM", MetricPrefix.MILLI( METRE ) );
    }


    /**
     * Given a unit name, return the formal javax.measure Unit of Measure.
     *
     * TODO: Do we want this or a separate method for each type of unit?
     * TODO: Should this method also attempt "parse" with javax.measure?
     * @param unitName The name String
     * @return the javax.measure if known, null otherwise.
     * @throws UnsupportedUnitException when unable to find the unit.
     */

    public static Unit<?> getUnit( String unitName )
    {
        Unit<?> unit = KNOWN_UNITS.get( unitName );

        if ( Objects.isNull( unit ) )
        {
            try
            {
                unit = AbstractUnit.parse( unitName );
            }
            catch ( MeasurementParseException mpe )
            {
                throw new UnsupportedUnitException( unitName,
                                                    KNOWN_UNITS.keySet(),
                                                    mpe );
            }
        }

        return unit;
    }


    /**
     * Given a flow unit name, return the formal javax.measure Unit of Measure.
     * Do we want to go down this road? Have multiple collections? Maybe a map
     * of maps by type? See below as well for how this snowballs.
     * TODO: decide whether to keep or axe
     */

    static Unit<VolumetricFlowRate> getFlowUnit( String unitName )
    {
        throw new NotImplementedException( "Do we want this?" );
    }


    /**
     * Given a length unit name, return the formal javax.measure Unit of Measure.
     * Do we want to go down this road? Have multiple collections? Maybe a map
     * of maps by type? See above as well for how this snowballs.
     * TODO: decide whether to keep or axe
     */

    static Unit<Length> getLengthUnit( String unitName )
    {
        throw new NotImplementedException( "Do we want this?" );
    }


    /**
     * Given a quantity of flow from and target unit to, convert.
     *
     * Probably not needed, given the "to" method on a Quantity.
     * @deprecated
     * @param from The quantity of flow rate to convert.
     * @param to The resulting quantity in the unit prescribed.
     * @return the primitive double value converted.
     */

    static double convertFlow( Quantity<VolumetricFlowRate> from,
                               Unit<VolumetricFlowRate> to )
    {
        UnitConverter converter = from.getUnit()
                                      .getConverterTo( to );
        Number converted = converter.convert( from.getValue() );
        return converted.doubleValue();
    }

    public static final class UnsupportedUnitException extends RuntimeException
    {
        private static final long serialVersionUID = -6873574285493867322L;

        UnsupportedUnitException( String unit,
                                  Set<String> supportedUnits,
                                  Throwable cause )
        {
            super( "Unable to find the measurement unit " + unit
                   + " among the following pre-loaded units " + supportedUnits
                   + ", nor was it able to be parsed into another known unit.",
                   cause );
        }
    }
}
