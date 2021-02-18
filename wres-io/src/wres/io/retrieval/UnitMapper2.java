package wres.io.retrieval;

import java.util.HashMap;
import java.util.Map;
import java.util.function.DoubleUnaryOperator;
import javax.measure.IncommensurableException;
import javax.measure.UnconvertibleException;
import javax.measure.Unit;
import javax.measure.UnitConverter;

import wres.datamodel.Units;
import wres.datamodel.Units.UnsupportedUnitException;

/**
 * Potential successor to UnitMapper, uses javax.measure/indriya instead of db.
 *
 * Experimental as of 2021-02-18.
 */
public class UnitMapper2
{
    /** @deprecated Not great, here for experimentation */
    @Deprecated
    private static final Map<Integer,String> LEGACY_TABLE;

    static
    {
        // This all assumes db changeset order stays the same and runs before
        // anybody puts any other units in, etc. Not a great assumption.
        // This is for testing out javax.measure and should go away.
        LEGACY_TABLE = new HashMap<>( 64 );
        LEGACY_TABLE.put( 1, "NONE" );
        LEGACY_TABLE.put( 2, "CMS" );
        LEGACY_TABLE.put( 3, "CFS" );
        LEGACY_TABLE.put( 4, "FT" );
        LEGACY_TABLE.put( 5, "F" );
        LEGACY_TABLE.put( 6, "C" );
        LEGACY_TABLE.put( 7, "IN" );
        LEGACY_TABLE.put( 8, "M" );
        LEGACY_TABLE.put( 9, "MS" );
        LEGACY_TABLE.put( 10, "HR" );
        LEGACY_TABLE.put( 11, "H" );
        LEGACY_TABLE.put( 12, "S" );
        LEGACY_TABLE.put( 13, "MM" );
        LEGACY_TABLE.put( 14, "CM" );
        LEGACY_TABLE.put( 15, "m3 s-1" );
        LEGACY_TABLE.put( 16, "kg m{-2}" );
        LEGACY_TABLE.put( 17, "%" );
        LEGACY_TABLE.put( 18, "ft/sec" );
        LEGACY_TABLE.put( 19, "gal/min" );
        LEGACY_TABLE.put( 20, "mgd" );
        LEGACY_TABLE.put( 21, "m/sec" );
        LEGACY_TABLE.put( 22, "ft3/day" );
        LEGACY_TABLE.put( 23, "ac-ft" );
        LEGACY_TABLE.put( 24, "mph" );
        LEGACY_TABLE.put( 25, "l/sec" );
        LEGACY_TABLE.put( 26, "ft3/s" );
        LEGACY_TABLE.put( 27, "m3/sec" );
        LEGACY_TABLE.put( 28, "mm/s" );
        LEGACY_TABLE.put( 29, "mm s^-1" );
        LEGACY_TABLE.put( 30, "mm s{-1}" );
        LEGACY_TABLE.put( 31, "mm s-1" );
        LEGACY_TABLE.put( 32, "mm h^-1" );
        LEGACY_TABLE.put( 33, "mm/h" );
        LEGACY_TABLE.put( 34, "mm h-1" );
        LEGACY_TABLE.put( 35, "mm h{-1}" );
        LEGACY_TABLE.put( 36, "kg/m^2" );
        LEGACY_TABLE.put( 37, "kg/m^2h" );
        LEGACY_TABLE.put( 38, "kg/m^2s" );
        LEGACY_TABLE.put( 39, "kg/m^2/s" );
        LEGACY_TABLE.put( 40, "kg/m^2/h" );
        LEGACY_TABLE.put( 41, "Pa" );
        LEGACY_TABLE.put( 42, "W/m^2" );
        LEGACY_TABLE.put( 43, "W m{-2}" );
        LEGACY_TABLE.put( 44, "W m-2" );
        LEGACY_TABLE.put( 45, "m s-1" );
        LEGACY_TABLE.put( 46, "m/s" );
        LEGACY_TABLE.put( 47, "m s{-1}" );
        LEGACY_TABLE.put( 48, "kg kg-1" );
        LEGACY_TABLE.put( 49, "kg kg{-1}" );
        LEGACY_TABLE.put( 50, "kg m-2" );
        LEGACY_TABLE.put( 51, "fraction" );
        LEGACY_TABLE.put( 52, "K" );
        LEGACY_TABLE.put( 53, "-" );
        LEGACY_TABLE.put( 54, "KCFS" );
        LEGACY_TABLE.put( 55, "in/hr" );
        LEGACY_TABLE.put( 56, "in hr^-1" );
        LEGACY_TABLE.put( 57, "in hr-1" );
        LEGACY_TABLE.put( 58, "in hr{-1}" );
        LEGACY_TABLE.put( 59, "in/h" );
        LEGACY_TABLE.put( 60, "in h-1" );
        LEGACY_TABLE.put( 61, "in h{-1}" );
        LEGACY_TABLE.put( 62, "CFSD" );
        LEGACY_TABLE.put( 63, "CMSD" );
        LEGACY_TABLE.put( 64, "m3/s" );
    }

    private final String desiredMeasurementUnitName;
    private final Unit<?> desiredUnit;

    private UnitMapper2( String desiredMeasurementUnitName )
    {
        // Keep the original name supplied for the sake of retaining the public
        // method getDesiredMeasurementUnitName in original UnitMapper api.
        this.desiredMeasurementUnitName = desiredMeasurementUnitName;

        // Immediately attempt to get a javax.measure.Unit.
        this.desiredUnit = Units.getUnit( desiredMeasurementUnitName );
    }

    public static UnitMapper2 of( String desiredMeasurementUnit )
    {
        return new UnitMapper2( desiredMeasurementUnit );
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
     * @throws UnsupportedUnitException When unable to support given unitName.
     */

    public DoubleUnaryOperator getUnitMapper( String unitName )
    {
        Unit<?> existingUnit = Units.getUnit( unitName );
        UnitConverter converter;

        try
        {
            converter = existingUnit.getConverterToAny( this.getDesiredUnit() );
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
     * The purpose of this is to try the use of javax.measure without changing
     * the database schema or retrieval code too much. This should go away.
     * @deprecated This uses a copy of wres.measurementunit, not great.
     * @param id
     * @return
     */
    @Deprecated
    DoubleUnaryOperator getUnitMapper( Integer id )
    {
        String unit = LEGACY_TABLE.get( id );
        return this.getUnitMapper( unit );
    }

    private Unit<?> getDesiredUnit()
    {
        return this.desiredUnit;
    }
}
