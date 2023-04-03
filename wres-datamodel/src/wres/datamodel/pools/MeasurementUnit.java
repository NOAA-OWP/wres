package wres.datamodel.pools;

import java.util.Objects;
import wres.datamodel.statistics.Statistic;

/**
 * A class that stores the measurement unit associated with a {@link Pool} or a {@link Statistic}. Examples 
 * of measurement units include "millimeter", and "cubic feet per second".
 * 
 * @author James Brown
 */

public class MeasurementUnit implements Comparable<MeasurementUnit>
{

    /**
     * Identifier for a dimensionless unit.
     */
    
    public static final String DIMENSIONLESS = "DIMENSIONLESS";
    
    /**
     * The unit.
     */
    private final String unit; 

    /**
     * Returns a {@link MeasurementUnit} that is {@link #DIMENSIONLESS}.
     * 
     * @return a {@link MeasurementUnit}
     */
    
    public static MeasurementUnit of()
    {
        return MeasurementUnit.of( DIMENSIONLESS );
    }
    
    /**
     * Returns a {@link MeasurementUnit} with a named dimension and {@link MeasurementUnit#isRealUnit()} that returns 
     * <code>false</code> if the dimension is "DIMENSIONLESS", <code>true</code> otherwise.
     * 
     * @param unit the unit string
     * @return a {@link MeasurementUnit}
     * @throws NullPointerException if the input string is null
     */
    
    public static MeasurementUnit of( final String unit )
    {
        return new MeasurementUnit( unit );
    }
    
    /**
     * Returns true if the metric data has an explicit unit, false if it is "DIMENSIONLESS".
     * 
     * @return true if the metric data has an explicit unit, false otherwise
     */
    
    public boolean isRealUnit()
    {
        return ! DIMENSIONLESS.equals( this.unit );
    }
    
    /**
     * Returns the unit.
     * 
     * @return the unit
     */

    public String getUnit()
    {
        return this.unit;
    }
    
    @Override
    public boolean equals( final Object o )
    {
        return o instanceof MeasurementUnit e && e.isRealUnit() == isRealUnit() && e.getUnit().equals( getUnit() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( isRealUnit(), unit );
    }

    @Override
    public String toString()
    {
        return getUnit();
    }

    @Override
    public int compareTo( MeasurementUnit o )
    {
        return unit.compareTo( o.getUnit() );
    }

    /**
     * Hidden constructor.
     * 
     * @param unit the dimension
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the unit is blank
     */

    private MeasurementUnit( final String unit )
    {
        Objects.requireNonNull( unit, "Specify a non-null unit string." );

        if( unit.isBlank() )
        {
            throw new IllegalArgumentException( "Cannot create a measurement unit with a blank input string." );
        }
        
        this.unit = unit;
    }
    
}
