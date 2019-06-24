package wres.datamodel.sampledata;

import java.util.Objects;

import wres.datamodel.statistics.Statistic;

/**
 * A class that stores the measurement unit associated with a {@link SampleData} or a {@link Statistic}. Examples 
 * of measurement units include "millimeter", and "cubic feet per second".
 * 
 * TODO: replace this simplistic abstraction with a more sophisticated representation, probably based on javax.measure,
 * which allows for a full description, as well as transformations between units.
 * 
 * @author james.brown@hydrosolved.com
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
        return o instanceof MeasurementUnit && ( (MeasurementUnit) o ).isRealUnit() == isRealUnit()
               && ( (MeasurementUnit) o ).getUnit().equals( getUnit() );
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
        Objects.requireNonNull( o, "Specify a non-null dimension to compare with this dimension." );

        return unit.compareTo( o.getUnit() );
    }

    /**
     * Hidden constructor.
     * 
     * @param unit the dimension
     * @throws NullPointerException if the input is null
     */

    private MeasurementUnit( final String unit )
    {
        Objects.requireNonNull( unit, "Specify a non-null unit string." );

        this.unit = unit;
    }
    
}
