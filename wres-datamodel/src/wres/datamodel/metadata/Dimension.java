package wres.datamodel.metadata;

import java.util.Objects;

import wres.datamodel.inputs.MetricInput;
import wres.datamodel.outputs.MetricOutput;

/**
 * A class that stores the dimension associated with a {@link MetricInput} or a {@link MetricOutput}. A dimension 
 * corresponds to the unit of measurement associated with a constant or a variable. Examples of measurement units
 * include "millimeter", and "cubic feet per second".
 * 
 * TODO: replace this simplistic abstraction with a more sophisticated representation, probably based on javax.measure,
 * which allows for a full description, as well as transformations between units.
 * 
 * @author james.brown@hydrosolved.com
 */

public class Dimension implements Comparable<Dimension>
{

    /**
     * The dimension.
     */
    private final String dimension; 

    /**
     * Returns an instance from the input.
     * 
     * @param dimension the dimension
     * @throws NullPointerException if the input is null
     * @return a dimension instance
     */

    public static Dimension of( String dimension )
    {
        return new Dimension( dimension );
    }

    /**
     * Returns true if the metric data has an explicit dimension, false if it is dimensionless.
     * 
     * @return true if the metric data has an explicit dimension, false otherwise
     */
    
    public boolean hasDimension()
    {
        return !"DIMENSIONLESS".equals( dimension );
    }
    
    /**
     * Returns the named dimension.
     * 
     * @return the named dimension
     */

    public String getDimension()
    {
        return dimension;
    }
    
    @Override
    public boolean equals( final Object o )
    {
        return o instanceof Dimension && ( (Dimension) o ).hasDimension() == hasDimension()
               && ( (Dimension) o ).getDimension().equals( getDimension() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( hasDimension(), dimension );
    }

    @Override
    public String toString()
    {
        return getDimension();
    }

    @Override
    public int compareTo( Dimension o )
    {
        Objects.requireNonNull( o, "Specify a non-null dimension to compare with this dimension." );

        return dimension.compareTo( o.getDimension() );
    }

    /**
     * Hidden constructor.
     * 
     * @param dimension the dimension
     * @throws NullPointerException if the input is null
     */

    private Dimension( final String dimension )
    {
        Objects.requireNonNull( dimension, "Specify a non-null dimension string." );

        this.dimension = dimension;
    }
    
}
