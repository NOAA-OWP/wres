package wres.datamodel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutputException;

/**
 * An immutable matrix of outputs associated with a metric.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.2
 * @since 0.1
 */
class SafeMatrixOutput implements MatrixOutput
{

    /**
     * The output data.
     */

    private final MatrixOfDoubles output;
    
    /**
     * A list of named elements in the matrix, stored in row-major order.
     */

    private final List<MetricDimension> names;
    
    /**
     * The metadata associated with the output.
     */

    private final MetricOutputMetadata meta;

    @Override
    public MetricOutputMetadata getMetadata()
    {
        return meta;
    }

    @Override
    public MatrixOfDoubles getData()
    {
        return output;
    }

    @Override
    public double getComponentAtIndex( int rowMajorIndex )
    {
        int row = rowMajorIndex / output.rows();
        int column = rowMajorIndex % output.rows();
        if( row >= output.rows() || column >= output.columns() )
        {
            throw new IndexOutOfBoundsException( "The row-major index '"+rowMajorIndex+"' is out of bounds." );
        }
        return output.getDoubles()[row][column];
    }

    @Override
    public boolean hasComponentNames()
    {
        return Objects.nonNull( names );
    }

    @Override
    public MetricDimension getComponentNameAtIndex( int rowMajorIndex )
    {
        return names.get( rowMajorIndex );
    }

    @Override
    public int size()
    {
        return output.rows() * output.columns();
    }    
    
    @Override
    public List<MetricDimension> getComponentNames()
    {
        return Collections.unmodifiableList( names );
    }
    
    @Override
    public Iterator<Double> iterator()
    {       
        return new Iterator<Double>()
        {
            
            /**
             * Index of current element.
             */
            
            int index = 0;
            
            @Override
            public boolean hasNext()
            {
                return index < size();
            }

            @Override
            public Double next()
            {
                if( ! hasNext() )
                {
                    throw new NoSuchElementException( "No more elements left to iterate." );
                }
                return getComponentAtIndex( index++ );
            }
            
            @Override
            public void remove() {
                throw new UnsupportedOperationException( "Cannot modify this immutable container." );
            }
            
        };
    }    

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof SafeMatrixOutput ) )
        {
            return false;
        }
        final SafeMatrixOutput m = (SafeMatrixOutput) o;
        boolean start = meta.equals( m.getMetadata() );
        start = start && m.getData().rows() == output.rows() && m.getData().columns() == output.columns();
        start = start && Arrays.deepEquals( output.getDoubles(), m.getData().getDoubles() );
        start = start && Objects.equals( names, m.names );
        return start;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( meta, Arrays.deepHashCode( output.getDoubles() ), names );
    }
    
    @Override
    public String toString()
    {
        return output.toString();
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output.
     * @param names an optional list of named components in row-major order. May be null.
     * @param meta the metadata.
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeMatrixOutput( final MatrixOfDoubles output, List<MetricDimension> names, final MetricOutputMetadata meta )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( "Specify a non-null output." );
        }
        // Further validate
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( "Specify non-null metadata." );
        }
        int size = output.rows() * output.columns();
        if( Objects.nonNull( names ) && names.size() != size )
        {
            throw new MetricOutputException( "The number of named components differs from the number of elements in "
                    + "the matrix ["+names.size()+","+size+"]." );
        }
        
        // Set
        this.output = ( (DefaultDataFactory) DefaultDataFactory.getInstance() ).safeMatrixOf( output );
        this.meta = meta;
        if( Objects.nonNull( names ) ) 
        {
            this.names = new ArrayList<>( names );
        }
        else
        {
            this.names = null;
        }
    }

}
