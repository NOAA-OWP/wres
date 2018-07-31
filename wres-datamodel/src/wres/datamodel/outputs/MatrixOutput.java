package wres.datamodel.outputs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.metadata.MetricOutputMetadata;

/**
 * Immutable matrix of outputs associated with a metric. The number of elements and the order in which they are stored, 
 * is prescribed by the metric from which the outputs originate. The elements may be iterated over in row-major.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MatrixOutput implements MetricOutput<MatrixOfDoubles>, Iterable<Double>
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

    /**
     * Construct the output.
     * 
     * @param output the verification output.
     * @param meta the metadata.
     * @throws MetricOutputException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static MatrixOutput
            of( final double[][] output, final MetricOutputMetadata meta )
    {
        return MatrixOutput.of( MatrixOfDoubles.of( output ), null, meta );
    }
    
    /**
     * Construct the output.
     * 
     * @param output the verification output.
     * @param names an optional list of named components in row-major order. May be null.
     * @param meta the metadata.
     * @throws MetricOutputException if any of the inputs are invalid
     * @throws NullPointerException if the output matrix is null
     * @return an instance of the output
     */

    public static MatrixOutput
            of( final double[][] output, List<MetricDimension> names, final MetricOutputMetadata meta )
    {
        return MatrixOutput.of( MatrixOfDoubles.of( output ), names, meta );
    }
    
    /**
     * Construct the output.
     * 
     * @param output the verification output.
     * @param names an optional list of named components in row-major order. May be null.
     * @param meta the metadata.
     * @throws MetricOutputException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static MatrixOutput
            of( final MatrixOfDoubles output, List<MetricDimension> names, final MetricOutputMetadata meta )
    {
        return new MatrixOutput( output, names, meta );
    }

    /**
     * Returns the component at the specified row-major index, which begins in the top left corner of the matrix
     * and counts from left-to-right and then top-to-bottom. The index is zero-based.
     * 
     * @param rowMajorIndex the zero-based row-major index
     * @return the value of the component at the rowMajor index
     * @throws IndexOutOfBoundsException if the rowMajor index is out of bounds
     */

    public double getComponentAtIndex( int rowMajorIndex )
    {
        int row = rowMajorIndex / output.rows();
        int column = rowMajorIndex % output.rows();
        if ( row >= output.rows() || column >= output.columns() )
        {
            throw new IndexOutOfBoundsException( "The row-major index '" + rowMajorIndex + "' is out of bounds." );
        }
        return output.getDoubles()[row][column];
    }

    /**
     * Returns <code>true</code> if the {@link MatrixOutput} has named components, otherwise <code>false</code>.
     * 
     * @return true if the output has named components, false otherwise
     */

    public boolean hasComponentNames()
    {
        return Objects.nonNull( names );
    }

    /**
     * Returns the component names associated with the {@link MatrixOutput} in row-major order or null.
     * 
     * @return the component names in row-major order or null
     */

    public List<MetricDimension> getComponentNames()
    {
        return Collections.unmodifiableList( names );
    }

    /**
     * Returns the component name at the specified row-major index or null if {@link #hasComponentNames()} returns 
     * <code>false</code>. The index begins in the top left corner of the matrix and counts from left-to-right and 
     * then top-to-bottom. The index is zero-based.
     * 
     * @param rowMajorIndex the zero-based row-major index
     * @return the named component at the rowMajor index or null
     * @throws IndexOutOfBoundsException if the rowMajor index is out of bounds
     */

    public MetricDimension getComponentNameAtIndex( int rowMajorIndex )
    {
        return names.get( rowMajorIndex );
    }

    /**
     * Returns the cardinality of the matrix, which is the product of the number of rows and columns.
     * 
     * @return the size of the output
     */

    public int size()
    {
        return output.rows() * output.columns();
    }

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
                if ( !hasNext() )
                {
                    throw new NoSuchElementException( "No more elements left to iterate." );
                }
                return getComponentAtIndex( index++ );
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException( "Cannot modify this immutable container." );
            }

        };
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof MatrixOutput ) )
        {
            return false;
        }
        final MatrixOutput m = (MatrixOutput) o;
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

    private MatrixOutput( final MatrixOfDoubles output, List<MetricDimension> names, final MetricOutputMetadata meta )
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
        if ( Objects.nonNull( names ) && names.size() != size )
        {
            throw new MetricOutputException( "The number of named components differs from the number of elements in "
                                             + "the matrix ["
                                             + names.size()
                                             + ","
                                             + size
                                             + "]." );
        }

        // Set
        this.output = output;
        this.meta = meta;
        if ( Objects.nonNull( names ) )
        {
            this.names = new ArrayList<>( names );
        }
        else
        {
            this.names = null;
        }
    }

}
