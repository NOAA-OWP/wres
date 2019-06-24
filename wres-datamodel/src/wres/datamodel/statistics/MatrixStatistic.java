package wres.datamodel.statistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;

/**
 * Immutable matrix of statistics associated with a metric. The number of elements and the order in which they are stored, 
 * is prescribed by the metric from which the outputs originate. The elements may be iterated over in row-major.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MatrixStatistic implements Statistic<MatrixOfDoubles>, Iterable<Double>
{

    /**
     * The statistics.
     */

    private final MatrixOfDoubles statistics;

    /**
     * A list of named elements in the matrix, stored in row-major order.
     */

    private final List<MetricDimension> names;

    /**
     * The metadata associated with the statistics.
     */

    private final StatisticMetadata meta;

    /**
     * Construct the output.
     * 
     * @param statistics the verification statistics.
     * @param meta the metadata.
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static MatrixStatistic
            of( final double[][] statistics, final StatisticMetadata meta )
    {
        return MatrixStatistic.of( MatrixOfDoubles.of( statistics ), null, meta );
    }

    /**
     * Construct the output.
     * 
     * @param statistics the verification statistics.
     * @param names an optional list of named components in row-major order. May be null.
     * @param meta the metadata.
     * @throws StatisticException if any of the inputs are invalid
     * @throws NullPointerException if the statistics matrix is null
     * @return an instance of the output
     */

    public static MatrixStatistic
            of( final double[][] statistics, List<MetricDimension> names, final StatisticMetadata meta )
    {
        return MatrixStatistic.of( MatrixOfDoubles.of( statistics ), names, meta );
    }

    /**
     * Construct the statistics.
     * 
     * @param statistics the verification statistics.
     * @param names an optional list of named components in row-major order. May be null.
     * @param meta the metadata.
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static MatrixStatistic
            of( final MatrixOfDoubles statistics, List<MetricDimension> names, final StatisticMetadata meta )
    {
        return new MatrixStatistic( statistics, names, meta );
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
        int row = rowMajorIndex / statistics.rows();
        int column = rowMajorIndex % statistics.rows();
        if ( row >= statistics.rows() || column >= statistics.columns() )
        {
            throw new IndexOutOfBoundsException( "The row-major index '" + rowMajorIndex + "' is out of bounds." );
        }
        return statistics.getDoubles()[row][column];
    }

    /**
     * Returns <code>true</code> if the {@link MatrixStatistic} has named components, otherwise <code>false</code>.
     * 
     * @return true if the output has named components, false otherwise
     */

    public boolean hasComponentNames()
    {
        return Objects.nonNull( names );
    }

    /**
     * Returns the component names associated with the {@link MatrixStatistic} in row-major order or null.
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
     * @return the number of statistics
     */

    public int size()
    {
        return statistics.rows() * statistics.columns();
    }

    @Override
    public StatisticMetadata getMetadata()
    {
        return meta;
    }

    @Override
    public MatrixOfDoubles getData()
    {
        return statistics;
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
        if ( ! ( o instanceof MatrixStatistic ) )
        {
            return false;
        }

        final MatrixStatistic m = (MatrixStatistic) o;
        boolean start = meta.equals( m.getMetadata() );
        start = start && m.getData().rows() == statistics.rows() && m.getData().columns() == statistics.columns();
        start = start && Arrays.deepEquals( statistics.getDoubles(), m.getData().getDoubles() );
        start = start && Objects.equals( names, m.names );
        
        return start;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( meta, Arrays.deepHashCode( statistics.getDoubles() ), names );
    }

    @Override
    public String toString()
    {
        return statistics.toString();
    }

    /**
     * Construct the statistics.
     * 
     * @param statistics the verification statistics
     * @param names an optional list of named components in row-major order. May be null.
     * @param meta the metadata.
     * @throws StatisticException if any of the inputs are invalid
     */

    private MatrixStatistic( final MatrixOfDoubles statistics, List<MetricDimension> names, final StatisticMetadata meta )
    {
        if ( Objects.isNull( statistics ) )
        {
            throw new StatisticException( "Specify non-null statistics." );
        }
        // Further validate
        if ( Objects.isNull( meta ) )
        {
            throw new StatisticException( "Specify non-null metadata." );
        }
        int size = statistics.rows() * statistics.columns();
        if ( Objects.nonNull( names ) && names.size() != size )
        {
            throw new StatisticException( "The number of named components differs from the number of elements in "
                                             + "the matrix ["
                                             + names.size()
                                             + ","
                                             + size
                                             + "]." );
        }

        // Set
        this.statistics = statistics;
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
