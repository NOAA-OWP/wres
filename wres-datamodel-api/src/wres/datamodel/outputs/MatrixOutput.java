package wres.datamodel.outputs;

import java.util.List;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;

/**
 * A matrix of outputs associated with a metric. The number of elements and the order in which they are stored, is
 * prescribed by the metric from which the outputs originate. The elements may be iterated over in row-major.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.2
 * @since 0.1
 */

public interface MatrixOutput extends MetricOutput<MatrixOfDoubles>, Iterable<Double>
{
    
    /**
     * Returns the component at the specified row-major index, which begins in the top left corner of the matrix
     * and counts from left-to-right and then top-to-bottom. The index is zero-based.
     * 
     * @param rowMajorIndex the zero-based row-major index
     * @return the value of the component at the rowMajor index
     * @throws IndexOutOfBoundsException if the rowMajor index is out of bounds
     */
    
    double getComponentAtIndex( int rowMajorIndex );
    
    /**
     * Returns <code>true</code> if the {@link MatrixOutput} has named components, otherwise <code>false</code>.
     * 
     * @return true if the output has named components, false otherwise
     */
    
    boolean hasComponentNames();
    
    /**
     * Returns the component names associated with the {@link MatrixOutput} in row-major order or null.
     * 
     * @return the component names in row-major order or null
     */
    
    List<MetricDimension> getComponentNames();
    
    /**
     * Returns the component name at the specified row-major index or null if {@link #hasComponentNames()} returns 
     * <code>false</code>. The index begins in the top left corner of the matrix and counts from left-to-right and 
     * then top-to-bottom. The index is zero-based.
     * 
     * @param rowMajorIndex the zero-based row-major index
     * @return the named component at the rowMajor index or null
     * @throws IndexOutOfBoundsException if the rowMajor index is out of bounds
     */    
    
    MetricDimension getComponentNameAtIndex( int rowMajorIndex );
    
    /**
     * Returns the cardinality of the matrix, which is the product of the number of rows and columns.
     * 
     * @return the size of the output
     */
    
    int size();
    
    
}
