package wres.datamodel;

/**
 * Provides a 2D array of primitive doubles.
 *
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface MatrixOfDoubles
{
    /**
     * Return the underlying array of primitive doubles.
     * 
     * @return the double array
     */

    double[][] getDoubles();

    /**
     * Return the number of rows in the matrix.
     * 
     * @return the number of rows
     */

    int rows();

    /**
     * Return the number of columns in the matrix.
     * 
     * @return the number of columns
     */

    int columns();

    /**
     * Returns true if {@link #rows()} == {@link #columns()}.
     * 
     * @return true if the matrix is square, false otherwise.
     */

    boolean isSquare();

}
