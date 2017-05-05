package wres.datamodel;

/**
 * A basic point, allowing 1 to 3 dimensions.
 * X is the first dimension.
 * Y is the second dimension.
 * Z is the third dimension.
 * 
 * For a list of location ids, X may be used as an index
 * For a list of two-dimensional data, X,Y may be used.
 * If there is a use-case for a third dimension, X,Y,Z may be used.
 * @author jesse
 *
 */
public interface WresPoint
{
    /** For two- or three-dimensional data, the X or easting component.
     * For single-dimensional data, the index. Must be set and not Integer.MIN_VALUE.
     * @return Value of the first dimension
     */
    public int getX();
    /** 
     * Optional. For two- or three-dimensional data, the Y or northing component.
     * @return The Y or northing component, otherwise Integer.MIN_VALUE
     */
    public int getY();
    /**
     * Optional. For three-dimensional data, the Z or elevation component.
     * @return The Z or elevation component, otherwise Integer.MIN_VALUE
     */
    public int getZ();
  
}
