package wres.datamodel;

import java.util.Arrays;

/**
 * A Safe way to share a boolean array between threads. It is cloned on the way in, and cloned on the way out. This way,
 * if the original array is mutated after construction, the clone during construction prevents surprise. Likewise, the
 * clone during get prevents state being leaked to other classes. There does not yet seem to be a performance penalty.
 * 
 * @author jesse
 */
class SafeVectorOfBooleans implements VectorOfBooleans
{
    private final boolean[] booleans;

    private SafeVectorOfBooleans(final boolean[] booleans)
    {
        this.booleans = booleans.clone();
    }

    public static VectorOfBooleans of(final boolean[] booleans)
    {
        return new SafeVectorOfBooleans(booleans);
    }

    @Override
    public boolean[] getBooleans()
    {
        return booleans.clone();
    }
    
    @Override
    public boolean equals( Object o )
    {
        if(! (o instanceof VectorOfBooleans) )
        {
            return false;
        }
        boolean[] in = ((VectorOfBooleans)o).getBooleans();
        return Arrays.equals( this.getBooleans(), in );
    }
    
    @Override
    public int hashCode( )
    {
        return Arrays.hashCode( this.getBooleans() );
    }       

    @Override
    public int size()
    {
        return booleans.length;
    }
}
