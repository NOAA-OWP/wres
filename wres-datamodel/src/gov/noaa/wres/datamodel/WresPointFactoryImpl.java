package gov.noaa.wres.datamodel;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of WresPoint with caching.
 * The first several points to be created will live in the JVM forever,
 * but will not ever be created again. Effectively hoping to save heap
 * at the expense of extra processing and never cleaning up some objects. 
 * 
 * This may or may not be useful. It can be an example of "why use 'of'" 
 * instead of raw constructors. We can possibly have our convenient small
 * objects without paying too heavy a price by reusing the same ones.
 * 
 * This class represents both the WresPoint and its Factory.
 * Not sure if this is the best model. Open to suggestions.
 * 
 * Intended to be thread-safe.
 * 
 * @see WresPoint
 * @author jesse
 *
 */
public class WresPointFactoryImpl
implements WresPoint, Comparable
{
    private final int x;
    private final int y;
    private final int z;
    private static AtomicInteger curSize = new AtomicInteger(0);
    private static final int CACHE_COUNT = 1024*8;
    private static final WresPoint[] cache = new WresPoint[CACHE_COUNT];

    private WresPointFactoryImpl(int x, int y, int z)
    {
        if (x == Integer.MIN_VALUE)
        {
            throw new UnsupportedOperationException("X must be set and not the integer min value");
        }
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public static WresPoint of(int x)
    {
        return WresPointFactoryImpl.of(x, Integer.MIN_VALUE, Integer.MIN_VALUE);
    }

    public static WresPoint of(int x, int y)
    {
        return WresPointFactoryImpl.of(x, y, Integer.MIN_VALUE);
    }

    public static WresPoint of(int x, int y, int z)
    {
        // search the cache, if not found, create, add, return.
        WresPoint maybeFoundPoint = cachedPoint(x,y,z); 
        if (maybeFoundPoint != null)
        {
            // definitely found now.
            return maybeFoundPoint; 
        }
        else if (curSize.get() >= CACHE_COUNT)
        {
            // immutable cache is full, don't bother adding.
            return new WresPointFactoryImpl(x,y,z);
        }
        else
        {
            WresPoint point = new WresPointFactoryImpl(x, y, z);
            // atomically set the position in the cache
            cache[curSize.getAndIncrement()] = point;
            return point;
        }
    }

    @Override
    public int getX()
    {
        return this.x;
    }

    @Override
    public int getY()
    {
        return this.y;
    }

    @Override
    public int getZ()
    {
        return this.z;
    }

    /**
     * Search through the cache for a point matching x,y,z.
     * @return the found point, null otherwise
     */
    private static WresPoint cachedPoint(int x, int y, int z)
    {
        for (int i = 0; i < curSize.get(); i++)
        {
            WresPoint point = cache[i];
            if (point.getX() == x
                && point.getY() == y
                && point.getZ() == z)
            {
                return point;
            }
        }
        return null;
    }

    @Override
    public int compareTo(Object o)
    {
        if (o instanceof WresPoint)
        {
            if (this.getX() < ((WresPoint)o).getX())
            {
                return -1;
            }
            else if (this.getX() > ((WresPoint)o).getX())
            {
                return 1;
            }
            else if (this.getY() < ((WresPoint)o).getY())
            {
                return -1;
            }
            else if (this.getY() > ((WresPoint)o).getY())
            {
                return 1;
            }
            else if (this.getZ() < ((WresPoint)o).getZ())
            {
                return -1;
            }
            else if (this.getZ() > ((WresPoint)o).getZ())
            {
                return 1;
            }
            return 0;
        }
        else if (o instanceof Integer)
        {
            if (this.getX() < (Integer)o)
            {
                return -1;
            }
            else if (this.getX() > (Integer)o)
            {
                return 1;
            }
            return 0;
        }
        else if (o instanceof Long)
        {
            if (this.getX() < (Long)o)
            {
                return -1;
            }
            else if (this.getX() > (Long)o)
            {
                return 1;
            }
            return 0;
        }
        throw new ClassCastException("Cannot compare WresPoint to " + o.getClass());
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + x;
        result = prime * result + y;
        result = prime * result + z;
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(!(obj instanceof WresPoint))
            return false;
        WresPoint other = (WresPoint)obj;
        if(this.getX() != other.getX())
            return false;
        if(this.getY() != other.getY())
            return false;
        if(this.getZ() != other.getZ())
            return false;
        return true;
    }
}
