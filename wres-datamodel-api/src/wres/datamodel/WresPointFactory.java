package wres.datamodel;

/**
 * A WresPointFactory provides static methods to create WresPoint objects.
 *
 * Calls the implementation provided at runtime.
 *
 * @author jesse
 *
 */
public class WresPointFactory
{
    public static WresPoint of(int x)
    {
        return WresPointFactoryImpl.of(x);
    }

    public static WresPoint of(int x, int y)
    {
        return WresPointFactoryImpl.of(x, y);
    }

    public static WresPoint of(int x, int y, int z)
    {
        return WresPointFactoryImpl.of(x, y, z);
    }
}
