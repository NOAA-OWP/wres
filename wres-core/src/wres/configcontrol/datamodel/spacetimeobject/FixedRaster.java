/**
 * 
 */
package wres.configcontrol.datamodel.spacetimeobject;

/**
 * @author ctubbs Fixed Grid For All Times
 */
public class FixedRaster extends SpaceTimeRaster implements TimeSeries, FixedObject
{

    /**
     * @param identifier the identifier
     */
    public FixedRaster(final String identifier)
    {
        super(identifier);
    }

    public FixedRaster()
    {
        super();
    }

    /*
     * (non-Javadoc)
     * @see wres.util.DeepCopy#deepCopy()
     */
    @Override
    public FixedRaster deepCopy()
    {
        final FixedRaster copy = new FixedRaster(this.getIdentifierKey());

        for(final Integer key: this.getKeys())
        {
            copy.setValue(key, this.getValue(key));
        }

        return copy;
    }

}
