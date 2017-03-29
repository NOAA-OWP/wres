/**
 * 
 */
package wres.configcontrol.datamodel.spacetimeobject;

/**
 * @author ctubbs
 */
public class FixedVector extends SpaceTimeVector implements TimeSeries, FixedObject
{

    /**
     * @param identifier the identifier
     */
    public FixedVector(final String identifier)
    {
        super(identifier);
    }

    /**
     * 
     */
    public FixedVector()
    {
        super();
    }

    /*
     * (non-Javadoc)
     * @see wres.util.DeepCopy#deepCopy()
     */
    @Override
    public FixedVector deepCopy()
    {
        final FixedVector copy = new FixedVector(this.getIdentifierKey());

        for(final Integer key: this.getKeys())
        {
            copy.setValue(key, this.getValue(key));
        }

        return copy;
    }

}
