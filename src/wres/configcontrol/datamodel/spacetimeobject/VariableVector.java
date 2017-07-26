/**
 * 
 */
package wres.configcontrol.datamodel.spacetimeobject;

/**
 * @author ctubbs
 */
public class VariableVector extends SpaceTimeVector implements TimeSeries, VariableObject
{

    /**
     * @param identifier the identifier
     */
    public VariableVector(final String identifier)
    {
        super(identifier);
    }

    /**
     * 
     */
    public VariableVector()
    {
        super();
    }

    /*
     * (non-Javadoc)
     * @see wres.util.DeepCopy#deepCopy()
     */
    @Override
    public VariableVector deepCopy()
    {
        final VariableVector copy = new VariableVector(this.getIdentifierKey());

        for(final Integer key: this.getKeys())
        {
            copy.setValue(key, this.getValue(key));
        }

        return copy;
    }

}
