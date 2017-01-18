/**
 * 
 */
package wres.configcontrol.datamodel.spacetimeobject;

/**
 * @author ctubbs
 *
 */
public class VariableVector extends SpaceTimeVector implements TimeSeries, VariableObject {

	/**
	 * @param identifier
	 */
	public VariableVector(String identifier) {
		super(identifier);
	}

	/**
	 * 
	 */
	public VariableVector() {
		super();
	}

	/* (non-Javadoc)
	 * @see wres.util.DeepCopy#deepCopy()
	 */
	@Override
	public VariableVector deepCopy() {
		VariableVector copy = new VariableVector(this.getIdentifierKey());
		
		for (Integer key : this.getKeys())
		{
			copy.setValue(key, this.getValue(key));
		}
		
		return copy;
	}

}
