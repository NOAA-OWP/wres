/**
 * 
 */
package wres.configcontrol.datamodel.spacetimeobject;

/**
 * @author ctubbs
 * Fixed Grid For All Times
 */
public class FixedRaster extends SpaceTimeRaster implements TimeSeries, FixedObject {

	/**
	 * 
	 */
	public FixedRaster(String identifier) {
		super(identifier);
	}
	
	public FixedRaster() {
		super();
	}

	/* (non-Javadoc)
	 * @see wres.util.DeepCopy#deepCopy()
	 */
	@Override
	public FixedRaster deepCopy() {
		FixedRaster copy = new FixedRaster(this.getIdentifierKey());
		
		for (Integer key : this.getKeys())
		{
			copy.setValue(key, this.getValue(key));
		}
		
		return copy;
	}

}
