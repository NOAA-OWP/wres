/**
 * 
 */
package wres.configcontrol.config.project;

/**
 * An elementary block of configuration for a data store, including:
 * 
 * <ul>
 * <li>A {@link wres.configcontrol.datamodel.spacetimeobject.SpaceTimeObjectStore}, which stores the elementary
 * space-time datasets for verification;</li>
 * <li>A {@link wres.configcontrol.datamodel.spacetimeobject.PairStore}, which stores paired datasets; and</li>
 * <li>A {@link wres.configcontrol.datamodel.spacetimeobject.VerificationResultStore}, which stores verification
 * results.</li>
 * </ul>
 * 
 * @author james.brown@hydrosolved.com
 */
public class DataStoreConfigurationUnit extends ConfigurationUnit<DataStoreConfigurationUnit> {

	@Override
	public DataStoreConfigurationUnit deepCopy() {
		// TODO Auto-generated method stub
		return null;
	}

}
