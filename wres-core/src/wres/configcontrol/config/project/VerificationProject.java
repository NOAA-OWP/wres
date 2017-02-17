/**
 * 
 */
package wres.configcontrol.config.project;

// Java dependencies
import javax.xml.bind.*;
import javax.xml.bind.annotation.*;

// WRES dependencies
import wres.configcontrol.config.*;
import wres.configcontrol.datamodel.spacetimeobject.SpaceTimeObjectStore;
import wres.configcontrol.datamodel.spacetimeobject.PairStore;
import wres.configcontrol.datamodel.spacetimeobject.VerificationResultStore;

/**
 * <p>
 * A top-level container for storing the configurations associated with a "verification project".
 * </p>
 * 
 * <p>
 * A {@link VerificationProject} allows for the grouping of one or more verification tasks into a structure that is
 * memorable or purposive for a user. A {@link VerificationProject} may be distinguished by a particular geographic
 * domain, modeling scenario or set of variables to verify, among other things. A {@link VerificationProject} may
 * contain some or all of the elementary tasks associated with verification, such as acquiring data, transforming data
 * by rescaling or conditioning, pairing data, conducting verification and generating verification products. While some
 * of these tasks may depend on other tasks and must, therefore, be implemented in a particular order, there is no
 * requirement that a {@link VerificationProject} contains a specific set of tasks. Indeed, to begin with, no tasks are
 * configured within a {@link VerificationProject}. Configurations are added via the instance methods.
 * </p>
 * 
 * <p>
 * A {@link VerificationProject} contains references to three separate stores of data upon which verification tasks may
 * be operated, namely:
 * </p>
 * 
 * <ol>
 * <li>A {@link SpaceTimeObjectStore}, which stores the elementary space-time datasets for verification;</li>
 * <li>A {@link PairStore}, which stores paired datasets; and</li>
 * <li>A {@link VerificationResultStore}, which stores verification results.</li>
 * </ol>
 * 
 * <p>
 * Similar types of configuration, such as tasks related to the acquisition of data, pairing, rescaling, verification or
 * product generation, are implemented within a {@link ConfigurationSet}. Each {@link ConfigurationSet} comprises one or
 * more {@link ConfigurationUnit} and each {@link ConfigurationUnit} provides the instructions necessary to perform a
 * specific task. A concrete {@link ConfigurationSet} cannot store more than one type of concrete
 * {@link ConfigurationUnit}. For example, a {@link ConfigurationSet} that collects together a series of rescaling tasks
 * may contain a {@link RescalingConfigurationUnit} that implements a temporal aggregation from hourly to daily and
 * another {@link RescalingConfigurationUnit} that implements a temporal aggregation from hourly to weekly. Each
 * {@link ConfigurationUnit} applies to a nominated {@link DataIdentifierSet}, and each {@link DataIdentifierSet}
 * contains one or more {@link DataIdentifier}, which uniquely identifies a dataset within a particular context (data
 * store). Datasets are assigned to configurations, rather than configurations to datasets, because the number of
 * datasets will, in general, greatly exceed the number of configurations (of interest).
 * </p>
 * 
 * <p>
 * A {@link VerificationProject} may contain some or all of the following sets of configurations:
 * </p>
 * 
 * <ol>
 * <li>A {@link ConfigurationSet} containing {@link InputConfigurationUnit}, which stores the configuration necessary to
 * read external datasets into an internal data store, such as a {@link SpaceTimeObjectStore};</li>
 * <li>A {@link ConfigurationSet} containing {@link DataStoreConfigurationUnit}, which configures the persistent data
 * stores, including the {@link SpaceTimeObjectStore}, the {@link PairStore} and the
 * {@link VerificationResultStore};</li>
 * <li>A {@link ConfigurationSet} containing {@link RescalingConfigurationUnit}, which configures the rescaling of
 * datasets;</li>
 * <li>A {@link ConfigurationSet} containing {@link PairingConfigurationUnit}, which configures the pairing together of
 * datasets based on location, time and other properties;</li>
 * <li>A {@link ConfigurationSet} containing {@link ConditioningConfigurationUnit}, which configures the selection or
 * sub-setting of data based on prescribed conditions;</li>
 * <li>A {@link ConfigurationSet} containing {@link MetricConfigurationUnit}, which configures the verification metrics
 * and the associated paired datasets for which they should be computed;</li>
 * <li>A {@link ConfigurationSet} containing {@link ProductConfigurationUnit}, which identifies the verification product
 * configurations (e.g. graphical and tabular outputs) and the datasets to which they apply;</li>
 * <li>A {@link ConfigurationSet} containing {@link OutputConfigurationUnit}, which configures the writing of outputs,
 * such as verification results, to external stores;</li>
 * <li>A {@link ConfigurationSet} containing {@link ConfigurationSequencerUnit}, which provides one or more sequences of
 * tasks and the order in which they should be implemented; and</li>
 * <li>A {@link ConfigurationSet} containing {@link ResourceConfigurationUnit}, which allocates computational resources
 * to particular instances of {@link ConfigurationSet} or {@link ConfigurationUnit}.</li>
 * </ol>
 * 
 * <p>
 * As indicated above, some elementary verification tasks will depend on the prior completion of other tasks. For
 * example, it would not be possible to pair one or more datasets that are absent from the {@link SpaceTimeObjectStore}
 * associated with the {@link VerificationProject}. In this case, a {@link InputConfigurationUnit} would first need to
 * acquire an external dataset and write this to the {@link SpaceTimeObjectStore} with the appropriate
 * {@link DataIdentifier}. In short, where the inputs to one configuration task depend on the outputs from another task,
 * a sequence must be operated. This sequence may be operated manually (e.g. by a user operating tasks within a GUI), or
 * configured, explicitly. A {@link ConfigurationSequencerUnit} is used to schedule a group of (ordered) verification
 * tasks and a {@link ResourceConfigurationUnit} can be used to assign or restrict computational resources to the group
 * of tasks (i.e. to the {@link ConfigurationSequencerUnit}) or to the individual tasks referenced therein. All
 * configurable tasks inherits from {@link Configurable}, which provides a unique {@link Identifier} to track and
 * reference that configuration in a particular context.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
@XmlRootElement(name = "verificationProject")
@XmlAccessorType(XmlAccessType.FIELD)
public class VerificationProject implements Configurable {

	/**
	 * The project identifier.
	 */

	private Identifier projectID = null;

	/**
	 * A store of {@link SpaceTimeObject}.
	 */

	private SpaceTimeObjectStore stoStore = null;

	/**
	 * A store of verification pairs.
	 */

	private PairStore pairStore = null;

	/**
	 * A store of verification results.
	 */

	private VerificationResultStore resultStore = null;

	/**
	 * The configurations necessary to read external data into a {@link SpaceTimeObjectStore}.
	 */

	private ConfigurationSet<InputConfigurationUnit> sourceConfig = new ConfigurationSet<>();

	/**
	 * The configurations associated with data stores, such as {@link SpaceTimeObjectStore}, {@link PairStore} and
	 * {@link verificationResultStore}.
	 */

	private ConfigurationSet<DataStoreConfigurationUnit> storeConfig = new ConfigurationSet<>();

	/**
	 * The configurations associated with data rescaling.
	 */

	private ConfigurationSet<RescalingConfigurationUnit> rescaleConfig = new ConfigurationSet<>();

	/**
	 * The configurations associated with pairing of datasets.
	 */

	private ConfigurationSet<PairingConfigurationUnit> pairConfig = new ConfigurationSet<>();

	/**
	 * The configurations associated with selecting or sub-setting of datasets.
	 */

	private ConfigurationSet<ConditioningConfigurationUnit> conditionConfig = new ConfigurationSet<>();

	/**
	 * The configurations associated with verification metrics.
	 */

	private ConfigurationSet<MetricConfigurationUnit> metricConfig = new ConfigurationSet<>();

	/**
	 * The configurations associated with verification products.
	 */

	private ConfigurationSet<ProductConfigurationUnit> productConfig = new ConfigurationSet<>();

	/**
	 * The configuration necessary to write verification outputs, such as verification results, to external stores.
	 */

	private ConfigurationSet<OutputConfigurationUnit> outputConfig = new ConfigurationSet<>();

	/**
	 * The configurations necessary to sequence verification tasks (i.e. {@link ConfigurationUnit}) and groups of tasks
	 * (i.e. {@link ConfigurationSet}).
	 */

	private ConfigurationSet<ConfigurationSequencerUnit> sequenceConfig = new ConfigurationSet<>();

	/**
	 * The resources associated with particular verification tasks (i.e. {@link ConfigurationUnit}) and groups of tasks
	 * (i.e. {@link ConfigurationSet}).
	 */

	private ConfigurationSet<ResourceConfigurationUnit> resourceConfig = new ConfigurationSet<>();

	/**
	 * Constructs a new verification project with a unique identifier.
	 * 
	 * @param id
	 *            the project identifier
	 * @throws ConfigurationException
	 *             if a {@link Identifier#PROJECT_IDENTIFIER} is absent from the project identifier
	 */

	public VerificationProject(Identifier id) {
		setID(id);
	}

	@Override
	public Identifier getID() {
		return projectID;
	}

	/**
	 * Returns the project identifier.
	 * 
	 * @return the project identifier as a string
	 */
	@Override
	public String toString() {
		return projectID.toString();
	}

	/**
	 * Returns the {@link SpaceTimeObjectStore} associated with the verification project.
	 * 
	 * @return the {@link SpaceTimeObjectStore}
	 */
	public SpaceTimeObjectStore getObjectStore() {
		return stoStore;
	}

	/**
	 * Returns the {@link PairStore} associated with the verification project.
	 * 
	 * @return the {@link PairStore}
	 */
	public PairStore getPairStore() {
		return pairStore;
	}

	/**
	 * Returns the {@link VerificationResultStore} associated with the verification project.
	 * 
	 * @return the {@link VerificationResultStore}
	 */
	public VerificationResultStore getResultStore() {
		return resultStore;
	}

	/**
	 * Returns the {@link ConfigurationSet} that configures the reading of input datasets.
	 * 
	 * @return the {@link ConfigurationSet} of {@link InputConfigurationUnit}
	 */
	public ConfigurationSet<InputConfigurationUnit> getSourceConfig() {
		return sourceConfig;
	}

	/**
	 * Returns the {@link ConfigurationSet} that configures the data stores.
	 * 
	 * @return the {@link ConfigurationSet} of {@link DataStoreConfigurationUnit}
	 */
	public ConfigurationSet<DataStoreConfigurationUnit> getStoreConfig() {
		return storeConfig;
	}

	/**
	 * Returns the {@link ConfigurationSet} that configures the rescaling of datasets.
	 * 
	 * @return the {@link ConfigurationSet} of {@link RescalingConfigurationUnit}
	 */
	public ConfigurationSet<RescalingConfigurationUnit> getRescaleConfig() {
		return rescaleConfig;
	}

	/**
	 * Returns the {@link ConfigurationSet} that configures the pairing of datasets.
	 * 
	 * @return the {@link ConfigurationSet} of {@link PairingConfigurationUnit}
	 */
	public ConfigurationSet<PairingConfigurationUnit> getPairConfig() {
		return pairConfig;
	}

	/**
	 * Returns the {@link ConfigurationSet} that configures the conditioning of datasets.
	 * 
	 * @return the {@link ConfigurationSet} of {@link ConditioningConfigurationUnit}
	 */
	public ConfigurationSet<ConditioningConfigurationUnit> getConditionConfig() {
		return conditionConfig;
	}

	/**
	 * Returns the {@link ConfigurationSet} that configures the verification metrics.
	 * 
	 * @return the {@link ConfigurationSet} of {@link MetricConfigurationUnit}
	 */
	public ConfigurationSet<MetricConfigurationUnit> getMetricConfig() {
		return metricConfig;
	}

	/**
	 * Returns the {@link ConfigurationSet} that configures the verification products.
	 * 
	 * @return the {@link ConfigurationSet} of {@link ProductConfigurationUnit}
	 */
	public ConfigurationSet<ProductConfigurationUnit> getProductConfig() {
		return productConfig;
	}

	/**
	 * Returns the {@link ConfigurationSet} that configures the writing of outputs to external stores.
	 * 
	 * @return the {@link ConfigurationSet} of {@link OutputConfigurationUnit}
	 */
	public ConfigurationSet<OutputConfigurationUnit> getOutputConfig() {
		return outputConfig;
	}

	/**
	 * Returns the {@link ConfigurationSet} that configures the sequencing of verification tasks.
	 * 
	 * @return the {@link ConfigurationSet} of {@link ConfigurationSequencerUnit}
	 */
	public ConfigurationSet<ConfigurationSequencerUnit> getSequenceConfig() {
		return sequenceConfig;
	}

	/**
	 * Returns the {@link ConfigurationSet} that configures the allocation of computational resources to tasks.
	 * 
	 * @return the {@link ConfigurationSet} of {@link ResourceConfigurationUnit}
	 */
	public ConfigurationSet<ResourceConfigurationUnit> getResourceConfig() {
		return resourceConfig;
	}

	/**
	 * Deep copies the configuration, but shallow copies the underlying data stores.
	 * 
	 * @return a deep copy of the configuration
	 */

	@SuppressWarnings("unchecked")
	@Override
	public Configurable deepCopy() {
		VerificationProject p = new VerificationProject(projectID);
		// Shallow copy the stores
		p.stoStore = stoStore;
		p.pairStore = pairStore;
		p.resultStore = resultStore;
		// Deep copy the configurations
		p.sourceConfig = (ConfigurationSet<InputConfigurationUnit>) sourceConfig.deepCopy();
		p.storeConfig = (ConfigurationSet<DataStoreConfigurationUnit>) storeConfig.deepCopy();
		p.rescaleConfig = (ConfigurationSet<RescalingConfigurationUnit>) rescaleConfig.deepCopy();
		p.pairConfig = (ConfigurationSet<PairingConfigurationUnit>) pairConfig.deepCopy();
		p.conditionConfig = (ConfigurationSet<ConditioningConfigurationUnit>) conditionConfig.deepCopy();
		p.metricConfig = (ConfigurationSet<MetricConfigurationUnit>) metricConfig.deepCopy();
		p.productConfig = (ConfigurationSet<ProductConfigurationUnit>) productConfig.deepCopy();
		p.outputConfig = (ConfigurationSet<OutputConfigurationUnit>) outputConfig.deepCopy();
		p.sequenceConfig = (ConfigurationSet<ConfigurationSequencerUnit>) sequenceConfig.deepCopy();
		p.resourceConfig = (ConfigurationSet<ResourceConfigurationUnit>) resourceConfig.deepCopy();
		return p;
	}

	/**
	 * Sets the project identifier.
	 * 
	 * @param id
	 *            the project identifier
	 * @throws ConfigurationException
	 *             if a {@link Identifier#CONFIGURATION_IDENTIFIER} is absent from the project identifier
	 */

	public void setID(Identifier projectID) {
		if (projectID == null) {
			throw new ConfigurationException("Specify a non-null identifier for the project.");
		}
		if (!projectID.contains(Identifier.CONFIGURATION_IDENTIFIER)) {
			throw new ConfigurationException("The identifier type must be a configuration identifier.");
		}
		this.projectID = projectID;
	}

	/**
	 * Sets the {@link SpaceTimeObjectStore} associated with the verification project.
	 * 
	 * @param stoStore
	 *            the object store
	 */
	public void setObjectStore(SpaceTimeObjectStore stoStore) {
		this.stoStore = stoStore;
	}

	/**
	 * Sets the {@link PairStore} associated with the verification project.
	 * 
	 * @param pairStore
	 *            the store of paired data
	 */
	public void setPairStore(PairStore pairStore) {
		this.pairStore = pairStore;
	}

	/**
	 * Sets the {@link VerificationResultStore} associated with the verification project.
	 * 
	 * @param resultStore
	 *            the store of verification results
	 */
	public void setResultStore(VerificationResultStore resultStore) {
		this.resultStore = resultStore;
	}

	/**
	 * Sets the {@link ConfigurationSet} that configures the reading of input datasets.
	 * 
	 * @param sourceConfig
	 *            a {@link ConfigurationSet} of {@link InputConfigurationUnit}
	 */
	public void setSourceConfig(ConfigurationSet<InputConfigurationUnit> sourceConfig) {
		this.sourceConfig = sourceConfig;
	}

	/**
	 * Sets the {@link ConfigurationSet} that configures the data stores.
	 * 
	 * @param storeConfig
	 *            a {@link ConfigurationSet} of {@link DataStoreConfigurationUnit}
	 */
	public void setStoreConfig(ConfigurationSet<DataStoreConfigurationUnit> storeConfig) {
		this.storeConfig = storeConfig;
	}

	/**
	 * Sets the {@link ConfigurationSet} that configures the rescaling of datasets.
	 * 
	 * @param rescaleConfig
	 *            a {@link ConfigurationSet} of {@link RescalingConfigurationUnit}
	 */
	public void setRescaleConfig(ConfigurationSet<RescalingConfigurationUnit> rescaleConfig) {
		this.rescaleConfig = rescaleConfig;
	}

	/**
	 * Sets the {@link ConfigurationSet} that configures the pairing of datasets.
	 * 
	 * @param pairConfig
	 *            a {@link ConfigurationSet} of {@link PairingConfigurationUnit}
	 */
	public void setPairConfig(ConfigurationSet<PairingConfigurationUnit> pairConfig) {
		this.pairConfig = pairConfig;
	}

	/**
	 * Sets the {@link ConfigurationSet} that configures the conditioning of datasets.
	 * 
	 * @param conditionConfig
	 *            a {@link ConfigurationSet} of {@link ConditioningConfigurationUnit}
	 */
	public void setConditionConfig(ConfigurationSet<ConditioningConfigurationUnit> conditionConfig) {
		this.conditionConfig = conditionConfig;
	}

	/**
	 * Sets the {@link ConfigurationSet} that configures the verification metrics.
	 * 
	 * @param metricConfig
	 *            a {@link ConfigurationSet} of {@link MetricConfigurationUnit}
	 */
	public void setMetricConfig(ConfigurationSet<MetricConfigurationUnit> metricConfig) {
		this.metricConfig = metricConfig;
	}

	/**
	 * Sets the {@link ConfigurationSet} that configures the verification products.
	 * 
	 * @param productConfig
	 *            a {@link ConfigurationSet} of {@link ProductConfigurationUnit}
	 */
	public void setProductConfig(ConfigurationSet<ProductConfigurationUnit> productConfig) {
		this.productConfig = productConfig;
	}

	/**
	 * Sets the {@link ConfigurationSet} that configures the writing of outputs to external stores.
	 * 
	 * @param outputConfig
	 *            a {@link ConfigurationSet} of {@link OutputConfigurationUnit}
	 */
	public void setOutputConfig(ConfigurationSet<OutputConfigurationUnit> outputConfig) {
		this.outputConfig = outputConfig;
	}

	/**
	 * Sets the {@link ConfigurationSet} that configures the sequencing of verification tasks.
	 * 
	 * @param sequenceConfig
	 *            a {@link ConfigurationSet} of {@link ConfigurationSequencerUnit}
	 */
	public void setSequenceConfig(ConfigurationSet<ConfigurationSequencerUnit> sequenceConfig) {
		this.sequenceConfig = sequenceConfig;
	}

	/**
	 * Sets the {@link ConfigurationSet} that configures the allocation of computational resources to tasks.
	 * 
	 * @param resourceConfig
	 *            a {@link ConfigurationSet} of {@link ResourceConfigurationUnit}
	 */
	public void setResourceConfig(ConfigurationSet<ResourceConfigurationUnit> resourceConfig) {
		this.resourceConfig = resourceConfig;
	}

	/**
	 * No argument constructor for marshalling.
	 */

	private VerificationProject() {}	

}
