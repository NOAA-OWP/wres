package wres.io.reading;

import wres.io.config.specification.*;
import wres.util.Collections;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

/**
 * @author ctubbs
 *
 */
public abstract class BasicSource {
	
	@SuppressWarnings("static-method")
    public void saveForecast() throws IOException
	{
		throw new IOException("Forecasts may not be saved using this type of source.");
	}
	
	@SuppressWarnings("static-method")
    public void saveObservation() throws IOException
	{
		throw new IOException("Observations may not be saved using this type of source.");
	}

	public String getFilename()
	{
		return filename;
	}
	
	protected void set_filename(String name)
	{
		filename = name;
	}
	
	protected String getAbsoluteFilename()
	{
		if (absolute_filename == null)
		{
			absolute_filename = Paths.get(getFilename()).toAbsolutePath().toString();
		}
		return absolute_filename;
	}
	
	public SourceType get_source_type()
	{
		return source_type;
	}
	
	protected void set_source_type(SourceType type)
	{
		source_type = type;
	}
	
	public void applySpecification(ProjectDataSpecification specification) {
	    this.earliestDate = specification.getEarliestDate();
	    this.latestDate = specification.getLatestDate();
	    this.minimumValue = specification.getMinimumValue();
	    this.maximumValue = specification.getMaximumValue();
	    this.timeOffset = specification.getTimeOffset();
	    
	    this.variables = specification.getVariables();
	    this.loadAllVariables = this.variables == null || this.variables.size() == 0;
	    
	    this.ensemblesToLoad = specification.getEnsembles();
	    this.loadAllEnsembles = this.ensemblesToLoad == null || this.ensemblesToLoad.size() == 0;
    
        this.features = specification.getFeatures();
        this.loadAllFeatures = this.features == null || this.features.size() == 0;
        
        this.isForecast = specification.isForecast();
        this.detailsSpecified = true;
	}
	
	protected String getEarliestDate() {
	    return this.earliestDate;
	}
	
	protected String getLatestDate() {
	    return this.latestDate;
	}
	
	protected String getMinimumValue() {
	    return this.minimumValue;
	}
	
	protected String getMaximumValue() {
	    return this.maximumValue;
	}
	
	protected boolean sourceIsForecast() {
	    return this.isForecast;
	}
	
	protected String getTimeOffset()
	{
	    return this.timeOffset;
	}
	
	protected boolean variableIsApproved(String name) {
	    return !this.detailsAreSpecified() ||
	           this.loadAllVariables || 
	           Collections.exists(this.variables, (VariableSpecification specification) -> {
	               return specification.name().equalsIgnoreCase(name);
	    });
	}
	
	protected boolean ensembleIsApproved(String name, String ensembleMemberID) {
	    return !this.detailsAreSpecified() ||
	           this.loadAllEnsembles || 
	           Collections.exists(this.ensemblesToLoad, (EnsembleSpecification specification) -> {
	               return specification.getName().equalsIgnoreCase(name) && specification.getMemberID().equalsIgnoreCase(ensembleMemberID);
	    });
	}
	
	protected boolean featureIsApproved(String lid, String comid) {
	    return !this.detailsAreSpecified() ||
	           this.loadAllFeatures ||
	           Collections.exists(this.features, (FeatureSpecification specification) -> {
	               return specification.isLocation() && (((LocationSpecification)specification).lid().equalsIgnoreCase(lid) || 
	                                                     ((LocationSpecification)specification).comid().equalsIgnoreCase(comid));
	           });
	}

	protected boolean detailsAreSpecified()
	{
		return this.detailsSpecified;
	}
	
	private String filename = "";
	private String absolute_filename;
	private SourceType source_type = SourceType.UNDEFINED;
	
	private String earliestDate;
	private String latestDate;
	private String minimumValue;
	private String maximumValue;
	private String timeOffset;
	private List<VariableSpecification> variables;
	private boolean loadAllVariables = true;
	private boolean loadAllEnsembles = true;
	private List<EnsembleSpecification> ensemblesToLoad;
	private boolean loadAllFeatures = true;
	private List<FeatureSpecification> features;
	
	private boolean isForecast = false;
	private boolean detailsSpecified = false;
}
