package wres.io.reading;

import wres.config.generated.ProjectConfig;
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
	
	protected void setFilename (String name)
	{
		filename = name;
	}
	
	protected String getAbsoluteFilename()
	{
		if (absoluteFilename == null)
		{
            absoluteFilename = Paths.get(getFilename()).toAbsolutePath().toString();
		}
		return absoluteFilename;
	}
	
	public SourceType getSourceType()
	{
		return sourceType;
	}
	
	protected void setSourceType(SourceType type)
	{
        sourceType = type;
	}

	public void setProjectConfig(ProjectConfig projectConfig)
	{
		this.projectConfig = projectConfig;
	}

	protected ProjectConfig getProjectConfig()
	{
		return this.projectConfig;
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
	private String absoluteFilename;
	private SourceType sourceType = SourceType.UNDEFINED;
	
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

	private ProjectConfig projectConfig;
}
