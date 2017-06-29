/**
 * 
 */
package wres.io.config.specification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.Debug;
import wres.util.XML;

/**
 * Specifies information about where to load data for a project
 * 
 * @author Christopher Tubbs
 */
public class ProjectDataSpecification extends SpecificationElement
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProjectDataSpecification.class);

	/**
	 * Constructor
	 * @param reader The xml reader that contains the specifications for the data source
	 */
	public ProjectDataSpecification(XMLStreamReader reader) 
	{
		super(reader);
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("observations", "forecasts", "datasource", "baseline", "source_one", "source_two");
	}
	
	@Override
	protected void getAttributes(XMLStreamReader reader) {
		for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex)
		{
			if (reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase("lazyLoad"))
			{
				this.lazyLoad = Boolean.valueOf(reader.getAttributeValue(attributeIndex));
			}
			else if (reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase("isForecast"))
			{
			    this.isForecast = Boolean.valueOf(reader.getAttributeValue(attributeIndex));
			}
			else if (reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase("sourceID")) {
			    this.setDataSourceIdentifier(reader.getAttributeValue(attributeIndex));
			}
		}
	}

	/* (non-Javadoc)
	 * @see config.data.ConfigElement#interpret(javax.xml.stream.XMLStreamReader)
	 */
	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException 
	{
	    if (reader == null)
	    {
	        LOGGER.trace("interpret - reader was null");
            throw new XMLStreamException("The XMLStreamReader was null and could not be used to process the project data source specification.");
	    }

		if (XML.tagIs(reader, "directories"))
		{
			while (reader.hasNext())
			{
				next(reader);
				if (XML.xmlTagClosed(reader, Collections.singletonList("directories")))
				{
					break;
				}
				
				if (reader.isStartElement())
				{
					addDirectory(new DirectorySpecification(reader));
				}
			}
		}
		else if(XML.tagIs(reader, "conditions"))
		{
			conditions = new ConditionSpecification(reader);
		}
		else if (XML.tagIs(reader, "variable"))
		{
			String variableName = null;
			String variableUnit = null;
			for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex)
			{
				if (reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase("name"))
				{
					variableName = reader.getAttributeValue(attributeIndex);
				}
				else if(reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase("unit"))
				{
					variableUnit = reader.getAttributeValue(attributeIndex);
				}				
			}	
			addVariable(new VariableSpecification(variableName, variableUnit));
		}
		else if (XML.tagIs(reader, "ensembles"))
		{
			parseEnsembles(reader);
		}
		else if (XML.tagIs(reader, "features"))
		{
			parseFeatures(reader);
		}
	}
	
	/**
	 * Parses data relevant to identifying features to load
	 * @param reader The XML Reader describing the features
	 * @throws XMLStreamException An exception is thrown if there is trouble reading the database
	 */
	private void parseFeatures(XMLStreamReader reader) throws XMLStreamException 
	{
		for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex)
		{
			if (reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase("all"))
			{
				this.loadAllFeatures = Boolean.valueOf(reader.getAttributeValue(attributeIndex));
			}
		}			
		
		while (reader.hasNext())
		{
			next(reader);
			if (XML.xmlTagClosed(reader, Collections.singletonList("features")))
			{
				break;
			}
			else if (!reader.isStartElement())
			{
				continue;
			}
			
			FeatureSpecification feature = null;			
			
			if (XML.tagIs(reader, "feature"))
			{
				feature = new LocationSpecification(reader);
			}
			else if (XML.tagIs(reader, "range"))
			{
				feature = new FeatureRangeSpecification(reader);
			}
			else if (XML.tagIs(reader, "polygon"))
			{
				feature = new PolygonSpecification(reader);
			}
			else if (XML.tagIs(reader, "point"))
			{
				feature = new PointSpecification(reader);
			}
			
			addFeature(feature);
		}	
	}

	/**
	 * Parses data detailing what ensembles to work with
	 * @param reader The XML Reader containing the information to parse
	 * @throws XMLStreamException An error will be thrown if the XML can't be properly read
	 */
	private void parseEnsembles(XMLStreamReader reader) throws XMLStreamException
	{
		for (int attribute_index = 0; attribute_index < reader.getAttributeCount(); ++attribute_index)
		{
			if (reader.getAttributeLocalName(attribute_index).equalsIgnoreCase("all"))
			{
				this.loadAllEnsembles = Boolean.valueOf(reader.getAttributeValue(attribute_index));
			}
		}			
		
		while (reader.hasNext())
		{
			next(reader);
			if (XML.xmlTagClosed(reader, Collections.singletonList("ensembles")))
			{
				break;
			}
			else if (!reader.isStartElement())
			{
				continue;
			}

			String name = "";
			String memberID = "";
			String qualifier = "";
			
			for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex)
			{
				if (reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase("name"))
				{
					name = reader.getAttributeValue(attributeIndex);
				}
				else if (reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase("member_id"))
				{
					memberID = reader.getAttributeValue(attributeIndex);
				}
				else if (reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase("qualifier"))
				{
					qualifier = reader.getAttributeValue(attributeIndex);
				}
			}
			
			addEnsemble(new EnsembleSpecification(name, memberID, qualifier));
		}
	}
	
	/**
	 * Adds a directory specification to the datasource
	 * @param directory The directory to add
	 */
	private void addDirectory(DirectorySpecification directory)
	{
		if (directory == null)
		{
			return;
		}
		
		if (this.directories == null)
		{
			this.directories = new ArrayList<>();
		}
		
		this.directories.add(directory);
	}
	
	/**
	 * Adds an ensemble specification to the data source
	 * @param ensemble The ensemble specification to add
	 */
	private void addEnsemble(EnsembleSpecification ensemble)
	{
		if (ensemble == null || (ensemble.getName().isEmpty() && 
								 ensemble.getMemberID().isEmpty() && 
								 ensemble.getQualifier().isEmpty()))
		{
			return;
		}
		
		if (this.ensembles == null)
		{
			this.ensembles = new ArrayList<>();
		}
		
		this.ensembles.add(ensemble);
	}
	
	/**
	 * @return The number of specified ensembles
	 */
	public int ensembleCount()
	{
		return getEnsembles().size();
	}
	
	/**
	 * Adds specifications for a feature to identify
	 * @param feature The specification for the feature to add
	 */
	private void addFeature(FeatureSpecification feature)
	{
		if (feature == null)
		{
			return;
		}
		
		if (this.features == null)
		{
			this.features = new ArrayList<>();
		}
		
		this.features.add(feature);
	}
	
	/**
	 * @return The number of specified features
	 */
	public int featureCount()
	{
		if (this.features == null)
		{
			this.features = new ArrayList<>();
		}
		
		return this.features.size();
	}
	
	/**
	 * @return A list of stored directory specifications
	 */
	public List<DirectorySpecification> getDirectories()
	{
		if (this.directories == null)
		{
			this.directories = new ArrayList<>();
		}
		return directories;
	}
	
	/**
	 * @return The number of specified directories
	 */
	public int directoryCount()
	{
		return getDirectories().size();
	}
	
	/**
	 * @return A list of feature specifications
	 */
	public List<FeatureSpecification> getFeatures()
	{
		if (this.features == null)
		{
			this.features = new ArrayList<>();
		}
		return this.features;
	}
	
	/**
	 * Retrieves the feature specification at the given position
	 * @param index The position to retrieve the feature specification from
	 * @return The feature specification at the given otherwise, if it exists. <b>null</b> otherwise.
	 */
	public FeatureSpecification getFeature(int index)
	{
	    FeatureSpecification feature = null;
	    
	    if (this.features == null)
	    {
	        this.features = new ArrayList<>();
	    }
	    
	    if (index < this.features.size())
	    {
	        feature = this.features.get(index);
	    }
	    
	    return feature;
	}
	
	/**
	 * @return The first feature specification, if there is one. <b>null</b> otherwise.
	 */
	public FeatureSpecification getFeature()
	{
	    return getFeature(0);
	}
	
	/**
	 * @return The conditions imposed upon this data
	 */
	public ConditionSpecification conditions()
	{
		return conditions;
	}
	
	/**
	 * @return Whether or not all features should be loaded
	 */
	public boolean loadAllFeatures()
	{
		return loadAllFeatures;
	}
	
	/**
	 * @return Whether or not all ensembles should be loaded
	 */
	public boolean loadAllEnsembles()
	{
		return loadAllEnsembles;
	}
	
	/**
	 * @return Whether or not to only load data if it doesn't exist
	 */
	public boolean loadLazily()
	{
		return lazyLoad;
	}
	
	public void setDataSourceIdentifier(String dataSourceIdentifier) {
	    this.dataSourceIdentifier = dataSourceIdentifier;
	}
	
	public String getDataSourceIdentifier() {
	    return this.dataSourceIdentifier;
	}
	
	public String getEnsembleCondition() throws Exception
	{
	    StringBuilder condition = new StringBuilder("ANY('{");

        boolean addComma = false;
        for (EnsembleSpecification ensemble : this.ensembles)
        {
            if (addComma)
            {
                condition.append(",");
            }
            else
            {
                addComma = true;
            }
            
            condition.append(ensemble.getID());
        }
        
        condition.append("}'::int[])");
	    
	    return condition.toString();
	}
	
	/**
	 * @return A list of all ensemble specification
	 */
	public List<EnsembleSpecification> getEnsembles()
	{
		if (this.ensembles == null)
		{
			this.ensembles = new ArrayList<>();
		}
		return ensembles;
	}
	
	/**
	 * @return A list of all variable specifications
	 */
	public List<VariableSpecification> getVariables()
	{
		if (this.variables == null)
		{
			this.variables = new ArrayList<>();
		}
		return this.variables;
	}
	
	public Integer getFirstVariablePositionID() throws Exception {
	    Integer ID = null;
	    FeatureSpecification feature = getFeature();
	    
	    try
	    {
    	    if (feature != null) {
    	        VariableSpecification firstVariable = this.getVariable();
    	        if (firstVariable != null && firstVariable.getVariableID() != null)
    	        {
    	            List<Integer> variablePositionIDs = feature.getVariablePositionIDs(firstVariable.getVariableID());
    	            if (variablePositionIDs.size() > 0) {
    	                ID = variablePositionIDs.get(0);
    	            }
    	        }
    	    }
	    }
	    catch(Exception error)
	    {
	        Debug.error(LOGGER, error);
	        throw error;
	    }
	    return ID;
	}
	
	public String getMeasurementUnit() {
	    String unit = null;
	    VariableSpecification firstVariable = getVariable();
	    
	    if (firstVariable != null)
	    {
	        unit = firstVariable.getUnit();
	    }
	    
	    return unit;
	}
	
	public String getTimeOffset() {
	    String offset = null;
	    
	    if (this.conditions != null && this.conditions.hasOffset()) {
	        offset = this.conditions.getOffset();
	    }
	    
	    return offset;
	}
	
	public String getEarliestDate() {
	    String earliestDate = null;
	    
	    if (this.conditions != null && this.conditions.hasEarliestDate()) {
	        earliestDate = this.conditions.getEarliestDate();
	    }
	    
	    return earliestDate;
	}
	
	public String getLatestDate() {
	    String latestDate = null;
	    
	    if (this.conditions != null && this.conditions.hasLatestDate()) {
	        latestDate = this.conditions.getLatestDate();
	    }
	    
	    return latestDate;
	}
	
	public String getMinimumValue() {
	    String minimum = null;
	    
	    if (this.conditions != null && this.conditions.hasMinimumValue()) {
	        minimum = this.conditions.getMinimumValue();
	    }
	    
	    return minimum;
	}
	
	public String getMaximumValue() {
	    String maximum = null;
	    
	    if (this.conditions != null && this.conditions.hasMaximumValue()) {
	        maximum = this.conditions.getMaximumValue();
	    }
	    
	    return maximum;
	}
	
	/**
	 * Returns the variable at a specific position, if there is one there
	 * @param index The position of the variable to retrieve
	 * @return The variable specification at a specific position. <b>null</b> otherwise.
	 */
	public VariableSpecification getVariable(int index)
	{
	    VariableSpecification variable = null;
	    
	    if (this.variables == null)
	    {
	        this.variables = new ArrayList<>();
	    }
	    
	    if (index < this.variables.size())
	    {
	        variable = this.variables.get(index);
	    }
	    
	    return variable;
	}
	
	/**
	 * @return The first defined variable, if there is one. <b>null</b> otherwise
	 */
	public VariableSpecification getVariable()
	{
	    return getVariable(0);
	}
	
	/**
	 * @return Returns the number of specified variables
	 */
	public int variableCount()
	{
		return getVariables().size();
	}
	
	/**
	 * Adds a specification for a variable for use in verification
	 * @param variable The specification to add
	 */
	public void addVariable(VariableSpecification variable)
	{
		if (variable == null)
		{
			return;
		}
		
		getVariables().add(variable);
	}
	
	public boolean isForecast()
	{
	    return this.isForecast;
	}
	
	@Override
	public String toString() {
		
		StringBuilder description = new StringBuilder();
		if (variableCount() == 1)
		{
			VariableSpecification variable = this.variables.get(0);
			description = new StringBuilder("Variable: ");
			description.append(String.valueOf(variable.name()));
			description.append(", measured in ");
			description.append(String.valueOf(variable.getUnit()));
			description.append(System.lineSeparator());
		}
		else if (variableCount() > 1)
		{
			description.append("Variables:");
			description.append(System.lineSeparator());
			description.append(System.lineSeparator());
			for (VariableSpecification variable : this.variables)
			{
				description.append("\tVariable: ");
				description.append(String.valueOf(variable.name()));
				description.append(", measured in ");
				description.append(String.valueOf(variable.getUnit()));
				description.append(System.lineSeparator());
			}
		}
		
		if (directoryCount() > 0)
		{
			description.append("Directories: ");
			description.append(System.lineSeparator());
			description.append(System.lineSeparator());
			for (DirectorySpecification directory : directories)
			{
				description.append(directory);
			}
		}
		
		if (lazyLoad != null && lazyLoad)
		{
			description.append("Data will only be loaded if it does not exist within the database.");
		}
		else if (lazyLoad != null)
		{
			description.append("Data will be loaded in full each time.");
		}
		
		description.append(System.lineSeparator());
		description.append(System.lineSeparator());
		
		if (loadAllFeatures)
		{
			description.append("All Features will be considered.");
			description.append(System.lineSeparator());
		}
		else if (featureCount() > 0)
		{
			description.append("The following features will be considered:");
			description.append(System.lineSeparator());
			for (FeatureSpecification feature : features)
			{
				description.append(feature.toString());
			}
		}
		description.append(System.lineSeparator());
		
		if (loadAllEnsembles)
		{
			description.append("All found ensembles will be considered.");
			description.append(System.lineSeparator());
		}
		else if (ensembleCount() > 0)
		{
			description.append("The following ensembles will be considered:");
			description.append(System.lineSeparator());
			for (EnsembleSpecification ensemble : ensembles)
			{
				description.append(ensemble.toString());
			}
		}
		
		if (conditions != null)
		{
			description.append(System.lineSeparator());
			description.append(conditions.toString());
		}
		
		description.append(System.lineSeparator());
		
		return description.toString();
	}

	private boolean loadAllFeatures;
	private List<DirectorySpecification> directories;
	private ConditionSpecification conditions;
	private boolean loadAllEnsembles;
	private List<EnsembleSpecification> ensembles;
	private Boolean lazyLoad;
	private List<VariableSpecification> variables;
	private List<FeatureSpecification> features;
	private boolean isForecast;
	private String dataSourceIdentifier;
    
    // TODO: Parse source information out of the configuration
    private List<String> sources;

    @Override
    public String toXML()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
