/**
 * 
 */
package config.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import data.caching.EnsembleCache;
import util.Utilities;

/**
 * Specifies information about where to load data for a project
 * 
 * @author Christopher Tubbs
 */
public class ProjectDataSource extends ConfigElement {

	/**
	 * Constructor
	 * @param reader The xml reader that contains the specifications for the data source
	 */
	public ProjectDataSource(XMLStreamReader reader) 
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
			if (reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase("lazy_load"))
			{
				this.lazyLoad = Boolean.valueOf(reader.getAttributeValue(attributeIndex));
			}
			else if (reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase("is_forecast"))
			{
			    this.isForecast = Boolean.valueOf(reader.getAttributeValue(attributeIndex));
			}
		}
	}

	/* (non-Javadoc)
	 * @see config.data.ConfigElement#interpret(javax.xml.stream.XMLStreamReader)
	 */
	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException 
	{		
		if (Utilities.tagIs(reader, "directories"))
		{
			while (reader.hasNext())
			{
				next(reader);
				if (Utilities.xmlTagClosed(reader, Arrays.asList("directories")))
				{
					break;
				}
				
				if (reader.isStartElement())
				{
					addDirectory(new Directory(reader));
				}
			}
		}
		else if(Utilities.tagIs(reader, "conditions"))
		{
			conditions = new Conditions(reader);
		}
		else if (Utilities.tagIs(reader, "variable"))
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
			addVariable(new Variable(variableName, variableUnit));
		}
		else if (Utilities.tagIs(reader, "ensembles"))
		{
			parseEnsembles(reader);
		}
		else if (Utilities.tagIs(reader, "features"))
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
			if (Utilities.xmlTagClosed(reader, Arrays.asList("features")))
			{
				break;
			}
			else if (!reader.isStartElement())
			{
				continue;
			}
			
			FeatureSelector feature = null;			
			
			if (Utilities.tagIs(reader, "feature"))
			{
				feature = new Location(reader);
			}
			else if (Utilities.tagIs(reader, "range"))
			{
				feature = new Range(reader);
			}
			else if (Utilities.tagIs(reader, "polygon"))
			{
				feature = new Polygon(reader);
			}
			else if (Utilities.tagIs(reader, "point"))
			{
				feature = new Point(reader);
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
			if (Utilities.xmlTagClosed(reader, Arrays.asList("ensembles")))
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
			
			addEnsemble(new Ensemble(name, memberID, qualifier));
		}
	}
	
	/**
	 * Adds a directory specification to the datasource
	 * @param directory The directory to add
	 */
	private void addDirectory(Directory directory)
	{
		if (directory == null)
		{
			return;
		}
		
		if (this.directories == null)
		{
			this.directories = new ArrayList<Directory>();
		}
		
		this.directories.add(directory);
	}
	
	/**
	 * Adds an ensemble specification to the data source
	 * @param ensemble The ensemble specification to add
	 */
	private void addEnsemble(Ensemble ensemble)
	{
		if (ensemble == null || (ensemble.getName().isEmpty() && 
								 ensemble.getMemberID().isEmpty() && 
								 ensemble.getQualifier().isEmpty()))
		{
			return;
		}
		
		if (this.ensembles == null)
		{
			this.ensembles = new ArrayList<Ensemble>();
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
	private void addFeature(FeatureSelector feature)
	{
		if (feature == null)
		{
			return;
		}
		
		if (this.features == null)
		{
			this.features = new ArrayList<FeatureSelector>();
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
			this.features = new ArrayList<FeatureSelector>();
		}
		
		return this.features.size();
	}
	
	/**
	 * @return A list of stored directory specifications
	 */
	public List<Directory> getDirectories()
	{
		if (this.directories == null)
		{
			this.directories = new ArrayList<Directory>();
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
	public List<FeatureSelector> getFeatures()
	{
		if (this.features == null)
		{
			this.features = new ArrayList<FeatureSelector>();
		}
		return this.features;
	}
	
	/**
	 * Retrieves the feature specification at the given position
	 * @param index The position to retrieve the feature specification from
	 * @return The feature specification at the given otherwise, if it exists. <b>null</b> otherwise.
	 */
	public FeatureSelector getFeature(int index)
	{
	    FeatureSelector feature = null;
	    
	    if (this.features == null)
	    {
	        this.features = new ArrayList<FeatureSelector>();
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
	public FeatureSelector getFeature()
	{
	    return getFeature(0);
	}
	
	/**
	 * @return The conditions imposed upon this data
	 */
	public Conditions conditions()
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
	
	public String getEnsembleCondition() throws Exception
	{
	    String condition = "ANY('{";

        boolean addComma = false;
        for (Ensemble ensemble : this.ensembles)
        {
            if (addComma)
            {
                condition += ",";
            }
            else
            {
                addComma = true;
            }
            
            condition += ensemble.getID();
        }
        
        condition += "}'::int[])";
	    
	    return condition;
	}
	
	/**
	 * @return A list of all ensemble specification
	 */
	public List<Ensemble> getEnsembles()
	{
		if (this.ensembles == null)
		{
			this.ensembles = new ArrayList<Ensemble>();
		}
		return ensembles;
	}
	
	/**
	 * @return A list of all variable specifications
	 */
	public List<Variable> getVariables()
	{
		if (this.variables == null)
		{
			this.variables = new ArrayList<Variable>();
		}
		return this.variables;
	}
	
	public Integer getFirstVariablePositionID() throws Exception {
	    Integer ID = null;
	    FeatureSelector feature = getFeature();
	    
	    if (feature != null) {
	        Variable firstVariable = this.getVariable();
	        if (firstVariable != null && firstVariable.getVariableID() != null)
	        {
	            List<Integer> variablePositionIDs = feature.getVariablePositionIDs(firstVariable.getVariableID());
	            if (variablePositionIDs.size() > 0) {
	                ID = variablePositionIDs.get(0);
	            }
	        }
	    }
	    
	    return ID;
	}
	
	public String getMeasurementUnit() {
	    String unit = null;
	    Variable firstVariable = getVariable();
	    
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
	public Variable getVariable(int index)
	{
	    Variable variable = null;
	    
	    if (this.variables == null)
	    {
	        this.variables = new ArrayList<Variable>();
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
	public Variable getVariable()
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
	public void addVariable(Variable variable)
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
		
		String description = "";
		if (variableCount() == 1)
		{
			Variable variable = this.variables.get(0);
			description = "Variable: ";
			description += String.valueOf(variable.name());
			description += ", measured in ";
			description += String.valueOf(variable.getUnit());
			description += System.lineSeparator();
		}
		else if (variableCount() > 1)
		{
			description += "Variables:";
			description += System.lineSeparator();
			description += System.lineSeparator();
			for (Variable variable : this.variables)
			{
				description += "\tVariable: ";
				description += String.valueOf(variable.name());
				description += ", measured in ";
				description += String.valueOf(variable.getUnit());
				description += System.lineSeparator();
			}
		}
		
		if (directoryCount() > 0)
		{
			description += "Directories: ";
			description += System.lineSeparator();
			description += System.lineSeparator();
			for (Directory directory : directories)
			{
				description += directory;
			}
		}
		
		if (lazyLoad != null && lazyLoad)
		{
			description += "Data will only be loaded if it does not exist within the database.";
		}
		else if (lazyLoad != null)
		{
			description += "Data will be loaded in full each time.";
		}
		
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		if (loadAllFeatures)
		{
			description += "All Features will be considered.";
			description += System.lineSeparator();
		}
		else if (featureCount() > 0)
		{
			description += "The following features will be considered:";
			description += System.lineSeparator();
			for (FeatureSelector feature : features)
			{
				description += feature.toString();
			}
		}
		description += System.lineSeparator();
		
		if (loadAllEnsembles)
		{
			description += "All found ensembles will be considered.";
			description += System.lineSeparator();
		}
		else if (ensembleCount() > 0)
		{
			description += "The following ensembles will be considered:";
			description += System.lineSeparator();
			for (Ensemble ensemble : ensembles)
			{
				description += ensemble.toString();
			}
		}
		
		if (conditions != null)
		{
			description += System.lineSeparator();
			description += conditions.toString();
		}
		
		description += System.lineSeparator();
		
		return description;
	}

	private boolean loadAllFeatures;
	private List<Directory> directories;
	private Conditions conditions;
	private boolean loadAllEnsembles;
	private List<Ensemble> ensembles;
	private Boolean lazyLoad;
	private List<Variable> variables;
	private List<FeatureSelector> features;
	private boolean isForecast;
    
    // TODO: Parse source information out of the configuration
    private List<String> sources;

    @Override
    public String toXML()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
