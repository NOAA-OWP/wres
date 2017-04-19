/**
 * 
 */
package config.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import util.Utilities;

/**
 * @author ctubbs
 *
 */
public class ProjectDataSource extends ConfigElement {

	/**
	 * @param reader
	 */
	public ProjectDataSource(XMLStreamReader reader) 
	{
		super(reader);
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("observations", "forecasts");
	}
	
	@Override
	protected void getAttributes(XMLStreamReader reader) {
		for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex)
		{
			if (reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase("lazy_load"))
			{
				this.lazyLoad = Boolean.valueOf(reader.getAttributeValue(attributeIndex));
			}
		}
	}

	/* (non-Javadoc)
	 * @see config.data.ConfigElement#interpret(javax.xml.stream.XMLStreamReader)
	 */
	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException 
	{		
		if (tagIs(reader, "directories"))
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
		else if(tagIs(reader, "conditions"))
		{
			conditions = new Conditions(reader);
		}
		else if (tagIs(reader, "variable"))
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
		else if (tagIs(reader, "ensembles"))
		{
			parseEnsembles(reader);
		}
		else if (tagIs(reader, "features"))
		{
			parseFeatures(reader);
		}
	}
	
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
			
			ClauseConfig feature = null;			
			
			if (tagIs(reader, "feature"))
			{
				feature = new Location(reader);
			}
			else if (tagIs(reader, "range"))
			{
				feature = new Range(reader);
			}
			else if (tagIs(reader, "polygon"))
			{
				feature = new Polygon(reader);
			}
			else if (tagIs(reader, "point"))
			{
				feature = new Point(reader);
			}
			
			addFeature(feature);
		}	
	}

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
	
	private void addEnsemble(Ensemble ensemble)
	{
		if (ensemble == null || (ensemble.getEnsembleName().isEmpty() && 
								 ensemble.getEnsemblememberID().isEmpty() && 
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
	
	public int ensembleCount()
	{
		return getEnsembles().size();
	}
	
	private void addFeature(ClauseConfig feature)
	{
		if (feature == null)
		{
			return;
		}
		
		if (this.features == null)
		{
			this.features = new ArrayList<ClauseConfig>();
		}
		
		this.features.add(feature);
	}
	
	public int featureCount()
	{
		if (this.features == null)
		{
			this.features = new ArrayList<ClauseConfig>();
		}
		
		return this.features.size();
	}
	
	public List<Directory> getDirectories()
	{
		if (this.directories == null)
		{
			this.directories = new ArrayList<Directory>();
		}
		return directories;
	}
	
	public int directoryCount()
	{
		return getDirectories().size();
	}
	
	public List<ClauseConfig> getFeatures()
	{
		if (this.features == null)
		{
			this.features = new ArrayList<ClauseConfig>();
		}
		return this.features;
	}
	
	public Conditions conditions()
	{
		return conditions;
	}
	
	public boolean loadAllFeatures()
	{
		return loadAllFeatures;
	}
	
	public boolean loadAllEnsembles()
	{
		return loadAllEnsembles;
	}
	
	public boolean loadLazily()
	{
		return lazyLoad;
	}
	
	public List<Ensemble> getEnsembles()
	{
		if (this.ensembles == null)
		{
			this.ensembles = new ArrayList<Ensemble>();
		}
		return ensembles;
	}
	
	public List<Variable> getVariables()
	{
		if (this.variables == null)
		{
			this.variables = new ArrayList<Variable>();
		}
		return this.variables;
	}
	
	public int variableCount()
	{
		return getVariables().size();
	}
	
	public void addVariable(Variable variable)
	{
		if (variable == null)
		{
			return;
		}
		
		getVariables().add(variable);
	}
	
	@Override
	public String toString() {
		
		String description = "";
		if (variableCount() == 1)
		{
			Variable variable = this.variables.get(0);
			description = "Variable: ";
			description += String.valueOf(variable.getName());
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
				description += String.valueOf(variable.getName());
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
			for (ClauseConfig feature : features)
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
	private List<ClauseConfig> features;
}
