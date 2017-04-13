/**
 * 
 */
package config.data;

import java.util.ArrayList;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import collections.ThreeTuple;
import collections.TwoTuple;

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
	protected String tag_name() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see config.data.ConfigElement#interpret(javax.xml.stream.XMLStreamReader)
	 */
	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException 
	{

	}
	
	public ArrayList<Directory> get_directories()
	{
		return directories;
	}
	
	public Conditions conditions()
	{
		return conditions;
	}
	
	public boolean load_all_ensembles()
	{
		return load_all_ensembles;
	}
	
	public boolean load_lazily()
	{
		return lazy_load;
	}
	
	public ArrayList<Ensemble> get_ensembles()
	{
		return ensembles;
	}

	private final ArrayList<Directory> directories = new ArrayList<Directory>();
	private Conditions conditions;
	private boolean load_all_ensembles;
	private final ArrayList<Ensemble> ensembles = new ArrayList<Ensemble>();
	private boolean lazy_load;
	private String variable = null;

}
