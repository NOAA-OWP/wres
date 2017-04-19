/**
 * 
 */
package config.data;

import collections.ThreeTuple;

/**
 * @author ctubbs
 *
 */
public class OutputType extends ThreeTuple<Boolean, String, String> {

	/**
	 * 
	 * @param shouldSave Whether or not this type of output should be saved out
	 * @param path The path to write to
	 * @param fileFormat the format to save the file to
	 */
	public OutputType(Boolean shouldSave, String path, String fileFormat) {
		super(shouldSave, path, fileFormat);
	}

	public Boolean shouldSave()
	{
		return get_item_one();
	}
	
	public String path()
	{
		return get_item_two();
	}
	
	public String fileFormat()
	{
		return get_item_three();
	}
	
	@Override
	public String toString() {
		String description = "Format: ";
		description += fileFormat();
		description += ", ";
		
		if (shouldSave())
		{
			description += "Saving at: '";
			description += path();
			description += "'";
		}
		else
		{
			description += "Not Saving";
		}
		
		description += System.lineSeparator();
		
		return description;
	}
}
