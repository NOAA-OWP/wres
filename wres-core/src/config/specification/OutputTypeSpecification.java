/**
 * 
 */
package config.specification;

/**
 * Specifications for how results need to be output
 * @author Christopher Tubbs
 */
public class OutputTypeSpecification {

	/**
	 * Constructor
	 * @param shouldSave Whether or not this type of output should be saved out
	 * @param path The path to write to
	 * @param fileFormat the format to save the file to
	 */
	public OutputTypeSpecification(Boolean shouldSave, String path, String fileFormat) {
		this.shouldSave = shouldSave;
		this.path = path;
		this.fileFormat = fileFormat;
	}

	/**
	 * @return Whether or not this output should be save to the file system on the next run
	 */
	public Boolean shouldSave()
	{
		return this.shouldSave && 
		       this.path != null && !this.path.isEmpty() && 
		       this.fileFormat != null && !this.fileFormat.isEmpty();
	}
	
	/**
	 * @return The path where the output should be saved
	 */
	public String path()
	{
		return this.path;
	}
	
	/**
	 * @return The format in which the file should be saved
	 */
	public String fileFormat()
	{
		return this.fileFormat;
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
	
	private final boolean shouldSave;
	private final String path;
	private final String fileFormat;
}
