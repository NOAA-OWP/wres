/**
 * 
 */
package wres.reading;


/**
 * @author ctubbs
 *
 */
public class SourceFactory {
	public static BasicSource get_source(String filename) throws Exception
	{
		SourceType type_of_file = get_filetype(filename);
		
		BasicSource source;
		
		switch (type_of_file)
		{
		case DATACARD:
			source = new wres.reading.nws.DatacardSource(filename);
			break;
		case ASCII:
			source = new wres.reading.misc.ASCIISource(filename);
			break;
		default:
			String message = "The file '%s' is not a valid data file.";
			throw new Exception(String.format(message, filename));
		}
		
		return source;
	}
	
	private static SourceType get_filetype(String filename)
	{
		SourceType type = SourceType.UNDEFINED;
		
		filename = filename.toLowerCase();
		if (filename.endsWith(".asc"))
		{
			type = SourceType.ASCII;
		}
		if (filename.endsWith("map06"))
		{
			type = SourceType.DATACARD;
		}

		return type;
	}
}
