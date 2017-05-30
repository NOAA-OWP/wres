package wres.io.reading;


/**
 * @author ctubbs
 *
 */
public class ReaderFactory {
    private ReaderFactory(){}
    
	public static BasicSource getReader(String filename) throws Exception
	{
		SourceType type_of_file = getFiletype(filename);
		
		BasicSource source = null;
		
		switch (type_of_file)
		{
		case DATACARD:
		    // TODO: Implement new Datacard reader that adheres to new schema
			//source = new wres.io.reading.nws.DatacardSource(filename);
			break;
		case ASCII:
		    // TODO: Implement new ASCII reader that adheres to new schema
			//source = new wres.io.reading.misc.ASCIISource(filename);
			break;
		case NETCDF:
			source = new wres.io.reading.ucar.NetCDFSource(filename);
			break;
		case PI_XML:
			source = new wres.io.reading.fews.FEWSSource(filename);
			break;
		default:
			String message = "The file '%s' is not a valid data file.";
			throw new Exception(String.format(message, filename));
		}
		
		return source;
	}
	
	private static SourceType getFiletype(String filename)
	{
		SourceType type = SourceType.UNDEFINED;
		
		filename = filename.toLowerCase();
		if (filename.endsWith(".asc"))
		{
			type = SourceType.ASCII;
		}
		else if (filename.endsWith("map06"))
		{
			type = SourceType.DATACARD;
		}
		else if (filename.endsWith(".nc") || filename.endsWith(".gz"))
		{
			type = SourceType.NETCDF;
		}
		else if (filename.endsWith(".xml"))
		{
			type = SourceType.PI_XML;
		}

		return type;
	}
}
