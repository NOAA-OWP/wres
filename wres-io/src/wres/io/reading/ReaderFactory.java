package wres.io.reading;

import wres.io.reading.fews.FEWSSource;
import wres.io.reading.fews.ZippedSource;
import wres.io.reading.ucar.NetCDFSource;

import java.io.IOException;

/**
 * @author ctubbs
 *
 */
public class ReaderFactory {
    private ReaderFactory(){}
    
	public static BasicSource getReader(String filename) throws IOException
	{
		SourceType type_of_file = getFiletype(filename);
		
		BasicSource source = null;

		switch (type_of_file) {
			case DATACARD:
				// TODO: Implement new Datacard reader that adheres to new schema
				//source = new wres.io.reading.nws.DatacardSource(filename);
				break;
			case ASCII:
				// TODO: Implement new ASCII reader that adheres to new schema
				//source = new wres.io.reading.misc.ASCIISource(filename);
				break;
            case ARCHIVE:
                source = new ZippedSource(filename);
                break;
			case NETCDF:
				source = new NetCDFSource(filename);
				break;
			case PI_XML:
				source = new FEWSSource(filename);
				break;
			default:
				String message = "The file '%s' is not a valid data file.";
				throw new IOException(String.format(message, filename));
		}
		
		return source;
	}
	
	public static SourceType getFiletype(String filename)
	{
		SourceType type;
		
		filename = filename.toLowerCase();
		if (filename.endsWith(".asc"))
		{
			type = SourceType.ASCII;
		}
		else if (filename.endsWith("tar.gz") || filename.endsWith(".tgz"))
		{
			type = SourceType.ARCHIVE;
		}
		else if (filename.endsWith(".nc") || filename.endsWith(".gz"))
		{
			type = SourceType.NETCDF;
		}
		else if (filename.endsWith(".xml"))
		{
			type = SourceType.PI_XML;
		}
		else
		{
			type = SourceType.DATACARD;
		}

		return type;
	}
}
