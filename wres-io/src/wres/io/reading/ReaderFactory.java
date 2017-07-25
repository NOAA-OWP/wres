package wres.io.reading;

import wres.config.generated.Format;
import wres.io.reading.fews.FEWSSource;
import wres.io.reading.ucar.NetCDFSource;
import wres.util.Internal;

import java.io.IOException;

/**
 * @author ctubbs
 *
 */
@Internal(exclusivePackage = "wres.io")
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

	public static SourceType getFileType (Format fileFormat)
    {
        SourceType type;

        String formatType = "";

        if (fileFormat != null)
        {
            formatType = fileFormat.toString().toLowerCase();
        }

        if (formatType.equalsIgnoreCase("PI-XML"))
        {
            type = SourceType.PI_XML;
        }
        else if (formatType.equalsIgnoreCase("datacard"))
        {
            type = SourceType.DATACARD;
        }
        else if (formatType.equalsIgnoreCase("netcdf"))
        {
            type = SourceType.NETCDF;
        }
        else if (formatType.equalsIgnoreCase("archive"))
        {
            type = SourceType.ARCHIVE;
        }
        else
        {
            type = SourceType.UNDEFINED;
        }

        return type;
    }
}
