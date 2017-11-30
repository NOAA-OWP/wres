package wres.io.reading;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import wres.config.generated.Format;
import wres.io.reading.fews.FEWSSource;
import wres.io.reading.nwm.NWMSource;
import wres.io.reading.usgs.USGSReader;
import wres.io.reading.datacard.DatacardSource;
import wres.util.Internal;
import wres.util.NetCDF;
import wres.util.Strings;

/**
 * @author ctubbs
 *
 */
@Internal(exclusivePackage = "wres.io")
public class ReaderFactory {
    private ReaderFactory(){}
    
	public static BasicSource getReader(String filename) throws IOException
	{
		SourceType typeOfFile = getFiletype(filename);
		
		BasicSource source = null;

		switch (typeOfFile)
        {
			case DATACARD:
				source = new DatacardSource(filename);
				break;
            case ARCHIVE:
                source = new ZippedSource(filename);
                break;
			case NETCDF:
				source = new NWMSource( filename);
				break;
			case PI_XML:
				source = new FEWSSource(filename);
				break;
            case USGS:
                source = new USGSReader();
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

		filename = Paths.get(filename).getFileName().toString().toLowerCase();

		// Can't do switch because of the PIXML logic

		if (filename.endsWith("tar.gz") || filename.endsWith(".tgz"))
		{
			type = SourceType.ARCHIVE;
		}
		else if ( filename.endsWith(".xml") || Strings.contains(filename, ".+\\.\\d+$"))
		{
			type = SourceType.PI_XML;
		}
		else if (filename.equalsIgnoreCase( "usgs" ))
        {
            type = SourceType.USGS;
        }
        else if ( NetCDF.isNWMData(filename ) )
        {
            type = SourceType.NETCDF;
        }
		else
		{
			type = SourceType.DATACARD;
		}

		return type;
	}

	public static SourceType getFileType (Format fileFormat)
    {
    	if (fileFormat == null)
		{
			return SourceType.UNDEFINED;
		}

        SourceType type;

        switch (fileFormat)
		{
			case PI_XML:
				type = SourceType.PI_XML;
				break;
            case DATACARD:
                type = SourceType.DATACARD;
                break;
            case NET_CDF:
                type = SourceType.NETCDF;
                break;
            case ARCHIVE:
                type = SourceType.ARCHIVE;
                break;
            case USGS:
                type = SourceType.USGS;
                break;
            default:
                type = SourceType.UNDEFINED;
		}

        return type;
    }
}
