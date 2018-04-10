package wres.io.reading;

import java.io.IOException;
import java.nio.file.Paths;

import wres.config.generated.Format;
import wres.config.generated.ProjectConfig;
import wres.io.reading.datacard.DatacardSource;
import wres.io.reading.fews.FEWSSource;
import wres.io.reading.nwm.NWMSource;
import wres.io.reading.usgs.USGSReader;
import wres.util.NetCDF;
import wres.util.Strings;

/**
 * @author ctubbs
 *
 */
public class ReaderFactory {
    private ReaderFactory(){}

    public static BasicSource getReader( ProjectConfig projectConfig,
                                         String filename )
            throws IOException
	{
        Format typeOfFile = getFiletype( filename );

		BasicSource source = null;

		switch (typeOfFile)
        {
			case DATACARD:
                source = new DatacardSource( projectConfig,
                                             filename );
				break;
            case ARCHIVE:
                source = new ZippedSource( projectConfig,
                                           filename );
				break;
            case NET_CDF:
                source = new NWMSource( projectConfig,
                                        filename );
				break;
			case PI_XML:
                source = new FEWSSource( projectConfig,
                                         filename );
				break;
            case USGS:
                source = new USGSReader( projectConfig );
                break;
			default:
				String message = "The file '%s' is not a valid data file.";
				throw new IOException(String.format(message, filename));
		}
		
		return source;
	}
	
    public static Format getFiletype( String filename )
	{
        Format type;

		filename = Paths.get(filename).getFileName().toString().toLowerCase();

		// Can't do switch because of the PIXML logic

		if (filename.endsWith("tar.gz") || filename.endsWith(".tgz"))
		{
            type = Format.ARCHIVE;
		}
		// Should we change ".+\\.\\d+$" to ".+\\.xml\\.\\d+$"? There's nothing
		// currently in that regex saying that is an xml file
		else if ( filename.endsWith(".xml") ||
				  (filename.endsWith(".xml.gz")) ||
				  Strings.contains(filename, ".+\\.\\d+$"))
		{
            type = Format.PI_XML;
		}
		else if (filename.equalsIgnoreCase( "usgs" ))
        {
            type = Format.USGS;
        }
        else if ( NetCDF.isNetCDFFile(filename ) )
        {
            type = Format.NET_CDF;
        }
		else
		{
            type = Format.DATACARD;
		}

		return type;
	}
}
