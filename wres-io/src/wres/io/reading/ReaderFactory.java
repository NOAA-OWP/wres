package wres.io.reading;

import java.io.IOException;
import java.net.URI;

import wres.config.generated.Format;
import wres.config.generated.ProjectConfig;
import wres.io.reading.commaseparated.CSVSource;
import wres.io.reading.datacard.DatacardSource;
import wres.io.reading.fews.FEWSSource;
import wres.io.reading.nwm.NWMSource;
import wres.io.reading.s3.S3Reader;
import wres.io.reading.usgs.USGSReader;
import wres.io.reading.waterml.WaterMLBasicSource;
import wres.io.reading.wrds.WRDSSource;
import wres.system.DatabaseLockManager;
import wres.util.NetCDF;
import wres.util.Strings;

/**
 * @author ctubbs
 *
 */
public class ReaderFactory {
    private ReaderFactory(){}

    public static BasicSource getReader( ProjectConfig projectConfig,
                                         DataSource dataSource,
                                         DatabaseLockManager lockManager )
            throws IOException
	{
        Format typeOfFile = getFiletype( dataSource.getUri() );

		BasicSource source;

		switch (typeOfFile)
        {
			case DATACARD:
                source = new DatacardSource( projectConfig,
                                             dataSource,
                                             lockManager );
				break;
            case ARCHIVE:
                source = new ZippedSource( projectConfig,
                                           dataSource,
                                           lockManager );
				break;
            case NET_CDF:
                source = new NWMSource( projectConfig,
                                        dataSource,
                                        lockManager );
				break;
			case PI_XML:
                source = new FEWSSource( projectConfig,
                                         dataSource,
                                         lockManager );
				break;
            case USGS:
                source = new USGSReader( projectConfig,
                                         dataSource,
                                         lockManager );
                break;
            case S_3:
                source = S3Reader.getReader( projectConfig,
                                             dataSource,
                                             lockManager );
                break;
            case WRDS:
                source = new WRDSSource( projectConfig,
                                         dataSource,
                                         lockManager );
                break;
            case CSV:
                source = new CSVSource( projectConfig,
                                        dataSource,
                                        lockManager );
                break;
            case WATERML:
                source = new WaterMLBasicSource( projectConfig,
                                                 dataSource,
                                                 lockManager );
                break;
			default:
				String message = "The uri '%s' is not a valid source of data.";
				throw new IOException(String.format(message, dataSource.getUri()));
		}

		return source;
	}
	
    static Format getFiletype( final URI filename )
	{
        Format type;

		String pathName = filename.toString().toLowerCase();

		// Can't do switch because of the PIXML logic

        if ( filename.getScheme() != null && filename.getScheme().startsWith( "http" ) )
        {
            if ( filename.getHost()
                         .toLowerCase()
                         .contains( "usgs.gov" ) )
            {
                type = Format.WATERML;
            }
            else
            {
                type = Format.WRDS;
            }
        }
		else if (pathName.endsWith("tar.gz") || pathName.endsWith(".tgz"))
		{
            type = Format.ARCHIVE;
		}
		else if ( pathName.endsWith(".xml") ||
				  (pathName.endsWith(".xml.gz")) ||
				  Strings.contains(pathName, ".+\\.\\d+$"))
		{
            type = Format.PI_XML;
		}
		else if (filename.equals( URI.create( "usgs" ) ) )
        {
            type = Format.USGS;
        }
        else if (filename.equals( URI.create( "s3" ) ) )
        {
            type = Format.S_3;
        }
        else if ( NetCDF.isNetCDFFile(pathName ) )
        {
            type = Format.NET_CDF;
        }
        else if(pathName.endsWith( ".json" ) || filename.toASCIIString().contains( "***REMOVED***eds-app1" ))
        {
            type = Format.WRDS;
        }
        else if (pathName.endsWith( ".csv" ))
        {
            type = Format.CSV;
        }
		else
		{
            type = Format.DATACARD;
		}

		return type;
	}
}
