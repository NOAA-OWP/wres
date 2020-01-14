package wres.io.reading;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.reading.wrds.nwm.NwmRootDocument;
import wres.system.DatabaseLockManager;

/**
 * Reads and ingests NWM data from WRDS NWM API.
 *
 * One per NWM URI to ingest. Creates and submits multiple TimeSeriesIngester
 * instances.
 *
 * Work in progress as of 2020-01-14.
 */

public class WrdsNwmReader implements Callable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsNwmReader.class );
    private static final WebClient WEB_CLIENT = new WebClient();
    private final ObjectMapper jsonObjectMapper;
    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;

    public WrdsNwmReader(  ProjectConfig projectConfig,
                           DataSource dataSource,
                           DatabaseLockManager lockManager )
    {
        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.lockManager = lockManager;
        this.jsonObjectMapper = new ObjectMapper()
                .registerModule( new JavaTimeModule() );
    }

    private ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    private DataSource getDataSource()
    {
        return this.dataSource;
    }

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    private ObjectMapper getJsonObjectMapper()
    {
        return this.jsonObjectMapper;
    }

    private URI getUri()
    {
        return this.getDataSource()
                   .getUri();
    }

    @Override
    public List<IngestResult> call()
    {
        NwmRootDocument document;

        try
        {
            InputStream dataStream = WEB_CLIENT.getFromWeb( this.getUri() )
                                               .getRight();
            document = this.getJsonObjectMapper()
                           .readValue( dataStream,
                                       NwmRootDocument.class );
            LOGGER.info( "Parsed this document: {}", document );
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Failed to read NWM data at "
                                          + this.getDataSource().getUri(),
                                          ioe );
        }

        // TODO: Transform deserialized JSON object tree into wres timeseries
        // TODO: ingest wres timeseries
        return Collections.emptyList();
    }
}
