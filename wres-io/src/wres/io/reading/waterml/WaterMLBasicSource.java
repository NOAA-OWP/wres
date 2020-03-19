package wres.io.reading.waterml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.io.concurrency.TimeSeriesIngester.GEO_ID_TYPE.GAGE_ID;

import wres.config.generated.ProjectConfig;
import wres.datamodel.time.TimeSeries;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.PreIngestException;
import wres.io.reading.WebClient;
import wres.system.DatabaseLockManager;
import wres.util.Strings;

/**
 * Adapter from BasicSource to WaterMLSource (to fit pattern in ReaderFactory).
 */

public class WaterMLBasicSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WaterMLBasicSource.class );
    private static final WebClient WEB_CLIENT = new WebClient();
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );

    private static final String MD5SUM_OF_EMPTY_STRING = "68b329da9893e34099c7d8ad5cb9c940";
    private final DatabaseLockManager lockManager;

    public WaterMLBasicSource( ProjectConfig projectConfig,
                               DataSource dataSource,
                               DatabaseLockManager lockManager )
    {
        super( projectConfig, dataSource );
        Objects.requireNonNull( lockManager );
        this.lockManager = lockManager;
    }

    @Override
    protected List<IngestResult> saveObservation() throws IOException
    {
        return this.ingest();
    }

    private List<IngestResult> ingest() throws IOException
    {
        InputStream data;
        Instant now = Instant.now();
        URI location = this.getDataSource()
                           .getUri();

        if ( location.getScheme()
                     .equals( "file" ) )
        {
            data = this.getFromFile( location );
        }
        else if ( location.getScheme()
                          .toLowerCase()
                          .startsWith( "http" ) )
        {
            Pair<Integer,InputStream> response = WEB_CLIENT.getFromWeb( location );
            int httpStatus = response.getLeft();
            data = response.getRight();

            if ( httpStatus == 404 )
            {
                LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                             httpStatus,
                             location );

                try
                {
                    // Cannot trust the DataSources.get() method to accurately
                    // report performedInsert(). Use other means here.
                    SourceDetails.SourceKey sourceKey =
                            new SourceDetails.SourceKey( location,
                                                         now.toString(),
                                                         null,
                                                         MD5SUM_OF_EMPTY_STRING.toUpperCase() );

                    SourceDetails details = this.createSourceDetails( sourceKey );
                    details.save();
                    boolean foundAlready = !details.performedInsert();

                    LOGGER.debug( "Found {}? {}", details, foundAlready );

                    if ( !foundAlready )
                    {
                        this.lockManager.lockSource( details.getId() );
                        SourceCompletedDetails completedDetails =
                                createSourceCompletedDetails( details );
                        completedDetails.markCompleted();
                        // A special case here, where we don't use
                        // source completer because we know there are no data
                        // rows to be inserted, therefore there will be no
                        // coordination with the use of synchronizers/latches.
                        // Therefore, plain lock and unlock here.
                        this.lockManager.unlockSource( details.getId() );

                        LOGGER.debug( "Empty source id {} marked complete.",
                                      details.getId() );
                    }

                    return IngestResult.singleItemListFrom(
                            this.projectConfig,
                            this.dataSource,
                            details.getId(),
                            foundAlready,
                            false
                    );
                }
                catch ( SQLException e )
                {
                    throw new IngestException( "Source metadata for '"
                                               + location +
                                               "' could not be stored in or retrieved from the database.",
                                               e );
                }
                finally
                {
                    if ( Objects.nonNull( data) )
                    {
                        try
                        {
                            data.close();
                        }
                        catch ( IOException ioe )
                        {
                            LOGGER.warn( "Could not close a data stream from {}",
                                         location, ioe );
                        }
                    }
                }

            }
            else if ( ! (httpStatus >= 200 && httpStatus < 300) )
            {
                throw new PreIngestException( "Failed to get data from '"
                                              + location +
                                              "' due to HTTP status code "
                                              + httpStatus );
            }
        }
        else
        {
            throw new UnsupportedOperationException( "Only file and http(s) "
                                                     + "are supported. Got: "
                                                     + location );
        }

        LOGGER.debug( "InputStream about to be read for {}", location );

        try
        {
            byte[] rawForecast = IOUtils.toByteArray( data );
            Response response = OBJECT_MAPPER.readValue( rawForecast,
                                                         Response.class );
            String hash = this.identifyUsgsData( response );
            WaterMLSource waterMLSource =
                    new WaterMLSource( this.dataSource,
                                       response,
                                       hash );
            List<TimeSeries<Double>> transformed = waterMLSource.call();
            List<IngestResult> ingestResults = new ArrayList<>( transformed.size() );

            for ( TimeSeries<Double> timeSeries : transformed )
            {
                TimeSeriesIngester ingester = TimeSeriesIngester.of( this.projectConfig,
                                                                     this.dataSource,
                                                                     this.lockManager,
                                                                     timeSeries,
                                                                     GAGE_ID );
                List<IngestResult> result = ingester.call();
                ingestResults.addAll( result );
            }

            return Collections.unmodifiableList( ingestResults );
        }
        catch ( JsonMappingException jme )
        {
            throw new PreIngestException( "Failed to parse the response body"
                                          + " from USGS url "
                                          + location,
                                          jme );
        }
        catch ( IngestException e )
        {
            throw new IngestException( "Values from USGS url "
                                       + location
                                       + " could not be ingested.",
                                       e );
        }
        finally
        {
            if ( Objects.nonNull( data) )
            {
                try 
                {   
                    data.close();
                    LOGGER.debug( "InputStream closed/ingested for {}", location );
                }
                catch ( IOException ioe )
                {
                    LOGGER.warn( "Could not close a data stream from {}",
                                 location, ioe );
                }
            }
        }
    }

    private InputStream getFromFile( URI uri ) throws FileNotFoundException
    {
        if ( !uri.getScheme().equals( "file" ) )
        {
            throw new IllegalArgumentException(
                    "Must pass a file uri, got " + uri );
        }

        Path forecastPath = Paths.get( uri );
        File forecastFile = forecastPath.toFile();
        return new FileInputStream( forecastFile );
    }



    String identifyUsgsData( Response response )
    {
        try
        {
            return Strings.getMD5Checksum( response );
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Unable to identify WaterML data from "
                                          + this.getDataSource().getUri(),
                                          ioe );
        }
    }

    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @param sourceKey the first arg to SourceDetails
     * @return a SourceDetails
     */

    SourceDetails createSourceDetails( SourceDetails.SourceKey sourceKey )
    {
        return new SourceDetails( sourceKey );
    }


    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @param sourceDetails the first arg to SourceCompletedDetails
     * @return a SourceCompleter
     */
    SourceCompletedDetails createSourceCompletedDetails( SourceDetails sourceDetails )
    {
        return new SourceCompletedDetails( sourceDetails );
    }

    @Override
    protected Logger getLogger()
    {
        return null;
    }
}
