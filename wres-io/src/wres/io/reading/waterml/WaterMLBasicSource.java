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
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.PreIngestException;
import wres.io.utilities.WebClient;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;
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
    private final SystemSettings systemSettings;
    private final Database database;
    private final Features featuresCache;
    private final Variables variablesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final DatabaseLockManager lockManager;

    public WaterMLBasicSource( SystemSettings systemSettings,
                               Database database,
                               Features featuresCache,
                               Variables variablesCache,
                               Ensembles ensemblesCache,
                               MeasurementUnits measurementUnitsCache,
                               ProjectConfig projectConfig,
                               DataSource dataSource,
                               DatabaseLockManager lockManager )
    {
        super( projectConfig, dataSource );
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( database );
        Objects.requireNonNull( featuresCache );
        Objects.requireNonNull( variablesCache );
        Objects.requireNonNull( ensemblesCache );
        Objects.requireNonNull( measurementUnitsCache );
        Objects.requireNonNull( lockManager );
        this.systemSettings = systemSettings;
        this.database = database;
        this.featuresCache = featuresCache;
        this.variablesCache = variablesCache;
        this.ensemblesCache = ensemblesCache;
        this.measurementUnitsCache = measurementUnitsCache;
        this.lockManager = lockManager;
    }

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private Database getDatabase()
    {
        return this.database;
    }

    private Features getFeaturesCache()
    {
        return this.featuresCache;
    }

    private Variables getVariablesCache()
    {
        return this.variablesCache;
    }

    private Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    private MeasurementUnits getMeasurementUnitsCache()
    {
        return this.measurementUnitsCache;
    }

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    @Override
    protected List<IngestResult> saveObservation() throws IOException
    {
        return this.ingest();
    }

    private SourceDetails saveLackOfData(URI location, int httpStatus) throws IOException {
        LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                httpStatus,
                location );

        try
        {
            // Cannot trust the DataSources.get() method to accurately
            // report performedInsert(). Use other means here.
            SourceDetails.SourceKey sourceKey =
                    new SourceDetails.SourceKey( location,
                            Instant.now().toString(),
                            null,
                            MD5SUM_OF_EMPTY_STRING.toUpperCase() );

            SourceDetails details = this.createSourceDetails( sourceKey );
            Database database = this.getDatabase();
            details.save( database );
            boolean foundAlready = !details.performedInsert();

            LOGGER.debug( "Found {}? {}", details, foundAlready );

            if ( !foundAlready )
            {
                this.lockManager.lockSource( details.getId() );
                SourceCompletedDetails completedDetails =
                        createSourceCompletedDetails( database, details );
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

            return details;
        }
        catch ( SQLException e )
        {
            throw new IngestException( "Source metadata for '"
                    + location +
                    "' could not be stored in or retrieved from the database.",
                    e );
        }
    }

    private Pair<Response, SourceDetails> deserializeInput(URI location) throws IOException {
        try {
            if (location.getScheme().equals("file")) {
                try (InputStream data = this.getFromFile(location)) {
                    byte[] rawForecast = IOUtils.toByteArray(data);
                    return Pair.of(OBJECT_MAPPER.readValue(rawForecast, Response.class), null);
                }
            } else if (location.getScheme().toLowerCase().startsWith("http")) {
                try (WebClient.ClientResponse response = WEB_CLIENT.getFromWeb(location)) {
                    int httpStatus = response.getStatusCode();

                    if (httpStatus == 404) {
                        return Pair.of(null, this.saveLackOfData(location, httpStatus));
                    } else if (!(httpStatus >= 200 && httpStatus < 300)) {
                        throw new PreIngestException("Failed to get data from '"
                                + location +
                                "' due to HTTP status code "
                                + httpStatus);
                    }

                    byte[] rawForecast = IOUtils.toByteArray(response.getResponse());
                    return Pair.of(OBJECT_MAPPER.readValue(rawForecast, Response.class), null);
                }
            }
        }
        catch ( JsonMappingException jme )
        {
            throw new PreIngestException( "Failed to parse the response body"
                    + " from USGS url "
                    + location,
                    jme );
        }

        throw new UnsupportedOperationException("Only file and http(s) "
                + "are supported. Got: "
                + location);
    }

    private List<IngestResult> ingest() throws IOException
    {
        URI location = this.getDataSource()
                           .getUri();

        Pair<Response, SourceDetails> responsePair = this.deserializeInput(location);

        if (responsePair.getLeft() == null) {
            return IngestResult.singleItemListFrom(
                    this.projectConfig,
                    this.dataSource,
                    responsePair.getRight().getId(),
                    !responsePair.getRight().performedInsert(),
                    false
            );
        }

        Response response = responsePair.getLeft();

        try
        {
            WaterMLSource waterMLSource = new WaterMLSource( this.dataSource, response );
            List<TimeSeries<Double>> transformed = waterMLSource.call();
            List<IngestResult> ingestResults = new ArrayList<>( transformed.size() );

            for ( TimeSeries<Double> timeSeries : transformed )
            {
                TimeSeriesIngester ingester = TimeSeriesIngester.of( this.getSystemSettings(),
                                                                     this.getDatabase(),
                                                                     this.getFeaturesCache(),
                                                                     this.getVariablesCache(),
                                                                     this.getEnsemblesCache(),
                                                                     this.getMeasurementUnitsCache(),
                                                                     this.getProjectConfig(),
                                                                     this.getDataSource(),
                                                                     this.getLockManager(),
                                                                     timeSeries,
                                                                     GAGE_ID );
                List<IngestResult> result = ingester.call();
                ingestResults.addAll( result );
            }

            if ( LOGGER.isInfoEnabled() )
            {
                long countIngested = ingestResults.stream()
                                                  .filter( f -> !f.requiresRetry() )
                                                  .count();
                LOGGER.info( "{} USGS time series ingested from URL {}",
                             countIngested, dataSource.getUri() );
            }

            return Collections.unmodifiableList( ingestResults );
        }
        catch ( IngestException e )
        {
            throw new IngestException( "Values from USGS url "
                                       + location
                                       + " could not be ingested.",
                                       e );
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
    SourceCompletedDetails createSourceCompletedDetails( Database database,
                                                         SourceDetails sourceDetails )
    {
        return new SourceCompletedDetails( database, sourceDetails );
    }

    @Override
    protected Logger getLogger()
    {
        return null;
    }
}
