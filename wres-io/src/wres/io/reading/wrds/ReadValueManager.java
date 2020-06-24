package wres.io.reading.wrds;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import wres.config.generated.ProjectConfig;
import wres.datamodel.FeatureKey;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.PreIngestException;
import wres.io.reading.WebClient;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SSLStuffThatTrustsOneCertificate;
import wres.system.SystemSettings;

public class ReadValueManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ReadValueManager.class );
    private static Pair<SSLContext,X509TrustManager> SSL_CONTEXT
            = ReadValueManager.getSslContextTrustingDodSigner();

    private static final WebClient WEB_CLIENT = new WebClient( SSL_CONTEXT );

    private final SystemSettings systemSettings;
    private final Database database;
    private final DataSources dataSourcesCache;
    private final Features featuresCache;
    private final Variables variablesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;

    ReadValueManager( SystemSettings systemSettings,
                      Database database,
                      DataSources dataSourcesCache,
                      Features featuresCache,
                      Variables variablesCache,
                      Ensembles ensemblesCache,
                      MeasurementUnits measurementUnitsCache,
                      final ProjectConfig projectConfig,
                      final DataSource datasource,
                      DatabaseLockManager lockManager )
    {
        this.systemSettings = systemSettings;
        this.database = database;
        this.dataSourcesCache = dataSourcesCache;
        this.featuresCache = featuresCache;
        this.variablesCache = variablesCache;
        this.ensemblesCache = ensemblesCache;
        this.measurementUnitsCache = measurementUnitsCache;
        this.projectConfig = projectConfig;
        this.dataSource = datasource;
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

    private DataSources getDataSourcesCache()
    {
        return this.dataSourcesCache;
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

    public List<IngestResult> save() throws IOException
    {
        InputStream forecastData;
        URI location = this.getLocation();

        if ( location.getScheme()
                     .equals( "file" ) )
        {
            forecastData = this.getFromFile( location );
        }
        else if ( location.getScheme()
                          .toLowerCase()
                          .startsWith( "http" ) )
        {
            Pair<Integer,InputStream> response = WEB_CLIENT.getFromWeb( location );
            int httpStatus = response.getLeft();
            forecastData = response.getRight();
            LOGGER.debug( "Got HTTP response code {} for {}", httpStatus, location );

            if ( httpStatus >= 400 && httpStatus < 500 )
            {
                LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                             httpStatus,
                             location );

                if ( Objects.nonNull( forecastData) )
                {
                    try
                    {
                        forecastData.close();
                    }
                    catch ( IOException ioe )
                    {
                        LOGGER.warn( "Could not close a data stream from {}",
                                     location, ioe );
                    }
                }

                return Collections.emptyList();
            }
        }
        else
        {
            throw new UnsupportedOperationException( "Only file and http(s) "
                                                     + "are supported. Got: "
                                                     + location );
        }


        // It is conceivable that we could tee/pipe the data to both
        // the md5sum and the parser at the same time, but this involves
        // more complexity and may not be worth it. For now assume that we are
        // not going to exhaust our heap by including the whole forecast
        // here in memory temporarily.
        byte[] rawForecast = IOUtils.toByteArray( forecastData );

        //Close the forecastData stream.
        if ( Objects.nonNull( forecastData) )
        {
            try
            {
                forecastData.close();
            }
            catch ( IOException ioe )
            {
                LOGGER.warn( "Could not close a data stream from {}",
                             location, ioe );
            }
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Forecast as bytes: {} and as UTF-8: {}",
                          rawForecast,
                          new String( rawForecast,
                                      StandardCharsets.UTF_8 ) );
        }

        ObjectMapper mapper = new ObjectMapper();

        try
        {
            ForecastResponse response = mapper.readValue( rawForecast,
                                                          ForecastResponse.class );
            List<IngestResult> results = new ArrayList<>( response.forecasts.length );

            for ( Forecast forecast : response.getForecasts() )
            {
                LOGGER.debug( "Parsing {}", forecast );
                TimeSeries<Double> timeSeries = this.read( forecast );
                List<IngestResult> result = this.ingest( timeSeries );
                results.addAll( result );
            }

            return Collections.unmodifiableList( results );
        }
        catch ( JsonMappingException | JsonParseException je )
        {
            throw new PreIngestException( "Failed to parse the response body"
                                          + " from WRDS url "
                                          + location,
                                          je );
        }
        catch ( IngestException e )
        {
            throw new IngestException( "Values from WRDS url "
                                       + location
                                       + " could not be ingested.",
                                       e );
        }
    }

    /**
     *
     * @param forecast
     * @return Populated TimeSeries<Double> when data was read, null otherwise.
     */
    private TimeSeries<Double> read( Forecast forecast )
    {
        URI location = this.getLocation();
        List<DataPoint> dataPointsList;

        if ( forecast.getMembers() != null
             && forecast.getMembers().length > 0
             && forecast.getMembers()[0].getDataPointsList().size() > 0 )
        {
            dataPointsList = forecast.getMembers()[0].getDataPointsList().get( 0 );
        }
        else
        {
            LOGGER.warn( "The forecast '{}' from '{}' did not have data to save.",
                         forecast, location );
            return null;
        }

        if ( dataPointsList.size() < 2 )
        {
            LOGGER.warn( "Fewer than two values present in the first forecast '{}' from '{}'.",
                         forecast, location );
            return null;
        }

        Duration timeDuration = Duration.between( dataPointsList.get( 0 ).getTime(),
                                                  dataPointsList.get( 1 ).getTime() );

        Map<ReferenceTimeType,Instant> datetimes = new HashMap<>( 2 );

        if ( Objects.nonNull( forecast.getBasisTime() ) )
        {
            Instant basisDateTime = forecast.getBasisTime()
                                            .toInstant();
            datetimes.put( ReferenceTimeType.T0, basisDateTime );
        }

        if ( Objects.nonNull( forecast.getIssuedTime() ) )
        {
            Instant issuedDateTime = forecast.getIssuedTime()
                                             .toInstant();
            datetimes.put( ReferenceTimeType.ISSUED_TIME, issuedDateTime );
        }

        if ( datetimes.isEmpty() )
        {
            LOGGER.warn( "Forecast at {} had neither a basis datetime nor an issued datetime. Skipping it.",
                         location );
            return null;
        }

        // Get the time scale information, if available
        TimeScale timeScale = TimeScaleFromParameterCodes.getTimeScale( forecast.getParameterCodes(), location );
        String measurementUnit = forecast.getUnits()
                                         .getUnitName();
        String variableName = forecast.getParameterCodes()
                                      .getPhysicalElement();
        String featureName = forecast.getLocation()
                                     .getNames()
                                     .getNwsLid();
        String featureDescription = forecast.getLocation()
                                            .getNames()
                                            .getNwsName();
        FeatureKey feature = new FeatureKey( featureName, featureDescription, null, null );
        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( datetimes,
                                       timeScale,
                                       variableName,
                                       feature,
                                       measurementUnit );

        // Before ingest, validate the timeseries as being a timeseries in the
        // sense that a timeseries is a sequence of values in time.
        this.validateTimeseries( dataPointsList );

        TimeSeries.TimeSeriesBuilder<Double> timeSeriesBuilder =
                new TimeSeries.TimeSeriesBuilder<Double>().setMetadata( metadata );

        for (DataPoint dataPoint : dataPointsList)
        {
            Event<Double> event = Event.of( dataPoint.getTime()
                                                     .toInstant(),
                                            dataPoint.getValue() );
            timeSeriesBuilder.addEvent( event );
        }

        return timeSeriesBuilder.build();
    }


    /**
     * Validate a timeseries. Return if valid, else throw PreIngestException.
     *
     * A timeseries according to this method is a sequence of values in time.
     * Therefore a list of data with duplicate values for any given datetime is
     * invalid and will cause a PreIngestException.
     *
     * @param dataPointsList the WRDS-formatted timeseries data points
     * @throws wres.io.reading.PreIngestException when invalid timeseries found
     */

    private void validateTimeseries( List<DataPoint> dataPointsList )
    {
        Objects.requireNonNull( dataPointsList );

        // Put each datetime in a set. We can compare the set size to the list
        // size and if they are identical: all good.
        Set<OffsetDateTime> dateTimes = new HashSet<>( dataPointsList.size() );

        // For error message purposes, track the exact datetimes that had more
        // than one value.
        Set<OffsetDateTime> multipleValues = new TreeSet<>();

        for ( DataPoint wrdsDataPoint : dataPointsList )
        {
            OffsetDateTime dateTimeForOneValue = wrdsDataPoint.getTime();
            boolean added = dateTimes.add( dateTimeForOneValue );

            if ( !added )
            {
                multipleValues.add( dateTimeForOneValue );
            }
        }

        // Check the size of the datetimes set vs the size of the list
        if ( dataPointsList.size() != dateTimes.size() )
        {
            String message = "Invalid timeseries data encountered. Multiple data"
                             + " found for each of the following datetimes in "
                             + "a forecast from " + this.getLocation()
                             + " : " + multipleValues;
            throw new PreIngestException( message );
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

    private URI getLocation()
    {
        return this.dataSource.getUri();
    }


    /**
     * Get an SSLContext that has a dod intermediate certificate trusted.
     * Uses a pem on the classpath.
     * @return the resulting SSLContext or the default SSLContext if not found.
     */
    public static Pair<SSLContext,X509TrustManager> getSslContextTrustingDodSigner()
    {
        String trustFileOnClassPath = "dod_sw_ca-54_expires_2022-11.pem";
        try ( InputStream inputStream = ReadValueManager.class
                .getClassLoader()
                .getResourceAsStream( trustFileOnClassPath ) )
        {
            // Avoid sending null, log a warning instead, use default.
            if ( inputStream == null )
            {
                LOGGER.warn( "Failed to load {} from classpath. Using default SSLContext.",
                             trustFileOnClassPath );

                X509TrustManager theTrustManager = null;
                for ( TrustManager manager : TrustManagerFactory.getInstance( TrustManagerFactory.getDefaultAlgorithm() )
                                                                .getTrustManagers() )
                {
                    if ( manager instanceof X509TrustManager )
                    {
                        LOGGER.warn( "Failed to load {} from classpath. Using this X509TrustManager: {}",
                                     trustFileOnClassPath, manager );
                        theTrustManager = (X509TrustManager) manager;
                    }
                }
                if ( Objects.isNull( theTrustManager) )
                {
                    throw new UnsupportedOperationException( "Could not find a default X509TrustManager" );
                }
                return Pair.of( SSLContext.getDefault(), theTrustManager );
            }
            SSLStuffThatTrustsOneCertificate sslGoo =
                    new SSLStuffThatTrustsOneCertificate( inputStream );
            return Pair.of( sslGoo.getSSLContext(), sslGoo.getTrustManager() );
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Unable to read "
                                          + trustFileOnClassPath
                                          + " from classpath in order to add it"
                                          + " to trusted certificate list for "
                                          + "requests made to WRDS services.",
                                          ioe );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new PreIngestException( "Unable to find "
                                          + trustFileOnClassPath
                                          + " on classpath in order to add it"
                                          + " to trusted certificate list for "
                                          + "requests made to WRDS services "
                                          + "and furthermore could not get the "
                                          + "default SSLContext.", nsae );
        }
    }


    /**
     * Perform ingest of the given timeSeries.
     *
     * A step toward separating ingest classes from reading classes.
     * Also facilitates testing.
     * @param timeSeries The timeSeries to ingest
     * @return The ingest results.
     * @throws IngestException When an exception occurs during ingest.
     */

    List<IngestResult> ingest( TimeSeries<Double> timeSeries )
            throws IngestException
    {
        TimeSeriesIngester ingester = TimeSeriesIngester.of( this.getSystemSettings(),
                                                             this.getDatabase(),
                                                             this.getFeaturesCache(),
                                                             this.getVariablesCache(),
                                                             this.getEnsemblesCache(),
                                                             this.getMeasurementUnitsCache(),
                                                             this.projectConfig,
                                                             this.dataSource,
                                                             this.lockManager,
                                                             timeSeries );
        try
        {
            return ingester.call();
        }
        catch ( IOException ioe )
        {
            throw new IngestException( "Failed to ingest data from "
                                       + this.getLocation(), ioe );
        }
    }
}
