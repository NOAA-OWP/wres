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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

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
import wres.config.generated.DatasourceType;
import wres.datamodel.MissingValues;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.TimeScales;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.PreIngestException;
import wres.io.utilities.WebClient;
import wres.statistics.generated.Geometry;
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
    private final TimeScales timeScalesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;

    ReadValueManager( SystemSettings systemSettings,
                      Database database,
                      DataSources dataSourcesCache,
                      Features featuresCache,
                      TimeScales timeScalesCache,
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
        this.timeScalesCache = timeScalesCache;
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

    private TimeScales getTimeScalesCache()
    {
        return this.timeScalesCache;
    }

    private Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    private MeasurementUnits getMeasurementUnitsCache()
    {
        return this.measurementUnitsCache;
    }


    private byte[] getInputBytes() throws IOException {
        URI location = this.getLocation();

        if ( location.getScheme().equals( "file" ) )
        {
            try (InputStream fileContents = this.getFromFile(location)) {
                return IOUtils.toByteArray( fileContents );
            }
        }
        else if ( location.getScheme().toLowerCase().startsWith( "http" ) )
        {
            try (WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( location )) {
                int httpStatus = response.getStatusCode();
                LOGGER.debug( "Got HTTP response code {} for {}", httpStatus, location );

                if ( httpStatus >= 400 && httpStatus < 500 ) {
                    LOGGER.warn("Treating HTTP response code {} as no data found from URI {}",
                                httpStatus,
                                location);
                    return null;
                }

                return IOUtils.toByteArray( response.getResponse() );
            }
        }
        else
        {
            throw new UnsupportedOperationException( "Only file and http(s) "
                                                     + "are supported. Got: "
                                                     + location );
        }
    }

    public List<IngestResult> save() throws IOException
    {
        URI location = this.getLocation();
        byte[] rawForecast = this.getInputBytes();

        if ( Objects.isNull( rawForecast ) )
        {
            return Collections.emptyList();
        }

        // It is conceivable that we could tee/pipe the data to both
        // the md5sum and the parser at the same time, but this involves
        // more complexity and may not be worth it. For now assume that we are
        // not going to exhaust our heap by including the whole forecast
        // here in memory temporarily.

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Time series as bytes: {} and as UTF-8: {}",
                          rawForecast,
                          new String( rawForecast,
                                      StandardCharsets.UTF_8 ) );
        }

        ObjectMapper mapper = new ObjectMapper();

        try
        {
            ForecastResponse response = mapper.readValue( rawForecast,
                                                          ForecastResponse.class );

            if (response.forecasts == null)
            {
                throw new IngestException("Failed to obtain response from the WRDS url. "
                    + "Was the correct URL provided in the declaration?");
            }

            // The response should include the missing values, but, in case we reuse
            // this code later to read other forecasts, I allow for null.  If not null
            // the output the list of missing values to debug. 
            if (response.getHeader().getMissing_values() != null)
            {
                LOGGER.debug ( "The time series specified the following missing values: " + 
                               Arrays.toString(response.getHeader().getMissing_values()) );
            }
            else 
            {
                LOGGER.debug ( "The time series specified no missing values." );
            }

            List<IngestResult> results = new ArrayList<>( response.forecasts.length );

            for ( Forecast forecast : response.getForecasts() )
            {
                LOGGER.debug( "Parsing {}", forecast );
                TimeSeries<Double> timeSeries = this.read( forecast, response.getHeader().getMissing_values() );

                if ( Objects.nonNull( timeSeries ) )
                {
                    List<IngestResult> result = this.ingest( timeSeries );
                    results.addAll( result );
                }
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
     * @param forecast The foreast portion of the response that will be read.
     * @param missingValues An array of values to be treated as missing by this read.
     * All of the values are replaced by the WRES missing value before storing.
     * @return Populated time series  when data was read, null otherwise.
     */
    protected TimeSeries<Double> read( Forecast forecast, double[] missingValues )
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
            LOGGER.warn( "The time series '{}' from '{}' did not have data to save.",
                         forecast, location );
            return null;
        }

        if ( dataPointsList.size() < 2 )
        {
            LOGGER.warn( "Fewer than two values present in the first time series '{}' from '{}'.",
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

        //If datetimes is empty, then, if the data are observations, use the latest time
        //associated with an observation as a "dummy" reference time.  Otherwise, the 
        //the data is either a forecast or simulation, and a reference this is required.
        //Since its not found, skip the time series with an appropriate message.
        if ( datetimes.isEmpty() )
        {
            if (this.dataSource.getContext().getType() == DatasourceType.OBSERVATIONS)
            {
                Instant latestTime = null;
                for ( DataPoint dataPoint : dataPointsList )
                {
                    Instant dataPointTime = dataPoint.getTime().toInstant();
                    if ( (latestTime == null) || dataPointTime.isAfter( latestTime ) )
                    {
                        latestTime = dataPointTime;
                    }
                }
                datetimes.put(ReferenceTimeType.LATEST_OBSERVATION, latestTime);
            }
            else
            {
                LOGGER.warn( "Forecast at {} had neither a basis datetime nor an issued datetime. Skipping it.",
                             location );
                return null;
            }
        }

        // Get the time scale information, if available
        TimeScaleOuter timeScale = TimeScaleFromParameterCodes.getTimeScale( forecast.getParameterCodes(), location );
        String measurementUnit = forecast.getMembers()[0].getUnits();
        if ( !Objects.nonNull( measurementUnit  ) )
        {
            //TODO Should check for existence.  
            measurementUnit = forecast.getUnits()
                                      .getUnitName();
        }
        String variableName = forecast.getParameterCodes()
                                      .getPhysicalElement();
        String featureName = forecast.getLocation()
                                     .getNames()
                                     .getNwsLid();
        String featureDescription = forecast.getLocation()
                                            .getNames()
                                            .getNwsName();
        
        Geometry geometry = MessageFactory.getGeometry( featureName, featureDescription, null, null );
        FeatureKey feature = FeatureKey.of( geometry );
        
        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( datetimes,
                                       timeScale,
                                       variableName,
                                       feature,
                                       measurementUnit );

        // Before ingest, validate the timeseries as being a timeseries in the
        // sense that a timeseries is a sequence of values in time.
        this.validateTimeseries( dataPointsList );

        TimeSeries.Builder<Double> timeSeriesBuilder =
                new TimeSeries.Builder<Double>().setMetadata( metadata );

        for (DataPoint dataPoint : dataPointsList)
        {
            double usedValue = dataPoint.getValue();
            
            //If missing values are provided, replace them with MissingValues.DOUBLE.
            if (missingValues != null)
            {
                for (double missing : missingValues)
                {
                    if (usedValue == missing)
                    {
                        usedValue = MissingValues.DOUBLE;
                        break;
                    }
                }
            }

            Event<Double> event = Event.of( dataPoint.getTime()
                                                     .toInstant(),
                                            usedValue );
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
            String message = "Invalid time series data encountered. Multiple data"
                             + " found for each of the following datetimes in "
                             + "a time series from " + this.getLocation()
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
        if (this.dataSource == null)
        {
            return null;
        }
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
                                                             this.getTimeScalesCache(),
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
        catch ( IngestException ie )
        {
            throw new IngestException( "Failed to ingest data from "
                                       + this.getLocation(), ie );
        }
    }
}
