package wres.io.reading;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DateCondition;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.PairConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.ingesting.PreIngestException;
import wres.io.reading.DataSource.DataDisposition;
import wres.system.SSLStuffThatTrustsOneCertificate;

/**
 * Utilities for file reading.
 * 
 * @author James Brown
 * @author Christopher Tubbs
 * @author Jesse Bickel
 */

public class ReaderUtilities
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ReaderUtilities.class );

    /** Default WRDS project name, required by the service. Yuck. */
    public static final String DEFAULT_WRDS_PROJ = "UNKNOWN_PROJECT_USING_WRES";

    /**
     * Transform a single trace into a {@link TimeSeries} of {@link Double} values
     * @param metadata the metadata of the timeseries
     * @param trace the raw data to build a TimeSeries
     * @param lineNumber the approximate location in the source to help with messaging
     * @param uri a uri to help with messaging
     * @return The complete TimeSeries
     */

    public static TimeSeries<Double> transform( TimeSeriesMetadata metadata,
                                                SortedMap<Instant, Double> trace,
                                                int lineNumber,
                                                URI uri )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( trace );

        if ( trace.isEmpty() )
        {
            String append = "";
            if ( lineNumber > -1 )
            {
                append = " from line number "
                         + lineNumber
                         + ".";
            }

            throw new IllegalArgumentException( "Cannot build a single-valued time-series from "
                                                + uri
                                                + " because there are no values in the trace with metadata "
                                                + metadata
                                                + append );
        }

        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        builder.setMetadata( metadata );

        for ( Map.Entry<Instant, Double> events : trace.entrySet() )
        {
            Event<Double> event = Event.of( events.getKey(), events.getValue() );
            builder.addEvent( event );
        }

        return builder.build();
    }

    /**
     * Transform a map of traces into a {@link TimeSeries} of {@link Ensemble} values (flip it) but
     * also validate the density and valid datetimes of the ensemble prior.
     * @param metadata The metadata of the timeseries.
     * @param traces The raw data to build a TimeSeries.
     * @param lineNumber The approximate location in the source.
     * @param uri a uri to help with messaging
     * @return The complete TimeSeries
     * @throws IllegalArgumentException When fewer than two traces given.
     * @throws PreIngestException When ragged (non-dense) data given.
     */

    public static TimeSeries<Ensemble> transformEnsemble( TimeSeriesMetadata metadata,
                                                          SortedMap<String, SortedMap<Instant, Double>> traces,
                                                          int lineNumber,
                                                          URI uri )
    {
        int traceCount = traces.size();

        // See #62993-4: be lenient.
        if ( traceCount < 2 )
        {
            LOGGER.debug( "Found 'ensemble' data with fewer than two traces: {}",
                          traces );
        }

        Map<Instant, double[]> reshapedValues = new HashMap<>();
        Map.Entry<String, SortedMap<Instant, Double>> previousTrace = null;
        int i = 0;

        String append = "";
        if ( lineNumber > -1 )
        {
            append = " with data at or before "
                     + "line number "
                     + lineNumber;
        }

        for ( Map.Entry<String, SortedMap<Instant, Double>> trace : traces.entrySet() )
        {
            SortedSet<Instant> theseInstants = new TreeSet<>( trace.getValue()
                                                                   .keySet() );

            if ( Objects.nonNull( previousTrace ) )
            {
                SortedSet<Instant> previousInstants = new TreeSet<>( previousTrace.getValue()
                                                                                  .keySet() );
                if ( !theseInstants.equals( previousInstants ) )
                {
                    throw new ReadException( "Cannot build ensemble from "
                                             + uri
                                             + append
                                             + " because the trace named "
                                             + trace.getKey()
                                             + " had these valid datetimes"
                                             + ": "
                                             + theseInstants
                                             + " but a previous trace named "
                                             + previousTrace.getKey()
                                             + " had different ones: "
                                             + previousInstants
                                             + " which is not allowed. All"
                                             + " traces must be dense and "
                                             + "match valid datetimes." );
                }
            }

            for ( Map.Entry<Instant, Double> event : trace.getValue()
                                                          .entrySet() )
            {
                Instant validDateTime = event.getKey();

                if ( !reshapedValues.containsKey( validDateTime ) )
                {
                    reshapedValues.put( validDateTime, new double[traceCount] );
                }

                double[] values = reshapedValues.get( validDateTime );
                values[i] = event.getValue();
            }

            previousTrace = trace;
            i++;
        }

        wres.datamodel.time.TimeSeries.Builder<Ensemble> builder =
                new wres.datamodel.time.TimeSeries.Builder<>();

        // Because the iteration is over a sorted map, assuming same order here.
        SortedSet<String> traceNamesSorted = new TreeSet<>( traces.keySet() );
        String[] traceNames = new String[traceNamesSorted.size()];
        traceNamesSorted.toArray( traceNames );
        Labels labels = Labels.of( traceNames );

        builder.setMetadata( metadata );

        for ( Map.Entry<Instant, double[]> events : reshapedValues.entrySet() )
        {
            Ensemble ensembleSlice = Ensemble.of( events.getValue(), labels );
            Event<Ensemble> ensembleEvent = Event.of( events.getKey(), ensembleSlice );
            builder.addEvent( ensembleEvent );
        }

        return builder.build();
    }

    /**
     * A helper that tries to guess the time-scale information from the composition of the supplied URI. This works 
     * when particular time-series data services use fixed time-scales, such as the USGS NWIS Instantaneous Values 
     * Service.
     * 
     * @param uri the uri
     * @return the time scale or null
     */

    public static TimeScaleOuter getTimeScaleFromUri( URI uri )
    {
        TimeScaleOuter returnMe = null;

        // Assume that the NWIS "IV" service implies "instantaneous" values
        if ( Objects.nonNull( uri ) && uri.toString()
                                          .contains( "/nwis/iv" ) )
        {
            returnMe = TimeScaleOuter.of();
        }

        LOGGER.debug( "Identified {} as a source of time-series data whose time-scale is always {}.", uri, returnMe );

        return returnMe;
    }

    /**
     * Validates a source as containing a readable file.
     * @param dataSource the data source
     * @param allowDirectory is true to allow a directory, otherwise validate as a regular file
     * @throws ReadException if there is not a readable file associated with the source
     */

    public static void validateFileSource( DataSource dataSource, boolean allowDirectory )
    {
        Objects.requireNonNull( dataSource );

        if ( !dataSource.hasSourcePath() )
        {
            throw new ReadException( "Found a file data source with an invalid path: "
                                     + dataSource );
        }

        Path path = Paths.get( dataSource.getUri() );

        if ( !Files.exists( path ) )
        {
            throw new ReadException( "The path: '" +
                                     path
                                     +
                                     "' was not found." );
        }
        else if ( !Files.isReadable( path ) )
        {
            throw new ReadException( "The path: '" + path
                                     + "' was not readable. Please set "
                                     + "the permissions of that path to "
                                     + "readable for user '"
                                     + System.getProperty( "user.name" )
                                     + "' or run WRES as a user with read"
                                     + " permissions on that path." );
        }
        else if ( !allowDirectory && !Files.isRegularFile( path ) )
        {
            throw new ReadException( "Expected a file source, but the source was not a file: " + dataSource + "." );
        }
    }

    /**
     * Validates the disposition of the source against the disposition of the reader.
     * @param dataSource the data source
     * @param readerDisposition the disposition of the reader, one or more
     * @throws ReadException if the dispositions do not match
     * @throws NullPointerException if either input is null
     */

    public static void validateDataDisposition( DataSource dataSource, DataDisposition... readerDisposition )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( readerDisposition );

        if ( Arrays.stream( readerDisposition )
                   .noneMatch( next -> next == dataSource.getDisposition() ) )
        {
            throw new ReadException( "The disposition of the data source was " + dataSource.getDisposition()
                                     + ", but the reader expected "
                                     + Arrays.toString( readerDisposition )
                                     + "." );
        }
    }

    /**
     * Validates against an empty time-series, which is not allowed.
     * @param <T> the time-series event value type
     * @param timeSeries the time-series to validate
     * @param uri the source URI to help with error messaging
     * @return the input series for convenience, such as when applying to a collection of series as a map function
     * @throws NullPointerException if the input is null
     * @throws ReadException if the time-series is empty
     */

    public static <T> TimeSeries<T> validateAgainstEmptyTimeSeries( TimeSeries<T> timeSeries, URI uri )
    {
        Objects.requireNonNull( timeSeries );

        if ( timeSeries.getEvents()
                       .isEmpty() )
        {
            throw new ReadException( "When attempting to read the time-series data source, " + uri
                                     + ", discovered that it contained an empty time-series, which is not allowed. "
                                     + "Please check this data source and remove all empty time-series before trying "
                                     + "again." );
        }
        
        return timeSeries;
    }

    /**
     * @param source the data source
     * @return whether the source points to the USGS NWIS
     * @throws NullPointerException if the source is null
     */

    public static boolean isUsgsSource( DataSource source )
    {
        Objects.requireNonNull( source );

        URI uri = source.getUri();
        InterfaceShortHand interfaceShortHand = source.getSource()
                                                      .getInterface();
        if ( Objects.nonNull( interfaceShortHand ) )
        {
            return interfaceShortHand.equals( InterfaceShortHand.USGS_NWIS );
        }

        // Fallback for unspecified interface.
        return uri.getHost()
                  .toLowerCase()
                  .contains( "usgs.gov" )
               || uri.getPath()
                     .toLowerCase()
                     .contains( "nwis" );
    }

    /**
     * @param source the data source
     * @return whether the source is a WRDS AHPS observed source
     * @throws NullPointerException if the source is null
     */

    public static boolean isWrdsObservedSource( DataSource source )
    {
        Objects.requireNonNull( source );

        InterfaceShortHand interfaceShortHand = source.getSource()
                                                      .getInterface();
        if ( Objects.nonNull( interfaceShortHand ) )
        {
            return interfaceShortHand == InterfaceShortHand.WRDS_OBS;
        }

        return false;
    }

    /**
     * @param source the data source
     * @return whether the source is a WRDS AHPS source
     * @throws NullPointerException if the source is null
     */

    public static boolean isWrdsAhpsSource( DataSource source )
    {
        Objects.requireNonNull( source );

        URI uri = source.getUri();
        InterfaceShortHand interfaceShortHand = source.getSource()
                                                      .getInterface();
        if ( Objects.nonNull( interfaceShortHand ) )
        {
            return interfaceShortHand == InterfaceShortHand.WRDS_AHPS;
        }

        // Fallback for unspecified interface.
        return uri.getPath()
                  .toLowerCase()
                  .endsWith( "ahps" )
               ||
               uri.getPath()
                  .toLowerCase()
                  .endsWith( "ahps/" );
    }

    /**
     * @param source the data source
     * @return whether the source is a WRDS NWM source
     * @throws NullPointerException if the source is null
     */

    public static boolean isWrdsNwmSource( DataSource source )
    {
        Objects.requireNonNull( source );

        URI uri = source.getUri();
        InterfaceShortHand interfaceShortHand = source.getSource()
                                                      .getInterface();
        if ( Objects.nonNull( interfaceShortHand ) )
        {
            return interfaceShortHand.equals( InterfaceShortHand.WRDS_NWM );
        }

        // Fallback for unspecified interface.
        return uri.getPath()
                  .toLowerCase()
                  .contains( "nwm" );
    }

    /**
     * @param dataSource the data source
     * @return whether the data source is a NWM source
     */

    public static boolean isNwmVectorSource( DataSource dataSource )
    {
        InterfaceShortHand interfaceType = dataSource.getSource()
                                                     .getInterface();

        if ( Objects.nonNull( interfaceType ) && interfaceType.name()
                                                              .toLowerCase()
                                                              .startsWith( "nwm_" ) )
        {
            LOGGER.debug( "Identified data source {} as a NWM vector source.", dataSource );
            return true;
        }

        LOGGER.debug( "Failed to identify data source {} as a NWM vector source because the interface shorthand did not "
                      + "begin with a case-insensitive \"NWM_\" designation.",
                      dataSource );

        return false;
    }

    /**
     * @param source the data source
     * @return whether the source is a web source, specifically whether it has an http(s) scheme
     * @throws NullPointerException if the source is null
     */

    public static boolean isWebSource( DataSource source )
    {
        Objects.requireNonNull( source );

        URI uri = source.getUri();

        return Objects.nonNull( uri )
               && Objects.nonNull( uri.getScheme() )
               && uri.getScheme().startsWith( "http" );
    }

    /**
     * Creates year ranges for requests.
     * @param pairConfig the pair declaration
     * @param dataSource the data source
     * @return year ranges
     */
    public static Set<Pair<Instant, Instant>> getYearRanges( PairConfig pairConfig,
                                                             DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( dataSource.getContext() );

        DateCondition dates = pairConfig.getDates();

        SortedSet<Pair<Instant, Instant>> yearRanges = new TreeSet<>();

        OffsetDateTime earliest;
        String specifiedEarliest = dates.getEarliest();

        OffsetDateTime latest;
        String specifiedLatest = dates.getLatest();

        OffsetDateTime nowDate = OffsetDateTime.now();

        earliest = OffsetDateTime.parse( specifiedEarliest )
                                 .with( TemporalAdjusters.firstDayOfYear() )
                                 .withHour( 0 )
                                 .withMinute( 0 )
                                 .withSecond( 0 )
                                 .withNano( 0 );

        LOGGER.debug( "Given {}, calculated {} for earliest.",
                      specifiedEarliest,
                      earliest );

        // Intentionally keep this raw, un-next-year-ified.
        latest = OffsetDateTime.parse( specifiedLatest );

        LOGGER.debug( "Given {}, parsed {} for latest.",
                      specifiedLatest,
                      latest );

        OffsetDateTime left = earliest;
        OffsetDateTime right = left.with( TemporalAdjusters.firstDayOfNextYear() );

        while ( left.isBefore( latest ) )
        {
            // Because we chunk a year at a time, and because these will not
            // be retrieved again if already present, we need to ensure the
            // right hand date does not exceed "now".
            if ( right.isAfter( nowDate ) )
            {
                if ( latest.isAfter( nowDate ) )
                {
                    right = nowDate;
                }
                else
                {
                    right = latest;
                }
            }

            Pair<Instant, Instant> range = Pair.of( left.toInstant(), right.toInstant() );
            LOGGER.debug( "Created year range {}", range );
            yearRanges.add( range );
            left = left.with( TemporalAdjusters.firstDayOfNextYear() );
            right = right.with( TemporalAdjusters.firstDayOfNextYear() );
        }

        LOGGER.debug( "Created year ranges: {}.", yearRanges );

        return Collections.unmodifiableSet( yearRanges );
    }

    /**
     * Get an SSLContext that has a dod intermediate certificate trusted for use with Water Resources Data Service 
     * (WRDS) services.
     * Looks for a system property first, then a pem on the classpath, then a default trust manager.
     * @return the resulting SSLContext or the default SSLContext if not found.
     * @throws PreIngestException if the context and trust manager cannot be built for any reason
     */
    public static Pair<SSLContext, X509TrustManager> getSslContextTrustingDodSignerForWrds()
    {
        // Look for a system property first: #106160
        String pathToTrustFile = System.getProperty( "wres.wrdsCertificateFileToTrust" );
        if ( Objects.nonNull( pathToTrustFile ) )
        {
            LOGGER.debug( "Discovered the system property wres.wrdsCertificateFileToTrust with value {}.",
                          pathToTrustFile );

            Path path = Paths.get( pathToTrustFile );
            try ( InputStream trustStream = Files.newInputStream( path ) )
            {
                SSLStuffThatTrustsOneCertificate sslGoo =
                        new SSLStuffThatTrustsOneCertificate( trustStream );
                return Pair.of( sslGoo.getSSLContext(), sslGoo.getTrustManager() );
            }
            catch ( IOException e )
            {
                throw new PreIngestException( "Unable to read "
                                              + pathToTrustFile
                                              + " from the supplied system property, wres.wrdsCertificateFileToTrust, "
                                              + "in order to add it to trusted certificate list for requests made to "
                                              + "WRDS services.",
                                              e );
            }
        }

        // Try classpath
        String trustFileOnClassPath = "DODSWCA_60.pem";
        try ( InputStream inputStream = ReaderUtilities.class.getClassLoader()
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
                                     trustFileOnClassPath,
                                     manager );
                        theTrustManager = (X509TrustManager) manager;
                    }
                }
                if ( Objects.isNull( theTrustManager ) )
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
                                          + "default SSLContext.",
                                          nsae );
        }
    }

    /**
     * Get a URI based on prescribed URI and a parameter set.
     * 
     * @param uri the uri to build upon
     * @param urlParameters the parameters to add to the uri
     * @return the uri with the urlParameters added, in repeatable/sorted order.
     * @throws NullPointerException when any argument is null.
     */

    public static URI getUriWithParameters( URI uri, Map<String, String> urlParameters )
    {
        Objects.requireNonNull( uri );
        Objects.requireNonNull( urlParameters );

        URIBuilder uriBuilder = new URIBuilder( uri );
        SortedMap<String, String> sortedUrlParameters = new TreeMap<>( urlParameters );

        for ( Map.Entry<String, String> parameter : sortedUrlParameters.entrySet() )
        {
            uriBuilder.setParameter( parameter.getKey(), parameter.getValue() );
        }

        try
        {
            URI finalUri = uriBuilder.build();
            LOGGER.debug( "Created URL {}", finalUri );
            return finalUri;
        }
        catch ( URISyntaxException e )
        {
            throw new IllegalArgumentException( "Could not create URI from "
                                                + uri.toString()
                                                + " and "
                                                + urlParameters.toString(),
                                                e );
        }
    }

    /**
     * Returns a list of time-series from the queue.
     * @param results the queued results
     * @param startGettingResults a latch indicating whether a result should be returned (if {@code <= 0})
     * @param sourceName the source name to help with messaging
     * @return a list of time-series
     */

    public static List<TimeSeriesTuple> getTimeSeries( BlockingQueue<Future<List<TimeSeriesTuple>>> results,
                                                       CountDownLatch startGettingResults,
                                                       String sourceName )
    {
        // Should attempt to get a result?
        if ( startGettingResults.getCount() <= 0 )
        {
            try
            {
                List<TimeSeriesTuple> result = null;

                if ( !results.isEmpty() )
                {
                    result = results.take()
                                    .get();
                }

                if ( Objects.nonNull( result ) )
                {
                    return result;
                }

                // Nothing to return
                LOGGER.debug( "Skipping chunk because no time-series were returned from " + sourceName + "." );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread()
                      .interrupt();

                throw new ReadException( "While attempting to acquire a time-series from " + sourceName + ".", e );
            }
            catch ( ExecutionException e )
            {
                throw new ReadException( "While attempting to acquire a time-series from " + sourceName + ".", e );
            }
        }

        LOGGER.debug( "Delaying retrieval of chunk until more tasks have been submitted." );

        return List.of();
    }

    /**
     * Do not construct.
     */

    private ReaderUtilities()
    {
    }
}
