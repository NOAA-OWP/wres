package wres.reading;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationException;
import wres.config.DeclarationUtilities;
import wres.config.components.DatasetOrientation;
import wres.config.components.EvaluationDeclaration;
import wres.config.components.FeatureAuthority;
import wres.config.components.GeneratedBaselines;
import wres.config.components.SourceInterface;
import wres.config.components.ThresholdSource;
import wres.config.components.TimeInterval;
import wres.datamodel.space.Feature;
import wres.datamodel.types.Ensemble;
import wres.datamodel.types.Ensemble.Labels;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.DoubleEvent;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.http.WebClient;
import wres.http.WebClientUtils;
import wres.reading.DataSource.DataDisposition;
import wres.reading.wrds.geography.FeatureFiller;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.TimeScale;
import wres.system.SSLStuffThatTrustsOneCertificate;

/**
 * Utilities for reading from data sources, such as files and data services.
 *
 * @author James Brown
 * @author Christopher Tubbs
 * @author Jesse Bickel
 */

public class ReaderUtilities
{
    /** Time Zone ID for UTC. */
    public static final ZoneId UTC = ZoneId.of( "UTC" );

    /** Default WRDS project name, required by the service. Yuck. */
    public static final String DEFAULT_WRDS_PROJ = "UNKNOWN_PROJECT_USING_WRES";

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ReaderUtilities.class );

    /** A web client to help with reading data from the web. */
    private static final WebClient WEB_CLIENT = new WebClient( WebClientUtils.defaultTimeoutHttpClient() );

    /**
     * Resolves any implicit declaration of features that require service calls to external web services. Currently,
     * the only supported web services are those within the umbrella of the Water Resources Data Service (WRDS), which
     * contains a collection of U.S. National Weather Service APIs.
     *
     * @param declaration the evaluation declaration
     * @return the declaration with any implicit features rendered explicit
     * @throws NullPointerException if the input is null
     */

    public static EvaluationDeclaration readAndFillFeatures( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        // Currently, there is only one feature service supported. If others are supported in the future, consider
        // separating the reading and filling concerns by adding a reading API and abstracting the filling to a helper
        // in DeclarationUtilities, similar to the approach used for thresholds
        return FeatureFiller.fillFeatures( declaration );
    }

    /**
     * Resolves any implicit declaration of thresholds that require service calls to external web services. Currently,
     * the only supported web services are those within the umbrella of the Water Resources Data Service (WRDS), which
     * contains a collection of U.S. National Weather Service APIs.
     *
     * @param declaration the evaluation declaration
     * @return the declaration with any implicit thresholds rendered explicit
     * @throws NullPointerException if either input is null
     */

    public static EvaluationDeclaration readAndFillThresholds( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        EvaluationDeclaration adjusted = declaration;

        Set<ThresholdSource> thresholdSources = declaration.thresholdSources();
        if ( !thresholdSources.isEmpty() )
        {
            // Adjust the declaration iteratively, once for each threshold source
            for ( ThresholdSource nextSource : declaration.thresholdSources() )
            {
                adjusted = ReaderUtilities.fillThresholds( nextSource, adjusted );
            }

            // Remove any features for which there are no thresholds in the sources
            adjusted = DeclarationUtilities.removeFeaturesWithoutThresholds( adjusted );
        }

        return adjusted;
    }

    /**
     * Transform a single trace into a {@link TimeSeries} of {@link Double} values
     * @param metadata the metadata of the timeseries
     * @param trace the raw data to build a TimeSeries
     * @param lineNumber the approximate location in the source to help with messaging
     * @param uri a uri to help with messaging
     * @return The complete TimeSeries
     * @throws ReadException if the trace is empty
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

            throw new ReadException( "Cannot build a single-valued time-series from "
                                     + uri
                                     + " because there are no values in the trace with metadata "
                                     + metadata
                                     + append );
        }

        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        builder.setMetadata( metadata );

        for ( Map.Entry<Instant, Double> events : trace.entrySet() )
        {
            Event<Double> event = DoubleEvent.of( events.getKey(), events.getValue() );
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
     * @throws PreReadException When ragged (non-dense) data given.
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

        if ( Objects.nonNull( uri ) )
        {
            append = " from " + uri;

            if ( lineNumber > -1 )
            {
                append = " with data at or before "
                         + "line number "
                         + lineNumber;
            }
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
                    throw new ReadException( "Could not build an ensemble time-series"
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
                                             + "have matching valid datetimes." );
                }
            }

            for ( Map.Entry<Instant, Double> event : trace.getValue()
                                                          .entrySet() )
            {
                Instant validDateTime = event.getKey();
                reshapedValues.putIfAbsent( validDateTime, new double[traceCount] );
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
     * @return the timescale or null
     */

    public static TimeScaleOuter getTimeScaleFromUri( URI uri )
    {
        TimeScaleOuter returnMe = null;

        // Assume that the NWIS "IV" service implies "instantaneous" values
        if ( Objects.nonNull( uri )
             && uri.toString()
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
        else if ( !allowDirectory
                  && !Files.isRegularFile( path ) )
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
                                     + "again. The empty time-series is: "
                                     + timeSeries
                                     + "." );
        }

        return timeSeries;
    }

    /**
     * @param source the data source
     * @return whether the source points to the USGS National Water Information System, NWIS
     * @throws NullPointerException if the source is null
     */

    public static boolean isUsgsSource( DataSource source )
    {
        Objects.requireNonNull( source );

        URI uri = source.getUri();
        SourceInterface interfaceShortHand = source.getSource()
                                                   .sourceInterface();
        if ( Objects.nonNull( interfaceShortHand ) )
        {
            return interfaceShortHand == SourceInterface.USGS_NWIS;
        }

        // Fallback for unspecified interface.
        return ReaderUtilities.isWebSource( uri )
               && ( ( Objects.nonNull( uri.getHost() )
                      && uri.getHost()
                            .toLowerCase()
                            .contains( "usgs.gov" ) )
                    || ( Objects.nonNull( uri.getPath() )
                         && uri.getPath()
                               .toLowerCase()
                               .contains( "nwis" ) ) );
    }

    /**
     * @param source the data source
     * @return whether the source is a WRDS AHPS observed source
     * @throws NullPointerException if the source is null
     */

    public static boolean isWrdsObservedSource( DataSource source )
    {
        Objects.requireNonNull( source );

        SourceInterface interfaceShortHand = source.getSource()
                                                   .sourceInterface();

        return interfaceShortHand == SourceInterface.WRDS_OBS;
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
        SourceInterface interfaceShortHand = source.getSource()
                                                   .sourceInterface();
        if ( Objects.nonNull( interfaceShortHand ) )
        {
            return interfaceShortHand == SourceInterface.WRDS_AHPS;
        }

        // Fallback for unspecified interface.
        return Objects.nonNull( uri.getPath() )
               && ( uri.getPath()
                       .toLowerCase()
                       .endsWith( "ahps" )
                    || uri.getPath()
                          .toLowerCase()
                          .endsWith( "ahps/" ) );
    }

    /**
     * @param source the data source
     * @return whether the source is a WRDS HEFS source
     * @throws NullPointerException if the source is null
     */

    public static boolean isWrdsHefsSource( DataSource source )
    {
        Objects.requireNonNull( source );

        URI uri = source.getUri();
        SourceInterface interfaceShortHand = source.getSource()
                                                   .sourceInterface();
        if ( Objects.nonNull( interfaceShortHand ) )
        {
            return interfaceShortHand == SourceInterface.WRDS_HEFS;
        }

        boolean isHefs = Objects.nonNull( uri.getPath() )
                         && uri.getPath()
                               .toLowerCase()
                               .contains( "/hefs/" );

        LOGGER.debug( "When attempting to detect whether the supplied data source refers to the WRDS HEFS "
                      + "service, could not detect an explicit interface shorthand of {}, so looked for a URI that "
                      + "contains '/hefs/', which was: {}. The data source is: {}.",
                      SourceInterface.WRDS_HEFS,
                      isHefs,
                      source );

        // Fallback for unspecified interface.
        return isHefs;
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
        SourceInterface interfaceShortHand = source.getSource()
                                                   .sourceInterface();
        if ( Objects.nonNull( interfaceShortHand ) )
        {
            return interfaceShortHand == SourceInterface.WRDS_NWM;
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
        SourceInterface interfaceType = dataSource.getSource()
                                                  .sourceInterface();

        if ( Objects.nonNull( interfaceType )
             && interfaceType.name()
                             .toLowerCase()
                             .startsWith( "nwm_" ) )
        {
            LOGGER.debug( "Identified data source {} as a NWM vector source.", dataSource );
            return true;
        }

        LOGGER.debug( "Failed to identify data source {} as a NWM vector source because the interface shorthand did "
                      + "not begin with a case-insensitive 'NWM_' designation.",
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

        return ReaderUtilities.isWebSource( uri );
    }

    /**
     * @param uri the uri
     * @return whether the URI scheme starts with http
     */
    public static boolean isWebSource( URI uri )
    {
        return Objects.nonNull( uri.getScheme() )
               && ( uri.getScheme()
                       .toLowerCase()
                       .startsWith( "http" )
                    || uri.getScheme()
                          .toLowerCase()
                          .startsWith( "cdms3" ) );
    }

    /**
     * Returns the complete datetime range from the declaration for use when forming requests. There is no chunking by
     * time range.
     *
     * @param declaration the declaration
     * @param dataSource the data source
     * @return a simple range
     */
    public static Pair<Instant, Instant> getSimpleRange( EvaluationDeclaration declaration,
                                                         DataSource dataSource )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( dataSource.getContext() );

        // Forecast data?
        boolean isForecast = DeclarationUtilities.isForecast( dataSource.getContext() );

        if ( ( isForecast
               && Objects.isNull( declaration.referenceDates() ) ) )
        {
            throw new ReadException( "While attempting to read forecasts from a web service, discovered an evaluation "
                                     + "with missing 'reference_dates', which is not allowed. Please declare "
                                     + "'reference_dates' to constrain the evaluation to a finite amount of "
                                     + "time-series data." );
        }
        else if ( !isForecast
                  && Objects.isNull( declaration.validDates() ) )
        {
            throw new ReadException( "While attempting to read a non-forecast data source from a web service, "
                                     + "discovered an evaluation with missing 'valid_dates', which is not allowed. "
                                     + "Please declare 'valid_dates' to constrain the evaluation to a finite amount of "
                                     + "time-series data. The data type was: "
                                     + dataSource.getContext()
                                                 .type()
                                     + "." );
        }

        // When dates are present, both bookends are present because this was validated on construction of the reader
        TimeInterval dates = declaration.validDates();

        if ( isForecast )
        {
            dates = declaration.referenceDates();
        }

        Instant earliest = dates.minimum();
        Instant latest = dates.maximum();
        return Pair.of( earliest, latest );
    }

    /**
     * Creates year ranges for requests.
     * @param declaration the pair declaration
     * @param dataSource the data source
     * @return year ranges
     */
    public static Set<Pair<Instant, Instant>> getYearRanges( EvaluationDeclaration declaration,
                                                             DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( dataSource.getContext() );

        SortedSet<Pair<Instant, Instant>> yearRanges = new TreeSet<>();

        // Get the boundaries
        Pair<Instant, Instant> dataInterval = ReaderUtilities.getDatasetInterval( dataSource, declaration );

        LOGGER.debug( "Requesting data for the following interval: {}.", dataInterval );

        Instant specifiedEarliest = dataInterval.getLeft();
        Instant specifiedLatest = dataInterval.getRight();

        ZonedDateTime earliest = specifiedEarliest.atZone( UTC )
                                                  .with( TemporalAdjusters.firstDayOfYear() )
                                                  .withHour( 0 )
                                                  .withMinute( 0 )
                                                  .withSecond( 0 )
                                                  .withNano( 0 );

        LOGGER.debug( "Given {}, calculated {} for earliest.",
                      specifiedEarliest,
                      earliest );

        // Intentionally keep this raw, un-next-year-ified.
        ZonedDateTime latest = specifiedLatest.atZone( UTC );

        ZonedDateTime nowDate = ZonedDateTime.now( UTC );

        LOGGER.debug( "Given {}, parsed {} for latest.",
                      specifiedLatest,
                      latest );

        ZonedDateTime left = earliest;
        ZonedDateTime right = left.with( TemporalAdjusters.firstDayOfNextYear() );

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
     * Splits the input line by the prescribed delimiter and treats quoted strings as atomic, removing the quotes.
     * @param line the line to split
     * @param delimiter the delimiter
     * @return the split string
     */
    public static String[] splitByDelimiter( String line, char delimiter )
    {
        // Could do this with a regex, but the look ahead on each character would be slow
        List<String> tokens = new ArrayList<>();
        int startPosition = 0;
        boolean isInQuotes = false;
        for ( int currentPosition = 0; currentPosition < line.length(); currentPosition++ )
        {
            if ( line.charAt( currentPosition ) == '\"' )
            {
                isInQuotes = !isInQuotes;
            }
            else if ( line.charAt( currentPosition ) == delimiter && !isInQuotes )
            {
                String substring = line.substring( startPosition, currentPosition );
                substring = substring.replace( "\"", "" );
                tokens.add( substring );
                startPosition = currentPosition + 1;
            }
        }

        String lastToken = line.substring( startPosition );
        if ( lastToken.equals( delimiter + "" ) )
        {
            tokens.add( "" );
        }
        else
        {
            lastToken = lastToken.replace( "\"", "" );
            tokens.add( lastToken );
        }

        return tokens.toArray( new String[0] );
    }

    /**
     * @return Returns a pair of the default, JDK SSL context and trust manager.
     * @throws PreReadException Thrown if any exceptions occur in the process of creating the
     * default context and trust manager.
     */
    private static Pair<SSLContext, X509TrustManager> getDefaultSSLContext() throws PreReadException
    {

        // No path to trust file. Try to set up a default ssl context using JDK certs.
        try
        {
            // Load the keyStore from the JDK default.
            KeyStore keyStore = KeyStore.getInstance( KeyStore.getDefaultType() );
            String cacertsPath = System.getProperty( "java.home" ) + "/lib/security/cacerts";
            FileInputStream fis = new FileInputStream( cacertsPath );
            String password = "changeit";
            keyStore.load( fis, password.toCharArray() );   // NOSONAR
            fis.close();

            // Initialize the factory for use.
            TrustManagerFactory tmf = TrustManagerFactory.getInstance( TrustManagerFactory.getDefaultAlgorithm() );
            tmf.init( keyStore );

            // Get the trust manager. This will use the last one found.
            X509TrustManager theTrustManager = null;
            for ( TrustManager manager : tmf.getTrustManagers() )
            {
                if ( manager instanceof X509TrustManager m )
                {
                    LOGGER.warn( "No system property wres.wrdsCertificateFileToTrust provided. Using the default "
                                 + "X509TrustManager: {}",
                                 manager );
                    theTrustManager = m;
                }
            }
            if ( Objects.isNull( theTrustManager ) )
            {
                throw new UnsupportedOperationException( "Could not find a default X509TrustManager" );
            }

            // Return the default context with the JDK-default certs trusted.
            return Pair.of( SSLContext.getDefault(), theTrustManager );
        }
        catch ( NoSuchAlgorithmException | KeyStoreException | CertificateException | IOException e )
        {
            throw new PreReadException( "No system property was provided and we could not get the "
                                        + "default SSLContext from the JDK to support reading data from WRDS.",
                                        e );
        }
    }

    /**
     * Get an SSLContext for use with Water Resources Data Service (WRDS) services. It looks for the 
     * system property. If found, it tries to load the file from the file system first, and then the
     * class path. If the file is not found, or it is found but cannot be used, then its an exception.
     * If no property is supplied, then it uses the JDK default trusted CAs. 
     * @return The resulting SSLContext and X509TrustManager to use for authenticating WRDS.
     * @throws PreReadException if the context and trust manager cannot be built for any reason
     */
    public static Pair<SSLContext, X509TrustManager> getSslContextForWrds()
    {
        // Look for a system property first: #106160. If its there, then use it and it must be valid.
        String pathToTrustFile = System.getProperty( "wres.wrdsCertificateFileToTrust" );
        String passwordForInternalTrustStore = System.getProperty( "wres.wrdsInternalTrustStorePassword" );
        if ( Objects.nonNull( pathToTrustFile ) )
        {
            LOGGER.debug( "Discovered the system property wres.wrdsCertificateFileToTrust with value {}.",
                          pathToTrustFile );

            Path path = Paths.get( pathToTrustFile );
            InputStream inputStream;
            try
            {
                // If the file exists, setup the input stream to point to it. IOException is handled below.
                if ( Files.exists( path ) && Files.isReadable( path ) )
                {
                    inputStream = Files.newInputStream( path );
                }
                // Otherwise, try to find it on the classpath. Again, IOException is handled below, while
                // a null return is handled via if-clause and implies the file was not found anywhere.
                else
                {
                    inputStream = ReaderUtilities.class.getClassLoader()
                                                       .getResourceAsStream( pathToTrustFile );
                    if ( inputStream == null )
                    {
                        throw new PreReadException( "The file "
                                                    + pathToTrustFile
                                                    + " supplied via system property, wres.wrdsCertificateFileToTrust, "
                                                    + "for obtaining data afrom WRDS services was neither found on the "
                                                    + "file system nor the class path." );
                    }
                }

                //With the stream established, setup the SSLContext.
                SSLStuffThatTrustsOneCertificate sslGoo =
                        new SSLStuffThatTrustsOneCertificate( inputStream,
                                                              passwordForInternalTrustStore );
                inputStream.close(); //Stream should not be null when I reach here. 
                return Pair.of( sslGoo.getSSLContext(), sslGoo.getTrustManager() );
            }
            catch ( IOException ioe )
            {
                throw new PreReadException( "The file "
                                            + pathToTrustFile
                                            + " was found on the file system or classpath but an attempt to add it to "
                                            + "the trusted certificate list for requests made to WRDS services failed.",
                                            ioe );

            }
        }

        //Otherwise, just return the default JDK context.
        return getDefaultSSLContext();
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
                                                + uri
                                                + " and "
                                                + urlParameters,
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
                LOGGER.debug( "Skipping chunk because no time-series were returned from {}.", sourceName );
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
     * Closes a web client response.
     * @param response the response
     */

    public static void closeWebClientResponse( WebClient.ClientResponse response )
    {
        if ( Objects.nonNull( response ) )
        {
            try
            {
                response.close();
            }
            catch ( IOException e )
            {
                LOGGER.warn( "Failed to close a web client response.", e );
            }
        }
    }

    /**
     * Returns the feature names for the prescribed data source.
     * @param geometries the geometries, required
     * @param dataSource the data source, required
     * @return the feature names
     */
    public static Set<String> getFeatureNamesFor( Set<GeometryTuple> geometries, DataSource dataSource )
    {
        Objects.requireNonNull( geometries );
        Objects.requireNonNull( dataSource );
        if ( dataSource.getDatasetOrientation() == DatasetOrientation.COVARIATE )
        {
            return DeclarationUtilities.getFeatureNamesFor( geometries,
                                                            dataSource.getCovariateFeatureOrientation() );
        }

        return DeclarationUtilities.getFeatureNamesFor( geometries,
                                                        dataSource.getDatasetOrientation() );
    }

    /**
     * Returns a byte stream from a web source or null if the web source returns no data and that is considered a
     * nominal response (see #isMissingResponse).
     *
     * @param uri the uri
     * @param isMissingResponse a test to determine whether the http response code indicates missing data
     * @param isErrorResponse a test to determine whether the http response code indicates an error
     * @param errorUnpacker unpacks an error message from the web client response for onward communication, optional
     * @param webClient an optional web client instance, otherwise the default will be used
     * @return the byte stream
     * @throws ReadException if the uri does not point to a web source or the stream could not be created for any reason
     */

    public static InputStream getByteStreamFromWebSource( URI uri,
                                                          IntPredicate isMissingResponse,
                                                          IntPredicate isErrorResponse,
                                                          Function<WebClient.ClientResponse, String> errorUnpacker,
                                                          WebClient webClient )
    {
        Objects.requireNonNull( uri );
        Objects.requireNonNull( isMissingResponse );
        Objects.requireNonNull( isErrorResponse );

        if ( !ReaderUtilities.isWebSource( uri ) )
        {
            throw new ReadException( "Cannot read time-series data from "
                                     + uri
                                     + " because it is not a web source." );
        }

        if ( Objects.isNull( errorUnpacker ) )
        {
            errorUnpacker = c -> ""; // Empty response
        }

        if ( Objects.isNull( webClient ) )
        {
            webClient = WEB_CLIENT;
        }

        try
        {
            // Stream is closed on completion of streaming data, unless there is an error response
            WebClient.ClientResponse response =
                    webClient.getFromWeb( uri, WebClientUtils.getDefaultRetryStates() );
            int httpStatus = response.getStatusCode();

            if ( isMissingResponse.test( httpStatus ) )
            {
                String possibleError = errorUnpacker.apply( response );

                if ( Objects.nonNull( possibleError ) )
                {
                    possibleError = " Received this error message: " + possibleError;
                }

                LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}.{}",
                             httpStatus,
                             uri,
                             possibleError );

                // Clean up now
                ReaderUtilities.closeWebClientResponse( response );

                // Flag to the caller as no data
                return null;
            }
            else if ( isErrorResponse.test( httpStatus ) )
            {
                String possibleError = errorUnpacker.apply( response );

                if ( Objects.nonNull( possibleError ) )
                {
                    possibleError = ". Received this error message: " + possibleError;
                }

                // Error response, so clean up now
                ReaderUtilities.closeWebClientResponse( response );

                throw new ReadException( "Failed to read data from '"
                                         + uri
                                         +
                                         "' due to HTTP status code "
                                         + httpStatus
                                         + possibleError );
            }

            return response.getResponse();
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to acquire a byte stream from "
                                     + uri
                                     + ".",
                                     e );
        }
    }

    /**
     * Parses an instant from a formatted date and time string.
     * @param date the date
     * @param time the time in UTC
     * @param offset the time zone offset
     * @return the instant
     */

    public static Instant parseInstant( String date, String time, ZoneOffset offset )
    {
        LocalDate localDate = LocalDate.parse( date );
        LocalTime localTime = LocalTime.parse( time );
        LocalDateTime localDateTime = LocalDateTime.of( localDate, localTime );
        return localDateTime.toInstant( offset );
    }

    /**
     * Parses the missing value from the {@link TimeSeriesHeader#missingValue()}.
     *
     * @param header the header
     * @return the missing value double
     * @throws ReadException if the missing value could not be parsed
     * @throws NullPointerException if the header is missing
     */

    public static double getMissingValueDouble( TimeSeriesHeader header )
    {
        Objects.requireNonNull( header );
        if ( Objects.isNull( header.missingValue() )
             || "null".equalsIgnoreCase( header.missingValue() ) )
        {
            return Double.NaN;
        }

        try
        {
            return Double.parseDouble( header.missingValue() );
        }
        catch ( NumberFormatException e )
        {
            throw new ReadException( "The missing value string is not a valid number: "
                                     + header.missingValue()
                                     + "." );
        }
    }

    /**
     * Translates a {@link TimeSeriesHeader} into a {@link TimeSeriesMetadata}.
     *
     * @param header the header
     * @param zoneOffset the time zone offset
     * @return the metadata
     */

    public static TimeSeriesMetadata getTimeSeriesMetadataFromHeader( TimeSeriesHeader header,
                                                                      ZoneOffset zoneOffset )
    {
        Map<ReferenceTime.ReferenceTimeType, Instant> referenceTimes =
                ReaderUtilities.getReferenceTimesFromHeader( header, zoneOffset );

        String locationWkt = ReaderUtilities.getWktFromHeader( header );
        Geometry geometry = MessageUtilities.getGeometry( header.locationId(),
                                                          header.locationDescription(),
                                                          null,
                                                          locationWkt );
        Feature feature = Feature.of( geometry );

        TimeScale.TimeScaleFunction function = TimeScale.TimeScaleFunction.UNKNOWN;
        TimeScaleOuter timeScale;

        if ( "instantaneous".equalsIgnoreCase( header.type() ) )
        {
            timeScale = TimeScaleOuter.of();
        }
        else if ( "nonequidistant".equals( header.timeStepUnit() ) )
        {
            LOGGER.debug( "Discovered a time-series without explicit time-scale information and with a non-equidistant "
                          + "time-step. The time-scale information must be constant, but cannot be determined." );
            timeScale = null;
        }
        else
        {
            // See #59438
            Duration timeStep = ReaderUtilities.getTimeStep( header.timeStepUnit(), header.timeStepMultiplier() );
            LOGGER.debug( "Detected a time-series with a non-instantaneous time scale. Assuming that the time scale "
                          + "period is equal to the nominated timestep, which is {}.", timeStep );
            timeScale = TimeScaleOuter.of( timeStep, function );
        }

        return TimeSeriesMetadata.of( referenceTimes, timeScale, header.parameterId(), feature, header.units() );
    }

    /**
     * Parses the time-step from a unit and multiplier.
     * @param unit the unit
     * @param multiplier the multiplier, possibly null
     * @return the time-step
     * @throws ReadException if the unit and/or multiplier are not correctly formatted
     */

    private static Duration getTimeStep( String unit, String multiplier )
    {
        Objects.requireNonNull( unit );

        if ( "nonequidistant".equalsIgnoreCase( unit ) )
        {
            LOGGER.debug( "Discovered a data-source with an irregular time-series, i.e., a "
                          + "nonequidistant time-step unit. " );

            return null;
        }

        String unitString = unit.toUpperCase() + "S";
        Objects.requireNonNull( multiplier );

        try
        {
            ChronoUnit chronoUnit = ChronoUnit.valueOf( unitString );
            int amount = Integer.parseInt( multiplier );
            return Duration.of( amount, chronoUnit );
        }
        catch ( NumberFormatException e )
        {
            throw new ReadException( "While attempting to read a time-series data source, could not parse the value of "
                                     + "the time step multiplier associated with one or more time-series. The "
                                     + "multiplier should be an integer, but was: '"
                                     + multiplier
                                     + "'. The time step unit was: '"
                                     + unit
                                     + "'." );
        }
        catch ( IllegalArgumentException e )
        {
            throw new ReadException( "While attempting to read a time-series data source, could not parse the value of "
                                     + "the time step unit associated with one or more time-series. The unit was: '"
                                     + unit
                                     + "'. An 'S' was appended to this unit string and interpreted as '"
                                     + unitString
                                     + "'." );
        }
    }

    /**
     * Creates a map of reference times from the header.
     * @param header the header
     * @param zoneOffset the time zone offset
     * @return map of reference times
     */
    private static Map<ReferenceTime.ReferenceTimeType, Instant> getReferenceTimesFromHeader( TimeSeriesHeader header,
                                                                                              ZoneOffset zoneOffset )
    {
        Map<ReferenceTime.ReferenceTimeType, Instant> referenceTimes =
                new EnumMap<>( ReferenceTime.ReferenceTimeType.class );

        if ( Objects.nonNull( header.forecastDateDate() )
             && Objects.nonNull( header.forecastDateTime() ) )
        {
            Instant t0 =
                    ReaderUtilities.parseInstant( header.forecastDateDate(), header.forecastDateTime(), zoneOffset );
            referenceTimes.put( ReferenceTime.ReferenceTimeType.T0, t0 );
        }

        return Collections.unmodifiableMap( referenceTimes );
    }

    /**
     * Creates a wkt from the time-series header.
     *
     * @param header the header
     * @return a wkt string
     */
    private static String getWktFromHeader( TimeSeriesHeader header )
    {
        String locationWkt = null;

        // When x and y are present, prefer those to lon, lat.
        // Going to Double back to String seems frivolous, but it validates data.
        if ( Objects.nonNull( header.x() )
             && Objects.nonNull( header.y() ) )
        {
            StringJoiner wktGeometry = new StringJoiner( " " );
            wktGeometry.add( "POINT (" );
            wktGeometry.add( header.x() );
            wktGeometry.add( header.y() );
            if ( Objects.nonNull( header.z() ) )
            {
                wktGeometry.add( header.z() );
            }
            wktGeometry.add( ")" );
            locationWkt = wktGeometry.toString();
        }
        else if ( Objects.nonNull( header.latitude() )
                  && Objects.nonNull( header.longitude() ) )
        {
            StringJoiner wktGeometry = new StringJoiner( " " );
            wktGeometry.add( "POINT (" );
            wktGeometry.add( header.longitude() );
            wktGeometry.add( header.latitude() );
            wktGeometry.add( ")" );
            locationWkt = wktGeometry.toString();
        }

        return locationWkt;
    }

    /**
     * Attempts to acquire thresholds from an external source and populate them in the supplied declaration, as needed.
     * @param thresholdSource the threshold source
     * @param evaluation the declaration to adjust
     * @return the adjusted declaration, including any thresholds acquired from the external source
     */

    private static EvaluationDeclaration fillThresholds( ThresholdSource thresholdSource,
                                                         EvaluationDeclaration evaluation )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( thresholdSource );

        if ( Objects.isNull( thresholdSource.uri() ) )
        {
            throw new ThresholdReadingException( "Cannot read from a threshold source with a missing URI. Please "
                                                 + "add a URI for each threshold source that should be read." );
        }

        // Acquire the feature names for which thresholds are required
        DatasetOrientation orientation = thresholdSource.featureNameFrom();

        if ( Objects.isNull( orientation ) )
        {
            throw new ThresholdReadingException( "The 'feature_name_from' is missing from the 'threshold_sources', "
                                                 + "which is not allowed." );
        }

        // If the orientation for service thresholds is 'BASELINE', then a baseline must be present
        if ( orientation == DatasetOrientation.BASELINE && !DeclarationUtilities.hasBaseline( evaluation ) )
        {
            throw new ThresholdReadingException( "The 'threshold_sources' declaration requested that feature names "
                                                 + "with an orientation of '"
                                                 + DatasetOrientation.BASELINE
                                                 + "' are used to correlate features with thresholds, but no "
                                                 + "'baseline' dataset was discovered. Please add a 'baseline' dataset "
                                                 + "or fix the 'feature_name_from' in the 'threshold_sources' "
                                                 + "declaration." );
        }

        // Assemble the features that require thresholds
        Set<GeometryTuple> features = DeclarationUtilities.getFeatures( evaluation );

        // No features?
        if ( features.isEmpty() )
        {
            throw new ThresholdReadingException( "While attempting to read thresholds from an external source, "
                                                 + "discovered no features in the declaration for which thresholds "
                                                 + "could be acquired. Please add some features to the declaration "
                                                 + "using 'features', 'feature_groups' or 'feature_service' and try "
                                                 + "again." );
        }

        // Get the feature authority for this data orientation
        FeatureAuthority featureAuthority = DeclarationUtilities.getFeatureAuthorityFor( evaluation, orientation );

        // Baseline orientation and some feature tuples present that are missing a baseline feature?
        if ( orientation == DatasetOrientation.BASELINE
             && features.stream()
                        .anyMatch( next -> !next.hasBaseline() ) )
        {
            throw new ThresholdReadingException( "Discovered declaration for 'threshold_sources', which requests "
                                                 + "thresholds whose feature names have an orientation of '"
                                                 + DatasetOrientation.BASELINE
                                                 + "'. However, some features were discovered with a missing '"
                                                 + DatasetOrientation.BASELINE
                                                 + "' feature name. Please fix the 'feature_name_from' in the "
                                                 + "'threshold_sources' declaration or supply fully composed feature "
                                                 + "tuples with an appropriate feature for the '"
                                                 + DatasetOrientation.BASELINE
                                                 + "' dataset." );
        }

        // Acquire a threshold reader
        ThresholdReader reader = ThresholdReaderFactory.getReader( thresholdSource );

        // Get the adjusted feature names mapped to the original names
        Set<String> featureNames = DeclarationUtilities.getFeatureNamesFor( features, orientation );

        // Continue to read the thresholds
        Set<wres.config.components.Threshold> thresholds = reader.read( thresholdSource,
                                                                        featureNames,
                                                                        featureAuthority );

        // Check that some thresholds are available for features to evaluate
        Set<String> intersection = thresholds.stream()
                                             .map( t -> t.feature()
                                                         .getName() )
                                             .collect( Collectors.toCollection( TreeSet::new ) );
        intersection.retainAll( featureNames );

        if ( intersection.isEmpty() )
        {
            throw new ThresholdReadingException( "While reading thresholds from an external source, failed to discover "
                                                 + "thresholds for any of the geographic features to evaluate. Please "
                                                 + "ensure that each threshold source contains thresholds for at least "
                                                 + "some of the geographic features to evaluate. The invalid threshold "
                                                 + "source is: "
                                                 + thresholdSource.uri() );
        }

        LOGGER.trace( "Read the following thresholds from {}: {}.", thresholdSource.uri(), thresholds );

        // Adjust the declaration and return it
        return DeclarationUtilities.addThresholds( evaluation, thresholds );
    }

    /**
     * Returns the datetime interval required for the specified data source.
     * @param dataSource the data source
     * @param declaration the evaluation declaration
     * @return the time interval required for the data source
     */

    private static Pair<Instant, Instant> getDatasetInterval( DataSource dataSource,
                                                              EvaluationDeclaration declaration )
    {
        // Get the boundaries
        TimeInterval dates = declaration.validDates();
        Instant specifiedEarliest = null;
        Instant specifiedLatest = null;
        if ( Objects.nonNull( dates ) )
        {
            specifiedEarliest = dates.minimum();
            specifiedLatest = dates.maximum();
        }

        // If the evaluation has a climatological baseline and EITHER this is the baseline dataset OR this data source
        // is the same as the baseline data source (i.e., duplicated), compare with the valid dates above and use the
        // longer of the two intervals. See #118435
        if ( ReaderUtilities.isBaselineOrHasSameDatasetAsBaseline( dataSource, declaration )
             && DeclarationUtilities.hasGeneratedBaseline( declaration.baseline() )
             && declaration.baseline()
                           .generatedBaseline()
                           .method() == GeneratedBaselines.CLIMATOLOGY
             && ( Objects.nonNull( declaration.baseline()
                                              .generatedBaseline()
                                              .minimumDate() )
                  || Objects.nonNull( declaration.baseline()
                                                 .generatedBaseline()
                                                 .maximumDate() ) ) )
        {
            Instant minimumDate = declaration.baseline()
                                             .generatedBaseline()
                                             .minimumDate();
            Instant maximumDate = declaration.baseline()
                                             .generatedBaseline()
                                             .maximumDate();

            if ( Objects.isNull( specifiedEarliest )
                 || ( Objects.nonNull( minimumDate )
                      && minimumDate != Instant.MIN
                      && specifiedEarliest.isAfter( minimumDate ) ) )
            {
                LOGGER.debug( "Used the 'minimum_date' of {} from the {} dataset on the {} dataset, because this is "
                              + "earlier than the 'minimum' value of the 'valid_dates', which is {}.",
                              minimumDate,
                              DatasetOrientation.BASELINE,
                              dataSource.getDatasetOrientation(),
                              specifiedEarliest );
                specifiedEarliest = minimumDate;
            }

            if ( Objects.isNull( specifiedLatest )
                 || ( Objects.nonNull( maximumDate )
                      && maximumDate != Instant.MAX
                      && specifiedLatest.isBefore( maximumDate ) ) )
            {
                LOGGER.debug( "Used the 'maximum_date' of {} from the {} dataset on the {} dataset, because this is "
                              + "later than the 'maximum' value of the 'valid_dates', which is {}.",
                              maximumDate,
                              DatasetOrientation.BASELINE,
                              dataSource.getDatasetOrientation(),
                              specifiedLatest );
                specifiedLatest = maximumDate;
            }
        }

        // Validate request
        if ( Objects.isNull( specifiedEarliest )
             || Objects.isNull( specifiedLatest ) )
        {
            throw new DeclarationException( "Could not determine the date range to request for a web data source. One "
                                            + "must declare a date range when requesting data from a web service, such "
                                            + "as the 'valid_dates' with both the 'minimum' and 'maximum'." );
        }

        Pair<Instant, Instant> range = Pair.of( specifiedEarliest, specifiedLatest );

        LOGGER.debug( "When building a reader for time-series data from a web source, received a complete pair "
                      + "declaration, which will be used to chunk requests by feature and time range: {}.",
                      range );

        return range;
    }

    /**
     * @param dataSource the data source to check
     * @param declaration the evaluation declaration
     * @return whether this data source is a baseline source or has the same dataset as a baseline source
     */

    private static boolean isBaselineOrHasSameDatasetAsBaseline( DataSource dataSource,
                                                                 EvaluationDeclaration declaration )
    {
        return dataSource.getDatasetOrientation() == DatasetOrientation.BASELINE
               || ( DeclarationUtilities.hasBaseline( declaration )
                    && declaration.baseline()
                                  .dataset()
                                  .sources() // #121751
                                  .equals( dataSource.getContext()
                                                     .sources() ) );
    }

    /**
     * Do not construct.
     */

    private ReaderUtilities()
    {
    }
}
