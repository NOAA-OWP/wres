package wres.io.thresholds.wrds;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ThresholdsConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.reading.wrds.ReadValueManager;
import wres.io.thresholds.exceptions.StreamIOException;
import wres.io.thresholds.wrds.response.GeneralThresholdExtractor;
import wres.io.thresholds.wrds.response.GeneralThresholdResponse;
import wres.io.utilities.WebClient;
import wres.io.retrieval.UnitMapper;
import wres.system.SystemSettings;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

public final class GeneralWRDSReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WRDSReader.class );
    private static Pair<SSLContext, X509TrustManager> SSL_CONTEXT = ReadValueManager.getSslContextTrustingDodSigner();
    private static final WebClient WEB_CLIENT = new WebClient( SSL_CONTEXT, true );
    private static final ObjectMapper JSON_OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );
    public static final int LOCATION_REQUEST_COUNT = 20;
    private final SystemSettings systemSettings;

    /**
     * The purpose for allowing an instance is to maintain the single-parameter
     * functional interface of getResponse to facilitate stream usage ~L110.
     * 
     * The reason SystemSettings is needed is to get the data directory to
     * complete relative paths given, for example "dir/file.csv" is given rather
     * than "file:///path/to/dir/file.csv".
     *
     * Instantiation not private so that unit tests can access methods.
     *
     * The public interface to this class remains as static helper functions.
     *
     * There are likely better ways to achieve these goals. Go for it.
     *
     * @param systemSettings The system settings to use to complete URIs.
     */

    GeneralWRDSReader( SystemSettings systemSettings )
    {
        Objects.requireNonNull( systemSettings );
        this.systemSettings = systemSettings;
    }

    /**
     * The top-level method to call to obtain thresholds
     * @param systemSettings Settings.
     * @param threshold User declaration.
     * @param unitMapper Target unit mapper.
     * @param features List of features user specified.
     * @return Map of feature to threshold.
     * @throws IOException At various points, this can be thrown.
     */
    public static Map<WrdsLocation, Set<ThresholdOuter>> readThresholds(
                                                                         final SystemSettings systemSettings,
                                                                         final ThresholdsConfig threshold,
                                                                         final UnitMapper unitMapper,
                                                                         final Set<String> features )
            throws IOException
    {
        ThresholdsConfig.Source source = (ThresholdsConfig.Source) threshold.getCommaSeparatedValuesOrSource();
        List<URI> addresses = new ArrayList<>();
        URI fullSourceAddress = GeneralWRDSReader.getAbsoluteUri( source.getValue(),
                                                                  systemSettings );

        if ( fullSourceAddress.getScheme()
                              .toLowerCase()
                              .equals( "file" ) )
        {
            addresses.add( fullSourceAddress );
        }
        else
        {
            Set<String> locationGroups = groupLocations( features );
            final String originalPath = fullSourceAddress.getPath();
            URIBuilder builder = new URIBuilder( fullSourceAddress );
            final String adjustedPath;

            if ( !originalPath.endsWith( "/" ) )
            {
                adjustedPath = originalPath + "/";
            }
            else
            {
                adjustedPath = originalPath;
            }

            LOGGER.debug( "Went from source {} to path {} to path {}",
                          source.getValue(),
                          originalPath,
                          adjustedPath );

            for ( String group : locationGroups )
            {
                String path = adjustedPath + group + "/";
                builder.setPath( path );

                try
                {
                    URI address = builder.build();
                    addresses.add( address );
                    LOGGER.debug( "Added uri {}", address );
                }
                catch ( URISyntaxException use )
                {
                    throw new RuntimeException( "Unable to build URI from "
                                                + builder,
                                                use );
                }
            }
        }

        GeneralWRDSReader reader = new GeneralWRDSReader( systemSettings );
        Map<WrdsLocation, Set<ThresholdOuter>> thresholdMapping;

        try
        {
            thresholdMapping = addresses.parallelStream()
                                        .map( reader::getResponse )
                                        .filter( Objects::nonNull )
                                        .map( thresholdResponse -> extract( thresholdResponse, threshold, unitMapper ) )
                                        .flatMap( featurePlusSetMap -> featurePlusSetMap.entrySet().stream() )
                                        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
        }
        catch ( StreamIOException streamReadingException )
        {
            throw new IOException( streamReadingException.getCause() );
        }

        // Filter out locations that only have all data
        thresholdMapping = thresholdMapping
                                           .entrySet()
                                           .parallelStream()
                                           .filter(
                                                    entry -> !entry.getValue()
                                                                   .stream()
                                                                   .allMatch( ThresholdOuter::isAllDataThreshold ) )
                                           .collect( Collectors.toUnmodifiableMap( Map.Entry::getKey,
                                                                                   Map.Entry::getValue ) );

        if ( thresholdMapping.isEmpty() )
        {
            throw new IOException( "No thresholds could be retrieved from " + fullSourceAddress.toString() );
        }

        return thresholdMapping;
    }

    /**
     * 
     * @param response The response to parse.
     * @param config The user declaration for the thresholds.
     * @param desiredUnitMapper The target unit mapper.
     * @return A map of WRDS feature to set of thresholds.
     */
    protected static Map<WrdsLocation, Set<ThresholdOuter>> extract( GeneralThresholdResponse response,
                                                                     ThresholdsConfig config,
                                                                     UnitMapper desiredUnitMapper )
    {
        ThresholdsConfig.Source source = (ThresholdsConfig.Source) config.getCommaSeparatedValuesOrSource();
        ThresholdConstants.ThresholdDataType side = ThresholdConstants.ThresholdDataType.LEFT;

        if ( Objects.nonNull( config.getApplyTo() ) )
        {
            side = ThresholdConstants.ThresholdDataType.valueOf( config.getApplyTo().name() );
        }

        ThresholdConstants.Operator operator = ThresholdConstants.Operator.GREATER;

        if ( Objects.nonNull( config.getOperator() ) )
        {
            operator = DataFactory.getThresholdOperator( config );
        }

        GeneralThresholdExtractor extractor = new GeneralThresholdExtractor( response )
                                                                                       .from( source.getProvider() )
                                                                                       .operatesBy( operator )
                                                                                       .onSide( side );

        if ( source.getRatingProvider() != null )
        {
            extractor.ratingFrom( source.getRatingProvider() );
        }

        //Flow is the default if the parameterToMeasure is not specified.  Note that this 
        //works for unified schema thresholds, such as recurrence flows, because the metadata
        //does not specify the parameter, so that parameterToMeasure is ignored.
        if ( ( source.getParameterToMeasure() != null )
             && ( source.getParameterToMeasure().toLowerCase().equals( "stage" ) ) )
        {
            extractor.readStage();
        }
        else
        {
            extractor.readFlow();
        }

        extractor.convertTo( desiredUnitMapper );

        return extractor.extract();
    }

    protected static Set<String> groupLocations( Set<String> features )
    {
        Set<String> locationGroups = new HashSet<>();
        StringJoiner locationJoiner = new StringJoiner( "," );
        int counter = 0;

        for ( String feature : features )
        {
            if ( counter % LOCATION_REQUEST_COUNT == 0 && locationJoiner.length() > 0 )
            {
                locationGroups.add( locationJoiner.toString() );
                locationJoiner = new StringJoiner( "," );
                counter = 0;
            }

            locationJoiner.add( feature );
            counter++;
        }

        if ( locationJoiner.length() > 0 )
        {
            locationGroups.add( locationJoiner.toString() );
        }

        return locationGroups;
    }

    /**
     * This either forwards the call to {@link #getRemoteResponse(URI)} if the URI
     * starts with http or accesses the address as a file if not.
     * @param address The URI from which to read.
     * @return The response or null if problems occur access the URI.
     * @throws StreamIOException If the URI call fails.
     */
    GeneralThresholdResponse getResponse( final URI address ) throws StreamIOException
    {
        LOGGER.debug( "Opening URI {}", address );
        try
        {
            URI fullAddress = GeneralWRDSReader.getAbsoluteUri( address,
                                                                this.systemSettings );

            if ( fullAddress.getScheme()
                            .toLowerCase()
                            .startsWith( "http" ) )
            {
                return getRemoteResponse( fullAddress );
            }
            else
            {
                File thresholdFile = new File( fullAddress );

                try ( InputStream data = new FileInputStream( thresholdFile ) )
                {
                    byte[] rawForecast = IOUtils.toByteArray( data );
                    return JSON_OBJECT_MAPPER.readValue( rawForecast, GeneralThresholdResponse.class );
                }
            }
        }
        catch ( IOException ioe )
        {
            throw new StreamIOException( "Error encountered while requesting WRDS threshold data", ioe );
        }
    }

    /**
     * Uses Jackson to obtain the resposne for a given URI.
     * @param inputAddress The URI
     * @return The response or null if the code returned is in the 400s.
     * @throws IOException If the URI call has issues.
     */
    private static GeneralThresholdResponse getRemoteResponse( URI inputAddress ) throws IOException
    {
        try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( inputAddress ) )
        {

            if ( response.getStatusCode() >= 400 && response.getStatusCode() < 500 )
            {
                LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                             response.getStatusCode(),
                             inputAddress );
                return null;
            }

            return JSON_OBJECT_MAPPER.readValue( response.getResponse(), GeneralThresholdResponse.class );
        }
    }


    /**
     * Create a complete-with-scheme, absolute URI for the given URI.
     * @param maybeIncomplete A potentially incomplete URI (relateive path)
     * @param systemSettings The settings to use to create a complete URI.
     * @return The complete URI with guaranteed scheme.
     */

    private static URI getAbsoluteUri( URI maybeIncomplete,
                                       SystemSettings systemSettings )
    {
        if ( Objects.isNull( maybeIncomplete.getScheme() ) )
        {
            return systemSettings.getDataDirectory()
                                 .toUri()
                                 .resolve( maybeIncomplete.getPath() );

        }

        return maybeIncomplete;
    }
}
