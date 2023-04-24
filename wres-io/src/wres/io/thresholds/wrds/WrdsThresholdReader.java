package wres.io.thresholds.wrds;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.config.yaml.components.ThresholdService;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.units.UnitMapper;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.geography.wrds.version.WrdsLocationRootVersionDocument;
import wres.io.ingesting.PreIngestException;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.web.WebClient;
import wres.io.thresholds.ThresholdReadingException;
import wres.io.thresholds.wrds.v2.ThresholdExtractor;
import wres.io.thresholds.wrds.v2.ThresholdResponse;
import wres.io.thresholds.wrds.v3.GeneralThresholdExtractor;
import wres.io.thresholds.wrds.v3.GeneralThresholdResponse;
import wres.statistics.generated.Threshold;

/**
 * Reads thresholds from different versions of the WRDS threshold service.
 *
 * @author James Brown
 * @author Hank Herr
 * @author Chris Tubbs
 */
public class WrdsThresholdReader
{
    /** The number of location requests." */
    static final int LOCATION_REQUEST_COUNT = 20;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsThresholdReader.class );

    /** The context for establishing a secure connection. */
    private static final Pair<SSLContext, X509TrustManager> SSL_CONTEXT;

    /** Path delimiter. */
    private static final String PATH_DELIM = "/";

    /** Mapper for reading service responses in JSON format and translating them to POJOs. */
    private static final ObjectMapper JSON_OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );

    // Load the SSL/TLS context upfront
    static
    {
        try
        {
            SSL_CONTEXT = ReaderUtilities.getSslContextTrustingDodSignerForWrds();
        }
        catch ( PreIngestException e )
        {
            throw new ExceptionInInitializerError( "Failed to acquire the TLS context for connecting to WRDS: "
                                                   + e.getMessage() );
        }
    }

    /** Client for web connections. */
    private static final WebClient WEB_CLIENT = new WebClient( SSL_CONTEXT, true );

    /**
     * Creates an instance.
     * @return an instance
     */

    public static WrdsThresholdReader of()
    {
        return new WrdsThresholdReader();
    }

    /**
     * Reads thresholds.
     * @param thresholdService the threshold service declaration
     * @param unitMapper a unit mapper to translate and set threshold units
     * @param featureNames the named features for which thresholds are required
     * @param featureAuthority the feature authority associated with the feature names
     * @return the thresholds mapped against features
     */
    public Map<WrdsLocation, Set<Threshold>> readThresholds( ThresholdService thresholdService,
                                                             UnitMapper unitMapper,
                                                             Set<String> featureNames,
                                                             FeatureAuthority featureAuthority )
    {
        Objects.requireNonNull( thresholdService );
        Objects.requireNonNull( unitMapper );
        Objects.requireNonNull( featureNames );

        // The list of URIs to acquire, build from the input URI as needed
        List<URI> addresses = new ArrayList<>();

        URI uri = thresholdService.uri();

        // Web service
        if ( uri.getScheme()
                .toLowerCase()
                .startsWith( "http" ) )
        {
            // Build the location groups to use.
            Set<String> locationGroups = this.chunkFeatures( featureNames );
            URIBuilder builder = new URIBuilder( uri );
            String originalPath = uri.getPath();
            String adjustedPath = this.getAdjustedPath( originalPath, featureAuthority );

            LOGGER.debug( "Reading features from the WRDS threshold service using a path of {}, which was obtained "
                          + "from the URI: {}.", adjustedPath, uri );

            // For each location group...
            for ( String group : locationGroups )
            {
                // Append it to the path.
                String path = adjustedPath + group + PATH_DELIM;
                builder.setPath( path );

                // Build the URI and store it in addresses.
                try
                {
                    URI address = builder.build();
                    addresses.add( address );

                    LOGGER.debug( "Added URI for which WRDS thresholds will be requested: {}", address );
                }
                catch ( URISyntaxException use )
                {
                    throw new ThresholdReadingException( "Unable to build a URI from "
                                                         + builder,
                                                         use );
                }
            }
        }
        // Read a file-like source
        else
        {
            uri = WrdsThresholdReader.getAbsoluteUri( uri );
            addresses.add( uri );
        }

        Map<WrdsLocation, Set<Threshold>> thresholdMapping = this.readThresholds( thresholdService,
                                                                                  addresses,
                                                                                  unitMapper );

        if ( thresholdMapping.isEmpty() )
        {
            throw new ThresholdReadingException( "No thresholds could be retrieved from " + uri );
        }

        LOGGER.debug( "The following thresholds were obtained from WRDS: {}.", thresholdMapping );

        return thresholdMapping;
    }

    /**
     * Reads thresholds from a collection of addresses.
     * @param addresses the addresses
     * @param unitMapper the unit mapper
     * @return the thresholds
     */
    private Map<WrdsLocation, Set<Threshold>> readThresholds( ThresholdService thresholdService,
                                                              List<URI> addresses,
                                                              UnitMapper unitMapper )
    {
        Map<WrdsLocation, Set<Threshold>> thresholdMapping;

        try
        {
            //Get the non-null responses for the addresses, extract the thresholds,
            //and collect them into a map.
            thresholdMapping = addresses.parallelStream()
                                        .map( this::getResponse )
                                        .filter( Objects::nonNull )
                                        .map( thresholdResponse -> this.extract( thresholdResponse,
                                                                                 thresholdService,
                                                                                 unitMapper ) )
                                        .flatMap( featurePlusSetMap -> featurePlusSetMap.entrySet().stream() )
                                        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );

        }
        catch ( StreamIOException streamReadingException )
        {
            throw new ThresholdReadingException( "Encountered an error while reading thresholds from WRDS.",
                                                 streamReadingException );
        }

        // Filter out locations that only have all data
        thresholdMapping = thresholdMapping
                .entrySet()
                .parallelStream()
                .filter(
                        entry -> !entry.getValue()
                                       .stream()
                                       .allMatch( next -> ThresholdOuter.ALL_DATA.getThreshold()
                                                                                 .equals( next ) ) )
                .collect( Collectors.toUnmodifiableMap( Map.Entry::getKey,
                                                        Map.Entry::getValue ) );

        return thresholdMapping;
    }

    /**
     * @param responseBytes array of bytes to parse
     * @param thresholdService the threshold service
     * @param desiredUnitMapper the desired units
     * @return the thresholds
     * @throws ThresholdReadingException if the thresholds could not be read
     */
    private Map<WrdsLocation, Set<Threshold>> extract( byte[] responseBytes,
                                                       ThresholdService thresholdService,
                                                       UnitMapper desiredUnitMapper )
    {
        ThresholdOrientation rawSide = thresholdService.applyTo();
        ThresholdOperator rawOperator = thresholdService.operator();

        // TODO: remove these duplicate enumerations
        ThresholdOrientation side = ThresholdOrientation.valueOf( rawSide.name() );
        ThresholdOperator operator = ThresholdOperator.valueOf( rawOperator.name() );

        try
        {
            // Read the version information from the response, first.  This is used to
            // identify version being processed
            WrdsLocationRootVersionDocument versionDoc =
                    JSON_OBJECT_MAPPER.readValue( responseBytes, WrdsLocationRootVersionDocument.class );

            // Extract using V3 API reader
            if ( versionDoc.isDeploymentInfoPresent() )
            {
                // Get the response and construct the extractor.
                GeneralThresholdResponse response =
                        JSON_OBJECT_MAPPER.readValue( responseBytes, GeneralThresholdResponse.class );
                GeneralThresholdExtractor extractor = new GeneralThresholdExtractor( response )
                        .from( thresholdService.provider() )
                        .operatesBy( operator )
                        .onSide( side );

                // If rating provider is not null, add it, too.
                if ( Objects.nonNull( thresholdService.ratingProvider() ) )
                {
                    extractor.ratingFrom( thresholdService.ratingProvider() );
                }

                // Flow is the default if the parameter is not specified. Note that this
                // works for unified schema thresholds, such as recurrence flows, because the metadata
                // does not specify the parameter, so that parameter is ignored.
                if ( "stage".equalsIgnoreCase( thresholdService.parameter() ) )
                {
                    extractor.readStage();
                }
                else
                {
                    extractor.readFlow();
                }

                // Establish target unit
                extractor.convertTo( desiredUnitMapper );

                return extractor.extract();
            }
            // Extract using V2 or older reader
            else
            {
                ThresholdResponse response = JSON_OBJECT_MAPPER.readValue( responseBytes, ThresholdResponse.class );
                ThresholdExtractor extractor = new ThresholdExtractor( response )
                        .from(thresholdService.provider() )
                        .operatesBy( operator )
                        .onSide( side );

                if ( Objects.nonNull( thresholdService.ratingProvider() ) )
                {
                    extractor.ratingFrom( thresholdService.ratingProvider() );
                }

                if ( "stage".equalsIgnoreCase( thresholdService.parameter() ) )
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
        }
        catch ( IOException ioe )
        {
            throw new ThresholdReadingException( "Error encountered while requesting WRDS threshold data", ioe );
        }
    }

    /**
     * @param features the features to group
     * @return the delimited features chunked by {@link #LOCATION_REQUEST_COUNT}
     */
    private Set<String> chunkFeatures( Set<String> features )
    {
        Set<String> locationGroups = new HashSet<>();
        StringJoiner locationJoiner = new StringJoiner( "," );
        int counter = 0;

        // Use a predictable iteration order
        Set<String> orderedFeatures = new TreeSet<>( features );
        for ( String feature : orderedFeatures )
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
     * This is protected to support testing.
     * @param address the address
     * @return The response in byte[], where the URI can point to a file or a website.
     */
    byte[] getResponse( final URI address ) throws StreamIOException
    {
        LOGGER.debug( "Opening URI {}", address );
        try
        {
            URI fullAddress = WrdsThresholdReader.getAbsoluteUri( address );

            if ( fullAddress.getScheme()
                            .toLowerCase()
                            .startsWith( "http" ) )
            {
                return getRemoteResponse( fullAddress );
            }
            else
            {
                try ( InputStream data = Files.newInputStream( Paths.get( fullAddress ) ) )
                {
                    return IOUtils.toByteArray( data );
                }
            }
        }
        catch ( IOException ioe )
        {
            throw new StreamIOException( "Error encountered while requesting WRDS threshold data", ioe );
        }
    }

    /**
     * @return The response from the remote URI as bytes[].
     */
    private static byte[] getRemoteResponse( URI inputAddress ) throws IOException
    {
        try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( inputAddress ) )
        {

            if ( response.getStatusCode() >= 400 && response.getStatusCode() < 500 )
            {
                LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                             response.getStatusCode(),
                             inputAddress );
                return new byte[0];
            }

            return response.getResponse().readAllBytes();
        }
    }

    /**
     * Create a complete-with-scheme, absolute URI for the given URI.
     * @param maybeIncomplete A potentially incomplete URI (relative path)
     * @return The complete URI with guaranteed scheme.
     */

    private static URI getAbsoluteUri( URI maybeIncomplete )
    {
        if ( Objects.isNull( maybeIncomplete.getScheme() ) )
        {
            Path dataDirectory = Path.of( System.getProperty( "user.dir" ) );
            return dataDirectory.toUri()
                                .resolve( maybeIncomplete.getPath() );

        }

        return maybeIncomplete;
    }

    /**
     * Adjusts the path to a properly formatted path for WRDS threshold requests, if possible.
     * @param originalPath the path to inspect and adjust, as needed
     * @param featureAuthority the feature authority used
     * @return the adjusted path
     * @throws ThresholdReadingException if the user specified an invalid path
     */
    private String getAdjustedPath( String originalPath, FeatureAuthority featureAuthority )
    {
        String adjustedPath;

        // First, add a slash to make it easy to use endsWith later.
        if ( !originalPath.endsWith( PATH_DELIM ) )
        {
            adjustedPath = originalPath + PATH_DELIM;
        }
        else
        {
            adjustedPath = originalPath;
        }

        // Adjust the path against the actual feature authority
        adjustedPath = this.getPathAdjustedByFeatureAuthority( adjustedPath,
                                                               featureAuthority,
                                                               // If the featureAuthority is one of these
                                                               // But see #113677-109. CUSTOM should probably be an
                                                               // error in this context
                                                               Set.of( FeatureAuthority.NWS_LID,
                                                                       FeatureAuthority.CUSTOM ),
                                                               // It must be adjusted to include this
                                                               FeatureAuthority.NWS_LID );

        adjustedPath = this.getPathAdjustedByFeatureAuthority( adjustedPath,
                                                               featureAuthority,
                                                               // If the featureAuthority is one of these
                                                               Set.of( FeatureAuthority.USGS_SITE_CODE ),
                                                               // It must be adjusted to include this
                                                               FeatureAuthority.USGS_SITE_CODE );

        return this.getPathAdjustedByFeatureAuthority( adjustedPath,
                                                       featureAuthority,
                                                       // If the featureAuthority is one of these
                                                       Set.of( FeatureAuthority.NWM_FEATURE_ID ),
                                                       // It must be adjusted to include this
                                                       FeatureAuthority.NWM_FEATURE_ID );
    }

    /**
     * Attempts to adjust the input path according to the authorities provided.
     * @param path the path to adjust
     * @param actualAuthority the actual feature authority
     * @param targetAuthorities the target set of authorities
     * @param goodAuthority the good feature authority to be used when modifying the path
     * @return the adjusted path
     * @throws ThresholdReadingException if the path is inconsistent with the actual feature authority
     */
    private String getPathAdjustedByFeatureAuthority( String path,
                                                      FeatureAuthority actualAuthority,
                                                      Set<FeatureAuthority> targetAuthorities,
                                                      FeatureAuthority goodAuthority )
    {
        Set<FeatureAuthority> badAuthorities = Arrays.stream( FeatureAuthority.values() )
                                                     .filter( next -> !targetAuthorities.contains( next ) )
                                                     .collect( Collectors.toSet() );

        String adjustedPath = path;
        if ( targetAuthorities.contains( actualAuthority ) )
        {
            if ( badAuthorities.stream()
                               .anyMatch( next -> path.endsWith( next.nameLowerCase() + PATH_DELIM ) ) )
            {
                throw new ThresholdReadingException( "The data orientation for loading thresholds from WRDS "
                                                     + "uses a feature authority of '"
                                                     + actualAuthority.nameLowerCase()
                                                     + "', but the declared path to the feature service includes a "
                                                     + "feature suffix that does not match that authority. Either "
                                                     + "remove the suffix from the path or specify a different data "
                                                     + "orientation for loading thresholds from WRDS. The inconsistent "
                                                     + "path was: "
                                                     + path );
            }
            else if ( !adjustedPath.endsWith( goodAuthority.nameLowerCase() + PATH_DELIM ) )
            {
                adjustedPath += goodAuthority.nameLowerCase() + PATH_DELIM;
            }
        }

        return adjustedPath;
    }

    /**
     * Hidden constructor.
     */
    private WrdsThresholdReader()
    {
    }
}

