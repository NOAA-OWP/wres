package wres.io.reading.wrds.thresholds;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.BinaryOperator;
import java.util.function.Function;
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

import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.config.yaml.components.ThresholdSource;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.units.UnitMapper;
import wres.io.reading.ThresholdReader;
import wres.io.reading.ThresholdReadingException;
import wres.io.reading.wrds.geography.Location;
import wres.io.reading.wrds.geography.LocationRootVersionDocument;
import wres.io.ingesting.PreIngestException;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.web.WebClient;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.Threshold;

/**
 * Reads thresholds from different versions of the WRDS threshold service.
 *
 * @author James Brown
 * @author Hank Herr
 * @author Chris Tubbs
 */
public class WrdsThresholdReader implements ThresholdReader
{
    /** The number of location requests." */
    private static final int DEFAULT_LOCATION_REQUEST_COUNT = 20;

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

    /** Location request count, i.e., chunk size. */
    private final int locationRequestCount;

    /**
     * Creates an instance.
     * @return an instance
     */

    public static WrdsThresholdReader of()
    {
        return new WrdsThresholdReader( DEFAULT_LOCATION_REQUEST_COUNT );
    }

    /**
     * Creates an instance with a specified chunk size, corresponding to the number of locations whose thresholds are
     * requested at once.
     * @param locationRequestCount the location request count, greater than zero
     * @return an instance
     * @throws IllegalArgumentException if the locationRequestCount is invalid
     */

    public static WrdsThresholdReader of( int locationRequestCount )
    {
        return new WrdsThresholdReader( locationRequestCount );
    }

    /**
     * Reads thresholds.
     * @param thresholdSource the threshold service declaration
     * @param unitMapper a unit mapper to translate and set threshold units
     * @param featureNames the named features for which thresholds are required
     * @param featureAuthority the feature authority associated with the feature names
     * @return the thresholds mapped against features
     */
    public Set<wres.config.yaml.components.Threshold> read( ThresholdSource thresholdSource,
                                                            UnitMapper unitMapper,
                                                            Set<String> featureNames,
                                                            FeatureAuthority featureAuthority )
    {
        Objects.requireNonNull( thresholdSource );
        Objects.requireNonNull( unitMapper );
        Objects.requireNonNull( featureNames );

        URI serviceUri = thresholdSource.uri();
        if ( Objects.isNull( serviceUri ) )
        {
            throw new ThresholdReadingException( "Cannot read from a threshold source with a missing URI. Please "
                                                 + "add a URI for each threshold source that should be read." );
        }

        // Acquire the feature names for which thresholds are required
        DatasetOrientation orientation = thresholdSource.featureNameFrom();

        if ( Objects.isNull( orientation ) )
        {
            throw new ThresholdReadingException( "The 'feature_name_from' is missing from the 'threshold_service' "
                                                 + "declaration, which is not allowed because the feature service "
                                                 + "request must use feature names with a prescribed feature "
                                                 + "authority." );
        }

        // Feature authority must be present if the URI is web-like and not a local file
        if ( ReaderUtilities.isWebSource( serviceUri )
             && Objects.isNull( featureAuthority ) )
        {
            throw new ThresholdReadingException( "The 'feature_authority' associated with the '"
                                                 + orientation
                                                 + "' data was not supplied, but this is needed to "
                                                 + "correlate feature names with thresholds. Please clarify by adding "
                                                 + "a 'feature_authority' to the '"
                                                 + orientation
                                                 + "' dataset. " );
        }

        // No features?
        if ( featureNames.isEmpty() )
        {
            throw new ThresholdReadingException( "While attempting to read thresholds from the WRDS feature service, "
                                                 + "discovered no features in the declaration for which thresholds "
                                                 + "could be acquired. Please add some features to the declaration "
                                                 + "using 'features', 'feature_groups' or 'feature_service' and try "
                                                 + "again." );
        }

        // Adjust the feature names, as required, and map them to the original/supplied names. An adjustment is needed
        // for feature names associated with the nws lid feature authority because WRDS only accepts "handbook 5"
        // names, i.e., the first five characters of the supplied name.
        Map<String, String> mappedAndAdjustedFeatureNames =
                WrdsThresholdReader.getMappedAndAdjustedFeatureNames( featureNames, featureAuthority, serviceUri );
        Set<String> adjustedFeatureNames = mappedAndAdjustedFeatureNames.keySet();

        // The list of URIs to acquire, build from the input URI as needed
        List<URI> addresses = new ArrayList<>();

        URI uri = thresholdSource.uri();

        // Web service
        if ( ReaderUtilities.isWebSource( uri ) )
        {
            // Build the location groups to use.
            Set<String> locationGroups = this.chunkFeatures( adjustedFeatureNames );
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
            addresses.add( uri );
        }

        // Get the accumulated warnings across threshold extractions
        Set<String> warnings = new HashSet<>();
        // Read the thresholds and accumulate any warnings
        Map<Location, Set<Threshold>> thresholdMapping = this.read( thresholdSource,
                                                                    addresses,
                                                                    unitMapper,
                                                                    warnings );

        if ( thresholdMapping.isEmpty() )
        {
            throw new NoThresholdsFoundException( "No thresholds could be retrieved from "
                                                  + uri
                                                  + ". The following warnings were encountered while extracting "
                                                  + "thresholds: "
                                                  + warnings );
        }

        LOGGER.debug( "The following thresholds were obtained from WRDS: {}.", thresholdMapping );

        // Validate the thresholds acquired from WRDS in relation to the features for which thresholds were required
        WrdsThresholdReader.validate( serviceUri, mappedAndAdjustedFeatureNames, thresholdMapping, featureAuthority );

        // Map the thresholds to featureful thresholds
        return WrdsThresholdReader.getFeaturefulThresholds( thresholdMapping,
                                                            orientation,
                                                            mappedAndAdjustedFeatureNames,
                                                            featureAuthority );
    }

    /**
     * Reads thresholds from a collection of addresses.
     * @param addresses the addresses
     * @param unitMapper the unit mapper
     * @param warnings the accumulated warnings
     * @return the thresholds
     */
    private Map<Location, Set<Threshold>> read( ThresholdSource thresholdSource,
                                                List<URI> addresses,
                                                UnitMapper unitMapper,
                                                Set<String> warnings )
    {
        Map<Location, Set<Threshold>> thresholdMapping;

        // Function to handle duplicate locations across responses: see Redmine issue #117009
        BinaryOperator<Set<Threshold>> merger = ( a, b ) ->
        {
            Set<Threshold> merged = new HashSet<>( a );
            merged.addAll( b );
            return Collections.unmodifiableSet( merged );
        };

        // Get the non-null responses for the addresses, extract the thresholds,
        // and collect them into a map.
        thresholdMapping = addresses.parallelStream()
                                    .map( this::getResponse )
                                    .filter( Objects::nonNull )
                                    .map( thresholdResponse -> this.extract( thresholdResponse,
                                                                             thresholdSource,
                                                                             unitMapper,
                                                                             warnings ) )
                                    .flatMap( map -> map.entrySet()
                                                        .stream() )
                                    .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue, merger ) );

        // Filter out locations that only have all data
        thresholdMapping = thresholdMapping
                .entrySet()
                .parallelStream()
                .filter( entry -> !entry.getValue()
                                        .stream()
                                        .allMatch( next -> ThresholdOuter.ALL_DATA.getThreshold()
                                                                                  .equals( next ) ) )
                .collect( Collectors.toUnmodifiableMap( Map.Entry::getKey,
                                                        Map.Entry::getValue ) );

        return thresholdMapping;
    }

    /**
     * @param responseBytes array of bytes to parse
     * @param thresholdSource the threshold service
     * @param desiredUnitMapper the desired units
     * @param warnings the accumulated warnings encountered
     * @return the thresholds
     * @throws ThresholdReadingException if the thresholds could not be read
     * @throws UnsupportedOperationException if the threshold API version is unsupported
     */
    private Map<Location, Set<Threshold>> extract( byte[] responseBytes,
                                                   ThresholdSource thresholdSource,
                                                   UnitMapper desiredUnitMapper,
                                                   Set<String> warnings )
    {
        ThresholdOrientation side = thresholdSource.applyTo();
        ThresholdOperator operator = thresholdSource.operator();

        try
        {
            // Read the version information from the response, first.  This is used to
            // identify version being processed
            LocationRootVersionDocument versionDoc =
                    JSON_OBJECT_MAPPER.readValue( responseBytes, LocationRootVersionDocument.class );

            if ( versionDoc.isDeploymentInfoPresent() )
            {
                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "Using WRDS API version {}.", versionDoc.getDeploymentInfo()
                                                                          .version() );
                }
            }
            else
            {
                throw new UnsupportedOperationException( "Unsupported API version: could not find the expected "
                                                         + "deployment information in the threshold response body." );
            }

            // Get the response and construct the extractor.
            ThresholdResponse response =
                    JSON_OBJECT_MAPPER.readValue( responseBytes, ThresholdResponse.class );
            ThresholdExtractor extractor = ThresholdExtractor.builder()
                                                             .response( response )
                                                             .operator( operator )
                                                             .orientation( side )
                                                             .provider( thresholdSource.provider() )
                                                             .ratingProvider( thresholdSource.ratingProvider() )
                                                             .unitMapper( desiredUnitMapper )
                                                             .build();

            // Flow is the default if the parameter is not specified. Note that this
            // works for unified schema thresholds, such as recurrence flows, because the metadata
            // does not specify the parameter, so that parameter is ignored.
            if ( "stage".equalsIgnoreCase( thresholdSource.parameter() ) )
            {
                extractor = extractor.toBuilder()
                                     .type( ThresholdType.STAGE )
                                     .build();
            }
            else
            {
                extractor = extractor.toBuilder()
                                     .type( ThresholdType.FLOW )
                                     .build();
            }

            Map<Location, Set<Threshold>> thresholds = extractor.extract();

            // Increment the warnings
            warnings.addAll( extractor.getWarnings() );

            return thresholds;
        }
        catch ( IOException ioe )
        {
            throw new ThresholdReadingException( "Error encountered while requesting WRDS threshold data", ioe );
        }
    }

    /**
     * @param features the features to group
     * @return the delimited features chunked by {@link #locationRequestCount}
     */
    private Set<String> chunkFeatures( Set<String> features )
    {
        Set<String> locationGroups = new HashSet<>();
        StringJoiner locationJoiner = new StringJoiner( "," );
        int counter = 0;

        // Use a predictable iteration order
        Set<String> orderedFeatures = new TreeSet<>( features );
        int chunkSize = this.getLocationRequestCount();
        for ( String feature : orderedFeatures )
        {
            if ( counter % chunkSize == 0 && locationJoiner.length() > 0 )
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
     * @return the location chunk size for threshold requests
     */
    private int getLocationRequestCount()
    {
        return this.locationRequestCount;
    }

    /**
     * @param address the address
     * @return The response in byte[], where the URI can point to a file or a website
     * @throws ThresholdReadingException if the thresholds could not be read
     */
    private byte[] getResponse( final URI address )
    {
        LOGGER.debug( "Opening URI {}", address );
        try
        {
            if ( ReaderUtilities.isWebSource( address ) )
            {
                return WrdsThresholdReader.getResponseFromWeb( address );
            }
            else
            {
                try ( InputStream data = Files.newInputStream( Paths.get( address ) ) )
                {
                    return IOUtils.toByteArray( data );
                }
            }
        }
        catch ( IOException ioe )
        {
            throw new ThresholdReadingException( "Error encountered while requesting WRDS threshold data", ioe );
        }
    }

    /**
     * @return The response from the remote URI as bytes[].
     */
    private static byte[] getResponseFromWeb( URI inputAddress ) throws IOException
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
     * Generates featureful thresholds from the inputs.
     * @param thresholds the thresholds
     * @param orientation the orientation of the dataset to which the feature names apply
     * @param featureNames the feature names
     * @param featureAuthority the feature authority to help with feature naming
     * @return the featureful thresholds
     */
    private static Set<wres.config.yaml.components.Threshold> getFeaturefulThresholds( Map<Location, Set<Threshold>> thresholds,
                                                                                       DatasetOrientation orientation,
                                                                                       Map<String, String> featureNames,
                                                                                       FeatureAuthority featureAuthority )
    {
        // Ordered, mapped thresholds
        Set<wres.config.yaml.components.Threshold> mappedThresholds = new HashSet<>();

        Set<String> featuresNotRequired = new HashSet<>();
        for ( Map.Entry<Location, Set<Threshold>> nextEntry : thresholds.entrySet() )
        {
            Location location = nextEntry.getKey();
            Set<Threshold> nextThresholds = nextEntry.getValue();

            String featureName = Location.getNameForAuthority( featureAuthority, location );

            if ( Objects.nonNull( featureName )
                 && featureNames.containsKey( featureName ) )
            {
                String originalFeatureName = featureNames.get( featureName );

                Geometry feature = Geometry.newBuilder()
                                           .setName( originalFeatureName )
                                           .build();
                Set<wres.config.yaml.components.Threshold> nextMappedThresholds =
                        nextThresholds.stream()
                                      .map( next -> ThresholdBuilder.builder()
                                                                    .threshold( next )
                                                                    .type( wres.config.yaml.components.ThresholdType.VALUE )
                                                                    .feature( feature )
                                                                    .featureNameFrom( orientation )
                                                                    .build() )
                                      .collect( Collectors.toSet() );
                mappedThresholds.addAll( nextMappedThresholds );
            }
            else if ( LOGGER.isDebugEnabled() )
            {
                if( Objects.isNull( featureName ) )
                {
                    featuresNotRequired.add( location.toString() );
                }
                else
                {
                    featuresNotRequired.add( featureName );
                }
            }
        }

        if ( LOGGER.isDebugEnabled() && !featuresNotRequired.isEmpty() )
        {
            LOGGER.debug( "Thresholds were discovered for the following features whose thresholds were not "
                          + "required: {}",
                          featuresNotRequired );
        }

        return Collections.unmodifiableSet( mappedThresholds );
    }

    /**
     * Returns the requested feature names mapped against the original names. The requested names will differ in some
     * cases. For example, when requesting names with the {@link FeatureAuthority#NWS_LID}, the names must be
     * "Handbook 5" names, which contain up to five characters.
     *
     * @param originalNames the original feature names
     * @param featureAuthority the feature authority
     * @param serviceUri the service URI
     * @return the feature names to use when forming a request to a web service
     */

    private static Map<String, String> getMappedAndAdjustedFeatureNames( Set<String> originalNames,
                                                                         FeatureAuthority featureAuthority,
                                                                         URI serviceUri )
    {
        Map<String, String> names;
        // Only handbook 5 names are allowed in this context, so use up to the first 5 characters of an NWS LID only
        if ( featureAuthority == FeatureAuthority.NWS_LID && ReaderUtilities.isWebSource( serviceUri ) )
        {
            names = originalNames.stream()
                                 .collect( Collectors.toUnmodifiableMap( n -> n.substring( 0,
                                                                                           Math.min( n.length(), 5 ) ),
                                                                         Function.identity() ) );
        }
        else
        {
            names = originalNames.stream()
                                 .collect( Collectors.toUnmodifiableMap( Function.identity(), Function.identity() ) );
        }

        return names;
    }

    /**
     * Validates the thresholds against features
     * @param uri the service URI
     * @param featureNames the feature names
     * @param thresholds the thresholds
     * @param featureAuthority the feature authority
     */

    private static void validate( URI uri,
                                  Map<String, String> featureNames,
                                  Map<Location, Set<Threshold>> thresholds,
                                  FeatureAuthority featureAuthority )
    {
        // No external thresholds declared
        if ( thresholds.isEmpty() )
        {
            LOGGER.debug( "No external thresholds to validate." );

            return;
        }

        LOGGER.debug( "Attempting to reconcile the {} features to evaluate with the {} features for which external "
                      + "thresholds are available.",
                      featureNames.size(),
                      thresholds.size() );

        // Identify the features that have thresholds
        Set<Location> thresholdFeatures = thresholds.keySet();
        Set<String> thresholdFeatureNames =
                thresholdFeatures.stream()
                                 .map( n -> Location.getNameForAuthority( featureAuthority, n ) )
                                 .filter( Objects::nonNull )
                                 .collect( Collectors.toSet() );

        Set<String> featureNamesWithThresholds = new TreeSet<>( featureNames.keySet() );
        featureNamesWithThresholds.retainAll( thresholdFeatureNames );
        Set<String> featureNamesWithoutThresholds = new TreeSet<>( featureNames.keySet() );
        featureNamesWithoutThresholds.removeAll( thresholdFeatureNames );

        Set<String> thresholdNamesWithoutFeatures = new TreeSet<>( thresholdFeatureNames );
        thresholdNamesWithoutFeatures.removeAll( featureNames.keySet() );

        if ( ( !featureNamesWithoutThresholds.isEmpty() || !thresholdNamesWithoutFeatures.isEmpty() )
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
                         "While attempting to reconcile the features to ",
                         "evaluate with the features for which thresholds ",
                         "are available, found ",
                         featureNames.size(),
                         " features to evaluate and ",
                         featureNamesWithThresholds.size(),
                         " features for which thresholds were found, but ",
                         featureNamesWithoutThresholds.size(),
                         " features for which thresholds could not be ",
                         "reconciled with features to evaluate. Features without ",
                         "thresholds will be skipped. If the number of features ",
                         "without thresholds is larger than expected, ensure that ",
                         "the source of feature names (featureNameFrom) is properly ",
                         "declared for the external thresholds. The ",
                         "declared features without thresholds are: ",
                         featureNamesWithoutThresholds,
                         ". The feature names associated with thresholds for which no features were declared are: ",
                         thresholdNamesWithoutFeatures );
        }

        if ( featureNamesWithoutThresholds.size() == featureNames.size() )
        {
            throw new ThresholdReadingException( "When reading thresholds from "
                                                 + uri
                                                 + ", failed to discover any features for which thresholds were "
                                                 + "available. Add some thresholds for one or more of the declared "
                                                 + "features, declare some features for which thresholds are available "
                                                 + "or remove the declaration of thresholds altogether. The names of "
                                                 + "features encountered without thresholds are: "
                                                 + thresholdNamesWithoutFeatures
                                                 + ". Thresholds were not discovered for any of the following declared "
                                                 + "features: "
                                                 + featureNamesWithoutThresholds
                                                 + "." );
        }

        LOGGER.info( "While reading thresholds from {}, discovered {} features to evaluate for which external "
                     + "thresholds were available and {} features with external thresholds that could not be evaluated "
                     + "(e.g., because there was no data for these features).",
                     uri,
                     featureNamesWithThresholds.size(),
                     thresholdNamesWithoutFeatures.size() );
    }

    /**
     * Hidden constructor.
     */
    private WrdsThresholdReader( int locationRequestCount )
    {
        if ( locationRequestCount < 1 )
        {
            throw new IllegalArgumentException( "Cannot create a WRDS threshold reader with a location chunk size of "
                                                + locationRequestCount
                                                + ". Increase the location chunk size to 1 or more locations." );
        }

        LOGGER.debug( "Creating a WRDS threshold reader with a chunk size of {} locations.", locationRequestCount );

        this.locationRequestCount = locationRequestCount;
    }
}

