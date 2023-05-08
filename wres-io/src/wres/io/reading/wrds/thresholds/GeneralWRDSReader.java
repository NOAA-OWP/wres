package wres.io.reading.wrds.thresholds;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.FeatureDimension;
import wres.config.generated.ThresholdsConfig;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdsGenerator;
import wres.io.reading.wrds.geography.WrdsLocation;
import wres.io.reading.wrds.geography.version.WrdsLocationRootVersionDocument;
import wres.io.ingesting.PreIngestException;
import wres.datamodel.units.UnitMapper;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.web.WebClient;
import wres.io.reading.wrds.thresholds.v2.ThresholdExtractor;
import wres.io.reading.wrds.thresholds.v2.ThresholdResponse;
import wres.io.reading.wrds.thresholds.v3.GeneralThresholdExtractor;
import wres.io.reading.wrds.thresholds.v3.GeneralThresholdResponse;
import wres.statistics.generated.Threshold;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reads thresholds from different versions of the WRDS threshold service.
 * @author Hank.Herr
 */
public final class GeneralWRDSReader
{
    /** The number of location requests." */
    static final int LOCATION_REQUEST_COUNT = 20;

    private static final Logger LOGGER = LoggerFactory.getLogger( GeneralWRDSReader.class );
    private static final Pair<SSLContext, X509TrustManager> SSL_CONTEXT;
    private static final String PATH_DELIM = "/";

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

    private static final WebClient WEB_CLIENT = new WebClient( SSL_CONTEXT, true );
    private static final ObjectMapper JSON_OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );

    /**
     * <p>The purpose for allowing an instance is to maintain the single-parameter
     * functional interface of getResponse to facilitate stream usage ~L110.
     *
     * <p>Instantiation not private so that unit tests can access methods.
     *
     * <p>The public interface to this class remains as static helper functions.
     *
     * <p>There are likely better ways to achieve these goals. Go for it.
     */

    GeneralWRDSReader()
    {
    }


    /**
     * Adjusts the path for access WRDS services for thresholds.
     * @param originalPath The original path.
     * @param featureDimension The feature dimension for the input side.
     * @return The adjust path.
     * @throws ThresholdReadingException if the user specified a path that
     * includes the feature type suffix, but its incorrect.
     */
    private static String adjustPath( String originalPath, FeatureDimension featureDimension )
    {
        String adjustedPath;

        //First, add a slash to make it easy to use endsWith later.
        if ( !originalPath.endsWith( PATH_DELIM ) )
        {
            adjustedPath = originalPath + PATH_DELIM;
        }
        else
        {
            adjustedPath = originalPath;
        }

        boolean foundUnmatchingSuffixError = false;

        //This uses a brute force if-then-else clause.  It could be turned into a method,
        //but that feels like overkill.

        //NWS lid 
        if ( ( featureDimension == FeatureDimension.NWS_LID )
             || ( featureDimension == FeatureDimension.CUSTOM ) )
        {
            if ( adjustedPath.endsWith( FeatureDimension.USGS_SITE_CODE.value() + PATH_DELIM )
                 || adjustedPath.endsWith( FeatureDimension.NWM_FEATURE_ID.value() + PATH_DELIM ) )
            {
                foundUnmatchingSuffixError = true;
            }
            else if ( !adjustedPath.endsWith( FeatureDimension.NWS_LID.value() + PATH_DELIM ) )
            {
                adjustedPath += FeatureDimension.NWS_LID + PATH_DELIM;
            }
        }

        //USGS site code
        if ( featureDimension == FeatureDimension.USGS_SITE_CODE )
        {
            if ( adjustedPath.endsWith( FeatureDimension.NWS_LID.value() + PATH_DELIM )
                 || adjustedPath.endsWith( FeatureDimension.NWM_FEATURE_ID.value() + PATH_DELIM ) )
            {
                foundUnmatchingSuffixError = true;
            }
            else if ( !adjustedPath.endsWith( FeatureDimension.USGS_SITE_CODE.value() + PATH_DELIM ) )
            {
                adjustedPath += FeatureDimension.USGS_SITE_CODE + PATH_DELIM;
            }
        }

        //NWM feature id
        if ( featureDimension == FeatureDimension.NWM_FEATURE_ID )
        {
            if ( adjustedPath.endsWith( FeatureDimension.NWS_LID.value() + PATH_DELIM )
                 || adjustedPath.endsWith( FeatureDimension.USGS_SITE_CODE.value() + PATH_DELIM ) )
            {
                foundUnmatchingSuffixError = true;
            }
            else if ( !adjustedPath.endsWith( FeatureDimension.NWM_FEATURE_ID.value() + PATH_DELIM ) )
            {
                adjustedPath += FeatureDimension.NWM_FEATURE_ID + PATH_DELIM;
            }
        }

        //An error was found.  Let the user know.    
        if ( foundUnmatchingSuffixError )
        {
            throw new ThresholdReadingException( "The input side for loading thresholds from WRDS"
                                                 + " uses feature dimension '"
                                                 + featureDimension.value()
                                                 + "', but "
                                                 + "the XML declared WRDS threshold source path, ..."
                                                 + originalPath
                                                 + ", already includes a feature suffix that does not match that "
                                                 + "feature dimension.  Either remove the suffix from the path or "
                                                 + "specify a different input side for loading thresholds from WRDS." );
        }

        return adjustedPath;
    }


    /**
     * The top-level method to call to obtain thresholds
     * @param threshold User declaration.
     * @param unitMapper Target unit mapper.
     * @param featureDimension The feature dimension of the provided list of features.
     * @param features List of features user specified.
     * @return Map of feature to threshold.
     * @throws IOException At various points, this can be thrown.
     */
    public static Map<WrdsLocation, Set<Threshold>> readThresholds( ThresholdsConfig threshold,
                                                                    UnitMapper unitMapper,
                                                                    FeatureDimension featureDimension,
                                                                    Set<String> features )
            throws IOException
    {
        //Get the declared source.
        ThresholdsConfig.Source source = ( ThresholdsConfig.Source ) threshold.getCommaSeparatedValuesOrSource();

        //Addresses will store a list of addresses to obtain, where each corresponds
        //to a group of locations.
        List<URI> addresses = new ArrayList<>();

        //Those address are built from the source address.
        URI fullSourceAddress = GeneralWRDSReader.getAbsoluteUri( source.getValue() );

        // Web service
        if ( ReaderUtilities.isWebSource( fullSourceAddress ) )
        {
            //Build the location groups to use.            
            Set<String> locationGroups = groupLocations( features );

            //Get the declared path and adjust it.  Note that adjusint could
            //result in a runtime error if the user declared a bad feature
            //dimensions suffix relative to the provided feature dimension.
            final String originalPath = fullSourceAddress.getPath();
            final String adjustedPath = adjustPath( originalPath, featureDimension );

            //Construct the URI builder.
            URIBuilder builder = new URIBuilder( fullSourceAddress );

            LOGGER.debug( "Went from source {} to path {} to path {}",
                          source.getValue(),
                          originalPath,
                          adjustedPath );

            //For each location group String...
            for ( String group : locationGroups )
            {
                //Append it to the adjustPath.
                String path = adjustedPath + group + PATH_DELIM;
                builder.setPath( path );

                //Build the URI and store it in addresses.
                try
                {
                    URI address = builder.build();
                    addresses.add( address );
                    LOGGER.debug( "Added uri {}", address );
                }
                catch ( URISyntaxException use )
                {
                    throw new ThresholdReadingException( "Unable to build URI from "
                                                         + builder,
                                                         use );
                }
            }
        }
        // File-like source
        else
        {
            addresses.add( fullSourceAddress );
        }

        //construct the reader and the threshold map.  
        GeneralWRDSReader reader = new GeneralWRDSReader();
        Map<WrdsLocation, Set<Threshold>> thresholdMapping;

        try
        {
            //Get the non-null responses for the addresses, extract the thresholds,
            //and collect them into a map.
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
                                       .allMatch( next -> ThresholdOuter.ALL_DATA.getThreshold()
                                                                                 .equals( next ) ) )
                .collect( Collectors.toUnmodifiableMap( Map.Entry::getKey,
                                                        Map.Entry::getValue ) );

        if ( thresholdMapping.isEmpty() )
        {
            throw new IOException( "No thresholds could be retrieved from " + fullSourceAddress );
        }

        LOGGER.debug( "The following thresholds were obtained from WRDS: {}.", thresholdMapping );

        return thresholdMapping;
    }

    /**
     * @param responseBytes Array of bytes to parse.
     * @param config The threshold configurations.
     * @param desiredUnitMapper The desired units.
     * @return A map of WRDS locations to thresholds, parsed using an appropriate version of the response.
     * @throws StreamIOException If an error is encountered while reading threshold data
     */
    static Map<WrdsLocation, Set<Threshold>> extract( byte[] responseBytes,
                                                      ThresholdsConfig config,
                                                      UnitMapper desiredUnitMapper )
    {
        //Obtain the source.
        ThresholdsConfig.Source source = ( ThresholdsConfig.Source ) config.getCommaSeparatedValuesOrSource();

        //Get the applyTo side.
        ThresholdOrientation side = ThresholdOrientation.LEFT;
        if ( Objects.nonNull( config.getApplyTo() ) )
        {
            side = ThresholdOrientation.valueOf( config.getApplyTo().name() );
        }

        //Get the operator.
        ThresholdOperator operator = ThresholdOperator.GREATER;
        if ( Objects.nonNull( config.getOperator() ) )
        {
            operator = ThresholdsGenerator.getThresholdOperator( config );
        }

        try
        {
            //Read the version information from the response, first.  This is used to 
            //identify version being processed.
            WrdsLocationRootVersionDocument versionDoc =
                    JSON_OBJECT_MAPPER.readValue( responseBytes, WrdsLocationRootVersionDocument.class );

            //Extract using V3 API reader.
            if ( versionDoc.isDeploymentInfoPresent() )
            {
                //Get the response and construct the extractor.
                GeneralThresholdResponse response =
                        JSON_OBJECT_MAPPER.readValue( responseBytes, GeneralThresholdResponse.class );
                GeneralThresholdExtractor extractor = new GeneralThresholdExtractor( response )
                        .from( source.getProvider() )
                        .operatesBy( operator )
                        .onSide( side );

                //If rating provider is not null, add it, too.
                if ( source.getRatingProvider() != null )
                {
                    extractor.ratingFrom( source.getRatingProvider() );
                }

                //Flow is the default if the parameterToMeasure is not specified.  Note that this 
                //works for unified schema thresholds, such as recurrence flows, because the metadata
                //does not specify the parameter, so that parameterToMeasure is ignored.
                if ( ( source.getParameterToMeasure() != null )
                     && ( source.getParameterToMeasure().equalsIgnoreCase( "stage" ) ) )
                {
                    extractor.readStage();
                }
                else
                {
                    extractor.readFlow();
                }

                //Establish target unit.
                extractor.convertTo( desiredUnitMapper );

                return extractor.extract();
            }
            //Extract using V2 or older reader.
            else //The deployment information is not available, implying its the older WRDS API.
            {
                ThresholdResponse response = JSON_OBJECT_MAPPER.readValue( responseBytes, ThresholdResponse.class );
                ThresholdExtractor extractor = new ThresholdExtractor( response )
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
                     && ( source.getParameterToMeasure().equalsIgnoreCase( "stage" ) ) )
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
            throw new StreamIOException( "Error encountered while requesting WRDS threshold data", ioe );
        }
    }

    /**
     * @param features Features to group.
     * @return The grouped features as a String.  
     */
    static Set<String> groupLocations( Set<String> features )
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
     * This is protected to support testing.
     * @param address the address
     * @return The response in byte[], where the URI can point to a file or a website.
     */
    byte[] getResponse( final URI address ) throws StreamIOException
    {
        LOGGER.debug( "Opening URI {}", address );
        try
        {
            URI fullAddress = GeneralWRDSReader.getAbsoluteUri( address );

            if ( ReaderUtilities.isWebSource( fullAddress ) )
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
            Path dataDirectory = Paths.get( System.getProperty( "user.dir" ) );
            return dataDirectory.toUri()
                                .resolve( maybeIncomplete.getPath() );

        }

        return maybeIncomplete;
    }
}

