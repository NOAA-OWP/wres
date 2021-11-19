package wres.io.thresholds.wrds;

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
import wres.datamodel.DataFactory;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.geography.wrds.version.WrdsLocationRootVersionDocument;
import wres.io.reading.wrds.ReadValueManager;
import wres.io.thresholds.ThresholdReadingException;
import wres.io.thresholds.exceptions.StreamIOException;
import wres.io.thresholds.wrds.v2.ThresholdExtractor;
import wres.io.thresholds.wrds.v2.ThresholdResponse;
import wres.io.thresholds.wrds.v3.GeneralThresholdExtractor;
import wres.io.thresholds.wrds.v3.GeneralThresholdResponse;
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

/**
 * This test should be removed in the near future, once I've implemented fully the toggle that detects threshold
 * version.
 * @author Hank.Herr
 *
 */
public final class GeneralWRDSReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger( GeneralWRDSReader.class );
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
     * Adjusts the path for access WRDS services for thresholds.
     * @param originalPath The original path.
     * @param featureDimension The feature dimension for the input side.
     * @return The adjust path.
     * @throws ThresholdReadingException if the user specified a path that
     * includes the feature type suffix, but its incorrect.
     */
    private static String adjustPath(String originalPath, FeatureDimension featureDimension)
    {
        String adjustedPath;

        //First, add a slash to make it easy to use endsWith later.
        if ( !originalPath.endsWith( "/" ) )
        {
            adjustedPath = originalPath + "/";
        }
        else
        {
            adjustedPath = originalPath;
        }
        
        boolean foundUnmatchingSuffixError = false;

        //This uses a brute force if-then-else clause.  It could be turned into a method,
        //but that feels like overkill.
        
        //NWS lid 
        if ((featureDimension == FeatureDimension.NWS_LID) 
                || (featureDimension == FeatureDimension.CUSTOM))
        {
            if (adjustedPath.endsWith( FeatureDimension.USGS_SITE_CODE.value() + "/" ) 
                    || adjustedPath.endsWith( FeatureDimension.NWM_FEATURE_ID.value() + "/" ))
            {
                foundUnmatchingSuffixError = true;
            }
            else if (!adjustedPath.endsWith (FeatureDimension.NWS_LID.value() + "/" ))
            {
                adjustedPath += FeatureDimension.NWS_LID + "/";
            }
        }
        
        //USGS site code
        if (featureDimension == FeatureDimension.USGS_SITE_CODE)
        {
            if (adjustedPath.endsWith( FeatureDimension.NWS_LID.value() +"/") 
                    || adjustedPath.endsWith( FeatureDimension.NWM_FEATURE_ID.value() + "/" ))
            {
                foundUnmatchingSuffixError = true;
            }
            else if (!adjustedPath.endsWith (FeatureDimension.USGS_SITE_CODE.value() + "/" ))
            {
                adjustedPath += FeatureDimension.USGS_SITE_CODE + "/";
            }
        }
        
        //NWM feature id
        if (featureDimension == FeatureDimension.NWM_FEATURE_ID)
        {
            if (adjustedPath.endsWith( FeatureDimension.NWS_LID.value() +"/") 
                    || adjustedPath.endsWith( FeatureDimension.NWM_FEATURE_ID.value() + "/" ))
            {
                foundUnmatchingSuffixError = true;
            }
            else if (!adjustedPath.endsWith (FeatureDimension.NWM_FEATURE_ID.value() + "/" ))
            {
                adjustedPath += FeatureDimension.NWM_FEATURE_ID + "/";
            }
        }
       
        //An error was found.  Let the user know.    
        if (foundUnmatchingSuffixError)
        {
            throw new ThresholdReadingException( "The input side for loading thresholds from WRDS"
                    + " uses feature dimension '" + featureDimension.value() + "', but "
                    + "the XML declared WRDS threshold source path, ..." + originalPath 
                    + ", already includes a feature suffix that does not match that "
                    + "feature dimension.  Either remove the suffix from the path or "
                    + "specify a different input side for loading thresholds from WRDS.");
        }
        
        return adjustedPath;
    }
    
    
    /**
     * The top-level method to call to obtain thresholds
     * @param systemSettings Settings.
     * @param threshold User declaration.
     * @param unitMapper Target unit mapper.
     * @param featureDimension The feature dimension of the provided list of features.
     * @param features List of features user specified.
     * @return Map of feature to threshold.
     * @throws IOException At various points, this can be thrown.
     */
    public static Map<WrdsLocation, Set<ThresholdOuter>> readThresholds(
                                                                         final SystemSettings systemSettings,
                                                                         final ThresholdsConfig threshold,
                                                                         final UnitMapper unitMapper,
                                                                         final FeatureDimension featureDimension,
                                                                         final Set<String> features )
            throws IOException
    {
        //Get the declared source.
        ThresholdsConfig.Source source = (ThresholdsConfig.Source) threshold.getCommaSeparatedValuesOrSource();
        
        //Addresses will store a list of addresses to obtain, where each corresponds
        //to a group of locations.
        List<URI> addresses = new ArrayList<>();
        
        //Those address are built from the source address.
        URI fullSourceAddress = GeneralWRDSReader.getAbsoluteUri( source.getValue(),
                                                                  systemSettings );

        //If the source is a file, implying a WRDS JSON formatted file, then
        //just use the file as is.
        if ( fullSourceAddress.getScheme()
                              .toLowerCase()
                              .equals( "file" ) )
        {
            addresses.add( fullSourceAddress );
        }
        
        //Otherwise, the source is a path to WRDS...
        else
        {
            //Build the location groups to use.            
            Set<String> locationGroups = groupLocations( features );
            
            //Get the declared path and adjust it.  Note that adjusint could
            //result in a runtime error if the user declared a bad feature
            //dimensions suffix relative to the provided feature dimension.
            final String originalPath = fullSourceAddress.getPath();
            final String adjustedPath = adjustPath(originalPath, featureDimension);

            //Construct the URI builder.
            URIBuilder builder = new URIBuilder( fullSourceAddress );
            
            LOGGER.debug( "Went from source {} to path {} to path {}",
                          source.getValue(),
                          originalPath,
                          adjustedPath );

            //For each location group String...
            for ( String group : locationGroups )
            {
                //Appebnd it to the adjustPath.
                String path = adjustedPath + group + "/";
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

        //construct the reader and the threshold map.  
        GeneralWRDSReader reader = new GeneralWRDSReader( systemSettings );
        Map<WrdsLocation, Set<ThresholdOuter>> thresholdMapping;

        try
        {
            //Get the non-null resonses for the addresses, extract the thresholds,
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
                                                                   .allMatch( ThresholdOuter::isAllDataThreshold ) )
                                           .collect( Collectors.toUnmodifiableMap( Map.Entry::getKey,
                                                                                   Map.Entry::getValue ) );

        if ( thresholdMapping.isEmpty() )
        {
            throw new IOException( "No thresholds could be retrieved from " + fullSourceAddress.toString() );
        }

        LOGGER.debug("The following thresholds were obtained from WRDS: " + thresholdMapping);        

        return thresholdMapping;
    }

    /**
     * @param responseBytes Array of bytes to parse.
     * @param config The threshold configurations.
     * @param desiredUnitMapper The desired units.
     * @return A map of WRDS locations to thresholds, parsed using an appropriate version of the response.
     * @throws StreamIOException If an error is encountered while reading threshold data
     */
    protected static Map<WrdsLocation, Set<ThresholdOuter>> extract( byte[] responseBytes,
                                                                     ThresholdsConfig config,
                                                                     UnitMapper desiredUnitMapper )
            throws StreamIOException
    {
        //Obtain the source.
        ThresholdsConfig.Source source = (ThresholdsConfig.Source) config.getCommaSeparatedValuesOrSource();
        
        //Get the applyTo side.
        ThresholdConstants.ThresholdDataType side = ThresholdConstants.ThresholdDataType.LEFT;
        if ( Objects.nonNull( config.getApplyTo() ) )
        {
            side = ThresholdConstants.ThresholdDataType.valueOf( config.getApplyTo().name() );
        }

        //Get the operator.
        ThresholdConstants.Operator operator = ThresholdConstants.Operator.GREATER;
        if ( Objects.nonNull( config.getOperator() ) )
        {
            operator = DataFactory.getThresholdOperator( config );
        }

        try
        {
            //Read the version information from the response, first.  This is used to 
            //identify version being processed.
            WrdsLocationRootVersionDocument versionDoc = JSON_OBJECT_MAPPER.readValue( responseBytes, WrdsLocationRootVersionDocument.class );
            
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
                     && ( source.getParameterToMeasure().toLowerCase().equals( "stage" ) ) )
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
     * This is protected to support testing.
     * @param address the address
     * @return The response in byte[], where the URI can point to a file or a website.
     */
    protected byte[] getResponse( final URI address ) throws StreamIOException
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
                    return rawForecast;
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
                return null;
            }

            return response.getResponse().readAllBytes();
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

