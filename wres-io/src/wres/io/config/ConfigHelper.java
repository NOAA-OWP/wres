package wres.io.config;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.*;
import wres.config.generated.ProjectConfig.Outputs;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.reading.PreIngestException;
import wres.io.utilities.NoDataException;
import wres.util.TimeHelper;

/**
 * The purpose of io's ConfigHelper is to help the io module translate raw
 * user-specified configuration elements into a reduced form, a more
 * actionable or meaningful form such as a SQL script, or to extract specific
 * elements from a particular config element, or other purposes that are common
 * to the io module.
 *
 * The general form of a helper method appropriate for ConfigHelper has a
 * ProjectConfig as the first argument and some other element(s) or hint(s) as
 * additional args. These are not hard-and-fast-rules. But the original purpose
 * was to help the io module avoid tedious repetition of common interpretations
 * of the raw user-specified configuration.
 *
 * Candidates for removal to a wres-config helper are those that purely operate
 * on, use, and return objects of classes that are specified in the wres-config
 * or JDK.
 *
 * Candidates that should stay are those returning SQL statements or are
 * currently useful only to the wres-io module.
 */

public class ConfigHelper
{

    public static final Logger LOGGER = LoggerFactory.getLogger( ConfigHelper.class );

    /**
     * Default exception message when a destination cannot be established.
     */

    public static final String OUTPUT_CLAUSE_BOILERPLATE = "Please include valid numeric output clause(s) in"
                                                           + " the project configuration. Example: <destination>"
                                                           + "<path>c:/Users/myname/wres_output/</path>"
                                                           + "</destination>";

    /**
     * String for null configuration error.
     */

    private static final String NULL_CONFIGURATION_ERROR = "The project configuration cannot be null.";

    private ConfigHelper()
    {
        // prevent construction
    }


    /**
     * Creates a hash for the indicated project configuration based on its
     * data ingested.
     *
     * TODO: introduce wres.Dataset table, hash sorted hashes of left, right,
     * baseline separately, treat each as a dataset. Link dataset to project.
     *
     * @param leftHashesIngested A collection of the hashes for the left sided
     *                           source data
     * @param rightHashesIngested A collection of the hashes for the right sided
     *                            source data
     * @param baselineHashesIngested A collection of hashes representing the baseline
     *                               source data
     * @return A unique hash code for the project's circumstances
     */
    public static String hashProject( final String[] leftHashesIngested,
                                      final String[] rightHashesIngested,
                                      final String[] baselineHashesIngested )
    {
        MessageDigest md5Digest;

        try
        {
            md5Digest = MessageDigest.getInstance( "MD5" );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new PreIngestException( "Couldn't use MD5 algorithm.",
                                          nsae );
        }

        // Sort for deterministic hash result for same list of ingested
        Arrays.sort( leftHashesIngested );

        for ( String leftHash : leftHashesIngested )
        {
            DigestUtils.updateDigest( md5Digest, leftHash );
        }

        // Sort for deterministic hash result for same list of ingested
        Arrays.sort( rightHashesIngested );

        for ( String rightHash : rightHashesIngested )
        {
            DigestUtils.updateDigest( md5Digest, rightHash );
        }

        // Sort for deterministic hash result for same list of ingested
        Arrays.sort( baselineHashesIngested );

        for ( String baselineHash : baselineHashesIngested )
        {
            DigestUtils.updateDigest( md5Digest, baselineHash );
        }

        byte[] digestAsHex = md5Digest.digest();
        return Hex.encodeHexString( digestAsHex );
    }

    /**
     * Returns whether the declared {@link DatasourceType} matches one of the forecast types, currently 
     * {@link DatasourceType#SINGLE_VALUED_FORECASTS} and {@link DatasourceType#ENSEMBLE_FORECASTS}.
     * @param dataSourceConfig the configuration
     * @return true when the type of data is a forecast type
     */
    public static boolean isForecast( DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( dataSourceConfig );

        return dataSourceConfig.getType() == DatasourceType.SINGLE_VALUED_FORECASTS
               || dataSourceConfig.getType() == DatasourceType.ENSEMBLE_FORECASTS;
    }


    public static ProjectConfig read( final String path ) throws IOException
    {
        Path actualPath = Paths.get( path );
        ProjectConfigPlus configPlus = ProjectConfigPlus.from( actualPath );
        return configPlus.getProjectConfig();
    }

    /**
     * Get the time zone offset from a datasource config or null if not found.
     * @param sourceConfig the configuration element to retrieve for
     * @return the time zone offset or null if not specified in dataSourceConfig
     * @throws ProjectConfigException when the date time could not be parsed
     */

    public static ZoneOffset getZoneOffset( DataSourceConfig.Source sourceConfig )
    {
        ZoneOffset result = null;

        if ( sourceConfig != null
             && sourceConfig.getZoneOffset() != null )
        {
            String configuredOffset = sourceConfig.getZoneOffset();

            // Look for CONUS-ish names like "EDT" and convert to offset.
            for ( ConusZoneId id : ConusZoneId.values() )
            {
                if ( configuredOffset.equalsIgnoreCase( id.name() ) )
                {
                    result = id.getZoneOffset();
                }
            }

            if ( result == null )
            {
                // Otherwise, try to parse directly into an offset
                try
                {
                    result = ZoneOffset.of( configuredOffset );
                }
                catch ( DateTimeException dte )
                {
                    String message = "Could not figure out the zoneOffset. "
                                     + "Try formatting it like this: -05:00.";
                    throw new ProjectConfigException( sourceConfig,
                                                      message,
                                                      dte );
                }
            }
        }

        return result;
    }


    /**
     * Return <code>true</code> if the project uses probability thresholds, otherwise <code>false</code>.
     * 
     * @param projectConfig the project declaration
     * @return Whether or not the project uses probability thresholds
     * @throws NullPointerException if the input is null or the metrics declaration is null
     */
    public static boolean hasProbabilityThresholds( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( projectConfig.getMetrics() );

        // Iterate metrics configuration
        for ( MetricsConfig next : projectConfig.getMetrics() )
        {
            // Check thresholds           
            if ( next.getThresholds()
                     .stream()
                     .anyMatch( a -> Objects.isNull( a.getType() )
                                     || a.getType() == ThresholdType.PROBABILITY ) )
            {
                return true;
            }
        }
        return false;
    }

    private enum ConusZoneId
    {
        UTC( "+0000" ),
        GMT( "+0000" ),
        EDT( "-0400" ),
        EST( "-0500" ),
        CDT( "-0500" ),
        CST( "-0600" ),
        MDT( "-0600" ),
        MST( "-0700" ),
        PDT( "-0700" ),
        PST( "-0800" ),
        AKDT( "-0800" ),
        AKST( "-0900" ),
        HADT( "-0900" ),
        HAST( "-1000" );

        private final transient ZoneOffset zoneOffset;

        ConusZoneId( String zoneOffset )
        {
            this.zoneOffset = ZoneOffset.of( zoneOffset );
        }

        public ZoneOffset getZoneOffset()
        {
            return this.zoneOffset;
        }
    }

    public static Duration getTimeShift( final DataSourceConfig dataSourceConfig )
    {
        Duration timeShift = null;

        if ( Objects.nonNull( dataSourceConfig )
             && Objects.nonNull( dataSourceConfig.getTimeShift() ) )
        {
            timeShift = Duration.of(
                                     dataSourceConfig.getTimeShift().getWidth(),
                                     ChronoUnit.valueOf(
                                                         dataSourceConfig.getTimeShift()
                                                                         .getUnit()
                                                                         .toString()
                                                                         .toUpperCase() ) );
        }

        return timeShift;
    }

    /**
     * Given a config and a data source, return which kind the datasource is
     *
     * TODO: this method cannot work. Two or more source declarations can be equal and the LRB context
     * is not part of the declaration. See #67774.
     * The above comment is one opinion about whether to use a method like this.
     * 
     * @param projectConfig the project config the source belongs to
     * @param config the config we wonder about
     * @return left or right or baseline
     * @throws IllegalArgumentException when the config doesn't belong to project
     */

    public static LeftOrRightOrBaseline getLeftOrRightOrBaseline( ProjectConfig projectConfig,
                                                                  DataSourceConfig config )
    {
        DataSourceConfig left = projectConfig.getInputs().getLeft();
        DataSourceConfig right = projectConfig.getInputs().getRight();
        DataSourceConfig baseline = projectConfig.getInputs().getBaseline();

        if ( config == left )
        {
            LOGGER.debug( "Config {} is a left config.", config );
            return LeftOrRightOrBaseline.LEFT;
        }
        else if ( config == right )
        {
            LOGGER.debug( "Config {} is a right config.", config );
            return LeftOrRightOrBaseline.RIGHT;
        }
        else if ( config == baseline )
        {
            LOGGER.debug( "Config {} is a baseline config.", config );
            return LeftOrRightOrBaseline.BASELINE;
        }
        else
        {
            // This means either .equals doesn't work or the caller has a bug.
            throw new IllegalArgumentException( "The project configuration "
                                                + projectConfig
                                                + " doesn't seem to contain the"
                                                + " source config "
                                                + config );
        }
    }

    /**
     * Returns a {@link DecimalFormat} from the input configuration or null if no formatter is required.
     * 
     * @param destinationConfig the destination configuration
     * @return a decimal formatter or null.
     */

    public static DecimalFormat getDecimalFormatter( DestinationConfig destinationConfig )
    {
        DecimalFormat decimalFormatter = null;
        if ( destinationConfig != null && destinationConfig.getDecimalFormat() != null
             && !destinationConfig.getDecimalFormat().isEmpty() )
        {
            decimalFormatter = new DecimalFormat();
            decimalFormatter.applyPattern( destinationConfig.getDecimalFormat() );
        }
        return decimalFormatter;
    }

    /**
     * <p>Returns an {@link OutputTypeSelection} from the input configuration or {@link OutputTypeSelection#DEFAULT} if 
     * no selection is provided.</p> 
     * 
     * <p>If an override exists for a metric with the identifier {@link MetricConfigName#ALL_VALID} and this has a 
     * designated {@link OutputTypeSelection}, the override is returned. If an override does not exist, the 
     * {@link OutputTypeSelection} associated with the destination configuration is returned instead.</p>
     * 
     * @param projectConfig the project configuration to search for overrides
     * @param destinationConfig the destination configuration
     * @return the required output type
     * @throws NullPointerException if any input is null
     */

    public static OutputTypeSelection getOutputTypeSelection( ProjectConfig projectConfig,
                                                              DestinationConfig destinationConfig )
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( destinationConfig, "Specify non-null destination configuration." );

        OutputTypeSelection returnMe = OutputTypeSelection.DEFAULT;
        if ( Objects.nonNull( destinationConfig.getOutputType() ) )
        {
            returnMe = destinationConfig.getOutputType();
        }

        return returnMe;
    }

    /**
     * Returns <code>true</code> if a generated baseline is required, otherwise <code>false</code>.
     * 
     * @param baselineConfig the declaration to inspect
     * @return true if a generated baseline is required
     */

    public static boolean hasGeneratedBaseline( DataSourceConfig baselineConfig )
    {
        // Currently only one generated type supported
        return Objects.nonNull( baselineConfig )
               && ( baselineConfig.getTransformation() == SourceTransformationType.PERSISTENCE ||
                    Objects.nonNull( baselineConfig.getPersistence() ) );
    }

    /**
     * Returns <code>true</code> if a baseline is present, otherwise <code>false</code>.
     * 
     * @param projectConfig the declaration to inspect
     * @return true if a baseline is present
     * @throws NullPointerException if the input is null
     */

    public static boolean hasBaseline( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );

        return Objects.nonNull( projectConfig.getInputs() )
               && Objects.nonNull( projectConfig.getInputs().getBaseline() );
    }

    /**
     * Gets the desired time scale associated with the pair declaration, if any.
     * 
     * @param pairConfig the pair declaration
     * @return the desired time scale or null
     */

    public static TimeScaleOuter getDesiredTimeScale( PairConfig pairConfig )
    {
        TimeScaleOuter returnMe = null;

        if ( Objects.nonNull( pairConfig )
             && Objects.nonNull( pairConfig.getDesiredTimeScale() ) )
        {
            returnMe = TimeScaleOuter.of( pairConfig.getDesiredTimeScale() );
        }

        return returnMe;
    }

    /**
     * Returns a list of output formats in the input configuration that can be mutated incrementally.
     * 
     * @param projectConfig the project configuration
     * @return the output formats in the configuration that can be mutated incrementally or the empty set
     * @throws NullPointerException if the input is null
     */

    public static Set<DestinationType> getIncrementalFormats( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Outputs output = projectConfig.getOutputs();

        if ( Objects.nonNull( output ) )
        {
            return output.getDestination()
                         .stream()
                         .map( DestinationConfig::getType )
                         .filter( next -> next == DestinationType.NETCDF || next == DestinationType.PROTOBUF )
                         .collect( Collectors.toUnmodifiableSet() );
        }

        // Return empty set
        return Collections.emptySet();
    }

    /**
     * Get the feature names relevant to a particular dataSource.
     *
     * The declaration only references names, not complete feature identities,
     * therefore we cannot have a full feature at this point, nor do we get one
     * from a database here, because the purpose here is to read names only.
     *
     * A dataset will have complete feature identities which will be ingested
     * at ingest-time. But to bootstrap ingest, we start with names only, which
     * can limit requests for data from data sources. After ingest we will have
     * the ability to get the full list of features for a dataset.
     *
     * This method is intended to be called by readers and with a fully dense
     * project declaration of features. In other words the project declaration
     * should have already been filled out either by the caller or by WRES
     * control module earlier, to have complete feature correlations.
     *
     * Not all readers require declared features, so those projects including
     * solely CSV or PI-XML, for example, will not need this method.
     *
     * This method is also used by FeatureFinder to get what is available from
     * a sparse declaration. It will give a dense declaration to the rest of the
     * evaluation pipeline so that reader will have a dense declaration.
     *
     * @param projectDeclaration The project declaration.
     * @param sourceDeclaration The source declared within the declaration.
     * @return A Set of String from the given declaration or empty when none.
     * @throws UnsupportedOperationException When called with no features.
     */

    public static Set<String> getFeatureNamesForSource( ProjectConfig projectDeclaration,
                                                        DataSourceConfig sourceDeclaration )
    {
        SortedSet<String> featureNames = new TreeSet<>();
        
        // Collect the features from the singleton groups and multi-feature groups
        PairConfig pairConfig = projectDeclaration.getPair();
        List<Feature> featuresConfigured = new ArrayList<>( pairConfig.getFeature() );
        List<Feature> groupedFeatures = pairConfig.getFeatureGroup()
                                                  .stream()
                                                  .flatMap( next -> next.getFeature().stream() )
                                                  .collect( Collectors.toList() );
        featuresConfigured.addAll( groupedFeatures );

        if ( featuresConfigured.isEmpty() )
        {
            LOGGER.debug( "No features found declared, returning empty set." );
            return Collections.emptySet();
        }

        LeftOrRightOrBaseline lrb = ConfigHelper.getLeftOrRightOrBaseline( projectDeclaration,
                                                                           sourceDeclaration );
        // Reference equality on purpose here.
        if ( lrb.equals( LeftOrRightOrBaseline.LEFT ) )
        {
            for ( Feature featureConfigured : featuresConfigured )
            {
                String leftName = featureConfigured.getLeft();

                if ( Objects.nonNull( leftName ) )
                {
                    if ( leftName.isBlank() )
                    {
                        LOGGER.warn( "Encountered blank name in left feature declaration {}",
                                     featureConfigured );
                    }
                    else
                    {
                        featureNames.add( featureConfigured.getLeft() );
                    }
                }
            }
        }
        else if ( lrb.equals( LeftOrRightOrBaseline.RIGHT ) )
        {
            for ( Feature featureConfigured : featuresConfigured )
            {
                String rightName = featureConfigured.getRight();

                if ( Objects.nonNull( rightName ) )
                {
                    if ( rightName.isBlank() )
                    {
                        LOGGER.warn( "Encountered blank name in right feature declaration {}",
                                     featureConfigured );
                    }
                    else
                    {
                        featureNames.add( rightName );
                    }
                }
            }
        }
        else if ( lrb.equals( LeftOrRightOrBaseline.BASELINE )
                  && Objects.nonNull( projectDeclaration.getInputs()
                                                        .getBaseline() ) )
        {
            for ( Feature featureConfigured : featuresConfigured )
            {
                String baselineName = featureConfigured.getBaseline();

                if ( Objects.nonNull( baselineName ) )
                {
                    if ( baselineName.isBlank() )
                    {
                        LOGGER.warn( "Encountered blank name in baseline feature declaration {}",
                                     featureConfigured );
                    }
                    else
                    {
                        featureNames.add( featureConfigured.getBaseline() );
                    }
                }
            }
        }

        return Collections.unmodifiableSortedSet( featureNames );
    }

    public static FeatureDimension getConcreteFeatureDimension( final DataSourceConfig datasource )
    {
        FeatureDimension dimension = datasource.getFeatureDimension();

        if ( dimension == null )
        {
            FeatureDimension foundDimension = null;

            for ( DataSourceConfig.Source source : datasource.getSource() )
            {
                String sourceInterface = "";

                if ( source.getInterface() != null )
                {
                    sourceInterface = source.getInterface().value().toLowerCase();
                }

                String address = "";

                if ( source.getValue() != null )
                {
                    address = source.getValue().toString().toLowerCase();
                }
                if ( sourceInterface.contains( "nwm" ) )
                {
                    if ( foundDimension == null || foundDimension == FeatureDimension.NWM_FEATURE_ID )
                    {
                        foundDimension = FeatureDimension.NWM_FEATURE_ID;
                    }
                    else
                    {
                        throw new IllegalStateException(
                                                         "External threshold identifiers cannot be interpretted if the input data is both "
                                                         +
                                                         foundDimension
                                                         + " and "
                                                         + FeatureDimension.NWM_FEATURE_ID );
                    }
                }
                else if ( sourceInterface.contains( "usgs" )
                          || address.contains( "usgs.gov/nwis" ) )
                {
                    if ( foundDimension == null || foundDimension == FeatureDimension.USGS_SITE_CODE )
                    {
                        foundDimension = FeatureDimension.USGS_SITE_CODE;
                    }
                    else
                    {
                        throw new IllegalStateException(
                                                         "External threshold identifiers cannot be interpretted if the input data is both "
                                                         +
                                                         foundDimension
                                                         + " and "
                                                         + FeatureDimension.USGS_SITE_CODE );
                    }
                }
                else if ( sourceInterface.contains( "ahps" ) )
                {
                    if ( foundDimension == null || foundDimension == FeatureDimension.NWS_LID )
                    {
                        foundDimension = FeatureDimension.NWS_LID;
                    }
                    else
                    {
                        throw new IllegalStateException(
                                                         "External threshold identifiers cannot be interpretted if the input data is both "
                                                         +
                                                         foundDimension
                                                         + " and "
                                                         + FeatureDimension.NWS_LID );
                    }
                }
            }

            dimension = foundDimension;
        }

        return dimension;
    }
}
