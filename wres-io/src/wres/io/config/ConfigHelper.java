package wres.io.config;

import java.text.DecimalFormat;
import java.time.DateTimeException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.xml.ProjectConfigException;
import wres.config.generated.*;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.datamodel.units.UnitMapper;
import wres.io.reading.wrds.geography.WrdsFeatureFiller;
import wres.io.reading.wrds.thresholds.WrdsThresholdFiller;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * <p>The purpose of io's ConfigHelper is to help the io module translate raw
 * user-specified configuration elements into a reduced form, a more
 * actionable or meaningful form such as a SQL script, or to extract specific
 * elements from a particular config element, or other purposes that are common
 * to the io module.
 *
 * <p>The general form of a helper method appropriate for ConfigHelper has a
 * ProjectConfig as the first argument and some other element(s) or hint(s) as
 * additional args. These are not hard-and-fast-rules. But the original purpose
 * was to help the io module avoid tedious repetition of common interpretations
 * of the raw user-specified configuration.
 *
 * <p>Candidates for removal to a wres-config helper are those that purely operate
 * on, use, and return objects of classes that are specified in the wres-config
 * or JDK.
 *
 * <p>Candidates that should stay are those returning SQL statements or are
 * currently useful only to the wres-io module.
 */

public class ConfigHelper
{
    private static final String EXTERNAL_THRESHOLD_IDENTIFIERS_CANNOT_BE_INTERPRETTED_IF_THE_INPUT_DATA_IS_BOTH =
            "External threshold identifiers cannot be interpreted if the input data is both ";

    private static final String AND = " and ";

    private static final Logger LOGGER = LoggerFactory.getLogger( ConfigHelper.class );

    /**
     * String for null configuration error.
     */

    private static final String NULL_CONFIGURATION_ERROR = "The project configuration cannot be null.";

    /**
     * Resolves any implicit declaration of features that require service calls to external web services. Currently,
     * the only supported web services are those within the umbrella of the Water Resources Data Service (WRDS), which
     * contains a collection of U.S. National Weather Service APIs.
     *
     * @param declaration the evaluation declaration
     * @return the declaration with any implicit features rendered explicit
     * @throws NullPointerException if the input is null
     */

    public static EvaluationDeclaration fillFeatures( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        // Currently, there is only one feature service supported
        return WrdsFeatureFiller.fillFeatures( declaration );
    }

    /**
     * Resolves any implicit declaration of thresholds that require service calls to external web services. Currently,
     * the only supported web services are those within the umbrella of the Water Resources Data Service (WRDS), which
     * contains a collection of U.S. National Weather Service APIs.
     *
     * @param declaration the evaluation declaration
     * @param unitMapper the unit mapper
     * @return the declaration with any implicit thresholds rendered explicit
     * @throws NullPointerException if either input is null
     */

    public static EvaluationDeclaration fillThresholds( EvaluationDeclaration declaration, UnitMapper unitMapper )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( unitMapper );

        // Currently, there is only one threshold service supported
        return WrdsThresholdFiller.fillThresholds( declaration, unitMapper );
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
     * <p>Get the feature names relevant to a particular dataSource.
     *
     * <p>The declaration only references names, not complete feature identities,
     * therefore we cannot have a full feature at this point, nor do we get one
     * from a database here, because the purpose here is to read names only.
     *
     * <p>A dataset will have complete feature identities which will be ingested
     * at ingest-time. But to bootstrap ingest, we start with names only, which
     * can limit requests for data from data sources. After ingest we will have
     * the ability to get the full list of features for a dataset.
     *
     * <p>This method is intended to be called by readers and with a fully dense
     * project declaration of features. In other words the project declaration
     * should have already been filled out either by the caller or by WRES
     * control module earlier, to have complete feature correlations.
     *
     * <p>Not all readers require declared features, so those projects including
     * solely CSV or PI-XML, for example, will not need this method.
     *
     * <p>This method is also used by WrdsFeatureFinder to get what is available from
     * a sparse declaration. It will give a dense declaration to the rest of the
     * evaluation pipeline so that reader will have a dense declaration.
     *
     * @param pairConfig The pair declaration.
     * @param sourceDeclaration The source declared within the declaration.
     * @param sourceOrientation The orientation of the source.
     * @return A Set of String from the given declaration or empty when none.
     * @throws UnsupportedOperationException When called with no features.
     */

    public static Set<String> getFeatureNamesForSource( PairConfig pairConfig,
                                                        DataSourceConfig sourceDeclaration,
                                                        LeftOrRightOrBaseline sourceOrientation )
    {
        Objects.requireNonNull( pairConfig );
        Objects.requireNonNull( sourceDeclaration );

        SortedSet<String> featureNames = new TreeSet<>();

        // Collect the features from the singleton groups and multi-feature groups
        List<NamedFeature> featuresConfigured = new ArrayList<>( pairConfig.getFeature() );
        List<NamedFeature> groupedFeatures = pairConfig.getFeatureGroup()
                                                       .stream()
                                                       .flatMap( next -> next.getFeature()
                                                                             .stream() )
                                                       .toList();
        featuresConfigured.addAll( groupedFeatures );

        if ( featuresConfigured.isEmpty() )
        {
            LOGGER.debug( "No features found declared, returning empty set." );
            return Collections.emptySet();
        }

        // Iterate the sides
        if ( sourceOrientation == LeftOrRightOrBaseline.LEFT )
        {
            for ( NamedFeature featureConfigured : featuresConfigured )
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
        else if ( sourceOrientation == LeftOrRightOrBaseline.RIGHT )
        {
            for ( NamedFeature featureConfigured : featuresConfigured )
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
        else if ( sourceOrientation == LeftOrRightOrBaseline.BASELINE )
        {
            for ( NamedFeature featureConfigured : featuresConfigured )
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

    /**
     * @param datasource the data source
     * @return the feature dimension
     */
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
                                EXTERNAL_THRESHOLD_IDENTIFIERS_CANNOT_BE_INTERPRETTED_IF_THE_INPUT_DATA_IS_BOTH
                                +
                                foundDimension
                                + AND
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
                                EXTERNAL_THRESHOLD_IDENTIFIERS_CANNOT_BE_INTERPRETTED_IF_THE_INPUT_DATA_IS_BOTH
                                +
                                foundDimension
                                + AND
                                + FeatureDimension.USGS_SITE_CODE );
                    }
                }
                else if ( sourceInterface.contains( "ahps" )
                          || sourceInterface.equalsIgnoreCase( InterfaceShortHand.WRDS_OBS.name() ) )
                {
                    if ( foundDimension == null || foundDimension == FeatureDimension.NWS_LID )
                    {
                        foundDimension = FeatureDimension.NWS_LID;
                    }
                    else
                    {
                        throw new IllegalStateException(
                                EXTERNAL_THRESHOLD_IDENTIFIERS_CANNOT_BE_INTERPRETTED_IF_THE_INPUT_DATA_IS_BOTH
                                +
                                foundDimension
                                + AND
                                + FeatureDimension.NWS_LID );
                    }
                }
            }

            dimension = foundDimension;
        }

        return dimension;
    }

    /**
     * Makes a best guess at the type of reference time from the data source type.
     * @param dataSourceType the data source type
     * @return the reference time type
     */

    public static ReferenceTimeType getReferenceTimeType( DatasourceType dataSourceType )
    {
        return switch ( dataSourceType )
                {
                    case ANALYSES, SIMULATIONS, OBSERVATIONS -> ReferenceTimeType.ANALYSIS_START_TIME;
                    case ENSEMBLE_FORECASTS, SINGLE_VALUED_FORECASTS -> ReferenceTimeType.T0;
                };
    }

    private ConfigHelper()
    {
        // prevent construction
    }
}
