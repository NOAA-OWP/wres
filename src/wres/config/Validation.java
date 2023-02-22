package wres.config;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.xml.bind.ValidationEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.xml.bind.Locatable;
import org.xml.sax.Locator;

import wres.config.generated.DataSourceBaselineConfig;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DataSourceConfig.Source;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.DesiredTimeScaleConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.NamedFeature;
import wres.config.generated.FeaturePool;
import wres.config.generated.IntBoundsType;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.LenienceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.config.generated.ProjectConfig.Outputs;
import wres.config.generated.SourceTransformationType;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeScaleConfig;
import wres.config.generated.TimeScaleFunction;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.config.generated.UnitAlias;
import wres.config.generated.UnnamedFeature;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.metrics.MetricConstantsFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.io.config.ConfigHelper;
import wres.system.SystemSettings;

/**
 * Helps validate project declarations at a higher level than parser, with
 * detailed messaging.
 *
 * TODO: formal interface for validation text rather than log messages
 */

public class Validation
{
    private static final String TRY_AGAIN = "try again.";

    private static final String POOLING_WINDOWS = "pooling windows:";

    private static final String AND_TRY_AGAIN = "and try again.";

    private static final String THIS_EVALUATION_APPEARS_TO_CONTAIN_GRIDDED_DATA = " This evaluation appears to contain "
                                                                                  + "gridded data because it declares a "
                                                                                  + "<gridSelection>. For gridded "
                                                                                  + "evaluations, the variable must be "
                                                                                  + "declared explicitly. No "
                                                                                  + "<variable> declaration was "
                                                                                  + "discovered for the {} data. "
                                                                                  + "Please add this declaration and "
                                                                                  + TRY_AGAIN;

    private static final String NOT_APPEAR_TO_BE_VALID_PLEASE_USE_NUMERIC =
            "not appear to be valid. Please use numeric ";

    private static final String THE_MONTH_AND_DAY_COMBINATION_DOES = " The month {} and day {} combination does ";

    private static final Logger LOGGER = LoggerFactory.getLogger( Validation.class );

    /** A message to display for programmers when null project config occurs */
    private static final String NON_NULL = "The args must not be null";

    /** The warning message boilerplate for logger (includes 3 placeholders) */
    private static final String FILE_LINE_COLUMN_BOILERPLATE =
            "In the project declaration from {}, near line {} and column {}, "
                                                               + "WRES found an issue.";

    private static final String API_SOURCE_MISSING_ISSUED_DATES_ERROR_MESSAGE =
            "One must specify issued dates with both earliest and latest (e.g. "
                                                                                + "<issuedDates earliest=\"2018-12-28T15:42:00Z\" "
                                                                                + "latest=\"2019-01-01T00:00:00Z\" />) when using a web API as a "
                                                                                + "source for forecasts (see source near line {} and column {}";
    private static final String API_SOURCE_MISSING_DATES_ERROR_MESSAGE =
            "One must specify dates with both earliest and latest (e.g. "
                                                                         + "<dates earliest=\"2018-12-28T15:42:00Z\" "
                                                                         + "latest=\"2019-01-01T00:00:00Z\" />) when using a web API as a "
                                                                         + "source for observations (see source near line {} and column {}";


    private Validation()
    {
        // prevent construction.
    }


    /**
     * Quick validation of the project declaration, will return detailed
     * information to the user regarding issues about the declaration. Strict
     * for now, i.e. return false even on minor xml problems. Does not return on
     * first issue, tries to inform the user of all issues before returning.
     *
     * @param systemSettings The system settings to use.
     * @param projectConfigPlus the project declaration
     * @return true if no issues were detected, false otherwise
     */

    public static boolean isProjectValid( SystemSettings systemSettings,
                                          ProjectConfigPlus projectConfigPlus )
    {
        // Assume valid until demonstrated otherwise
        boolean result = true;

        // Warned about format already?
        // TODO: remove this warning when releasing 7.0
        boolean formatWarnedAlready = false;

        for ( ValidationEvent ve : projectConfigPlus.getValidationEvents() )
        {
            // TODO: remove this exception to validation when releasing 7.0
            if ( ve.getMessage()
                   .contains( "'format' is not allowed to appear in element 'source'" ) )
            {
                if ( LOGGER.isWarnEnabled() && !formatWarnedAlready )
                {
                    formatWarnedAlready = true;
                    LOGGER.warn( " The 'format' attribute is deprecated in left/"
                                 + "right/baseline declarations (non-threshold "
                                 + "declarations) and will be removed in a future "
                                 + "release. Alternatives: specify one source per "
                                 + "resource, use the 'pattern' attribute with a glob "
                                 + "pattern specified (e.g. 'pattern=\"**/D*.xml\"' to "
                                 + "include all files starting with 'D' and ending with"
                                 + "'.xml' in all subdirectories), or to leave pattern "
                                 + "unspecified and let WRES auto-detect what it can "
                                 + "read on all files included in/under the URI." );
                }
            }
            else
            {
                result = false;

                if ( LOGGER.isErrorEnabled() )
                {
                    if ( ve.getLocator() != null )
                    {
                        LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                      + " The parser said: {}",
                                      projectConfigPlus.getOrigin(),
                                      ve.getLocator().getLineNumber(),
                                      ve.getLocator().getColumnNumber(),
                                      ve.getMessage(),
                                      ve.getLinkedException() );
                    }
                    else
                    {
                        LOGGER.error( "In the project declaration from {}, WRES found an issue. The XML parser/"
                                      + "validator reports this message: {}; and this linked exception: {}.",
                                      projectConfigPlus.getOrigin(),
                                      ve.getMessage(),
                                      ve.getLinkedException() );
                    }
                }
            }
        }

        // Validate data sources
        result = Validation.isInputsConfigValid( projectConfigPlus )
                 && result;

        // Validate pair section
        result = Validation.isPairConfigValid( projectConfigPlus ) && result;

        // Validate combination of data source section with pair section
        result = Validation.isInputsAndPairCombinationValid( projectConfigPlus )
                 && result;

        // Validate metrics section
        result = Validation.isMetricsConfigValid( systemSettings,
                                                  projectConfigPlus )
                 && result;

        // Validate combination of data source section with metrics section
        result = Validation.isInputsAndMetricsCombinationValid( projectConfigPlus )
                 && result;

        // Validate outputs section
        result = Validation.isOutputConfigValid( projectConfigPlus ) && result;

        return result;
    }

    /**
     * Checks that there is no more than one <code>destination</code> of a given <code>type</code>, otherwise the
     * declaration is invalid. See #58737.
     * 
     * @param projectConfigPlus the project to validate
     * @return true when valid, false otherwise
     */
    static boolean hasUpToOneDestinationPerDestinationType( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus );
        Objects.requireNonNull( projectConfigPlus.getProjectConfig() );

        Outputs outputs = projectConfigPlus.getProjectConfig()
                                           .getOutputs();

        Objects.requireNonNull( outputs );

        List<DestinationConfig> destinations = projectConfigPlus.getProjectConfig()
                                                                .getOutputs()
                                                                .getDestination();

        Objects.requireNonNull( destinations );

        boolean isValid = true;

        // Create a map of destination types and counts
        Map<DestinationType, Integer> destinationsByType = new EnumMap<>( DestinationType.class );
        for ( DestinationConfig destination : destinations )
        {
            DestinationType nextType = destination.getType();

            // Normalize synonyms
            if ( nextType == DestinationType.PNG )
            {
                nextType = DestinationType.GRAPHIC;
            }
            else if ( nextType == DestinationType.CSV )
            {
                nextType = DestinationType.NUMERIC;
            }

            // Map the type
            if ( destinationsByType.containsKey( nextType ) )
            {
                int currentCount = destinationsByType.get( nextType );
                int newCount = currentCount + 1;
                destinationsByType.put( nextType, newCount );
                isValid = false;
            }
            else
            {
                destinationsByType.put( nextType, 1 );
            }

            // Warn about deprecated types
            if ( LOGGER.isWarnEnabled() && ( nextType == DestinationType.CSV || nextType == DestinationType.NUMERIC ) )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " The declaration requests {} outputs. This output format has been marked deprecated, "
                             + "for removal. We recommend that you choose {} instead. Format {} will be removed in a "
                             + "future version of the software.",
                             projectConfigPlus.getOrigin(),
                             outputs.sourceLocation().getLineNumber(),
                             outputs.sourceLocation().getColumnNumber(),
                             nextType,
                             DestinationType.CSV2,
                             nextType );
            }
        }

        if ( !isValid && LOGGER.isErrorEnabled() )
        {
            // Make the synonyms intelligible
            String mapString = destinationsByType.toString();

            mapString = mapString.replaceAll( DestinationType.GRAPHIC.name(), "GRAPHIC/PNG" );
            mapString = mapString.replaceAll( DestinationType.NUMERIC.name(), "NUMERIC/CSV" );

            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                          + " The declaration contains more than one destination of a given type, which is not allowed. "
                          + "Please declare only one destination per destination type. The number of destinations by "
                          + "type is: {}.",
                          projectConfigPlus.getOrigin(),
                          outputs.sourceLocation().getLineNumber(),
                          outputs.sourceLocation().getColumnNumber(),
                          mapString );
        }

        return isValid;
    }

    /**
     * Warns about the zone offset.
     * 
     * @param projectConfigPlus the config
     * @param source the particular source element to check
     * @throws NullPointerException when any arg is null
     */

    static void warnAboutZoneOffset( ProjectConfigPlus projectConfigPlus,
                                     DataSourceConfig.Source source )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );
        Objects.requireNonNull( source, NON_NULL );

        if ( source.getZoneOffset() != null && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                         + " The zoneOffset attribute is only applied to "
                         + "datacard data. If there are no datacard data "
                         + "in this evaluation the time zone offset will "
                         + "come from the data itself and the value in the "
                         + "attribute will be ignored.",
                         projectConfigPlus.getOrigin(),
                         source.sourceLocation().getLineNumber(),
                         source.sourceLocation().getColumnNumber() );

        }
    }

    /**
     * Checks to see whether the combination of inputs and pair declaration is valid.
     * @param projectConfigPlus the project declaration to check
     * @return false if there are known invalid combinations present
     */

    private static boolean isInputsAndPairCombinationValid( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus );

        boolean isValid = Validation.isInputsAndFeatureCombinationValid( projectConfigPlus );

        isValid = Validation.isInputsAndLeadDurationCombinationValid( projectConfigPlus ) && isValid;

        return Validation.hasVariablesIfGridded( projectConfigPlus ) && isValid;
    }

    /**
     * Checks to see whether the combination of inputs and metrics declaration is valid.
     * @param projectConfigPlus the project declaration to check
     * @return false if there are known invalid combinations present
     */

    private static boolean isInputsAndMetricsCombinationValid( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus );

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        boolean valid = true;

        // If there are no ensemble forecasts, there should be no ensemble declaration
        if ( !ConfigHelper.hasEnsembleForecasts( projectConfig )
             && projectConfig.getMetrics()
                             .stream()
                             .anyMatch( next -> Objects.nonNull( next.getEnsembleAverage() ) ) )
        {
            valid = false;

            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + " The \"ensembleAverage\" cannot be declared unless the \"inputs\" contains a source "
                              + "of ensemble forecasts, but no ensemble forecasts were found. Change the \"inputs\" "
                              + "declaration to include a source of ensemble forecasts or remove the "
                              + "\"ensembleAverage\" from the \"metrics\" declaration.",
                              projectConfigPlus.getOrigin(),
                              projectConfig.getInputs()
                                           .sourceLocation()
                                           .getLineNumber(),
                              projectConfig.getInputs()
                                           .sourceLocation()
                                           .getColumnNumber() );

            }
        }

        return valid;
    }

    /**
     * Checks to see whether the combination of inputs and feature declaration is valid.
     * @param projectConfigPlus the project declaration to check
     * @return false if there are known invalid combinations present
     */

    private static boolean isInputsAndFeatureCombinationValid( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus );
        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();
        Locatable firstSourceThatRequiresFeatures = Validation.getFirstSourceThatRequiresFeatures( projectConfig );
        boolean noFeatureDeclaration = false;

        PairConfig pairDeclaration = projectConfig.getPair();
        if ( ( Objects.isNull( pairDeclaration.getFeature() )
               || pairDeclaration.getFeature()
                                 .isEmpty() )
             && ( Objects.isNull( pairDeclaration.getFeatureService() )
                  || Objects.isNull( pairDeclaration.getFeatureService()
                                                    .getBaseUrl() ) )
             && ( Objects.isNull( pairDeclaration.getFeatureGroup() )
                  || pairDeclaration.getFeatureGroup()
                                    .isEmpty() ) )
        {
            noFeatureDeclaration = true;
        }

        boolean isValid = true;

        if ( Objects.nonNull( firstSourceThatRequiresFeatures )
             && noFeatureDeclaration )
        {
            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + " At least one of the declared data sources "
                              + "required one or more <feature> or "
                              + "<featureGroup> declarations under <pair> or a "
                              + "<featureService> declaration under <pair> "
                              + "but no such feature-related declaration was "
                              + "found. Add <feature> or <featureGroup> "
                              + "declarations such that WRES can get data for "
                              + "those geographic features using the declared "
                              + "data source.",
                              projectConfigPlus.getOrigin(),
                              firstSourceThatRequiresFeatures.sourceLocation()
                                                             .getLineNumber(),
                              firstSourceThatRequiresFeatures.sourceLocation()
                                                             .getColumnNumber() );
            }

            isValid = false;
        }

        Locatable baselineFeatureNameButNoBaseline = Validation.getBaselineFeatureNameButNoBaseline( projectConfig );

        if ( baselineFeatureNameButNoBaseline != null )
        {
            isValid = false;

            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + " At least one <feature> was declared with a "
                              + "baseline dataset geographic feature name, but "
                              + "there was no baseline dataset declared in the "
                              + "<inputs> declaration. WRES will look in the "
                              + "left, right, and baseline datasets for the "
                              + "names declared in <feature> attributes left, "
                              + "right, and baseline, respectively. Either add "
                              + "a baseline dataset or remove the baseline "
                              + "feature name to resolve this issue. If the "
                              + "name is known for some dimension not used by "
                              + "either the left or right datasets, such as an "
                              + "evaluation of National Water Model data "
                              + "(nwm_feature_id) on the right versus USGS data "
                              + "(usgs_site_code) on the left, while only the "
                              + "NWS Location ID (nws_lid) is known, then use "
                              + "the <featureService> with a <group> for each "
                              + "feature, e.g. with <type>nws_lid</type> (the "
                              + "known dimension) and <value>DRRC2</value> (the "
                              + "known feature name in that dimension) and "
                              + "omit the <feature> declarations too.",
                              projectConfigPlus.getOrigin(),
                              baselineFeatureNameButNoBaseline.sourceLocation()
                                                              .getLineNumber(),
                              baselineFeatureNameButNoBaseline.sourceLocation()
                                                              .getColumnNumber() );
            }
        }

        return isValid;
    }

    /**
     * Checks the declaration for the first source that requires features
     * in the pair declaration.
     * @param projectConfig the project declaration to check
     * @return the first source that requires features or null
     */

    private static Locatable getFirstSourceThatRequiresFeatures( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );

        Locatable firstSourceThatRequiresFeatures = null;

        DataSourceConfig left = projectConfig.getInputs()
                                             .getLeft();
        DataSourceConfig right = projectConfig.getInputs()
                                              .getRight();
        DataSourceBaselineConfig baseline =
                projectConfig.getInputs()
                             .getBaseline();

        for ( DataSourceConfig.Source source : left.getSource() )
        {
            if ( Validation.requiresFeatureOrFeatureService( source ) )
            {
                firstSourceThatRequiresFeatures = source;

                return firstSourceThatRequiresFeatures;
            }
        }

        for ( DataSourceConfig.Source source : right.getSource() )
        {
            if ( Validation.requiresFeatureOrFeatureService( source ) )
            {
                firstSourceThatRequiresFeatures = source;

                return firstSourceThatRequiresFeatures;
            }
        }

        if ( Objects.nonNull( baseline ) )
        {
            for ( DataSourceConfig.Source source : baseline.getSource() )
            {
                if ( Validation.requiresFeatureOrFeatureService( source ) )
                {
                    firstSourceThatRequiresFeatures = source;

                    return firstSourceThatRequiresFeatures;
                }
            }
        }

        return firstSourceThatRequiresFeatures;
    }

    /**
     * Checks and returns a baseline feature when no baseline declaration is present, otherwise null.
     * @param projectConfig the project declaration
     * @return the baseline feature or null
     */

    private static Locatable getBaselineFeatureNameButNoBaseline( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );

        Locatable baselineFeatureNameButNoBaseline = null;

        PairConfig pairConfig = projectConfig.getPair();
        DataSourceBaselineConfig baseline =
                projectConfig.getInputs()
                             .getBaseline();

        if ( baseline == null && pairConfig.getFeature() != null )
        {
            baselineFeatureNameButNoBaseline = pairConfig.getFeature()
                                                         .stream()
                                                         .filter( next -> Objects.nonNull( next.getBaseline() ) )
                                                         .findFirst()
                                                         .orElse( null );
        }

        if ( baselineFeatureNameButNoBaseline == null
             && baseline == null
             && pairConfig.getFeatureGroup() != null )
        {
            for ( FeaturePool featurePool : pairConfig.getFeatureGroup() )
            {
                baselineFeatureNameButNoBaseline = featurePool.getFeature()
                                                              .stream()
                                                              .filter( next -> Objects.nonNull( next.getBaseline() ) )
                                                              .findFirst()
                                                              .orElse( null );

                if ( baselineFeatureNameButNoBaseline != null )
                {
                    break;
                }
            }
        }

        return baselineFeatureNameButNoBaseline;
    }

    /**
     * Checks to see whether the combination of inputs and lead duration declaration is valid.
     * @param projectConfigPlus the project declaration to check
     * @return false if there are known invalid combinations present
     */

    private static boolean isInputsAndLeadDurationCombinationValid( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus );

        // Any references to lead duration constraints or lead duration pools is not valid in this context
        boolean isValid = true;

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();
        Inputs inputsConfig = projectConfig.getInputs();
        PairConfig pair = projectConfig.getPair();

        // None of the data sources/sides contains forecasts...
        if ( !ConfigHelper.isForecast( inputsConfig.getLeft() ) && !ConfigHelper.isForecast( inputsConfig.getRight() )
             && ( !ConfigHelper.hasBaseline( projectConfig )
                  || !ConfigHelper.isForecast( inputsConfig.getBaseline() ) ) )
        {
            // ...but there are constraints on lead hours, which is not allowed
            if ( Objects.nonNull( pair.getLeadHours() ) )
            {
                isValid = false;

                if ( LOGGER.isErrorEnabled() )
                {
                    LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                  + "The declaration includes a constraint on forecast lead times (\"leadHours\"), but "
                                  + "none of the data sources contain forecasts. Please add a forecast source or "
                                  + "remove the \"leadHours\" declaration and try again.",
                                  projectConfigPlus.getOrigin(),
                                  inputsConfig.getLeft().sourceLocation().getLineNumber(),
                                  inputsConfig.getLeft().sourceLocation().getColumnNumber() );
                }
            }

            // ...but there are lead duration pools, which is not allowed
            if ( Objects.nonNull( pair.getLeadTimesPoolingWindow() ) )
            {
                isValid = false;

                if ( LOGGER.isErrorEnabled() )
                {
                    LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                  + "The declaration includes lead duration pools (\"leadTimesPoolingWindow\"), but "
                                  + "none of the data sources contain forecasts. Please add a forecast source or "
                                  + "remove the \"leadTimesPoolingWindow\" declaration and try again.",
                                  projectConfigPlus.getOrigin(),
                                  inputsConfig.getLeft().sourceLocation().getLineNumber(),
                                  inputsConfig.getLeft().sourceLocation().getColumnNumber() );
                }
            }
        }

        return isValid;
    }

    /**
     * @param projectConfigPlus the declaration
     * @return true if the declaration is valid (is not gridded or has variables declared if gridded), otherwise false
     */

    private static boolean hasVariablesIfGridded( ProjectConfigPlus projectConfigPlus )
    {
        PairConfig pairConfig = projectConfigPlus.getProjectConfig().getPair();

        if ( pairConfig.getGridSelection().isEmpty() )
        {
            return true;
        }

        Inputs inputsConfig = projectConfigPlus.getProjectConfig().getInputs();

        boolean isValid = true;
        if ( Objects.isNull( inputsConfig.getLeft().getVariable() ) )
        {
            isValid = false;
            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + THIS_EVALUATION_APPEARS_TO_CONTAIN_GRIDDED_DATA,
                              projectConfigPlus.getOrigin(),
                              inputsConfig.getLeft().sourceLocation().getLineNumber(),
                              inputsConfig.getLeft().sourceLocation().getColumnNumber(),
                              LeftOrRightOrBaseline.LEFT );
            }
        }
        if ( Objects.isNull( inputsConfig.getRight().getVariable() ) )
        {
            isValid = false;
            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + THIS_EVALUATION_APPEARS_TO_CONTAIN_GRIDDED_DATA,
                              projectConfigPlus.getOrigin(),
                              inputsConfig.getRight().sourceLocation().getLineNumber(),
                              inputsConfig.getRight().sourceLocation().getColumnNumber(),
                              LeftOrRightOrBaseline.RIGHT );
            }
        }

        if ( Objects.nonNull( inputsConfig.getBaseline() )
             && Objects.isNull( inputsConfig.getBaseline().getVariable() ) )
        {
            isValid = false;
            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + THIS_EVALUATION_APPEARS_TO_CONTAIN_GRIDDED_DATA,
                              projectConfigPlus.getOrigin(),
                              inputsConfig.getBaseline().sourceLocation().getLineNumber(),
                              inputsConfig.getBaseline().sourceLocation().getColumnNumber(),
                              LeftOrRightOrBaseline.BASELINE );
            }
        }

        return isValid;
    }

    private static boolean requiresFeatureOrFeatureService( DataSourceConfig.Source source )
    {
        return Objects.nonNull( source.getInterface() ) && ( source.getInterface()
                                                                   .equals( InterfaceShortHand.WRDS_AHPS )
                                                             || source.getInterface()
                                                                      .equals( InterfaceShortHand.WRDS_NWM )
                                                             || source.getInterface()
                                                                      .equals( InterfaceShortHand.USGS_NWIS )
                                                             || source.getInterface()
                                                                      .value()
                                                                      .toLowerCase()
                                                                      .startsWith( "nwm_" ) );
    }

    /**
     * Validates the metrics portion of the project config.
     * 
     * @param projectConfigPlus the project declaration
     * @return true if the output declaration is valid, false otherwise
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean isMetricsConfigValid( SystemSettings systemSettings,
                                                 ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        // Validate that metric declaration is internally consistent
        boolean result = Validation.isAllMetricsConfigInternallyConsistent( projectConfigPlus );

        // Check that each named metric is consistent with the other declaration
        result = Validation.isAllMetricsConfigConsistentWithOtherConfig( projectConfigPlus ) && result;

        // Check that any external thresholds refer to readable files
        return Validation.areAllPathsToThresholdsReadable( systemSettings, projectConfigPlus ) && result;
    }


    /**
     * Validates the output portion of the project config.
     * 
     * @param projectConfigPlus the project declaration
     * @return true if the output declaration is valid, false otherwise
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean isOutputConfigValid( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        boolean result = true;

        // #58737
        result = Validation.hasUpToOneDestinationPerDestinationType( projectConfigPlus );

        result = Validation.isNetcdfOutputConfigValid( projectConfigPlus.toString(),
                                                       projectConfigPlus.getProjectConfig()
                                                                        .getOutputs()
                                                                        .getDestination() )
                 && result;

        return Validation.areNetcdfOutputsValid( projectConfigPlus ) && result;
    }

    /**
     * Checks that only zero or one type of 'netcdf', 'netcdf2' are declared.
     * @param projectConfigPlus The project to validate.
     * @return true when valid, false otherwise
     */
    private static boolean areNetcdfOutputsValid( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus );
        Objects.requireNonNull( projectConfigPlus.getProjectConfig() );
        Objects.requireNonNull( projectConfigPlus.getProjectConfig()
                                                 .getOutputs() );
        Objects.requireNonNull( projectConfigPlus.getProjectConfig()
                                                 .getOutputs()
                                                 .getDestination() );
        boolean foundTemplateNetcdf = false;
        boolean foundScratchNetcdf = false;
        Locator locator = null;

        boolean isValid = true;

        for ( DestinationConfig destination : projectConfigPlus.getProjectConfig()
                                                               .getOutputs()
                                                               .getDestination() )
        {
            if ( Objects.nonNull( destination.getType() ) )
            {
                if ( destination.getType()
                                .equals( DestinationType.NETCDF ) )
                {
                    foundTemplateNetcdf = true;
                    locator = destination.sourceLocation();
                }
                else if ( destination.getType()
                                     .equals( DestinationType.NETCDF_2 ) )
                {
                    foundScratchNetcdf = true;
                    locator = destination.sourceLocation();
                }
            }
        }

        if ( foundScratchNetcdf && foundTemplateNetcdf )
        {
            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                          + " In this version of WRES, \"netcdf\" and "
                          + "\"netcdf2\" are mutually exclusive within a single "
                          + "evaluation. Declare one or the other but not both "
                          + "simultaneously.",
                          projectConfigPlus.getOrigin(),
                          locator.getLineNumber(),
                          locator.getColumnNumber() );
            isValid = false;
        }

        // Legacy netcdf not supported with feature groups
        if ( foundTemplateNetcdf && !projectConfigPlus.getProjectConfig()
                                                      .getPair()
                                                      .getFeatureGroup()
                                                      .isEmpty() )
        {
            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                          + " In this version of WRES, \"netcdf\" cannot "
                          + "be declared in combination with \"featureGroup\". "
                          + "Declare one or the other, but not both "
                          + "simultaneously. Hint: the \"netcdf2\" format is "
                          + "supported alongside \"featureGroup\".",
                          projectConfigPlus.getOrigin(),
                          locator.getLineNumber(),
                          locator.getColumnNumber() );
            isValid = false;
        }

        // The netcdf2 is supported but results may be surprising
        if ( foundScratchNetcdf && !projectConfigPlus.getProjectConfig()
                                                     .getPair()
                                                     .getFeatureGroup()
                                                     .isEmpty() )
        {
            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                          + " In this version of WRES, while \"netcdf2\" is "
                          + "supported in combination with \"featureGroup\" "
                          + "the statistics for a feature group will be repeated"
                          + " for each individual feature in that group in the"
                          + " resulting netCDF statistics data.",
                          projectConfigPlus.getOrigin(),
                          locator.getLineNumber(),
                          locator.getColumnNumber() );
        }

        return isValid;
    }


    /**
     * @deprecated since 5.2 ('netcdf' is deprecated, 'netcdf2' ignores extra declarations)
     */
    @Deprecated( since = "5.2" )
    private static boolean isNetcdfOutputConfigValid( String path, List<DestinationConfig> destinations )
    {
        Objects.requireNonNull( destinations, NON_NULL );

        boolean isValid = true;

        // Look for a Netcdf Destination config that has a template that doesn't exist
        DestinationConfig templateMissing = wres.util.Collections.find( destinations,
                                                                        destinationConfig -> destinationConfig.getNetcdf() != null
                                                                                             &&
                                                                                             destinationConfig.getNetcdf()
                                                                                                              .getTemplatePath() != null
                                                                                             &&
                                                                                             Files.notExists(
                                                                                                              Paths.get( destinationConfig.getNetcdf()
                                                                                                                                          .getTemplatePath() ) ) );

        if ( templateMissing != null && LOGGER.isErrorEnabled() )
        {
            isValid = false;
            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                          + " The indicated Netcdf Output template does not exist.",
                          path,
                          templateMissing.sourceLocation().getLineNumber(),
                          templateMissing.sourceLocation().getColumnNumber() );
        }

        // Look for destinations that aren't for Netcdf but have netcdf specifications
        Collection<DestinationConfig> incorrectDestinations = wres.util.Collections.where(
                                                                                           destinations,
                                                                                           config -> config.getType() != DestinationType.NETCDF
                                                                                                     && config.getNetcdf() != null );

        if ( !incorrectDestinations.isEmpty() )
        {
            isValid = false;

            if ( LOGGER.isErrorEnabled() )
            {
                for ( DestinationConfig destinationConfig : incorrectDestinations )
                {
                    LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                  + " Netcdf output declarations are only valid for Netcdf Output.",
                                  path,
                                  destinationConfig.sourceLocation().getLineNumber(),
                                  destinationConfig.sourceLocation().getColumnNumber() );
                }
            }
        }

        return isValid;
    }

    /**
     * Checks that the metric declaration is internally consistent.
     * 
     * @param projectConfigPlus the project declaration
     * @return true if the metric declaration is internally consistent, false otherwise
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean isAllMetricsConfigInternallyConsistent( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        // Assume valid
        boolean returnMe = true;

        // Check all metrics config
        for ( MetricsConfig next : projectConfigPlus.getProjectConfig().getMetrics() )
        {
            // Check the sample size
            Integer minimumSampleSize = next.getMinimumSampleSize();
            if ( Objects.nonNull( minimumSampleSize ) && minimumSampleSize < 0 )
            {
                if ( LOGGER.isErrorEnabled() )
                {
                    LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                  + " The minimum sample size must be greater than zero ({}).",
                                  projectConfigPlus,
                                  next.sourceLocation()
                                      .getLineNumber(),
                                  next.sourceLocation()
                                      .getColumnNumber(),
                                  minimumSampleSize );
                }

                returnMe = false;
            }

            if ( !Validation.isOneMetricsConfigInternallyConsistent( projectConfigPlus, next ) )
            {
                returnMe = false;
            }
        }
        return returnMe;
    }

    /**
     * Checks that the metric declaration is internally consistent.
     * 
     * @param projectConfigPlus the project declaration
     * @param metrics the metrics declaration
     * @return true if the metric declaration is internally consistent, false otherwise
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean isOneMetricsConfigInternallyConsistent( ProjectConfigPlus projectConfigPlus,
                                                                   MetricsConfig metrics )
    {
        Objects.requireNonNull( metrics, NON_NULL );

        // Assume valid
        boolean returnMe = true;

        // Must define one of metric or timeSeriesMetric per metrics

        List<MetricConfig> metricConfig = metrics.getMetric();
        List<TimeSeriesMetricConfig> timeSeriesMetrics = metrics.getTimeSeriesMetric();
        if ( metricConfig.isEmpty() && timeSeriesMetrics.isEmpty() )
        {
            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + " No metrics are listed for calculation: add a regular metric or a time-series metric.",
                              projectConfigPlus,
                              metrics.sourceLocation()
                                     .getLineNumber(),
                              metrics.sourceLocation()
                                     .getColumnNumber() );
            }
            returnMe = false;
        }

        // Currently, timeSeriesMetric require single-valued forecasts
        if ( !timeSeriesMetrics.isEmpty()
             && projectConfigPlus.getProjectConfig()
                                 .getInputs()
                                 .getRight()
                                 .getType() != DatasourceType.SINGLE_VALUED_FORECASTS )
        {
            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + " Currently, time-series metrics can only be applied to single-valued forecasts.",
                              projectConfigPlus,
                              metrics.sourceLocation()
                                     .getLineNumber(),
                              metrics
                                     .sourceLocation()
                                     .getColumnNumber() );
            }
            returnMe = false;
        }

        // Cannot define specific metrics together with all valid        
        for ( MetricConfig next : metricConfig )
        {
            //Unnamed metric
            if ( MetricConfigName.ALL_VALID == next.getName() && metricConfig.size() > 1 )
            {
                if ( LOGGER.isErrorEnabled() )
                {
                    LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                  + " All valid' metrics cannot be requested alongside named metrics.",
                                  projectConfigPlus,
                                  next.sourceLocation().getLineNumber(),
                                  next.sourceLocation().getColumnNumber() );
                }
                returnMe = false;
            }
        }
        return returnMe;
    }

    /**
     * Checks that the metric declaration is internally consistent.
     * 
     * @param projectConfigPlus the project declaration
     * @return true if the metric declaration is internally consistent, false otherwise
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean isAllMetricsConfigConsistentWithOtherConfig( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        // Assume valid
        boolean returnMe = true;

        // Check all metrics config
        for ( MetricsConfig next : projectConfigPlus.getProjectConfig().getMetrics() )
        {
            if ( !Validation.isOneMetricsConfigConsistentWithOtherConfig( projectConfigPlus, next ) )
            {
                returnMe = false;
            }
        }
        return returnMe;
    }


    /**
     * Checks that the metric declaration is consistent with the other declaration.
     *
     * @param projectConfigPlus the project declaration
     * @param metrics the metrics declaration
     * @return true if the metric is consistent with other declaration
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean isOneMetricsConfigConsistentWithOtherConfig( ProjectConfigPlus projectConfigPlus,
                                                                        MetricsConfig metrics )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        // Assume valid
        AtomicBoolean result = new AtomicBoolean( true );

        ProjectConfig config = projectConfigPlus.getProjectConfig();

        // Has legacy CSV?
        boolean hasLegacyCsv = ProjectConfigs.hasLegacyCsv( config );

        // Filter for non-null and not all valid
        metrics.getMetric()
               .stream()
               .filter( next -> Objects.nonNull( next ) && next.getName() != MetricConfigName.ALL_VALID )
               .forEach( nextMetric -> {

                   try
                   {
                       MetricConstants checkMe = MetricConstantsFactory.from( nextMetric.getName() );

                       // Check that the named metric is consistent with any pooling window declaration
                       if ( checkMe != null && hasLegacyCsv
                            && ! ( checkMe.isInGroup( StatisticType.DOUBLE_SCORE )
                                   || checkMe.isInGroup( StatisticType.DURATION_SCORE ) ) )
                       {

                           // Issued dates pooling window
                           if ( projectConfigPlus.getProjectConfig().getPair().getIssuedDatesPoolingWindow() != null )
                           {
                               if ( LOGGER.isWarnEnabled() )
                               {
                                   LOGGER.warn( "In the project declaration from {}, a metric named {} was requested, "
                                                + "which is not currently supported in combination with "
                                                + "issuedDatesPoolingWindow and legacy CSV/numeric output. The "
                                                + "statistics for this metric will not be written to the legacy CSV "
                                                + "format. Please consider using the CSV2 format instead, which "
                                                + "supports all metrics.",
                                                projectConfigPlus.getOrigin(),
                                                nextMetric.getName(),
                                                nextMetric.getName() );
                               }
                           }

                           // Valid dates pooling window
                           if ( projectConfigPlus.getProjectConfig().getPair().getValidDatesPoolingWindow() != null )
                           {
                               if ( LOGGER.isWarnEnabled() )
                               {
                                   LOGGER.warn( "In the project declaration from {}, a metric named {} was requested, "
                                                + "which is not currently supported in combination with "
                                                + "validDatesPoolingWindow and legacy CSV/numeric output. The "
                                                + "statistics for this metric will not be written to the legacy CSV "
                                                + "format. Please consider using the CSV2 format instead, which "
                                                + "supports all metrics.",
                                                projectConfigPlus.getOrigin(),
                                                nextMetric.getName(),
                                                nextMetric.getName() );
                               }
                           }
                       }

                       // Check that the CRPS has an explicit baseline
                       if ( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE.equals( checkMe )
                            && config.getInputs().getBaseline() == null )
                       {
                           result.set( false );

                           if ( LOGGER.isErrorEnabled() )
                           {
                               LOGGER.error( "In the project declaration from {} a metric named {} was requested, "
                                             + "which requires an explicit baseline. Remove this metric or add "
                                             + "the required baseline declaration.",
                                             projectConfigPlus.getOrigin(),
                                             nextMetric.getName() );
                           }
                       }
                   }
                   // Handle the situation where a metric is recognized by the xsd but not by the ConfigMapper. This is
                   // unlikely and implies an incomplete implementation of a metric by the system  
                   catch ( MetricConfigException e )
                   {
                       if ( LOGGER.isErrorEnabled() )
                       {
                           LOGGER.error( "In the project declaration from {}, a metric named {} was requested, "
                                         + "but is not recognized.",
                                         projectConfigPlus.getOrigin(),
                                         nextMetric.getName() );
                       }
                       result.set( false );
                   }
               } );

        // Check that each metric has the required thresholds to obtain the input data
        if ( !doesEachMetricHaveExpectedThresholds( projectConfigPlus, metrics ) )
        {
            result.set( false );
        }

        // Check that each metric is consistent with the declared data type
        if ( !isEachMetricConsistentWithTheDataTypeDeclared( projectConfigPlus, metrics ) )
        {
            result.set( false );
        }

        return result.get();
    }

    /**
     * Validates that each metric in the input group has the thresholds it expects within the same group. This 
     * validation remains applicable while the software ingests time-series for continuous variables. If this 
     * assumption changes to additionally support categorical variables, then this validation is no longer applicable.
     * Also relies on the {@link DataSourceConfig#getType()} for the right data, which is brittle. See #65422.
     * 
     * @param projectConfigPlus the augmented declaration
     * @param metricsConfig the metrics declaration
     * @return true if the validation passes, false otherwise
     */

    private static boolean doesEachMetricHaveExpectedThresholds( ProjectConfigPlus projectConfigPlus,
                                                                 MetricsConfig metricsConfig )
    {
        boolean isValid = true;

        // Obtain the metrics as internal types, which self-identify, ignoring ALL_VALID which, by definition, selects
        // valid metrics according to the other declaration.
        Set<MetricConstants> metrics = metricsConfig.getMetric()
                                                    .stream()
                                                    .filter( next -> next.getName() != MetricConfigName.ALL_VALID )
                                                    .map( next -> MetricConstantsFactory.from( next.getName() ) )
                                                    .collect( Collectors.toSet() );

        // Filter categorical metrics
        Set<MetricConstants> categorical = metrics.stream()
                                                  .filter( next -> next.isInGroup( SampleDataGroup.DICHOTOMOUS )
                                                                   || next.isInGroup( SampleDataGroup.MULTICATEGORY ) )
                                                  .collect( Collectors.toUnmodifiableSet() );

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        // By default, a missing type is interpreted as probability
        boolean eventThresholds = metricsConfig.getThresholds()
                                               .stream()
                                               .anyMatch( next -> Objects.isNull( next.getType() )
                                                                  || next.getType() == ThresholdType.VALUE
                                                                  || next.getType() == ThresholdType.PROBABILITY );

        // Has decision thresholds?
        boolean decisionThresholds = metricsConfig.getThresholds()
                                                  .stream()
                                                  .anyMatch( next -> next.getType() == ThresholdType.PROBABILITY_CLASSIFIER );

        DatasourceType declaredRightDataType = projectConfig.getInputs()
                                                            .getRight()
                                                            .getType();

        // Not ensemble forecasts
        if ( decisionThresholds && declaredRightDataType != DatasourceType.ENSEMBLE_FORECASTS )
        {
            isValid = false;

            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + " The declaration contains decision thresholds, which are only valid for ensemble or "
                              + "probability forecasts and no such forecasts were declared. Please remove the "
                              + "thresholds of type 'probability classifier' from this evaluation.",
                              projectConfigPlus.getOrigin(),
                              metricsConfig.sourceLocation().getLineNumber(),
                              metricsConfig.sourceLocation().getColumnNumber() );
            }
        }

        // Input type declaration is ensemble
        if ( !categorical.isEmpty()
             && declaredRightDataType == DatasourceType.ENSEMBLE_FORECASTS )
        {
            // Order by number of failures - up to two possible
            if ( !eventThresholds && !decisionThresholds )
            {
                isValid = false;

                if ( LOGGER.isErrorEnabled() )
                {
                    LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                  + " The categorical metrics {} require two types of thresholds to obtain "
                                  + "categorical pairs from the ensemble pairs. First, event thresholds "
                                  + "are required to obtain probability pairs from the ensemble members. Second, "
                                  + "decision thresholds or 'probability classifiers' are required to obtain "
                                  + "categorical pairs from the probability pairs. Neither of these thresholds were "
                                  + "supplied. Add some thresholds of type {} or {} to the metrics declaration and, "
                                  + "separately, add some thresholds of type {}. Otherwise, remove these metrics.",
                                  projectConfigPlus.getOrigin(),
                                  metricsConfig.sourceLocation().getLineNumber(),
                                  metricsConfig.sourceLocation().getColumnNumber(),
                                  categorical,
                                  ThresholdType.VALUE.name(),
                                  ThresholdType.PROBABILITY.name(),
                                  ThresholdType.PROBABILITY_CLASSIFIER.name() );
                }
            }
            else if ( !eventThresholds )
            {
                isValid = false;

                if ( LOGGER.isErrorEnabled() )
                {
                    LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                  + " The categorical metrics {} require event thresholds to obtain categorical "
                                  + "pairs from the continuous numerical pairs, but no event thresholds were "
                                  + "supplied. Add some thresholds of type {} or {} to the metrics declaration or "
                                  + "remove these metrics before proceeding.",
                                  projectConfigPlus.getOrigin(),
                                  metricsConfig.sourceLocation().getLineNumber(),
                                  metricsConfig.sourceLocation().getColumnNumber(),
                                  categorical,
                                  ThresholdType.VALUE.name(),
                                  ThresholdType.PROBABILITY.name() );
                }
            }
        }
        // Other (single-valued) types
        else if ( !categorical.isEmpty() && !eventThresholds )
        {
            isValid = false;

            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + " The categorical metrics {} require event thresholds to obtain categorical pairs "
                              + "from the continuous numerical pairs, but no event thresholds were supplied. Add "
                              + "some thresholds of type {} or {} to the metrics declaration or remove these metrics "
                              + "before proceeding.",
                              projectConfigPlus.getOrigin(),
                              metricsConfig.sourceLocation().getLineNumber(),
                              metricsConfig.sourceLocation().getColumnNumber(),
                              categorical,
                              ThresholdType.VALUE,
                              ThresholdType.PROBABILITY );
            }
        }

        return isValid;
    }

    /**
     * Validates that each metric in the input group is consistent with the {@link DataSourceConfig#getType()} for 
     * the right data. For example, cannot declare metrics for ensemble data unless the data type reflects that. 
     * Assumes that all ingested time-series are continuous numerical variables. See #65422.
     * 
     * @param projectConfigPlus the augmented declaration
     * @param metricsConfig the metrics declaration
     * @return true if the validation passes, false otherwise
     */

    private static boolean isEachMetricConsistentWithTheDataTypeDeclared( ProjectConfigPlus projectConfigPlus,
                                                                          MetricsConfig metricsConfig )
    {
        boolean isValid = true;

        // Obtain the metrics as internal types, which self-identify, ignoring ALL_VALID which, by definition, selects
        // valid metrics according to the other declaration.
        Set<MetricConstants> metrics = metricsConfig.getMetric()
                                                    .stream()
                                                    .filter( next -> next.getName() != MetricConfigName.ALL_VALID )
                                                    .map( next -> MetricConstantsFactory.from( next.getName() ) )
                                                    .collect( Collectors.toSet() );


        Set<MetricConstants> ensemble = metrics.stream()
                                               .filter( next -> next.isInGroup( SampleDataGroup.ENSEMBLE )
                                                                && next != MetricConstants.SAMPLE_SIZE )
                                               .collect( Collectors.toUnmodifiableSet() );

        Set<MetricConstants> discreteProbability = metrics.stream()
                                                          .filter( next -> next.isInGroup( SampleDataGroup.DISCRETE_PROBABILITY ) )
                                                          .collect( Collectors.toUnmodifiableSet() );

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        DatasourceType type = projectConfig.getInputs().getRight().getType();

        // Ensemble metrics, but not ensemble type data
        if ( !ensemble.isEmpty() && type != DatasourceType.ENSEMBLE_FORECASTS )
        {
            isValid = false;

            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + " The ensemble metrics {} require ensemble pairs, but the declared data type for the RIGHT "
                              + "data source is '{}'. Either correct this data type to 'ensemble forecasts' or remove the "
                              + "ensemble metrics before proceeding.",
                              projectConfigPlus.getOrigin(),
                              projectConfig.getInputs().getRight().sourceLocation().getLineNumber(),
                              projectConfig.getInputs().getRight().sourceLocation().getColumnNumber(),
                              ensemble,
                              type.name().toLowerCase() );
            }
        }

        if ( !discreteProbability.isEmpty() && type != DatasourceType.ENSEMBLE_FORECASTS )
        {
            isValid = false;

            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + " The discrete probability metrics {} require ensemble pairs, but the declared data type for "
                              + "the RIGHT data source is '{}'. Either correct this data type to 'ensemble forecasts' or "
                              + "remove the discrete probability metrics before proceeding.",
                              projectConfigPlus.getOrigin(),
                              projectConfig.getInputs().getRight().sourceLocation().getLineNumber(),
                              projectConfig.getInputs().getRight().sourceLocation().getColumnNumber(),
                              discreteProbability,
                              type.name().toLowerCase() );
            }
        }

        return isValid;
    }

    /**
     * Validates the paths to external thresholds.
     *
     * @param projectConfigPlus the project declaration
     * @return true if all have readable files, false otherwise
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean areAllPathsToThresholdsReadable( SystemSettings systemSettings,
                                                            ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        boolean result = true;

        final String pleaseUpdate = "Please update the project declaration with a readable source of external "
                                    + "thresholds.";

        // Iterate through the metric group
        for ( MetricsConfig nextMetric : projectConfigPlus.getProjectConfig().getMetrics() )
        {

            // Iterate through the thresholds within each group
            for ( ThresholdsConfig nextThreshold : nextMetric.getThresholds() )
            {

                Object nextSource = nextThreshold.getCommaSeparatedValuesOrSource();

                // Locate a threshold with an external source
                if ( nextSource instanceof ThresholdsConfig.Source )
                {
                    URI thresholdData = ( (ThresholdsConfig.Source) nextSource ).getValue();

                    final Path destinationPath;
                    try
                    {
                        if ( !thresholdData.isAbsolute() )
                        {
                            destinationPath = systemSettings.getDataDirectory()
                                                            .resolve( thresholdData.getPath() );
                        }
                        else if ( thresholdData.getScheme().toLowerCase().startsWith( "http" ) )
                        {
                            // Further checks are not really reasonable since it is entirely likely that we
                            // could get 404s when we hit a correct server because the URL passed in won't be the
                            // complete request. The best we can do is see if it can actually be used as a URL
                            URL possibleURL = thresholdData.toURL();
                            LOGGER.debug(
                                          "The remote thresholds at {} can presumably be accessed since {} is a valid "
                                          + "url.",
                                          thresholdData,
                                          possibleURL );
                            continue;
                        }
                        else
                        {
                            destinationPath = Paths.get( thresholdData );
                        }
                    }
                    catch ( MalformedURLException exception )
                    {
                        LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE +
                                      "The URL '{}' is not a proper address and therefore cannot be used to access a "
                                      +
                                      "remote threshold dataset. {}",
                                      projectConfigPlus.getOrigin(),
                                      nextThreshold.sourceLocation().getLineNumber(),
                                      nextThreshold.sourceLocation().getColumnNumber(),
                                      thresholdData,
                                      pleaseUpdate );

                        result = false;
                        continue;
                    }
                    catch ( InvalidPathException | FileSystemNotFoundException | SecurityException e )
                    {
                        LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                      + " The path {} could not be used. "
                                      + " {}",
                                      projectConfigPlus.getOrigin(),
                                      nextThreshold.sourceLocation().getLineNumber(),
                                      nextThreshold.sourceLocation().getColumnNumber(),
                                      thresholdData,
                                      pleaseUpdate );

                        result = false;
                        continue;
                    }

                    File destinationFile = destinationPath.toFile();

                    if ( !destinationFile.canRead() || destinationFile.isDirectory() )
                    {
                        LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                      + " The path {} is not a readable file."
                                      + " {}",
                                      projectConfigPlus.getOrigin(),
                                      nextThreshold.sourceLocation().getLineNumber(),
                                      nextThreshold.sourceLocation().getColumnNumber(),
                                      thresholdData,
                                      pleaseUpdate );

                        result = false;
                    }
                }
            }
        }

        return result;
    }

    private static boolean isPairConfigValid( ProjectConfigPlus projectConfigPlus )
    {
        boolean result = true;

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        PairConfig pairConfig = projectConfig.getPair();

        result = Validation.areUnitAliasDeclarationsValid( projectConfigPlus,
                                                           pairConfig );

        result = Validation.areFeaturesValidInSingletonContext( pairConfig.getFeature() ) && result;

        result = Validation.areFeatureGroupsValid( projectConfigPlus, pairConfig ) && result;

        result = Validation.areDatesValid( projectConfigPlus, pairConfig.getDates() ) && result;

        result = Validation.areDatesValid( projectConfigPlus, pairConfig.getIssuedDates() ) && result;

        result = Validation.areLeadTimesValid( projectConfigPlus, pairConfig.getLeadHours() ) && result;

        result = Validation.isSeasonValid( projectConfigPlus, pairConfig ) && result;

        result = Validation.isDesiredTimeScaleValid( projectConfigPlus, pairConfig ) && result;

        result = Validation.areTimeWindowsValid( projectConfigPlus, pairConfig ) && result;

        return Validation.isGridSelectionValid( projectConfigPlus, pairConfig ) && result;
    }

    private static boolean areUnitAliasDeclarationsValid( ProjectConfigPlus projectConfigPlus,
                                                          PairConfig pairConfig )
    {
        Map<String, String> aliases = new HashMap<>();
        boolean noDuplicates = true;

        for ( UnitAlias alias : pairConfig.getUnitAlias() )
        {
            String existing = aliases.put( alias.getAlias(),
                                           alias.getUnit() );

            if ( existing != null )
            {
                noDuplicates = false;

                String msg = FILE_LINE_COLUMN_BOILERPLATE + " Multiple"
                             + " declarations for a single unit alias are not "
                             + "supported. Found repeated '"
                             + alias.getAlias()
                             + "' alias. Remove all but one declaration for "
                             + "alias '"
                             + alias.getAlias()
                             + "'.";

                LOGGER.error( msg,
                              projectConfigPlus.getOrigin(),
                              alias.sourceLocation().getLineNumber(),
                              alias.sourceLocation()
                                   .getColumnNumber() );
            }
        }

        return noDuplicates;
    }

    private static boolean isGridSelectionValid( ProjectConfigPlus projectConfigPlus,
                                                 PairConfig pairConfig )
    {
        boolean valid = true;

        for ( UnnamedFeature feature : pairConfig.getGridSelection() )
        {
            if ( Objects.nonNull( feature.getCoordinate() ) )
            {
                valid = false;

                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " In the pair declaration, the grid selection contains a <coordinate> filter, which "
                             + "is not currently supported. Please replace this declaration with a <polygon> filter "
                             + AND_TRY_AGAIN;

                LOGGER.error( msg,
                              projectConfigPlus.getOrigin(),
                              pairConfig.sourceLocation().getLineNumber(),
                              pairConfig.sourceLocation()
                                        .getColumnNumber() );

                break;
            }
        }

        return valid;
    }

    /**
     * Validates features.
     * 
     * TODO: Currently, features declared in the context of a featureGroup do not require that a feature name occurs 
     * only once per feature dimension, but do require that feature tuples appear only once. When the former constraint
     * is relaxed, define a single helper for validation of features to be re-used in all contexts.
     * 
     * @param features the features to validate
     * @return true if the features are valid, false otherwise
     */

    static boolean areFeaturesValidInSingletonContext( List<NamedFeature> features )
    {
        boolean valid = Validation.doesEachFeatureHaveSomethingDeclared( features );
        List<String> leftRawNames = Validation.getFeatureNames( features,
                                                                LeftOrRightOrBaseline.LEFT );
        List<String> rightRawNames = Validation.getFeatureNames( features,
                                                                 LeftOrRightOrBaseline.RIGHT );
        List<String> baselineRawNames = getFeatureNames( features,
                                                         LeftOrRightOrBaseline.BASELINE );
        valid = Validation.validateFeatureNames( leftRawNames, LeftOrRightOrBaseline.LEFT ) && valid;
        valid = Validation.validateFeatureNames( rightRawNames, LeftOrRightOrBaseline.RIGHT ) && valid;
        return Validation.validateFeatureNames( baselineRawNames, LeftOrRightOrBaseline.BASELINE ) && valid;
    }

    /**
     * Validates features in a grouped context.
     * 
     * TODO: Currently, features declared in the context of a featureGroup do not require that a feature name occurs 
     * only once per feature dimension, but do require that feature tuples appear only once. When the former constraint
     * is relaxed, define a single helper for validation of features to be re-used in all contexts instead of two 
     * separate helpers, namely {@link #areFeaturesValidInSingletonContext(List)} and this one.
     * 
     * @param features the features to validate
     * @param context some optional context information to help understand the origin of the features (e.g., group name)
     * @param projectConfigPlus the project declaration
     * @param pairConfig the pair declaration
     * @return true if the features are valid, false otherwise
     */

    static boolean areFeaturesValidInGroupedContext( List<NamedFeature> features,
                                                     String context,
                                                     ProjectConfigPlus projectConfigPlus,
                                                     PairConfig pairConfig )
    {
        boolean valid = Validation.doesEachFeatureHaveSomethingDeclared( features );

        // Check that there are no duplicate feature tuples
        Map<NamedFeature, Long> duplicateTuplesWithCounts = features.stream()
                                                                    .collect( Collectors.groupingBy( next -> next,
                                                                                                     Collectors.counting() ) )
                                                                    .entrySet()
                                                                    .stream()
                                                                    .filter( next -> next.getValue() > 1 )
                                                                    .collect( Collectors.toMap( Map.Entry::getKey,
                                                                                                Map.Entry::getValue ) );

        String contextString = "";

        if ( Objects.nonNull( context ) )
        {
            contextString = "associated with " + context + " ";
        }

        if ( !duplicateTuplesWithCounts.isEmpty() )
        {
            valid = false;

            String msg = FILE_LINE_COLUMN_BOILERPLATE
                         + " Discovered {} feature tuples {}that were duplicated one or more times, which is not "
                         + "allowed. The duplicate feature tuples "
                         + "and their counts are {}.";

            LOGGER.error( msg,
                          projectConfigPlus.getOrigin(),
                          pairConfig.sourceLocation().getLineNumber(),
                          pairConfig.sourceLocation().getColumnNumber(),
                          duplicateTuplesWithCounts.size(),
                          contextString,
                          duplicateTuplesWithCounts );
        }

        // Check for blank names, which are not allowed
        Predicate<NamedFeature> blankPolicer =
                feature -> ( Objects.nonNull( feature.getLeft() ) && feature.getLeft().isBlank() )
                           || ( Objects.nonNull( feature.getRight() ) && feature.getRight().isBlank() )
                           || ( Objects.nonNull( feature.getBaseline() ) && feature.getBaseline().isBlank() );

        Set<NamedFeature> featuresWithBlankNames = features.stream()
                                                           .filter( blankPolicer )
                                                           .collect( Collectors.toSet() );

        if ( !featuresWithBlankNames.isEmpty() )
        {
            valid = false;

            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE + " Discovered {} feature tuples {}that contained one or more "
                          + "blank names, which is now allowed. For each blank name, omit the name instead. The following "
                          + "feature tuples had one or more blank names: {}.",
                          projectConfigPlus.getOrigin(),
                          pairConfig.sourceLocation().getLineNumber(),
                          pairConfig.sourceLocation().getColumnNumber(),
                          featuresWithBlankNames.size(),
                          contextString,
                          featuresWithBlankNames );
        }

        return valid;
    }

    /**
     * Validates any feature groups in the pair declaration.
     * 
     * @param projectConfigPlus the project declaration
     * @param pairConfig the pair declaration
     * @return true when the feature group declaration is valid, otherwise false
     */

    private static boolean areFeatureGroupsValid( ProjectConfigPlus projectConfigPlus,
                                                  PairConfig pairConfig )
    {
        boolean valid = true;

        List<FeaturePool> groups = pairConfig.getFeatureGroup();

        if ( !groups.isEmpty() )
        {
            if ( !pairConfig.getGridSelection().isEmpty() )
            {
                valid = false;

                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " Feature grouping is not supported for gridded evaluations. Please remove the "
                             + "<featureGroup> declaration and try again.";

                LOGGER.error( msg,
                              projectConfigPlus.getOrigin(),
                              pairConfig.sourceLocation().getLineNumber(),
                              pairConfig.sourceLocation().getColumnNumber() );
            }

            // Validate the group names
            valid = Validation.validateFeatureGroupNames( groups, projectConfigPlus, pairConfig ) && valid;

            // Validate the individual features
            valid = Validation.validateIndividualFeaturesFromFeatureGroups( groups,
                                                                            projectConfigPlus,
                                                                            pairConfig )
                    && valid;
        }

        return valid;
    }

    private static boolean validateFeatureGroupNames( List<FeaturePool> groups,
                                                      ProjectConfigPlus projectConfigPlus,
                                                      PairConfig pairConfig )
    {
        boolean valid = true;

        // Non-unique group names?
        Set<String> duplicates = groups.stream()
                                       // Remove groups without a declared name as the software will choose one
                                       .filter( next -> Objects.nonNull( next.getName() ) )
                                       // Group by name and count instances
                                       .collect( Collectors.groupingBy( FeaturePool::getName,
                                                                        Collectors.counting() ) )
                                       .entrySet()
                                       .stream()
                                       // Find duplicates
                                       .filter( next -> next.getValue() > 1 )
                                       .map( Map.Entry::getKey )
                                       .collect( Collectors.toSet() );

        if ( !duplicates.isEmpty() )
        {
            valid = false;

            String msg = FILE_LINE_COLUMN_BOILERPLATE
                         + " Each feature group or <featureGroup> must contain a unique name. The following "
                         + "feature groups contained duplicate names {}. Please de-duplicate these feature group "
                         + "names and try again.";

            LOGGER.error( msg,
                          projectConfigPlus.getOrigin(),
                          pairConfig.sourceLocation().getLineNumber(),
                          pairConfig.sourceLocation().getColumnNumber(),
                          duplicates );
        }

        // Group names that are too long?
        Set<String> tooLongNames = groups.stream()
                                         // Remove groups without a declared name as the software will choose one
                                         .filter( next -> Objects.nonNull( next.getName() )
                                                          && next.getName()
                                                                 .length() > wres.datamodel.space.FeatureGroup.MAXIMUM_NAME_LENGTH )
                                         .map( FeaturePool::getName )
                                         .collect( Collectors.toSet() );

        if ( !tooLongNames.isEmpty() )
        {
            valid = false;

            String msg = FILE_LINE_COLUMN_BOILERPLATE
                         + " Feature group names cannot exceed {} characters. Discovered {} feature groups whose names "
                         + "exceeded this maximum length. Please rename these groups using a shorter name. The "
                         + "offending feature group names are {}.";

            LOGGER.error( msg,
                          projectConfigPlus.getOrigin(),
                          pairConfig.sourceLocation().getLineNumber(),
                          pairConfig.sourceLocation().getColumnNumber(),
                          wres.datamodel.space.FeatureGroup.MAXIMUM_NAME_LENGTH,
                          tooLongNames.size(),
                          tooLongNames );
        }

        return valid;
    }


    /**
     * Validates individual features that are supplied in a grouped context.
     * 
     * TODO: Currently, features declared in the context of a featureGroup do not require that a feature name occurs 
     * only once per feature dimension, but do require that feature tuples appear only once. When the former constraint
     * is relaxed, define a single helper for validation of features to be re-used in all contexts. The new helper will 
     * be called by this method, not {@link #areFeaturesValidInGroupedContext(List, String, ProjectConfigPlus, PairConfig)}.
     * 
     * @param groups the groups to evaluate
     * @param projectConfigPlus the project declaration
     * @param pairConfig the pair declaration
     * @return true if the features are valid in all groups, otherwise false
     */

    private static boolean validateIndividualFeaturesFromFeatureGroups( List<FeaturePool> groups,
                                                                        ProjectConfigPlus projectConfigPlus,
                                                                        PairConfig pairConfig )
    {
        boolean valid = true;

        for ( FeaturePool group : groups )
        {
            valid = Validation.areFeaturesValidInGroupedContext( group.getFeature(),
                                                                 group.getName(),
                                                                 projectConfigPlus,
                                                                 pairConfig )
                    && valid;
        }

        return valid;
    }

    /**
     * Each attribute is individually optional in feature, but at least one must
     * be present.
     * @param features The features to check.
     * @return True when every feature has at least one of the attributes.
     */

    private static boolean doesEachFeatureHaveSomethingDeclared( List<NamedFeature> features )
    {
        int countOfEmptyFeatures = 0;

        for ( NamedFeature feature : features )
        {
            if ( Objects.isNull( feature.getLeft() )
                 && Objects.isNull( feature.getRight() )
                 && Objects.isNull( feature.getBaseline() ) )
            {
                countOfEmptyFeatures++;
            }
        }

        if ( countOfEmptyFeatures > 0 )
        {
            LOGGER.error( "Found {} features with no left nor right nor baseline name declared.",
                          countOfEmptyFeatures );
            return false;
        }

        return true;
    }

    /**
     * Get the raw list of features on left or right or baseline, including null
     * and including blank, in order of the original features list given.
     * @param features The declared list of features.
     * @param leftOrRightOrBaseline Which declaration to get.
     * @return A list of features including null and blank.
     */

    private static List<String> getFeatureNames( List<NamedFeature> features,
                                                 LeftOrRightOrBaseline leftOrRightOrBaseline )

    {
        List<String> allNames = new ArrayList<>( features.size() );

        if ( leftOrRightOrBaseline.equals( LeftOrRightOrBaseline.LEFT ) )
        {
            for ( NamedFeature feature : features )
            {
                allNames.add( feature.getLeft() );
            }
        }
        else if ( leftOrRightOrBaseline.equals( LeftOrRightOrBaseline.RIGHT ) )
        {
            for ( NamedFeature feature : features )
            {
                allNames.add( feature.getRight() );
            }
        }
        else if ( leftOrRightOrBaseline.equals( LeftOrRightOrBaseline.BASELINE ) )
        {
            for ( NamedFeature feature : features )
            {
                allNames.add( feature.getBaseline() );
            }
        }

        return Collections.unmodifiableList( allNames );
    }

    private static boolean validateFeatureNames( List<String> names,
                                                 LeftOrRightOrBaseline leftOrRightOrBaseline )
    {
        boolean isValid = true;
        int blankCount = 0;

        for ( String name : names )
        {
            if ( Objects.nonNull( name ) && name.isBlank() )
            {
                blankCount++;
            }
        }

        if ( blankCount > 0 )
        {
            isValid = false;

            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( "Found {} blank feature name(s) on {}. Instead of {}{}",
                              blankCount,
                              leftOrRightOrBaseline.value(),
                              leftOrRightOrBaseline.value(),
                              "=\"\", omit the attribute altogether." );
            }
        }

        return isValid;
    }

    private static boolean areDatesValid( ProjectConfigPlus projectConfigPlus,
                                          DateCondition dates )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        boolean result = true;

        if ( dates != null )
        {
            String earliestRaw = dates.getEarliest();
            String latestRaw = dates.getLatest();
            if ( earliestRaw != null )
            {
                result = Validation.isDateStringValid( projectConfigPlus,
                                                       dates,
                                                       earliestRaw )
                         && result;
            }

            if ( latestRaw != null )
            {
                result = Validation.isDateStringValid( projectConfigPlus,
                                                       dates,
                                                       latestRaw )
                         && result;

                // If we plan on using USGS, but we want a date later than now,
                // break; that's impossible.
                boolean usesUSGSData = Validation.usesUSGSData( projectConfigPlus.getProjectConfig() );

                Instant now = Instant.now();
                Instant latest = Instant.parse( latestRaw );

                if ( result && usesUSGSData && latest.isAfter( now ) )
                {
                    result = false;
                    if ( LOGGER.isErrorEnabled() )
                    {
                        String msg = FILE_LINE_COLUMN_BOILERPLATE
                                     + " Data from the future cannot be"
                                     + " requested from USGS; the latest date"
                                     + " specified was "
                                     + latestRaw
                                     + " but it is currently "
                                     + now;

                        LOGGER.error( msg,
                                      projectConfigPlus.getOrigin(),
                                      dates.sourceLocation().getLineNumber(),
                                      dates.sourceLocation()
                                           .getColumnNumber() );
                    }
                }
            }

        }

        return result;
    }

    private static boolean usesUSGSData( ProjectConfig projectConfig )
    {
        for ( DataSourceConfig.Source source : projectConfig.getInputs()
                                                            .getLeft()
                                                            .getSource() )
        {
            if ( Validation.usesUSGSData( source ) )
            {
                return true;
            }
        }

        for ( DataSourceConfig.Source source : projectConfig.getInputs().getRight().getSource() )
        {
            if ( Validation.usesUSGSData( source ) )
            {
                return true;
            }
        }

        if ( projectConfig.getInputs().getBaseline() != null )
        {
            for ( DataSourceConfig.Source source : projectConfig.getInputs()
                                                                .getBaseline()
                                                                .getSource() )
            {
                if ( Validation.usesUSGSData( source ) )
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static boolean usesUSGSData( DataSourceConfig.Source source )
    {
        InterfaceShortHand interfaceShortHand = source.getInterface();
        URI uri = source.getValue();

        return ( Objects.nonNull( interfaceShortHand )
                 && interfaceShortHand.equals( InterfaceShortHand.USGS_NWIS ) )
               || ( Objects.nonNull( uri )
                    && Objects.nonNull( uri.getPath() )
                    && uri.getPath()
                          .toLowerCase()
                          .contains( "/nwis/" ) );
    }

    private static boolean isDateStringValid( ProjectConfigPlus projectConfigPlus,
                                              Locatable locatable,
                                              String date )
    {
        boolean result = true;

        Objects.requireNonNull( projectConfigPlus, NON_NULL );
        Objects.requireNonNull( locatable, NON_NULL );
        Objects.requireNonNull( date, NON_NULL );

        try
        {
            Instant.parse( date );
        }
        catch ( DateTimeException dte )
        {
            if ( LOGGER.isErrorEnabled() )
            {
                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " In the pair declaration, the text '"
                             + date
                             + "' was not able to be converted to an "
                             + "Instant. Please use the ISO8601 format, the UTC"
                             + " zoneOffset, and second-precision, e.g. "
                             + "'2017-11-27 17:36:00Z'.";

                LOGGER.error( msg,
                              projectConfigPlus.getOrigin(),
                              locatable.sourceLocation().getLineNumber(),
                              locatable.sourceLocation()
                                       .getColumnNumber() );
            }

            result = false;
        }

        return result;
    }

    /**
     * Validates the lead times. The left bound must be less than or equal to the right bound.
     * 
     * @param projectConfigPlus the project declaration to help with messaging
     * @param leadTimes the lead times to validate
     * @return true if the lead times are null or one-sided or the right is greater than 
     *            the left, otherwise false
     */

    private static boolean areLeadTimesValid( ProjectConfigPlus projectConfigPlus,
                                              IntBoundsType leadTimes )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        boolean isValid = true;

        if ( Objects.nonNull( leadTimes ) && Objects.nonNull( leadTimes.getMinimum() )
             && Objects.nonNull( leadTimes.getMaximum() )
             && leadTimes.getMinimum() > leadTimes.getMaximum() )
        {
            isValid = false;

            String msg = FILE_LINE_COLUMN_BOILERPLATE
                         + " The maximum lead time '{}' cannot be greater "
                         + "than the minimum lead time '{}'.";

            LOGGER.error( msg,
                          projectConfigPlus.getOrigin(),
                          leadTimes.sourceLocation().getLineNumber(),
                          leadTimes.sourceLocation().getColumnNumber(),
                          leadTimes.getMaximum(),
                          leadTimes.getMinimum() );
        }

        return isValid;
    }

    /**
     * Returns true when seasonal verification config is valid, false otherwise
     * @param projectConfigPlus the project config
     * @param pairConfig the pair element to check
     * @return true when valid, false otherwise
     */
    private static boolean isSeasonValid( ProjectConfigPlus projectConfigPlus,
                                          PairConfig pairConfig )
    {
        boolean result = true;

        PairConfig.Season season = pairConfig.getSeason();

        if ( season != null )
        {

            MonthDay start = null;
            MonthDay end = null;

            try
            {
                start = MonthDay.of( season.getEarliestMonth(),
                                     season.getEarliestDay() );
            }
            catch ( DateTimeException dte )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + THE_MONTH_AND_DAY_COMBINATION_DOES
                              + NOT_APPEAR_TO_BE_VALID_PLEASE_USE_NUMERIC
                              + "month and numeric day, such as 4 for April "
                              + "and 20 for 20th.",
                              projectConfigPlus.getOrigin(),
                              season.sourceLocation().getLineNumber(),
                              season.sourceLocation().getColumnNumber(),
                              season.getEarliestMonth(),
                              season.getEarliestDay() );
                result = false;
            }

            try
            {
                end = MonthDay.of( season.getLatestMonth(),
                                   season.getLatestDay() );
            }
            catch ( DateTimeException dte )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + THE_MONTH_AND_DAY_COMBINATION_DOES
                              + NOT_APPEAR_TO_BE_VALID_PLEASE_USE_NUMERIC
                              + "month and numeric day, such as 8 for August"
                              + " and 30 for 30th.",
                              projectConfigPlus.getOrigin(),
                              season.sourceLocation().getLineNumber(),
                              season.sourceLocation().getColumnNumber(),
                              season.getLatestMonth(),
                              season.getLatestDay() );
                result = false;
            }

            // Is valid, but deserves a warning, 
            if ( Objects.nonNull( start ) && Objects.nonNull( end ) && start.isAfter( end ) )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + " The minimum month '{}' and day '{}' is later"
                              + " than the maximum month '{}' and day '{}': wrapping is"
                              + " allowed, but unusual.",
                              projectConfigPlus.getOrigin(),
                              season.sourceLocation().getLineNumber(),
                              season.sourceLocation().getColumnNumber(),
                              season.getEarliestMonth(),
                              season.getEarliestDay(),
                              season.getLatestMonth(),
                              season.getLatestDay() );
            }

        }

        return result;
    }

    /**
     * @param projectConfigPlus the project declaration
     * @param dataSourceConfig the data source declaration
     * @param lrb the left/right/baseline context to help with messaging
     * @return true if the {@code existingTimeScale} is valid, false otherwise
     */

    private static boolean isExistingTimeScaleValid( ProjectConfigPlus projectConfigPlus,
                                                     DataSourceConfig dataSourceConfig,
                                                     LeftOrRightOrBaseline lrb )
    {
        // Absent, so must be valid
        if ( Objects.isNull( dataSourceConfig.getExistingTimeScale() ) )
        {
            return true;
        }

        TimeScaleConfig timeScaleConfig = dataSourceConfig.getExistingTimeScale();

        boolean result = true;

        if ( Objects.isNull( timeScaleConfig.getPeriod() ) || Objects.isNull( timeScaleConfig.getUnit() ) )
        {
            result = false;

            String msg = FILE_LINE_COLUMN_BOILERPLATE
                         + " In the inputs declaration, the existing time scale of the {} sources was incorrectly "
                         + "specified. An existing time scale requires both a period and a unit.";

            LOGGER.error( msg,
                          projectConfigPlus.getOrigin(),
                          timeScaleConfig.sourceLocation().getLineNumber(),
                          timeScaleConfig.sourceLocation()
                                         .getColumnNumber(),
                          lrb );
        }

        return result;
    }

    /**
     * @param projectConfigPlus the project declaration
     * @param pairConfig the pair declaration
     * @return true if the {@code desiredTimeScale} is valid, false otherwise
     */

    private static boolean isDesiredTimeScaleValid( ProjectConfigPlus projectConfigPlus,
                                                    PairConfig pairConfig )
    {
        DesiredTimeScaleConfig timeScaleConfig = pairConfig.getDesiredTimeScale();

        // No declaration, must be valid
        if ( Objects.isNull( timeScaleConfig ) )
        {
            return true;
        }

        // Check that the expected elements are complete
        boolean valid = Validation.isDesiredTimeScaleComplete( projectConfigPlus, pairConfig );

        // Check that the period is valid
        valid = Validation.isNonNullPeriodOfDesiredTimeScaleGreaterThanZero( projectConfigPlus, pairConfig ) && valid;

        // Check that the time units are present when required
        valid = Validation.isDesiredTimeScaleUnitPresentWhenRequired( projectConfigPlus, pairConfig ) && valid;

        // Check that the frequency is valid
        valid = Validation.isNonNullFrequencyOfDesiredTimeScaleGreaterThanZero( projectConfigPlus, pairConfig )
                && valid;

        // Check that the time scale is non-instantaneous
        valid = Validation.isDesiredTimeScaleNonInstantaneous( projectConfigPlus, pairConfig ) && valid;

        // Check that the function is valid
        valid = Validation.isDesiredTimeScaleFunctionValid( projectConfigPlus, pairConfig ) && valid;

        // Check that the period is valid 
        valid = Validation.isDesiredTimeScaleConsistentWithExistingTimeScales( projectConfigPlus, pairConfig ) && valid;

        // Check that the monthdays are valid
        valid = Validation.areDesiredTimeScaleMonthDaysValid( projectConfigPlus, pairConfig ) && valid;

        // Check that the lenience declaration is valid
        valid = Validation.isLenientRescalingValid( projectConfigPlus, pairConfig ) && valid;

        return valid;
    }

    /**
     * @param projectConfigPlus the project declaration
     * @param pairConfig the pair declaration
     * @return true if the {@code desiredTimeScale} has all expected elements, false otherwise
     */

    private static boolean isDesiredTimeScaleComplete( ProjectConfigPlus projectConfigPlus,
                                                       PairConfig pairConfig )
    {
        DesiredTimeScaleConfig timeScaleConfig = pairConfig.getDesiredTimeScale();

        if ( Objects.isNull( timeScaleConfig ) )
        {
            return true;
        }

        boolean result = true;

        boolean earliestMonthPresent = Objects.nonNull( timeScaleConfig.getEarliestMonth() );
        boolean earliestDayPresent = Objects.nonNull( timeScaleConfig.getEarliestDay() );
        boolean latestMonthPresent = Objects.nonNull( timeScaleConfig.getLatestMonth() );
        boolean latestDayPresent = Objects.nonNull( timeScaleConfig.getLatestDay() );

        // Both elements of a monthday must have the same state
        if ( earliestMonthPresent != earliestDayPresent )
        {
            result = false;

            if ( LOGGER.isErrorEnabled() )
            {
                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " In the pair declaration, the desired time scale was incorrectly specified. When "
                             + "declaring an earliest monthday, both the month and the day must be present.";

                LOGGER.error( msg,
                              projectConfigPlus.getOrigin(),
                              timeScaleConfig.sourceLocation().getLineNumber(),
                              timeScaleConfig.sourceLocation()
                                             .getColumnNumber() );
            }
        }

        if ( latestMonthPresent != latestDayPresent )
        {
            result = false;

            if ( LOGGER.isErrorEnabled() )
            {
                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " In the pair declaration, the desired time scale was incorrectly specified. When "
                             + "declaring a latest monthday, both the month and the day must be present.";

                LOGGER.error( msg,
                              projectConfigPlus.getOrigin(),
                              timeScaleConfig.sourceLocation().getLineNumber(),
                              timeScaleConfig.sourceLocation()
                                             .getColumnNumber() );
            }
        }

        // Period required, unless both monthdays are present
        if ( Objects.isNull( timeScaleConfig.getPeriod() )
             && ( !earliestMonthPresent || !earliestDayPresent || !latestMonthPresent || !latestDayPresent ) )
        {
            result = false;

            if ( LOGGER.isErrorEnabled() )
            {
                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " In the pair declaration, the desired time scale was incomplete. Either declare a "
                             + "period and associated unit or declare an earliest and latest monthday.";

                LOGGER.error( msg,
                              projectConfigPlus.getOrigin(),
                              timeScaleConfig.sourceLocation().getLineNumber(),
                              timeScaleConfig.sourceLocation()
                                             .getColumnNumber() );
            }
        }

        // Period cannot be present when both monthdays are present
        if ( Objects.nonNull( timeScaleConfig.getPeriod() ) && earliestMonthPresent
             && earliestDayPresent
             && latestMonthPresent
             && latestDayPresent )
        {
            result = false;

            if ( LOGGER.isErrorEnabled() )
            {
                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " In the pair declaration, the desired time scale was incorrectly specified. Either "
                             + "include a period and associated unit or declare an earliest and latest monthday, but "
                             + "do not declare both.";

                LOGGER.error( msg,
                              projectConfigPlus.getOrigin(),
                              timeScaleConfig.sourceLocation().getLineNumber(),
                              timeScaleConfig.sourceLocation()
                                             .getColumnNumber() );
            }
        }

        return result;
    }

    /**
     * @param projectConfigPlus the project declaration
     * @param pairConfig the pair declaration
     * @return true if the {@code desiredTimeScale} is valid, false otherwise
     */

    private static boolean isDesiredTimeScaleNonInstantaneous( ProjectConfigPlus projectConfigPlus,
                                                               PairConfig pairConfig )
    {
        DesiredTimeScaleConfig timeScaleConfig = pairConfig.getDesiredTimeScale();

        if ( Objects.isNull( timeScaleConfig ) )
        {
            return true;
        }

        boolean result = true;

        String msg = FILE_LINE_COLUMN_BOILERPLATE
                     + " In the pair declaration, the desired time scale is prescriptive so it cannot be an "
                     + "'instant', it needs to be larger, such as 'one hour'.";

        // Check for an explicit period
        if ( Objects.nonNull( timeScaleConfig.getPeriod() ) && Objects.nonNull( timeScaleConfig.getUnit() ) )
        {
            Duration period = ProjectConfigs.getDurationFromTimeScale( timeScaleConfig );

            if ( TimeScaleOuter.INSTANTANEOUS_DURATION.compareTo( period ) >= 0 )
            {
                result = false;

                if ( LOGGER.isErrorEnabled() )
                {
                    LOGGER.error( msg,
                                  projectConfigPlus.getOrigin(),
                                  timeScaleConfig.sourceLocation().getLineNumber(),
                                  timeScaleConfig.sourceLocation()
                                                 .getColumnNumber() );
                }
            }
        }

        boolean earliestMonthPresent = Objects.nonNull( timeScaleConfig.getEarliestMonth() );
        boolean earliestDayPresent = Objects.nonNull( timeScaleConfig.getEarliestDay() );
        boolean latestMonthPresent = Objects.nonNull( timeScaleConfig.getLatestMonth() );
        boolean latestDayPresent = Objects.nonNull( timeScaleConfig.getLatestDay() );

        if ( earliestMonthPresent && latestMonthPresent
             && earliestDayPresent
             && latestDayPresent
             && Objects.equals( timeScaleConfig.getEarliestMonth(), timeScaleConfig.getLatestMonth() )
             && Objects.equals( timeScaleConfig.getEarliestDay(), timeScaleConfig.getLatestDay() ) )
        {
            result = false;

            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( msg,
                              projectConfigPlus.getOrigin(),
                              timeScaleConfig.sourceLocation().getLineNumber(),
                              timeScaleConfig.sourceLocation()
                                             .getColumnNumber() );
            }
        }

        return result;
    }

    /**
     * @param projectConfigPlus the project declaration
     * @param pairConfig the pair declaration
     * @return false if the {@code desiredTimeScale} has a {@code frequency} that is <=0, true otherwise
     */

    private static boolean isNonNullFrequencyOfDesiredTimeScaleGreaterThanZero( ProjectConfigPlus projectConfigPlus,
                                                                                PairConfig pairConfig )
    {
        DesiredTimeScaleConfig timeScaleConfig = pairConfig.getDesiredTimeScale();

        if ( Objects.isNull( timeScaleConfig ) )
        {
            return true;
        }

        boolean result = true;

        // Non-null frequency must be >= 1
        if ( timeScaleConfig.getFrequency() != null && timeScaleConfig.getFrequency() < 1 )
        {
            result = false;

            if ( LOGGER.isErrorEnabled() )
            {
                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " In the pair declaration, the frequency associated with the desired time scale must be "
                             + "empty or greater than zero.";

                LOGGER.error( msg,
                              projectConfigPlus.getOrigin(),
                              timeScaleConfig.sourceLocation().getLineNumber(),
                              timeScaleConfig.sourceLocation()
                                             .getColumnNumber() );
            }
        }

        return result;
    }

    /**
     * @param projectConfigPlus the project declaration
     * @param pairConfig the pair declaration
     * @return false if the {@code desiredTimeScale} has a {@code period} that is <=0, true otherwise
     */

    private static boolean isNonNullPeriodOfDesiredTimeScaleGreaterThanZero( ProjectConfigPlus projectConfigPlus,
                                                                             PairConfig pairConfig )
    {
        DesiredTimeScaleConfig timeScaleConfig = pairConfig.getDesiredTimeScale();

        if ( Objects.isNull( timeScaleConfig ) )
        {
            return true;
        }

        boolean result = true;

        // Non-null period must be >= 1
        if ( timeScaleConfig.getPeriod() != null && timeScaleConfig.getPeriod() < 1 )
        {
            result = false;

            if ( LOGGER.isErrorEnabled() )
            {
                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " In the pair declaration, the period associated with the desired time scale must be "
                             + "empty or greater than zero.";

                LOGGER.error( msg,
                              projectConfigPlus.getOrigin(),
                              timeScaleConfig.sourceLocation().getLineNumber(),
                              timeScaleConfig.sourceLocation()
                                             .getColumnNumber() );
            }
        }

        return result;
    }

    /**
     * @param projectConfigPlus the project declaration
     * @param pairConfig the pair declaration
     * @return true if the {@code desiredTimeScale} has a {@code unit} present when required, false otherwise
     */

    private static boolean isDesiredTimeScaleUnitPresentWhenRequired( ProjectConfigPlus projectConfigPlus,
                                                                      PairConfig pairConfig )
    {
        DesiredTimeScaleConfig timeScaleConfig = pairConfig.getDesiredTimeScale();

        if ( Objects.isNull( timeScaleConfig ) )
        {
            return true;
        }

        boolean result = true;

        // Units expected
        if ( Objects.isNull( timeScaleConfig.getUnit() ) && ( Objects.nonNull( timeScaleConfig.getPeriod() )
                                                              || Objects.nonNull( timeScaleConfig.getFrequency() ) ) )
        {
            result = false;

            if ( LOGGER.isErrorEnabled() )
            {
                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " In the pair declaration, the units associated with the desired time scale must be "
                             + "declared when either the period or frequency is declared, since they are prescribed in "
                             + "time units.";

                LOGGER.error( msg,
                              projectConfigPlus.getOrigin(),
                              timeScaleConfig.sourceLocation().getLineNumber(),
                              timeScaleConfig.sourceLocation()
                                             .getColumnNumber() );
            }
        }

        return result;
    }

    /**
     * @param projectConfigPlus the project declaration
     * @param pairConfig the pair declaration
     * @return true if the {@code desiredTimeScale} has earliest and latest monthdays that are valid, false otherwise
     */

    private static boolean areDesiredTimeScaleMonthDaysValid( ProjectConfigPlus projectConfigPlus,
                                                              PairConfig pairConfig )
    {
        DesiredTimeScaleConfig timeScaleConfig = pairConfig.getDesiredTimeScale();

        if ( Objects.isNull( timeScaleConfig ) )
        {
            return true;
        }

        boolean result = true;

        boolean earliestMonthPresent = Objects.nonNull( timeScaleConfig.getEarliestMonth() );
        boolean earliestDayPresent = Objects.nonNull( timeScaleConfig.getEarliestDay() );
        boolean latestMonthPresent = Objects.nonNull( timeScaleConfig.getLatestMonth() );
        boolean latestDayPresent = Objects.nonNull( timeScaleConfig.getLatestDay() );

        // Check the earliest monthday
        // Another validation asserts pairs whose values are both present or absent
        MonthDay earliestMonthDay = null;
        if ( earliestMonthPresent && earliestDayPresent )
        {
            try
            {
                earliestMonthDay = MonthDay.of( timeScaleConfig.getEarliestMonth(), timeScaleConfig.getEarliestDay() );

                LOGGER.debug( "Discovered an earliest monthday of {}.", earliestMonthDay );
            }
            catch ( DateTimeException e )
            {
                result = false;

                if ( LOGGER.isErrorEnabled() )
                {
                    String msg = FILE_LINE_COLUMN_BOILERPLATE
                                 + " In the pair declaration, discovered a desired time scale with an invalid earliest "
                                 + "month and day: {}.";

                    LOGGER.error( msg,
                                  projectConfigPlus.getOrigin(),
                                  timeScaleConfig.sourceLocation().getLineNumber(),
                                  timeScaleConfig.sourceLocation()
                                                 .getColumnNumber(),
                                  e.getMessage() );
                }
            }
        }

        // Check the latest monthday
        MonthDay latestMonthDay = null;
        if ( latestMonthPresent && latestDayPresent )
        {
            try
            {
                latestMonthDay = MonthDay.of( timeScaleConfig.getLatestMonth(), timeScaleConfig.getLatestDay() );

                LOGGER.debug( "Discovered a latest monthday of {}.", latestMonthDay );
            }
            catch ( DateTimeException e )
            {
                result = false;

                if ( LOGGER.isErrorEnabled() )
                {
                    String msg = FILE_LINE_COLUMN_BOILERPLATE
                                 + " In the pair declaration, discovered a desired time scale with an invalid latest "
                                 + "month and day: {}.";

                    LOGGER.error( msg,
                                  projectConfigPlus.getOrigin(),
                                  timeScaleConfig.sourceLocation().getLineNumber(),
                                  timeScaleConfig.sourceLocation()
                                                 .getColumnNumber(),
                                  e.getMessage() );
                }
            }
        }

        return result;
    }

    /**
     * Determines whether the settings for lenient rescaling are valid.
     * 
     * @param projectConfigPlus the wrapped project declaration
     * @param pairConfig the pair declaration
     * @return true if the declaration is valid, otherwise false
     */

    private static boolean isLenientRescalingValid( ProjectConfigPlus projectConfigPlus,
                                                    PairConfig pairConfig )
    {
        boolean isValid = true;

        DesiredTimeScaleConfig desiredTimeScale = pairConfig.getDesiredTimeScale();

        if ( LOGGER.isWarnEnabled() && Objects.nonNull( desiredTimeScale )
             && desiredTimeScale.getLenient() != LenienceType.NONE
             && desiredTimeScale.getLenient() != LenienceType.FALSE )
        {
            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                         + " Discovered a rescaling leniency of {}. Care should be exercised when performing lenient "
                         + "rescaling, as it implies that a rescaled value will be computed even when a majority of "
                         + "data is missing. Care is especially needed when performing lenient rescaling of model "
                         + "predictions because the missing data is unlikely to be missing at random.",
                         projectConfigPlus.getOrigin(),
                         pairConfig.sourceLocation().getLineNumber(),
                         pairConfig.sourceLocation().getColumnNumber(),
                         desiredTimeScale.getLenient() );
        }

        return isValid;
    }

    /**
     * Returns true if the time aggregation function associated with the desiredTimeScale is valid given the time
     * aggregation functions associated with the existingTimeScale for each source.
     * 
     * See Redmine issue 40389.
     * 
     * Not all attributes of a valid aggregation can be checked from the declaration alone, but some attributes, 
     * notably whether the aggregation function is applicable, can be checked in advance. Having a valid time 
     * aggregation function does not imply that the system actually supports it.
     * 
     * @param projectConfigPlus the project declaration
     * @param pairConfig the pair declaration
     * @return true if the time aggregation function associated with the desiredTimeScaleis valid
     */

    private static boolean isDesiredTimeScaleFunctionValid( ProjectConfigPlus projectConfigPlus,
                                                            PairConfig pairConfig )
    {
        boolean returnMe = true;

        TimeScaleFunction desired = pairConfig.getDesiredTimeScale()
                                              .getFunction();
        Inputs inputConfig = projectConfigPlus.getProjectConfig()
                                              .getInputs();

        // Time aggregation is a sum
        if ( TimeScaleFunction.TOTAL.equals( desired ) )
        {
            returnMe = isDesiredTimeScaleSumValid( projectConfigPlus, inputConfig );
        }

        return returnMe;
    }

    /**
     * Tests a desired time aggregation function that is a sum. Returns true if function is valid, given the input 
     * declaration, false otherwise.
     * 
     * @param projectConfigPlus the project declaration
     * @param inputConfig the input declaration
     * @return true if the time aggregation function is valid, given the inputConfig
     */

    private static boolean isDesiredTimeScaleSumValid( ProjectConfigPlus projectConfigPlus,
                                                       Inputs inputConfig )
    {
        boolean returnMe = true;
        // Sum for left must be valid
        if ( inputConfig.getLeft() != null && inputConfig.getLeft().getExistingTimeScale() != null )
        {
            returnMe = isDesiredTimeScaleSumValid( projectConfigPlus,
                                                   inputConfig.getLeft().getExistingTimeScale(),
                                                   "left" );
        }
        // Sum for right must be valid
        if ( inputConfig.getRight() != null && inputConfig.getRight().getExistingTimeScale() != null )
        {
            returnMe = isDesiredTimeScaleSumValid( projectConfigPlus,
                                                   inputConfig.getRight().getExistingTimeScale(),
                                                   "right" )
                       && returnMe;
        }
        // Sum for baseline must be valid
        if ( inputConfig.getBaseline() != null && inputConfig.getBaseline().getExistingTimeScale() != null )
        {
            returnMe = isDesiredTimeScaleSumValid( projectConfigPlus,
                                                   inputConfig.getBaseline().getExistingTimeScale(),
                                                   "baseline" )
                       && returnMe;
        }
        return returnMe;
    }

    /**
     * Tests a desired time aggregation function that is a sum. Returns true if function is valid, given the 
     * declaration for a specific input, false otherwise.
     * 
     * @param projectConfigPlus the project declaration
     * @param inputConfig the input declaration
     * @param helper a helper string for context
     * @return true if the time aggregation function is valid, given the inputConfig
     */

    private static boolean isDesiredTimeScaleSumValid( ProjectConfigPlus projectConfigPlus,
                                                       TimeScaleConfig inputConfig,
                                                       String helper )
    {
        boolean returnMe = true;

        TimeScaleOuter timeScale = TimeScaleOuter.of( inputConfig );

        // If not instantaneous, the existing function must be a total or mean
        if ( !timeScale.isInstantaneous() && ! ( inputConfig.getFunction() == TimeScaleFunction.MEAN
                                                 || inputConfig.getFunction() == TimeScaleFunction.TOTAL ) )
        {
            returnMe = false;

            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                          + " When using a desired time scale function of {}, the existing time scale on the {} must "
                          + "be instantaneous or the time scale function must be a {} or a {}.",
                          projectConfigPlus.getOrigin(),
                          inputConfig.sourceLocation().getLineNumber(),
                          inputConfig.sourceLocation().getColumnNumber(),
                          TimeScaleFunction.TOTAL,
                          helper,
                          TimeScaleFunction.MEAN,
                          TimeScaleFunction.TOTAL );
        }

        return returnMe;
    }

    /**
     * Returns true if the time aggregation period associated with the desiredTimeScale is valid given the time
     * aggregation periods associated with the existingTimeScale for each source.
     * 
     * See Redmine issue 40389.
     * 
     * Not all attributes of a valid aggregation can be checked from the declaration alone, but some attributes, 
     * can be checked in advance. Having a valid time aggregation period does not imply that the system actually 
     * supports aggregation to that period.
     * 
     * @param projectConfigPlus the project declaration
     * @param pairConfig the pair declaration
     * @return true if the time aggregation period associated with the desiredTimeScale is valid
     */

    private static boolean isDesiredTimeScaleConsistentWithExistingTimeScales( ProjectConfigPlus projectConfigPlus,
                                                                               PairConfig pairConfig )
    {
        // Only proceed if the desiredTimeScale is non-null and one or more existingTimeScale
        // are non-null
        TimeScaleConfig desiredTimeScaleDeclaration = pairConfig.getDesiredTimeScale();
        Inputs input = projectConfigPlus.getProjectConfig().getInputs();
        TimeScaleConfig left = input.getLeft().getExistingTimeScale();
        TimeScaleConfig right = input.getRight().getExistingTimeScale();
        TimeScaleConfig baseline = null;

        if ( input.getBaseline() != null )
        {
            baseline = input.getBaseline().getExistingTimeScale();
        }

        if ( desiredTimeScaleDeclaration == null )
        {
            LOGGER.debug( "A desired time scale was not discovered, so it cannot be inconsistent with any existing "
                          + "time scale." );

            return true;
        }

        if ( left == null && right == null && baseline == null )
        {
            LOGGER.debug( "An existing time scale was not discovered, so it cannot be inconsistent with any desired "
                          + "time scale." );

            return true;
        }

        TimeScaleOuter desiredTimeScale = TimeScaleOuter.of( desiredTimeScaleDeclaration );

        // Currently checks the period only. If/when an existing time scale can include month-days, then update this
        // validation to consider the month-days
        if ( !desiredTimeScale.hasPeriod() )
        {
            LOGGER.debug( "The desired time scale does not have a declared period, so the period cannot be "
                          + "inconsistent with the period associated with any existing time scale." );

            return true;
        }

        boolean returnMe = Validation.isDesiredTimeScaleConsistentWithExistingTimeScales( left,
                                                                                          desiredTimeScale,
                                                                                          LeftOrRightOrBaseline.LEFT,
                                                                                          projectConfigPlus );

        returnMe = Validation.isDesiredTimeScaleConsistentWithExistingTimeScales( right,
                                                                                  desiredTimeScale,
                                                                                  LeftOrRightOrBaseline.RIGHT,
                                                                                  projectConfigPlus )
                   && returnMe;

        return Validation.isDesiredTimeScaleConsistentWithExistingTimeScales( baseline,
                                                                              desiredTimeScale,
                                                                              LeftOrRightOrBaseline.BASELINE,
                                                                              projectConfigPlus )
               && returnMe;
    }

    /**
     * Returns true if the desired time scale is consistent with the existing time scale.
     * @param existingConfig the existing time scale configuration
     * @param desired the desired time scale
     * @param lrb the data orientation
     * @param projectConfigPlus the project configuration
     * @return true if the time scales are consistent, false otherwise
     */

    private static boolean isDesiredTimeScaleConsistentWithExistingTimeScales( TimeScaleConfig existingConfig,
                                                                               TimeScaleOuter desired,
                                                                               LeftOrRightOrBaseline lrb,
                                                                               ProjectConfigPlus projectConfigPlus )
    {
        boolean returnMe = true;

        if ( Objects.nonNull( existingConfig ) )
        {
            TimeScaleOuter existing = TimeScaleOuter.of( existingConfig );

            if ( !existing.isInstantaneous() )
            {
                returnMe = Validation.isDesiredTimeScalePeriodConsistent( projectConfigPlus,
                                                                          desired.getPeriod(),
                                                                          existing.getPeriod(),
                                                                          existingConfig,
                                                                          lrb.toString() );
            }
        }

        return returnMe;
    }

    /**
     * Returns true if the desired aggregation period is consistent with the existing aggregation period, false 
     * otherwise. A time aggregation may be valid in principle without being supported by the system in practice.
     * 
     * @param projectConfigPlus the project declaration
     * @param desired the desired period
     * @param existing the existing period
     * @param helper a helper to locate the existing period being checked
     * @param helperString a helper string to inform the user about the location
     * @return true if the proposed time aggregation is valid, in principle
     */

    private static boolean isDesiredTimeScalePeriodConsistent( ProjectConfigPlus projectConfigPlus,
                                                               Duration desired,
                                                               Duration existing,
                                                               TimeScaleConfig helper,
                                                               String helperString )
    {
        boolean returnMe = true;
        // Disaggregation is not allowed
        if ( desired.compareTo( existing ) < 0 )
        {
            returnMe = false;
            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                          + " The desired time aggregation of the pairs is smaller than the existing time "
                          + "aggregation of the {}: disaggregation is not supported.",
                          projectConfigPlus.getOrigin(),
                          helper.sourceLocation().getLineNumber(),
                          helper.sourceLocation().getColumnNumber(),
                          helperString );
        }
        // Desired is not an integer multiple of existing
        if ( desired.toMillis() % existing.toMillis() != 0 )
        {
            returnMe = false;
            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                          + " The desired time aggregation of the pairs is not an integer multiple of the "
                          + "existing time aggregation of the {}.",
                          projectConfigPlus.getOrigin(),
                          helper.sourceLocation().getLineNumber(),
                          helper.sourceLocation().getColumnNumber(),
                          helperString );
        }
        return returnMe;
    }

    /**
     * Validates the time windows. 
     * 
     * @param projectConfigPlus the project declaration, which helps with messaging
     * @param pairConfig the pair declaration
     * @return true if the time windows are valid, otherwise false
     */

    private static boolean areTimeWindowsValid( ProjectConfigPlus projectConfigPlus, PairConfig pairConfig )
    {
        boolean valid = Validation.areDateTimeIntervalsValid( projectConfigPlus, pairConfig );

        valid = Validation.isValidDatesPoolingWindowValid( projectConfigPlus, pairConfig ) && valid;

        valid = Validation.isIssuedDatesPoolingWindowValid( projectConfigPlus, pairConfig ) && valid;

        return Validation.isLeadTimesPoolingWindowValid( projectConfigPlus, pairConfig ) && valid;
    }

    /**
     * Checks the internal consistency of the issued and valid time intervals.
     * 
     * @param projectConfigPlus the project declaration, which helps with messaging
     * @param pairConfig the pair declaration
     * @return true if the issued and valid time intervals are valid, otherwise false
     */

    private static boolean areDateTimeIntervalsValid( ProjectConfigPlus projectConfigPlus, PairConfig pairConfig )
    {
        boolean valid = true;

        // Valid dates
        DateCondition validDates = pairConfig.getDates();
        if ( Objects.nonNull( validDates ) && Objects.nonNull( validDates.getEarliest() )
             && Objects.nonNull( validDates.getLatest() ) )
        {
            Instant earliest = Instant.parse( validDates.getEarliest() );
            Instant latest = Instant.parse( validDates.getLatest() );

            if ( earliest.isAfter( latest ) )
            {
                valid = false;

                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + " The earliest valid datetime cannot be later the latest valid datetime, but the "
                              + "earliest valid datetime is {} and the latest valid datetime is {}. Please correct "
                              + "this declaration and try again.",
                              projectConfigPlus.getOrigin(),
                              validDates.sourceLocation().getLineNumber(),
                              validDates.sourceLocation().getColumnNumber(),
                              earliest,
                              latest );
            }
        }

        // Issued dates
        DateCondition issuedDates = pairConfig.getIssuedDates();
        if ( Objects.nonNull( issuedDates ) && Objects.nonNull( issuedDates.getEarliest() )
             && Objects.nonNull( issuedDates.getLatest() ) )
        {
            Instant earliest = Instant.parse( issuedDates.getEarliest() );
            Instant latest = Instant.parse( issuedDates.getLatest() );

            if ( earliest.isAfter( latest ) )
            {
                valid = false;

                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + " The earliest issued datetime cannot be later than the latest issued datetime, but the "
                              + "earliest issued datetime is {} and the latest issued datetime is {}. Please correct "
                              + "this declaration and try again.",
                              projectConfigPlus.getOrigin(),
                              issuedDates.sourceLocation().getLineNumber(),
                              issuedDates.sourceLocation().getColumnNumber(),
                              earliest,
                              latest );
            }
        }

        return valid;
    }

    /**
     * Checks the valid dates pooling windows for consistency with other declaration, as well as internal consistency.
     * 
     * @param projectConfigPlus the project declaration, which helps with messaging
     * @param pairConfig the pair declaration
     * @return true if the valid dates pooling windows are undefined or valid, otherwise false
     */

    private static boolean isValidDatesPoolingWindowValid( ProjectConfigPlus projectConfigPlus, PairConfig pairConfig )
    {
        boolean valid = true;

        // Validate any validDatesPoolingWindow
        if ( Objects.nonNull( pairConfig.getValidDatesPoolingWindow() ) )
        {
            PoolingWindowConfig validDatesPoolingConfig = pairConfig.getValidDatesPoolingWindow();

            String validBoiler = " Error when evaluating the declaration for valid dates "
                                 + POOLING_WINDOWS;

            // Check that the valid dates are defined
            if ( Objects.isNull( pairConfig.getDates() ) )
            {
                valid = false;

                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + "{} the earliest and latest valid dates are required, but were not found. Please add "
                              + "these dates and try again.",
                              projectConfigPlus.getOrigin(),
                              pairConfig.sourceLocation().getLineNumber(),
                              pairConfig.sourceLocation().getColumnNumber(),
                              validBoiler );
            }
            else
            {
                // Check for the minimum
                if ( Objects.isNull( pairConfig.getDates().getEarliest() ) )
                {
                    valid = false;

                    LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                  + "{} the earliest valid date is required, but was not found. Please add this date "
                                  + AND_TRY_AGAIN,
                                  projectConfigPlus.getOrigin(),
                                  pairConfig.getDates().sourceLocation().getLineNumber(),
                                  pairConfig.getDates().sourceLocation().getColumnNumber(),
                                  validBoiler );
                }

                // Check for the maximum
                if ( Objects.isNull( pairConfig.getDates().getLatest() ) )
                {
                    valid = false;

                    LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                  + "{} the latest valid date is required, but was not found. Please add this date and"
                                  + TRY_AGAIN,
                                  projectConfigPlus.getOrigin(),
                                  pairConfig.getDates().sourceLocation().getLineNumber(),
                                  pairConfig.getDates().sourceLocation().getColumnNumber(),
                                  validBoiler );
                }

                // Both are present, check that they can produce a time window
                if ( Objects.nonNull( pairConfig.getDates().getEarliest() )
                     && Objects.nonNull( pairConfig.getDates().getLatest() ) )
                {
                    // Create the elements necessary to increment the windows
                    ChronoUnit periodUnits = ChronoUnit.valueOf( validDatesPoolingConfig.getUnit()
                                                                                        .toString()
                                                                                        .toUpperCase() );
                    // Period associated with the validDatesPoolingWindow
                    Duration period = Duration.of( validDatesPoolingConfig.getPeriod(), periodUnits );

                    Instant start = Instant.parse( pairConfig.getDates().getEarliest() );
                    Instant end = Instant.parse( pairConfig.getDates().getLatest() );

                    if ( start.plus( period ).isAfter( end ) )
                    {
                        valid = false;

                        LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                      + " Could not generate any valid time pools because the earliest valid time ({}) "
                                      + "plus the period associated with the validDatesPoolingWindow ({}) is later than "
                                      + "the latest valid time ({}). There must be at least one pool per evaluation "
                                      + "and, for a pool to be valid, its rightmost valid time must be earlier than the "
                                      + "latest valid time.",
                                      projectConfigPlus.getOrigin(),
                                      pairConfig.getDates().sourceLocation().getLineNumber(),
                                      pairConfig.getDates().sourceLocation().getColumnNumber(),
                                      start,
                                      period,
                                      end );
                    }
                }
            }

            // Validate the contents
            valid = Validation.isPoolingWindowValid( projectConfigPlus, validDatesPoolingConfig, "valid dates" )
                    && valid;

        }

        return valid;
    }

    /**
     * Checks for issued dates pooling windows and validates for consistency with other
     * declaration, as well as internal consistency.
     * 
     * @param projectConfigPlus the project declaration, which helps with messaging
     * @param pairConfig the pair declaration
     * @return true if the issued dates pooling windows are undefined or valid, otherwise false
     */

    private static boolean isIssuedDatesPoolingWindowValid( ProjectConfigPlus projectConfigPlus, PairConfig pairConfig )
    {
        boolean valid = true;

        // Validate any issuedDatesPoolingWindow
        if ( Objects.nonNull( pairConfig.getIssuedDatesPoolingWindow() ) )
        {
            PoolingWindowConfig issuedDatesPoolingConfig = pairConfig.getIssuedDatesPoolingWindow();

            String issuedBoiler = " Error when evaluating the declaration for issued dates "
                                  + POOLING_WINDOWS;

            // Check that the issued dates are defined
            if ( Objects.isNull( pairConfig.getIssuedDates() ) )
            {
                valid = false;

                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + "{} the earliest and latest issued dates are required, but were not found. Please add "
                              + "these dates and try again.",
                              projectConfigPlus.getOrigin(),
                              pairConfig.sourceLocation().getLineNumber(),
                              pairConfig.sourceLocation().getColumnNumber(),
                              issuedBoiler );
            }
            else
            {
                // Check for the minimum
                if ( Objects.isNull( pairConfig.getIssuedDates().getEarliest() ) )
                {
                    valid = false;

                    LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                  + "{} the earliest issued date is required, but was not found. Please add this date "
                                  + AND_TRY_AGAIN,
                                  projectConfigPlus.getOrigin(),
                                  pairConfig.getIssuedDates().sourceLocation().getLineNumber(),
                                  pairConfig.getIssuedDates().sourceLocation().getColumnNumber(),
                                  issuedBoiler );
                }

                // Check for the maximum
                if ( Objects.isNull( pairConfig.getIssuedDates().getLatest() ) )
                {
                    valid = false;

                    LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                  + "{} the latest issued date is required, but was not found. Please add this date and "
                                  + TRY_AGAIN,
                                  projectConfigPlus.getOrigin(),
                                  pairConfig.getIssuedDates().sourceLocation().getLineNumber(),
                                  pairConfig.getIssuedDates().sourceLocation().getColumnNumber(),
                                  issuedBoiler );
                }

                // Both are present, check that they can produce a time window
                if ( Objects.nonNull( pairConfig.getIssuedDates().getEarliest() )
                     && Objects.nonNull( pairConfig.getIssuedDates().getLatest() ) )
                {
                    // Create the elements necessary to increment the windows
                    ChronoUnit periodUnits = ChronoUnit.valueOf( issuedDatesPoolingConfig.getUnit()
                                                                                         .toString()
                                                                                         .toUpperCase() );
                    // Period associated with the issuedDatesPoolingWindow
                    Duration period = Duration.of( issuedDatesPoolingConfig.getPeriod(), periodUnits );

                    Instant start = Instant.parse( pairConfig.getIssuedDates().getEarliest() );
                    Instant end = Instant.parse( pairConfig.getIssuedDates().getLatest() );

                    if ( start.plus( period ).isAfter( end ) )
                    {
                        valid = false;

                        LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                      + " Could not generate any issued time pools because the earliest issued time "
                                      + "({}) plus the period associated with the issuedDatesPoolingWindow ({}) is "
                                      + "later than the latest issued time ({}). There must be at least one pool per "
                                      + "evaluation and, for a pool to be valid, its rightmost issued time must be "
                                      + "earlier than the latest issued time.",
                                      projectConfigPlus.getOrigin(),
                                      pairConfig.getIssuedDates().sourceLocation().getLineNumber(),
                                      pairConfig.getIssuedDates().sourceLocation().getColumnNumber(),
                                      start,
                                      period,
                                      end );
                    }
                }
            }

            // Validate the contents
            valid = Validation.isPoolingWindowValid( projectConfigPlus, issuedDatesPoolingConfig, "issued dates" )
                    && valid;

        }

        return valid;
    }

    /**
     * Checks for lead times pooling windows and validates for consistency with other
     * declaration, as well as internal consistency.
     * 
     * @param projectConfigPlus the project declaration, which helps with messaging
     * @param pairConfig the pair declaration
     * @return true if the lead time pooling windows are undefined or valid, otherwise false
     */

    private static boolean isLeadTimesPoolingWindowValid( ProjectConfigPlus projectConfigPlus, PairConfig pairConfig )
    {
        boolean valid = true;

        // Validate any leadTimesPoolingWindow
        if ( Objects.nonNull( pairConfig.getLeadTimesPoolingWindow() ) )
        {
            PoolingWindowConfig leadTimesPoolingConfig = pairConfig.getLeadTimesPoolingWindow();

            String leadBoiler = " Error when evaluating the declaration for lead times " +
                                POOLING_WINDOWS;

            // Check that the lead times are defined
            if ( Objects.isNull( pairConfig.getLeadHours() ) )
            {
                valid = false;

                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + "{} the minimum and maximum lead durations are required but were not found. Please add "
                              + "these durations and try again.",
                              projectConfigPlus.getOrigin(),
                              pairConfig.sourceLocation().getLineNumber(),
                              pairConfig.sourceLocation().getColumnNumber(),
                              leadBoiler );
            }
            else
            {
                // Check for the minimum
                if ( Objects.isNull( pairConfig.getLeadHours().getMinimum() ) )
                {
                    valid = false;

                    LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                  + "{} the minimum lead duration is required, but was not found. Please add this "
                                  + "duration and try again.",
                                  projectConfigPlus.getOrigin(),
                                  pairConfig.getLeadHours().sourceLocation().getLineNumber(),
                                  pairConfig.getLeadHours().sourceLocation().getColumnNumber(),
                                  leadBoiler );
                }

                // Check for the maximum
                if ( Objects.isNull( pairConfig.getLeadHours().getMaximum() ) )
                {
                    valid = false;

                    LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                  + "{} the maximum lead duration is required, but was not found. Please add this "
                                  + "duration and try again.",
                                  projectConfigPlus.getOrigin(),
                                  pairConfig.getLeadHours().sourceLocation().getLineNumber(),
                                  pairConfig.getLeadHours().sourceLocation().getColumnNumber(),
                                  leadBoiler );
                }

                // Both are present, check that they can produce a time window
                if ( Objects.nonNull( pairConfig.getLeadHours().getMinimum() )
                     && Objects.nonNull( pairConfig.getLeadHours().getMaximum() ) )
                {
                    // Create the elements necessary to increment the windows
                    ChronoUnit periodUnits = ChronoUnit.valueOf( leadTimesPoolingConfig.getUnit()
                                                                                       .toString()
                                                                                       .toUpperCase() );
                    // Period associated with the leadTimesPoolingWindow
                    Duration period = Duration.of( leadTimesPoolingConfig.getPeriod(), periodUnits );

                    Duration start = Duration.ofHours( pairConfig.getLeadHours().getMinimum() );
                    Duration end = Duration.ofHours( pairConfig.getLeadHours().getMaximum() );

                    if ( start.plus( period ).compareTo( end ) > 0 )
                    {
                        valid = false;

                        LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                                      + " Could not generate any lead duration pools because the minimum lead duration "
                                      + "({}) plus the period associated with the leadTimesPoolingWindow ({}) is later "
                                      + "than the maximum lead duration ({}). There must be at least one pool per "
                                      + "evaluation and, for a pool to be valid, its rightmost lead duration must be "
                                      + "smaller than the maximum lead duration.",
                                      projectConfigPlus.getOrigin(),
                                      pairConfig.getIssuedDates().sourceLocation().getLineNumber(),
                                      pairConfig.getIssuedDates().sourceLocation().getColumnNumber(),
                                      start,
                                      period,
                                      end );
                    }
                }
            }

            // Validate the contents
            valid = Validation.isPoolingWindowValid( projectConfigPlus, leadTimesPoolingConfig, "lead times" ) && valid;

        }

        return valid;
    }

    /**
     * Validates the specified time windows. 
     * 
     * @param projectConfigPlus the project declaration, which helps with messaging
     * @param windowConfig the time window declaration
     * @param windowType a string that identifies the window type for messaging 
     * @return true if the time windows are valid, otherwise false
     */

    private static boolean isPoolingWindowValid( ProjectConfigPlus projectConfigPlus,
                                                 PoolingWindowConfig windowConfig,
                                                 String windowType )
    {
        boolean valid = true;

        // Frequency must be >= 1
        if ( Objects.nonNull( windowConfig.getFrequency() ) && windowConfig.getFrequency() < 1 )
        {
            valid = false;

            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                          + " Error when evaluating the declaration for {} "
                          + "pooling windows: the frequency must be at least 1 "
                          + "but was '{}', which is not valid.",
                          projectConfigPlus.getOrigin(),
                          windowConfig.sourceLocation().getLineNumber(),
                          windowConfig.sourceLocation().getColumnNumber(),
                          windowType,
                          windowConfig.getFrequency() );
        }

        // Period must be >= 0
        // #66118 
        if ( Objects.nonNull( windowConfig.getPeriod() ) && windowConfig.getPeriod() < 0 )
        {
            valid = false;

            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                          + " Error when evaluating the declaration for {} "
                          + "pooling windows: the period must be zero or greater "
                          + "but was '{}', which is not valid.",
                          projectConfigPlus.getOrigin(),
                          windowConfig.sourceLocation().getLineNumber(),
                          windowConfig.sourceLocation().getColumnNumber(),
                          windowType,
                          windowConfig.getPeriod() );
        }

        return valid;
    }

    private static boolean isInputsConfigValid( ProjectConfigPlus projectConfigPlus )
    {
        boolean result = true;

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        DataSourceConfig left = projectConfig.getInputs().getLeft();
        DataSourceConfig right = projectConfig.getInputs().getRight();
        DataSourceConfig baseline = projectConfig.getInputs().getBaseline();

        // #72042
        if ( left.getType() == DatasourceType.ENSEMBLE_FORECASTS &&
             right.getType() == DatasourceType.ENSEMBLE_FORECASTS )
        {
            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                          + " Cannot evaluate ensemble forecasts against ensemble forecasts.",
                          projectConfigPlus,
                          left.sourceLocation().getLineNumber(),
                          left.sourceLocation().getColumnNumber() );

            result = false;
        }
        
        result = Validation.isVariableSpecifiedIfRequired( projectConfigPlus, left ) && result;
        
        result = Validation.isVariableSpecifiedIfRequired( projectConfigPlus, right ) && result;
        
        result = Validation.areDataSourcesValid( projectConfigPlus, left ) && result;

        result = Validation.areDataSourcesValid( projectConfigPlus, right ) && result;

        result = Validation.isTypeConsistentWithOtherDeclaration( projectConfigPlus, left ) && result;

        result = Validation.isTypeConsistentWithOtherDeclaration( projectConfigPlus, right ) && result;

        result = Validation.isExistingTimeScaleValid( projectConfigPlus, left, LeftOrRightOrBaseline.LEFT ) && result;

        result = Validation.isExistingTimeScaleValid( projectConfigPlus, right, LeftOrRightOrBaseline.RIGHT ) && result;

        if ( baseline != null )
        {
            result = Validation.isVariableSpecifiedIfRequired( projectConfigPlus, baseline ) && result;
            
            result = Validation.areDataSourcesValid( projectConfigPlus, baseline ) && result;

            result = Validation.areLeftAndBaselineConsistent( projectConfigPlus, left, baseline ) && result;

            result = Validation.isTypeConsistentWithOtherDeclaration( projectConfigPlus, baseline ) && result;

            result = Validation.isExistingTimeScaleValid( projectConfigPlus, baseline, LeftOrRightOrBaseline.BASELINE )
                     && result;
        }

        return result;
    }

    /**
     * Ticket 112950. Checks that given DataSourceConfig has a variable if that is required.
     * It is only required for the USGS NWIS and WRDS NWM sources, currently.
     * @param projectConfigPlus the evaluation project declaration plus
     * @param sideConfig the inputs side declaration to validate
     * @return true when valid, false otherwise.
     */
    protected static boolean isVariableSpecifiedIfRequired( ProjectConfigPlus projectConfigPlus,
                                                          DataSourceConfig sideConfig )
    {
        boolean result = true;

        if ( sideConfig
                       .getSource()
                       .stream()
                       .map( Source::getInterface )
                       .anyMatch( next -> next == InterfaceShortHand.USGS_NWIS
                                          || next == InterfaceShortHand.WRDS_NWM )
             && sideConfig.getVariable() == null )
        {
            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE +
                          " A source uses either the {} or {} interface, both of which require a "
                          + "variable in the side declaration, but the variable is not specified. "
                          + "Add a variable to the side and try again.",
                          projectConfigPlus,
                          sideConfig.sourceLocation().getLineNumber(),
                          sideConfig.sourceLocation().getColumnNumber(),
                          InterfaceShortHand.USGS_NWIS,
                          InterfaceShortHand.WRDS_NWM );
            result = false;
        }
        return result;
    }

    /**
     * Checks that given DataSourceConfig has at least one
     * DataSourceConfig.Source and checks validity of each inner
     * DataSourceConfig.Source.
     * @param projectConfigPlus the evaluation project declaration plus
     * @param dataSourceConfig the data source declaration to validate
     * @return true when valid, false otherwise.
     */

    private static boolean areDataSourcesValid( ProjectConfigPlus projectConfigPlus,
                                                DataSourceConfig dataSourceConfig )
    {
        boolean dataSourcesValid = true;

        if ( dataSourceConfig.getSource() == null )
        {
            if ( LOGGER.isErrorEnabled() )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                              + "A source needs to exist within each of the "
                              + "left and right sections of the "
                              + "declaration.",
                              projectConfigPlus,
                              dataSourceConfig.sourceLocation()
                                              .getLineNumber(),
                              dataSourceConfig.sourceLocation()
                                              .getColumnNumber() );
            }

            dataSourcesValid = false;
        }
        else
        {
            for ( DataSourceConfig.Source s : dataSourceConfig.getSource() )
            {
                dataSourcesValid = dataSourcesValid && Validation.isSourceValid( projectConfigPlus,
                                                                                 dataSourceConfig,
                                                                                 s );
            }
        }

        // Validate any persistence transformation
        // Start by warning against a transformation, of which there is only one type supported
        if ( Objects.nonNull( dataSourceConfig.getTransformation() ) )
        {
            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                         + " The declaration contains a <transformation>persistence</transformation>, which has been "
                         + "marked deprecated for removal. This feature will be removed without warning in a future "
                         + "release. Instead, declare <persistence>1</persistence> for the same behavior.",
                         projectConfigPlus.getOrigin(),
                         dataSourceConfig.sourceLocation().getLineNumber(),
                         dataSourceConfig.sourceLocation().getColumnNumber() );
            dataSourcesValid = true;
        }

        // Do not declare persistence in two different ways
        if ( Objects.nonNull( dataSourceConfig.getPersistence() )
             && Objects.nonNull( dataSourceConfig.getTransformation() )
             && dataSourceConfig.getTransformation() == SourceTransformationType.PERSISTENCE )
        {
            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                          + " The declaration contains both a <transformation>persistence</transformation> and a "
                          + "<persistence>{}</persistence>, which is not allowed. Please delete the "
                          + "<transformation>persistence</transformation> and try again.",
                          projectConfigPlus.getOrigin(),
                          dataSourceConfig.sourceLocation().getLineNumber(),
                          dataSourceConfig.sourceLocation().getColumnNumber(),
                          dataSourceConfig.getPersistence() );
            dataSourcesValid = false;
        }

        // The lag associated with persistence is non-negative
        if ( Objects.nonNull( dataSourceConfig.getPersistence() ) && dataSourceConfig.getPersistence() < 0 )
        {
            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE
                          + " The declaration contains a persistence transformation with a lag of {}, which is not "
                          + "allowed. The lag must be non-negative. Please correct the lag to 0 or greater and try "
                          + "again.",
                          projectConfigPlus.getOrigin(),
                          dataSourceConfig.sourceLocation().getLineNumber(),
                          dataSourceConfig.sourceLocation().getColumnNumber(),
                          dataSourceConfig.getPersistence() );
            dataSourcesValid = false;
        }

        return dataSourcesValid;
    }

    /**
     * Checks the validity of an individual DataSourceConfig.Source
     * @param projectConfigPlus the evaluation project declaration
     * @param dataSourceConfig the dataSourceConfig being checked
     * @param source the source being checked
     * @return true if valid, false otherwise
     */
    private static boolean isSourceValid( ProjectConfigPlus projectConfigPlus,
                                          DataSourceConfig dataSourceConfig,
                                          DataSourceConfig.Source source )
    {
        Validation.warnAboutZoneOffset( projectConfigPlus, source );

        boolean sourceValid = Validation.isURIDefinedInSourceWhenExpected( projectConfigPlus, source );

        if ( source.getInterface() != null
             && source.getInterface()
                      .equals( InterfaceShortHand.WRDS_AHPS ) )
        {
            sourceValid = Validation.isWRDSSourceValid( projectConfigPlus,
                                                        dataSourceConfig,
                                                        source )
                          && sourceValid;
        }

        return sourceValid;
    }

    /**
     * Checks the consistency of the left and baseline declaration when the baseline is generated and uses the same
     * source of data as the left.
     * 
     * 
     * @param projectConfigPlus the evaluation project declaration
     * @param leftConfig the left data declaration
     * @param baselineConfig the baseline data declaration
     */

    private static boolean areLeftAndBaselineConsistent( ProjectConfigPlus projectConfigPlus,
                                                         DataSourceConfig left,
                                                         DataSourceConfig baseline )
    {
        Objects.requireNonNull( projectConfigPlus );
        Objects.requireNonNull( left );
        Objects.requireNonNull( baseline );

        boolean valid = true;

        // If there is a generated baseline that uses the same data as the left, then the time scales must be consistent
        // See: #92480
        if ( Objects.nonNull( baseline.getTransformation() ) && Objects.equals( left.getSource(), baseline.getSource() )
             && !Objects.equals( left.getExistingTimeScale(), baseline.getExistingTimeScale() ) )
        {
            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE +
                          " The baseline source is the same as the left source, but the existing time scales do not "
                          + "match. Fix or remove the existing time scale of one or both of the left and baseline "
                          + "sources so that they are consistent.",
                          projectConfigPlus,
                          baseline.sourceLocation().getLineNumber(),
                          baseline.sourceLocation().getColumnNumber() );

            valid = false;
        }

        return valid;
    }

    /**
     * Checks that the {@link DataSourceConfig#getType()} is consistent with the other declaration.
     *  
     * @param projectConfigPlus the evaluation project declaration plus
     * @param dataSourceConfig the data source declaration to validate
     * @return true when valid, false otherwise.
     */

    private static boolean isTypeConsistentWithOtherDeclaration( ProjectConfigPlus projectConfigPlus,
                                                                 DataSourceConfig dataSourceConfig )
    {
        boolean valid = true;

        LeftOrRightOrBaseline side = ConfigHelper.getLeftOrRightOrBaseline( projectConfigPlus.getProjectConfig(),
                                                                            dataSourceConfig );

        // Ensemble filter not allowed unless type is ENSEMBLE_FORECASTS
        if ( dataSourceConfig.getType() != DatasourceType.ENSEMBLE_FORECASTS
             && !dataSourceConfig.getEnsemble().isEmpty() )
        {
            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE +
                          " The {} dataset declares ensemble filters using <ensemble> but the data source <type> is "
                          + "{}, which is inconsistent with the filters. Remove the <ensemble> or correct the <type> to "
                          + "{} and then try again.",
                          projectConfigPlus,
                          dataSourceConfig.sourceLocation().getLineNumber(),
                          dataSourceConfig.sourceLocation().getColumnNumber(),
                          side,
                          dataSourceConfig.getType(),
                          DatasourceType.ENSEMBLE_FORECASTS );

            valid = false;
        }

        // Type and interface shorthands consistent with each other?
        return Validation.isTypeConsistentWithEachSourceInterface( projectConfigPlus, dataSourceConfig ) && valid;
    }

    /**
     * <p>Returns <code>true</code> if the source has a URI when expected, otherwise
     * <code>false</code>. A URI is expected even when full path and parameters
     * are not included.
     * 
     * @param projectConfigPlus the project declaration
     * @param source the source to inspect
     * @return true if the source has a URI when a URI is expected, otherwise false
     */

    private static boolean isURIDefinedInSourceWhenExpected( ProjectConfigPlus projectConfigPlus,
                                                             DataSourceConfig.Source source )
    {
        Objects.requireNonNull( projectConfigPlus );
        Objects.requireNonNull( source );
        boolean result = true;

        if ( Objects.isNull( source.getValue() )
             || source.getValue().toString().isBlank() )
        {
            LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE +
                          " A source has an invalid URI: please add a valid URI, which cannot be empty.",
                          projectConfigPlus,
                          source.sourceLocation().getLineNumber(),
                          source.sourceLocation().getColumnNumber() );

            result = false;
        }

        return result;
    }

    private static boolean isWRDSSourceValid( ProjectConfigPlus projectConfigPlus,
                                              DataSourceConfig dataSourceConfig,
                                              DataSourceConfig.Source source )
    {
        boolean wrdsSourceValid = true;

        if ( dataSourceConfig.equals( projectConfigPlus.getProjectConfig()
                                                       .getInputs()
                                                       .getLeft() ) )
        {
            DateCondition dates = projectConfigPlus.getProjectConfig()
                                                   .getPair()
                                                   .getDates();
            if ( dates == null )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE + " "
                              + API_SOURCE_MISSING_DATES_ERROR_MESSAGE,
                              projectConfigPlus,
                              projectConfigPlus.getProjectConfig()
                                               .getPair()
                                               .sourceLocation()
                                               .getLineNumber(),
                              projectConfigPlus.getProjectConfig()
                                               .getPair()
                                               .sourceLocation()
                                               .getColumnNumber(),
                              source.sourceLocation()
                                    .getLineNumber(),
                              source.sourceLocation()
                                    .getColumnNumber() );
                wrdsSourceValid = false;
            }
            else if ( dates.getEarliest() == null
                      || dates.getLatest() == null )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE + " "
                              + API_SOURCE_MISSING_DATES_ERROR_MESSAGE,
                              projectConfigPlus,
                              dates.sourceLocation()
                                   .getLineNumber(),
                              dates.sourceLocation()
                                   .getColumnNumber(),
                              source.sourceLocation()
                                    .getLineNumber(),
                              source.sourceLocation()
                                    .getColumnNumber() );
                wrdsSourceValid = false;
            }

        }
        else
        {

            DateCondition issuedDates = projectConfigPlus.getProjectConfig()
                                                         .getPair()
                                                         .getIssuedDates();
            if ( issuedDates == null )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE + " "
                              + API_SOURCE_MISSING_ISSUED_DATES_ERROR_MESSAGE,
                              projectConfigPlus,
                              projectConfigPlus.getProjectConfig()
                                               .getPair()
                                               .sourceLocation()
                                               .getLineNumber(),
                              projectConfigPlus.getProjectConfig()
                                               .getPair()
                                               .sourceLocation()
                                               .getColumnNumber(),
                              source.sourceLocation()
                                    .getLineNumber(),
                              source.sourceLocation()
                                    .getColumnNumber() );
                wrdsSourceValid = false;
            }
            else if ( issuedDates.getEarliest() == null
                      || issuedDates.getLatest() == null )
            {
                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE + " "
                              + API_SOURCE_MISSING_ISSUED_DATES_ERROR_MESSAGE,
                              projectConfigPlus,
                              issuedDates.sourceLocation()
                                         .getLineNumber(),
                              issuedDates.sourceLocation()
                                         .getColumnNumber(),
                              source.sourceLocation()
                                    .getLineNumber(),
                              source.sourceLocation()
                                    .getColumnNumber() );
                wrdsSourceValid = false;
            }

        }

        return wrdsSourceValid;
    }

    /**
     * Checks for consistency between the declared type of the data source and the type implied by each data source 
     * interface shorthand.
     * 
     * @param projectConfigPlus the project declaration
     * @param dataSourceConfig the data source declaration, including the type
     * @return true if the information is consistent, false otherwise
     */

    private static boolean isTypeConsistentWithEachSourceInterface( ProjectConfigPlus projectConfigPlus,
                                                                    DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( projectConfigPlus );
        Objects.requireNonNull( dataSourceConfig );

        Set<InterfaceShortHand> interfaces = dataSourceConfig.getSource()
                                                             .stream()
                                                             .map( Source::getInterface )
                                                             .collect( Collectors.toSet() );

        boolean isValid = true;

        for ( InterfaceShortHand nextInterface : interfaces )
        {
            isValid = Validation.isTypeConsistentWithSourceInterface( projectConfigPlus,
                                                                      dataSourceConfig,
                                                                      nextInterface )
                      && isValid;
        }

        return isValid;
    }

    /**
     * Checks for consistency between the declared type of a data source and the type implied by the data source 
     * interface shorthand.
     * 
     * @param projectConfigPlus the project declaration
     * @param dataSourceConfig the data source declaration, including the type
     * @param anInterface the interface
     * @return true if the information is consistent, false otherwise
     */

    private static boolean isTypeConsistentWithSourceInterface( ProjectConfigPlus projectConfigPlus,
                                                                DataSourceConfig dataSourceConfig,
                                                                InterfaceShortHand anInterface )
    {
        Objects.requireNonNull( projectConfigPlus );
        Objects.requireNonNull( dataSourceConfig );

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();
        DatasourceType type = dataSourceConfig.getType();

        LeftOrRightOrBaseline lrb = ConfigHelper.getLeftOrRightOrBaseline( projectConfig, dataSourceConfig );

        DatasourceType interfaceType = Validation.getDatasourceTypeFromInterfaceShortHand( anInterface );

        boolean isValid = true;

        if ( Objects.nonNull( interfaceType ) && interfaceType != type )
        {
            if ( Validation.isDataSourceTypeObservationLike( type ) &&
                 Validation.isDataSourceTypeObservationLike( interfaceType ) )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE +
                             " A source on the {} has an interface of {}, which is allowed but is not strictly "
                             + "consistent with the data source type of {}. The interface of {} would normally have a "
                             + "data source type of {}. If this represents an error in the declaration, it should be "
                             + "fixed.",
                             projectConfigPlus,
                             dataSourceConfig.sourceLocation().getLineNumber(),
                             dataSourceConfig.sourceLocation().getColumnNumber(),
                             lrb,
                             anInterface,
                             type,
                             anInterface,
                             interfaceType,
                             interfaceType,
                             type );
            }
            else
            {
                isValid = false;

                LOGGER.error( FILE_LINE_COLUMN_BOILERPLATE +
                              " A source on the {} has an interface of {}, which is inconsistent with the data source "
                              + "type of {}. The interface of {} should have a data source type of {}. Please change "
                              + "the data source type to {} or change the interface for consistency with the type {}.",
                              projectConfigPlus,
                              dataSourceConfig.sourceLocation().getLineNumber(),
                              dataSourceConfig.sourceLocation().getColumnNumber(),
                              lrb,
                              anInterface,
                              type,
                              anInterface,
                              interfaceType,
                              interfaceType,
                              type );
            }
        }

        return isValid;
    }

    /**
     * Returns the data source type for the specified interface.
     * @param anInterface the interface
     * @return the data source type or null if unknown
     */

    private static DatasourceType getDatasourceTypeFromInterfaceShortHand( InterfaceShortHand anInterface )
    {
        if ( Objects.isNull( anInterface ) )
        {
            LOGGER.debug( "No interface shorthand declared from which to obtain a corresponding data source type." );
            return null;
        }
        switch ( anInterface )
        {
            case NWM_ANALYSIS_ASSIM_CHANNEL_RT_CONUS:
            case NWM_ANALYSIS_ASSIM_CHANNEL_RT_HAWAII:
            case NWM_ANALYSIS_ASSIM_CHANNEL_RT_PUERTORICO:
            case NWM_ANALYSIS_ASSIM_EXTEND_CHANNEL_RT_CONUS:
            case NWM_ANALYSIS_ASSIM_EXTEND_NO_DA_CHANNEL_RT_CONUS:
            case NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_CONUS:
            case NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_HAWAII:
            case NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_PUERTORICO:
                return DatasourceType.ANALYSES;
            case NWM_LONG_RANGE_CHANNEL_RT_CONUS:
            case NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_CONUS:
            case NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_CONUS_HOURLY:
            case NWM_MEDIUM_RANGE_NO_DA_DETERMINISTIC_CHANNEL_RT_CONUS:
            case NWM_SHORT_RANGE_CHANNEL_RT_CONUS:
            case NWM_SHORT_RANGE_CHANNEL_RT_HAWAII:
            case NWM_SHORT_RANGE_CHANNEL_RT_PUERTORICO:
            case NWM_SHORT_RANGE_NO_DA_CHANNEL_RT_HAWAII:
            case NWM_SHORT_RANGE_NO_DA_CHANNEL_RT_PUERTORICO:
            case WRDS_AHPS:
                return DatasourceType.SINGLE_VALUED_FORECASTS;
            case NWM_MEDIUM_RANGE_ENSEMBLE_CHANNEL_RT_CONUS:
            case NWM_MEDIUM_RANGE_ENSEMBLE_CHANNEL_RT_CONUS_HOURLY:
                return DatasourceType.ENSEMBLE_FORECASTS;
            case USGS_NWIS:
            case WRDS_OBS:
                return DatasourceType.OBSERVATIONS;
            case WRDS_NWM: // Could be any of several types
                LOGGER.debug( "Discovered an interface shorthand {} that represents any of several data source types. "
                              + "This will not be validated against the declared type.",
                              InterfaceShortHand.WRDS_NWM );
                return null;
            default:
                LOGGER.warn( "When attempting to identify a data source type for the interface {}, failed to "
                             + "recognize the interface shorthand {}.",
                             anInterface,
                             anInterface );
                return null;

        }
    }

    /**
     * @param datasourceType the data source type
     * @return true if the type is observation-like, false otherwise
     */

    private static boolean isDataSourceTypeObservationLike( DatasourceType datasourceType )
    {
        switch ( datasourceType )
        {
            case ANALYSES:
            case OBSERVATIONS:
            case SIMULATIONS:
                return true;
            default:
                return false;

        }
    }
}
