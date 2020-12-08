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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.xml.bind.ValidationEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.xml.bind.Locatable;

import wres.config.generated.DataSourceBaselineConfig;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.DesiredTimeScaleConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.Format;
import wres.config.generated.IntBoundsType;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeScaleConfig;
import wres.config.generated.TimeScaleFunction;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.scale.TimeScaleOuter;
import wres.engine.statistics.metric.config.MetricConfigHelper;
import wres.system.SystemSettings;


/**
 * Helps validate project configurations at a higher level than parser, with
 * detailed messaging.
 *
 * TODO: formal interface for validation text rather than log messages
 */

public class Validation
{

    private static final Logger LOGGER = LoggerFactory.getLogger( Validation.class );

    /** A message to display for programmers when null project config occurs */
    private static final String NON_NULL = "The args must not be null";

    /** The warning message boilerplate for logger (includes 3 placeholders) */
    private static final String FILE_LINE_COLUMN_BOILERPLATE =
            "In file {}, near line {} and column {}, WRES found an issue with "
                                                               + "the project configuration.";

    private static final String API_SOURCE_MISSING_ISSUED_DATES_ERROR_MESSAGE =
            "One must specify issued dates with both earliest and latest (e.g. "
                                                                                + "<issuedDates earliest=\"2018-12-28T15:42:00Z\" "
                                                                                + "latest=\"2019-01-01T00:00:00Z\" />) when using a web API as a "
                                                                                + "source for forecasts (see source near line {} and column {}";


    private Validation()
    {
        // prevent construction.
    }


    /**
     * Quick validation of the project configuration, will return detailed
     * information to the user regarding issues about the configuration. Strict
     * for now, i.e. return false even on minor xml problems. Does not return on
     * first issue, tries to inform the user of all issues before returning.
     *
     * @param systemSettings The system settings to use.
     * @param projectConfigPlus the project configuration
     * @return true if no issues were detected, false otherwise
     */

    public static boolean isProjectValid( SystemSettings systemSettings,
                                          ProjectConfigPlus projectConfigPlus )
    {
        // Assume valid until demonstrated otherwise
        boolean result = true;

        for ( ValidationEvent ve : projectConfigPlus.getValidationEvents() )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                if ( ve.getLocator() != null )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + " The parser said: {}",
                                 projectConfigPlus.getOrigin(),
                                 ve.getLocator().getLineNumber(),
                                 ve.getLocator().getColumnNumber(),
                                 ve.getMessage(),
                                 ve.getLinkedException() );
                }
                else
                {
                    LOGGER.warn( "In file {}, WRES found an issue with the "
                                 + "project configuration. The parser said: {}",
                                 projectConfigPlus.getOrigin(),
                                 ve.getMessage(),
                                 ve.getLinkedException() );
                }
            }

            // Any validation event means we fail.
            result = false;
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

        // Validate outputs section
        result = Validation.isOutputConfigValid( projectConfigPlus ) && result;

        // Validate graphics portion
        result = Validation.isGraphicsPortionOfProjectValid( projectConfigPlus )
                 && result;

        return result;
    }

    /**
     * Checks to see if there are input declarations requiring other declaration
     * in the pair declaration.
     * @param projectConfigPlus The project declaration to check.
     * @return false if there are known invalid combinations present.
     */

    private static boolean isInputsAndPairCombinationValid( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus );
        Locatable firstSourceThatRequiresFeatures = null;
        boolean noFeatureDeclaration = false;
        DataSourceConfig left = projectConfigPlus.getProjectConfig()
                                                 .getInputs()
                                                 .getLeft();
        DataSourceConfig right = projectConfigPlus.getProjectConfig()
                                                  .getInputs()
                                                  .getRight();
        DataSourceBaselineConfig baselineConfig =
                projectConfigPlus.getProjectConfig()
                                 .getInputs()
                                 .getBaseline();

        for ( DataSourceConfig.Source source : left.getSource() )
        {
            if ( Validation.requiresFeatureOrFeatureService( source ) )
            {
                firstSourceThatRequiresFeatures = source;
                break;
            }
        }

        for ( DataSourceConfig.Source source : right.getSource() )
        {
            if ( Validation.requiresFeatureOrFeatureService( source ) )
            {
                firstSourceThatRequiresFeatures = source;
                break;
            }
        }

        if ( Objects.nonNull( baselineConfig ) )
        {
            for ( DataSourceConfig.Source source : baselineConfig.getSource() )
            {
                if ( Validation.requiresFeatureOrFeatureService( source ) )
                {
                    firstSourceThatRequiresFeatures = source;
                    break;
                }
            }
        }

        PairConfig pairDeclaration = projectConfigPlus.getProjectConfig()
                                                      .getPair();

        if ( ( Objects.isNull( pairDeclaration.getFeature() )
               || pairDeclaration.getFeature()
                                 .isEmpty() )
             && ( Objects.isNull( pairDeclaration.getFeatureService() )
                  || Objects.isNull( pairDeclaration.getFeatureService()
                                                    .getBaseUrl() ) ) )
        {
            noFeatureDeclaration = true;
        }

        if ( Objects.nonNull( firstSourceThatRequiresFeatures )
             && noFeatureDeclaration )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + ": at least one data source declaration required"
                             + " <feature> or <featureService> declaration but"
                             + " none was declared.",
                             projectConfigPlus.getOrigin(),
                             firstSourceThatRequiresFeatures.sourceLocation()
                                                            .getLineNumber(),
                             firstSourceThatRequiresFeatures.sourceLocation()
                                                            .getColumnNumber() );
            }

            return false;
        }

        return true;
    }

    private static boolean requiresFeatureOrFeatureService( DataSourceConfig.Source source )
    {
        if ( Objects.nonNull( source.getInterface() ) )
        {
            if ( source.getInterface()
                       .equals( InterfaceShortHand.WRDS_AHPS ) )
            {
                return true;
            }
            else if ( source.getInterface()
                            .equals( InterfaceShortHand.WRDS_NWM ) )
            {
                return true;
            }
            else if ( source.getInterface()
                            .equals( InterfaceShortHand.USGS_NWIS ) )
            {
                return true;
            }
            else if ( source.getInterface()
                            .value()
                            .toLowerCase()
                            .startsWith( "nwm_" ) )
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Validates the metrics portion of the project config.
     * 
     * @param projectConfigPlus the project configuration
     * @return true if the output configuration is valid, false otherwise
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean isMetricsConfigValid( SystemSettings systemSettings,
                                                 ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        // Validate that metric configuration is internally consistent
        boolean result = Validation.isAllMetricsConfigInternallyConsistent( projectConfigPlus );

        // Check that each named metric is consistent with the other configuration
        result = result && Validation.isAllMetricsConfigConsistentWithOtherConfig( projectConfigPlus );

        // Check that any external thresholds refer to readable files
        result = result && Validation.areAllPathsToThresholdsReadable( systemSettings,
                                                                       projectConfigPlus );

        return result;
    }


    /**
     * Validates the output portion of the project config.
     * 
     * @param projectConfigPlus the project configuration
     * @return true if the output configuration is valid, false otherwise
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean isOutputConfigValid( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        return Validation.isNetcdfOutputConfigValid(
                                                     projectConfigPlus.toString(),
                                                     projectConfigPlus.getProjectConfig()
                                                                      .getOutputs()
                                                                      .getDestination() );
    }

    private static boolean isNetcdfOutputConfigValid( String path, List<DestinationConfig> destinations )
    {
        Objects.requireNonNull( destinations, NON_NULL );

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

        if ( templateMissing != null )
        {
            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
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
            for ( DestinationConfig destinationConfig : incorrectDestinations )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " Netcdf output configurations are only valid for Netcdf Output.",
                             path,
                             destinationConfig.sourceLocation().getLineNumber(),
                             destinationConfig.sourceLocation().getColumnNumber() );
            }
        }

        return templateMissing == null && incorrectDestinations.isEmpty();
    }

    /**
     * Checks that the metric configuration is internally consistent.
     * 
     * @param projectConfigPlus the project configuration
     * @return true if the metric configuration is internally consistent, false otherwise
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
            if ( !Validation.isOneMetricsConfigInternallyConsistent( projectConfigPlus, next ) )
            {
                returnMe = false;
            }
        }
        return returnMe;
    }

    /**
     * Checks that the metric configuration is internally consistent.
     * 
     * @param projectConfigPlus the project configuration
     * @param metrics the metrics configuration
     * @return true if the metric configuration is internally consistent, false otherwise
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
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
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
            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                         + " Currently, time-series metrics can only be applied to single-valued forecasts.",
                         projectConfigPlus,
                         metrics.sourceLocation()
                                .getLineNumber(),
                         metrics
                                .sourceLocation()
                                .getColumnNumber() );
            returnMe = false;
        }

        // Cannot define specific metrics together with all valid        
        for ( MetricConfig next : metricConfig )
        {
            //Unnamed metric
            if ( MetricConfigName.ALL_VALID == next.getName() && metricConfig.size() > 1 )
            {
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
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
     * Checks that the metric configuration is internally consistent.
     * 
     * @param projectConfigPlus the project configuration
     * @return true if the metric configuration is internally consistent, false otherwise
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
     * Checks that the metric configuration is consistent with the other configuration.
     *
     * @param projectConfigPlus the project configuration
     * @param metrics the metrics configuration 
     * @return true if the metric is consistent with other configuration
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean isOneMetricsConfigConsistentWithOtherConfig( ProjectConfigPlus projectConfigPlus,
                                                                        MetricsConfig metrics )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        // Assume valid
        AtomicBoolean result = new AtomicBoolean( true );

        ProjectConfig config = projectConfigPlus.getProjectConfig();

        // Filter for non-null and not all valid
        metrics.getMetric()
               .stream()
               .filter( next -> Objects.nonNull( next ) && next.getName() != MetricConfigName.ALL_VALID )
               .forEach( nextMetric -> {

                   try
                   {
                       MetricConstants checkMe = MetricConfigHelper.from( nextMetric.getName() );

                       // Check that the named metric is consistent with any pooling window configuration
                       if ( projectConfigPlus.getProjectConfig().getPair().getIssuedDatesPoolingWindow() != null
                            && checkMe != null
                            && ! ( checkMe.isInGroup( StatisticType.DOUBLE_SCORE )
                                   || checkMe.isInGroup( StatisticType.DURATION_SCORE ) ) )
                       {
                           result.set( false );
                           LOGGER.warn( "In file {}, a metric named {} was requested, but is not allowed. "
                                        + "Only verification scores are allowed in "
                                        + "combination with a pooling window configuration.",
                                        projectConfigPlus.getOrigin(),
                                        nextMetric.getName() );
                       }

                       // Check that the CRPS has an explicit baseline
                       if ( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE.equals( checkMe )
                            && config.getInputs().getBaseline() == null )
                       {
                           result.set( false );
                           LOGGER.warn( "In file {}, a metric named {} was requested, which requires an explicit "
                                        + "baseline. Remove this metric or add the required baseline configuration.",
                                        projectConfigPlus.getOrigin(),
                                        nextMetric.getName() );
                       }
                   }
                   // Handle the situation where a metric is recognized by the xsd but not by the ConfigMapper. This is
                   // unlikely and implies an incomplete implementation of a metric by the system  
                   catch ( MetricConfigException e )
                   {
                       LOGGER.warn( "In file {}, a metric named {} was requested, but is not recognized by the system.",
                                    projectConfigPlus.getOrigin(),
                                    nextMetric.getName() );
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
                                                    .map( next -> MetricConfigHelper.from( next.getName() ) )
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

            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                         + " The declaration contains decision thresholds, which are only valid for ensemble or "
                         + "probability forecasts and no such forecasts were declared. Please remove the thresholds "
                         + "of type 'probability classifier' from this evaluation.",
                         projectConfigPlus.getOrigin(),
                         metricsConfig.sourceLocation().getLineNumber(),
                         metricsConfig.sourceLocation().getColumnNumber() );
        }

        // Input type declaration is ensemble
        if ( !categorical.isEmpty()
             && declaredRightDataType == DatasourceType.ENSEMBLE_FORECASTS )
        {

            // Order by number of failures - up to two possible
            if ( !eventThresholds && !decisionThresholds )
            {
                isValid = false;

                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
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
            else if ( !eventThresholds && decisionThresholds )
            {
                isValid = false;

                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
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
            else if ( !decisionThresholds )
            {
                isValid = false;

                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " The categorical metrics {} require decision thresholds or 'probability "
                             + "classifiers' to obtain categorical pairs from the probability pairs, but no "
                             + "decision thresholds were supplied. Add some thresholds of type {} to the "
                             + "metrics declaration or remove these metrics before proceeding.",
                             projectConfigPlus.getOrigin(),
                             metricsConfig.sourceLocation().getLineNumber(),
                             metricsConfig.sourceLocation().getColumnNumber(),
                             categorical,
                             ThresholdType.PROBABILITY_CLASSIFIER.name() );
            }
        }
        // Other (single-valued) types
        else if ( !categorical.isEmpty() )
        {
            if ( !eventThresholds )
            {
                isValid = false;

                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
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
                                                    .map( next -> MetricConfigHelper.from( next.getName() ) )
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

            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                         + " The ensemble metrics {} require ensemble pairs, but the declared data type for the RIGHT "
                         + "data source is '{}'. Either correct this data type to 'ensemble forecasts' or remove the "
                         + "ensemble metrics before proceeding.",
                         projectConfigPlus.getOrigin(),
                         projectConfig.getInputs().getRight().sourceLocation().getLineNumber(),
                         projectConfig.getInputs().getRight().sourceLocation().getColumnNumber(),
                         ensemble,
                         type.name().toLowerCase() );
        }

        if ( !discreteProbability.isEmpty() && type != DatasourceType.ENSEMBLE_FORECASTS )
        {
            isValid = false;

            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                         + " The discrete probability metrics {} require ensemble pairs, but the declared data type for "
                         + "the RIGHT data source is '{}'. Either correct this data type to 'ensemble forecasts' or "
                         + "remove the discrete probability metrics before proceeding.",
                         projectConfigPlus.getOrigin(),
                         projectConfig.getInputs().getRight().sourceLocation().getLineNumber(),
                         projectConfig.getInputs().getRight().sourceLocation().getColumnNumber(),
                         discreteProbability,
                         type.name().toLowerCase() );
        }

        return isValid;
    }

    /**
     * Validates the paths to external thresholds.
     *
     * @param projectConfigPlus the project configuration
     * @return true if all have readable files, false otherwise
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean areAllPathsToThresholdsReadable( SystemSettings systemSettings,
                                                            ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        boolean result = true;

        final String PLEASE_UPDATE = "Please update the project configuration "
                                     + "with a readable source of external thresholds.";

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
                                          "The remote thresholds at {} can presumably be accessed since it is a valid url",
                                          thresholdData );
                            continue;
                        }
                        else
                        {
                            destinationPath = Paths.get( thresholdData );
                        }
                    }
                    catch ( MalformedURLException exception )
                    {
                        LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE +
                                     "The URL '{}' is not a proper address and therefore cannot be used to access a "
                                     +
                                     "remote threshold dataset. {}",
                                     projectConfigPlus.getOrigin(),
                                     nextThreshold.sourceLocation().getLineNumber(),
                                     nextThreshold.sourceLocation().getColumnNumber(),
                                     thresholdData,
                                     PLEASE_UPDATE );

                        result = false;
                        continue;
                    }
                    catch ( InvalidPathException | FileSystemNotFoundException | SecurityException e )
                    {
                        LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                     + " The path {} could not be used. "
                                     + " {}",
                                     projectConfigPlus.getOrigin(),
                                     nextThreshold.sourceLocation().getLineNumber(),
                                     nextThreshold.sourceLocation().getColumnNumber(),
                                     thresholdData,
                                     PLEASE_UPDATE );

                        result = false;
                        continue;
                    }

                    File destinationFile = destinationPath.toFile();

                    if ( !destinationFile.canRead() || destinationFile.isDirectory() )
                    {
                        LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                     + " The path {} is not a readable file."
                                     + " {}",
                                     projectConfigPlus.getOrigin(),
                                     nextThreshold.sourceLocation().getLineNumber(),
                                     nextThreshold.sourceLocation().getColumnNumber(),
                                     thresholdData,
                                     PLEASE_UPDATE );

                        result = false;
                    }
                }
            }
        }

        return result;
    }


    /**
     * Validates graphics portion, similar to isProjectValid, but targeted.
     *
     * @param projectConfigPlus the project configuration
     * @return true if the graphics configuration is valid, false otherwise
     */

    private static boolean isGraphicsPortionOfProjectValid( ProjectConfigPlus projectConfigPlus )
    {

        boolean result = true;

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        for ( DestinationConfig d : projectConfig.getOutputs()
                                                 .getDestination() )
        {
            String customString = projectConfigPlus.getGraphicsStrings()
                                                   .get( d );

            if ( customString != null )
            {
                result = Validation.isCustomGraphicsStringValid( projectConfigPlus,
                                                                 d,
                                                                 customString )
                         && result;
            }
        }

        return result;
    }


    /**
     * Validates a single custom graphics string from a given destination config
     * @param projectConfigPlus the project configuration
     * @param d the destination config we are validating
     * @param customString the non-null string we have already gotten from d
     * @return true if the string is valid, false otherwise
     * @throws NullPointerException when any args are null
     */

    private static boolean isCustomGraphicsStringValid( ProjectConfigPlus projectConfigPlus,
                                                        DestinationConfig d,
                                                        String customString )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );
        Objects.requireNonNull( d, NON_NULL );
        Objects.requireNonNull( customString, NON_NULL );

        boolean result = true;

        final String BEGIN_TAG = "<chartDrawingParameters>";
        final String END_TAG = "</chartDrawingParameters>";
        final String BEGIN_COMMENT = "<!--";
        final String END_COMMENT = "-->";

        // For to give a helpful message, find closeby tag without NPE
        Locatable nearbyTag = Validation.getNearbyTag( d );

        // If a custom vis config was provided, make sure string either
        // starts with the correct tag or starts with a comment.
        String trimmedCustomString = customString.trim();

        if ( !trimmedCustomString.startsWith( BEGIN_TAG )
             && !trimmedCustomString.startsWith( BEGIN_COMMENT ) )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " If custom graphics configuration is "
                             + "provided, please start it with "
                             + BEGIN_TAG,
                             projectConfigPlus.getOrigin(),
                             nearbyTag.sourceLocation().getLineNumber(),
                             nearbyTag.sourceLocation()
                                      .getColumnNumber() );
            }

            result = false;
        }

        if ( !trimmedCustomString.endsWith( END_TAG )
             && !trimmedCustomString.endsWith( END_COMMENT ) )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " If custom graphics configuration is "
                             + "provided, please end it with "
                             + END_TAG,
                             projectConfigPlus.getOrigin(),
                             nearbyTag.sourceLocation().getLineNumber(),
                             nearbyTag.sourceLocation()
                                      .getColumnNumber() );
            }

            result = false;
        }

        return result;
    }

    private static Locatable getNearbyTag( DestinationConfig d )
    {
        Locatable nearbyTag;

        if ( d.getGraphical() != null
             && d.getGraphical().getConfig() != null )
        {
            // Best case
            nearbyTag = d.getGraphical().getConfig();
        }
        else if ( d.getGraphical() != null )
        {
            // Not as targeted but close
            nearbyTag = d.getGraphical();
        }
        else
        {
            // Destination tag.
            nearbyTag = d;
        }

        return nearbyTag;
    }

    private static boolean isPairConfigValid( ProjectConfigPlus projectConfigPlus )
    {
        boolean result = true;

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        PairConfig pairConfig = projectConfig.getPair();

        TimeScaleConfig aggregationConfig =
                pairConfig.getDesiredTimeScale();

        if ( aggregationConfig != null
             && TimeScaleOuter.of( aggregationConfig ).isInstantaneous() )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " In the pair configuration, the aggregation "
                             + "duration provided for pairing is prescriptive "
                             + "so it cannot be 'instant' it needs to be "
                             + "one of the other time units such as 'hour'.";

                LOGGER.warn( msg,
                             projectConfigPlus.getOrigin(),
                             aggregationConfig.sourceLocation().getLineNumber(),
                             aggregationConfig.sourceLocation()
                                              .getColumnNumber() );
            }

            result = false;
        }

        result = Validation.areFeaturesValid( pairConfig.getFeature() )
                 && result;

        result = Validation.areDatesValid( projectConfigPlus,
                                           pairConfig.getDates() )
                 && result;

        result = Validation.areDatesValid( projectConfigPlus,
                                           pairConfig.getIssuedDates() )
                 && result;

        result = Validation.areLeadTimesValid( projectConfigPlus,
                                               pairConfig.getLeadHours() )
                 && result;

        result = Validation.isSeasonValid( projectConfigPlus,
                                           pairConfig )
                 && result;

        result = Validation.isDesiredTimeScaleValid( projectConfigPlus,
                                                     pairConfig )
                 && result;

        result = Validation.areTimeWindowsValid( projectConfigPlus, pairConfig )
                 && result;

        return result;
    }

    static boolean areFeaturesValid( List<Feature> features )
    {
        boolean valid = true;

        valid = Validation.doesEachFeatureHaveSomethingDeclared( features )
                && valid;
        List<String> leftRawNames = Validation.getFeatureNames( features,
                                                                LeftOrRightOrBaseline.LEFT );
        List<String> rightRawNames = Validation.getFeatureNames( features,
                                                                 LeftOrRightOrBaseline.RIGHT );
        List<String> baselineRawNames = getFeatureNames( features,
                                                         LeftOrRightOrBaseline.BASELINE );
        valid = Validation.validateFeatureNames( leftRawNames,
                                                 LeftOrRightOrBaseline.LEFT )
                && valid;
        valid = Validation.validateFeatureNames( rightRawNames,
                                                 LeftOrRightOrBaseline.RIGHT )
                && valid;
        valid = Validation.validateFeatureNames( baselineRawNames,
                                                 LeftOrRightOrBaseline.BASELINE )
                && valid;
        return valid;
    }

    /**
     * Each attribute is individually optional in feature, but at least one must
     * be present.
     * @param features The features to check.
     * @return True when every feature has at least one of the attributes.
     */

    private static boolean doesEachFeatureHaveSomethingDeclared( List<Feature> features )
    {
        int countOfEmptyFeatures = 0;

        for ( Feature feature : features )
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
            LOGGER.warn( "Found {} features with no left nor right nor baseline name declared.",
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

    private static List<String> getFeatureNames( List<Feature> features,
                                                 LeftOrRightOrBaseline leftOrRightOrBaseline )

    {
        List<String> allNames = new ArrayList<>( features.size() );

        if ( leftOrRightOrBaseline.equals( LeftOrRightOrBaseline.LEFT ) )
        {
            for ( Feature feature : features )
            {
                allNames.add( feature.getLeft() );
            }
        }
        else if ( leftOrRightOrBaseline.equals( LeftOrRightOrBaseline.RIGHT ) )
        {
            for ( Feature feature : features )
            {
                allNames.add( feature.getRight() );
            }
        }
        else if ( leftOrRightOrBaseline.equals( LeftOrRightOrBaseline.BASELINE ) )
        {
            for ( Feature feature : features )
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
        Set<String> distinctNames = new HashSet<>( names.size() );
        Set<String> duplicateNames = new HashSet<>( 1 );
        int nullCount = 0;
        int blankCount = 0;

        for ( String name : names )
        {
            if ( Objects.nonNull( name )
                 && distinctNames.contains( name ) )
            {
                duplicateNames.add( name );
            }
            else if ( Objects.isNull( name ) )
            {
                // As long as there is a service declared, OK, but don't add to
                // the Set.
                nullCount++;
            }
            else if ( name.isBlank() )
            {
                blankCount++;
            }
            else
            {
                distinctNames.add( name );
            }
        }

        if ( !duplicateNames.isEmpty() )
        {
            isValid = false;

            if ( LOGGER.isWarnEnabled() )
            {
                // TODO: Enhance WRES to make this statement no longer true.
                LOGGER.warn( "Found multiple instances of these names on the {}: {}. {}",
                             leftOrRightOrBaseline.value(),
                             duplicateNames,
                             "This version of WRES requires that a feature be declared no more than once." );
            }
        }

        if ( blankCount > 0 )
        {
            isValid = false;

            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( "Found {} blank feature name(s) on {}. Instead of {}{}",
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
                    if ( LOGGER.isWarnEnabled() )
                    {
                        String msg = FILE_LINE_COLUMN_BOILERPLATE
                                     + " Data from the future cannot be"
                                     + " requested from USGS; the latest date"
                                     + " specified was "
                                     + latestRaw
                                     + " but it is currently "
                                     + now;

                        LOGGER.warn( msg,
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

        if ( Objects.nonNull( interfaceShortHand )
             && interfaceShortHand.equals( InterfaceShortHand.USGS_NWIS ) )
        {
            return true;
        }
        else if ( Objects.nonNull( uri )
                  && Objects.nonNull( uri.getPath() )
                  && uri.getPath()
                        .toLowerCase()
                        .contains( "/nwis/" ) )
        {
            return true;
        }

        return false;
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
            if ( LOGGER.isWarnEnabled() )
            {
                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " In the pair configuration, the text '"
                             + date
                             + "' was not able to be converted to an "
                             + "Instant. Please use the ISO8601 format, the UTC"
                             + " zoneOffset, and second-precision, e.g. "
                             + "'2017-11-27 17:36:00Z'.";

                LOGGER.warn( msg,
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

            LOGGER.warn( msg,
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
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " The month {} and day {} combination does "
                             + "not appear to be valid. Please use numeric "
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
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " The month {} and day {} combination does "
                             + "not appear to be valid. Please use numeric "
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
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
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

    private static boolean isDesiredTimeScaleValid( ProjectConfigPlus projectConfigPlus,
                                                    PairConfig pairConfig )
    {
        DesiredTimeScaleConfig aggregationConfig = pairConfig.getDesiredTimeScale();

        // No configuration, must be valid
        if ( aggregationConfig == null )
        {
            return true;
        }

        boolean valid = true;

        StringBuilder warning = new StringBuilder();

        // Non-null frequency must be >= 1
        if ( aggregationConfig.getFrequency() != null && aggregationConfig.getFrequency() < 0 )
        {
            valid = false;

            if ( warning.length() > 0 )
            {
                warning.append( System.lineSeparator() );
            }

            warning.append( "A time aggregation frequency of " +
                            aggregationConfig.getFrequency()
                            +
                            " is not valid; it must be at least 1 in order to "
                            +
                            "move on to the next window." );
        }

        if ( aggregationConfig.getPeriod() < 1 )
        {
            valid = false;

            if ( warning.length() > 0 )
            {
                warning.append( System.lineSeparator() );
            }

            warning.append( "The period of a window for time aggregation " +
                            "must be at least 1." );
        }

        // TODO: validate time units

        if ( !valid && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( warning.toString() );
        }

        // Check that the time aggregation function is valid
        valid = isDesiredTimeScaleFunctionValid( projectConfigPlus, pairConfig ) && valid;

        // Check that the time aggregation period is valid 
        valid = isDesiredTimeScalePeriodValid( projectConfigPlus, pairConfig ) && valid;

        return valid;
    }

    /**
     * Returns true if the time aggregation function associated with the desiredTimeScale is valid given the time
     * aggregation functions associated with the existingTimeScale for each source.
     * 
     * See Redmine issue 40389.
     * 
     * Not all attributes of a valid aggregation can be checked from the configuration alone, but some attributes, 
     * notably whether the aggregation function is applicable, can be checked in advance. Having a valid time 
     * aggregation function does not imply that the system actually supports it.
     * 
     * @param projectConfigPlus the project configuration
     * @param pairConfig the pair configuration
     * @return true if the time aggregation function associated with the desiredTimeScaleis valid
     */

    private static boolean isDesiredTimeScaleFunctionValid( ProjectConfigPlus projectConfigPlus,
                                                            PairConfig pairConfig )
    {
        boolean returnMe = true;

        TimeScaleFunction desired = pairConfig.getDesiredTimeScale().getFunction();
        Inputs inputConfig = projectConfigPlus.getProjectConfig().getInputs();

        // Time aggregation is a sum
        if ( TimeScaleFunction.TOTAL.equals( desired ) )
        {
            returnMe = isDesiredTimeScaleSumValid( projectConfigPlus, inputConfig );
        }

        return returnMe;
    }

    /**
     * Tests a desired time aggregation function that is a sum. Returns true if function is valid, given the input 
     * configuration, false otherwise.
     * 
     * @param projectConfigPlus the project configuration
     * @param inputConfig the input configuration
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
     * configuration for a specific input, false otherwise.
     * 
     * @param projectConfigPlus the project configuration
     * @param inputConfig the input configuration
     * @param helper a helper string for context
     * @return true if the time aggregation function is valid, given the inputConfig
     */

    private static boolean isDesiredTimeScaleSumValid( ProjectConfigPlus projectConfigPlus,
                                                       TimeScaleConfig inputConfig,
                                                       String helper )
    {
        boolean returnMe = true;
        // Existing aggregation cannot be an instant
        if ( TimeScaleOuter.of( inputConfig ).isInstantaneous() )
        {
            returnMe = false;

            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                         + " When using a desired time aggregation of {}, "
                         + "the existing time aggregation on the {} cannot "
                         + "be instantaneous.",
                         projectConfigPlus.getOrigin(),
                         inputConfig.sourceLocation().getLineNumber(),
                         inputConfig.sourceLocation().getColumnNumber(),
                         TimeScaleFunction.TOTAL,
                         helper );
        }

        // Existing function must be a sum
        if ( !inputConfig.getFunction()
                         .equals( TimeScaleFunction.TOTAL ) )
        {
            returnMe = false;

            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                         + " When using a desired time aggregation of {}, "
                         + "the existing time aggregation on the {} "
                         + "must also be a {}.",
                         projectConfigPlus.getOrigin(),
                         inputConfig.sourceLocation().getLineNumber(),
                         inputConfig.sourceLocation().getColumnNumber(),
                         TimeScaleFunction.TOTAL,
                         helper,
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
     * Not all attributes of a valid aggregation can be checked from the configuration alone, but some attributes, 
     * can be checked in advance. Having a valid time aggregation period does not imply that the system actually 
     * supports aggregation to that period.
     * 
     * @param projectConfigPlus the project configuration
     * @param pairConfig the pair configuration
     * @return true if the time aggregation period associated with the desiredTimeScale is valid
     */

    private static boolean isDesiredTimeScalePeriodValid( ProjectConfigPlus projectConfigPlus,
                                                          PairConfig pairConfig )
    {
        // Only proceed if the desiredTimeScale is non-null and one or more existingTimeScale
        // are non-null
        TimeScaleConfig timeAgg = pairConfig.getDesiredTimeScale();
        Inputs input = projectConfigPlus.getProjectConfig().getInputs();
        TimeScaleConfig left = input.getLeft().getExistingTimeScale();
        TimeScaleConfig right = input.getRight().getExistingTimeScale();
        TimeScaleConfig baseline = null;
        if ( input.getBaseline() != null )
        {
            baseline = input.getBaseline().getExistingTimeScale();
        }
        if ( timeAgg == null )
        {
            return true;
        }
        if ( left == null && right == null && baseline == null )
        {
            return true;
        }

        // Assume fine
        boolean returnMe = true;

        Duration desired = Duration.of( timeAgg.getPeriod(),
                                        ChronoUnit.valueOf( timeAgg.getUnit().toString().toUpperCase() ) );
        if ( left != null && !TimeScaleOuter.of( left ).isInstantaneous() )
        {
            Duration leftExists = Duration.of( left.getPeriod(),
                                               ChronoUnit.valueOf( left.getUnit().toString().toUpperCase() ) );
            returnMe = isDesiredTimeScalePeriodConsistent( projectConfigPlus, desired, leftExists, left, "left" );
        }
        if ( right != null && !TimeScaleOuter.of( right ).isInstantaneous() )
        {
            Duration rightExists = Duration.of( right.getPeriod(),
                                                ChronoUnit.valueOf( right.getUnit().toString().toUpperCase() ) );
            returnMe = isDesiredTimeScalePeriodConsistent( projectConfigPlus,
                                                           desired,
                                                           rightExists,
                                                           right,
                                                           "right" )
                       && returnMe;
        }
        if ( baseline != null && !TimeScaleOuter.of( baseline ).isInstantaneous() )
        {
            Duration baselineExists = Duration.of( baseline.getPeriod(),
                                                   ChronoUnit.valueOf( baseline.getUnit().toString().toUpperCase() ) );
            returnMe = isDesiredTimeScalePeriodConsistent( projectConfigPlus,
                                                           desired,
                                                           baselineExists,
                                                           baseline,
                                                           "baseline" )
                       && returnMe;
        }
        return returnMe;
    }

    /**
     * Returns true if the desired aggregation period is consistent with the existing aggregation period, false 
     * otherwise. A time aggregation may be valid in principle without being supported by the system in practice.
     * 
     * @param projectConfigPlus the project configuration
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
            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
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
            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
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
     * @param pairConfig the pair configuration
     * @return true if the time windows are valid, otherwise false
     */

    private static boolean areTimeWindowsValid( ProjectConfigPlus projectConfigPlus, PairConfig pairConfig )
    {
        boolean valid = true;

        valid = Validation.isIssuedDatesPoolingWindowValid( projectConfigPlus, pairConfig );

        valid = valid && Validation.isLeadTimesPoolingWindowValid( projectConfigPlus, pairConfig );

        return valid;
    }

    /**
     * Checks for issued dates pooling windows and validates for consistency with other
     * declaration, as well as internal consistency.
     * 
     * @param projectConfigPlus the project declaration, which helps with messaging
     * @param pairConfig the pair configuration
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
                                  + "pooling windows:";

            // Check that the issued dates are defined
            if ( Objects.isNull( pairConfig.getIssuedDates() ) )
            {
                valid = false;

                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + "{} the minimum and maximum issued dates "
                             + "are required, but were not found.",
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

                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + "{} the minimum issued date was not found but is required.",
                                 projectConfigPlus.getOrigin(),
                                 pairConfig.getIssuedDates().sourceLocation().getLineNumber(),
                                 pairConfig.getIssuedDates().sourceLocation().getColumnNumber(),
                                 issuedBoiler );
                }

                // Check for the maximum
                if ( Objects.isNull( pairConfig.getIssuedDates().getLatest() ) )
                {
                    valid = false;

                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + "{} the maximum issued date was not found but is required.",
                                 projectConfigPlus.getOrigin(),
                                 pairConfig.getIssuedDates().sourceLocation().getLineNumber(),
                                 pairConfig.getIssuedDates().sourceLocation().getColumnNumber(),
                                 issuedBoiler );
                }
            }

            // Validate the contents
            valid = valid
                    && Validation.isPoolingWindowValid( projectConfigPlus, issuedDatesPoolingConfig, "issued dates" );

        }

        return valid;
    }

    /**
     * Checks for lead times pooling windows and validates for consistency with other
     * declaration, as well as internal consistency.
     * 
     * @param projectConfigPlus the project declaration, which helps with messaging
     * @param pairConfig the pair configuration
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
                                "pooling windows:";

            // Check that the lead times are defined
            if ( Objects.isNull( pairConfig.getLeadHours() ) )
            {
                valid = false;

                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + "{} the minimum and maximum lead times "
                             + "are required but were not found.",
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

                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + "{} the minimum lead time was not found but is required.",
                                 projectConfigPlus.getOrigin(),
                                 pairConfig.getLeadHours().sourceLocation().getLineNumber(),
                                 pairConfig.getLeadHours().sourceLocation().getColumnNumber(),
                                 leadBoiler );
                }

                // Check for the maximum
                if ( Objects.isNull( pairConfig.getLeadHours().getMaximum() ) )
                {
                    valid = false;

                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + "{} the maximum lead time was not found but is required.",
                                 projectConfigPlus.getOrigin(),
                                 pairConfig.getLeadHours().sourceLocation().getLineNumber(),
                                 pairConfig.getLeadHours().sourceLocation().getColumnNumber(),
                                 leadBoiler );
                }
            }

            // Validate the contents
            valid = valid && Validation.isPoolingWindowValid( projectConfigPlus, leadTimesPoolingConfig, "lead times" );

        }

        return valid;
    }

    /**
     * Validates the specified time windows. 
     * 
     * @param projectConfigPlus the project declaration, which helps with messaging
     * @param windowConfig the time window configuration
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

            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
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

            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
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
            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                         + " Cannot evaluate ensemble forecasts against ensemble forecasts.",
                         projectConfigPlus,
                         left.sourceLocation().getLineNumber(),
                         left.sourceLocation().getColumnNumber() );

            result = false;
        }

        result = Validation.areDataSourcesValid( projectConfigPlus,
                                                 left )
                 && result;

        result = Validation.areDataSourcesValid( projectConfigPlus,
                                                 right )
                 && result;

        if ( baseline != null )
        {
            result = Validation.areDataSourcesValid( projectConfigPlus,
                                                     baseline )
                     && result;
        }

        return result;
    }

    /**
     * Checks that given DataSourceConfig has at least one
     * DataSourceConfig.Source and checks validity of each inner
     * DataSourceConfig.Source.
     * @param projectConfigPlus the evaluation project configuration plus
     * @param dataSourceConfig the data source configuration to validate
     * @return true when valid, false otherwise.
     */

    private static boolean areDataSourcesValid( ProjectConfigPlus projectConfigPlus,
                                                DataSourceConfig dataSourceConfig )
    {
        boolean dataSourcesValid = true;

        if ( dataSourceConfig.getSource() == null )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + "A source needs to exist within each of the "
                             + "left and right sections of the "
                             + "configuration.",
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
                dataSourcesValid = Validation.isSourceValid( projectConfigPlus,
                                                             dataSourceConfig,
                                                             s );
            }
        }

        return dataSourcesValid;
    }

    /**
     * Checks the validity of an individual DataSourceConfig.Source
     * @param projectConfigPlus the evaluation project configuration
     * @param dataSourceConfig the dataSourceConfig being checked
     * @param source the source being checked
     * @return
     */
    private static boolean isSourceValid( ProjectConfigPlus projectConfigPlus,
                                          DataSourceConfig dataSourceConfig,
                                          DataSourceConfig.Source source )
    {
        boolean sourceValid = true;

        sourceValid = Validation.isDateConfigValid( projectConfigPlus, source )
                      && sourceValid;

        sourceValid = Validation.isURIDefinedInSourceWhenExpected( projectConfigPlus, source )
                      && sourceValid;

        if ( source.getFormat() == Format.WRDS )
        {
            sourceValid = Validation.isWRDSSourceValid( projectConfigPlus,
                                                        dataSourceConfig,
                                                        source )
                          && sourceValid;
        }

        return sourceValid;
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
            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE +
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
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE +
                             " WRDS observations are not supported. Please use WRDS on 'right' or 'baseline'",
                             projectConfigPlus,
                             source.sourceLocation().getLineNumber(),
                             source.sourceLocation().getColumnNumber() );
            }

            wrdsSourceValid = false;
        }

        DateCondition issuedDates = projectConfigPlus.getProjectConfig()
                                                     .getPair()
                                                     .getIssuedDates();
        if ( issuedDates == null )
        {
            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE + " "
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
            LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE + " "
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


        return wrdsSourceValid;
    }


    /**
     * Checks validity of date and time configuration such as zone and offset.
     * @param projectConfigPlus the config
     * @param source the particular source element to check
     * @return true if valid, false otherwise
     * @throws NullPointerException when any arg is null
     */

    static boolean isDateConfigValid( ProjectConfigPlus projectConfigPlus,
                                      DataSourceConfig.Source source )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );
        Objects.requireNonNull( source, NON_NULL );

        boolean result = true;

        if ( source.getZoneOffset() != null
             && source.getFormat() != null
             && source.getFormat().equals( Format.PI_XML ) )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " Please remove the zoneOffset from PI-XML "
                             + "source configuration. WRES requires PI-XML to "
                             + "include a zone offset in the data and will use "
                             + "that. If you wish to have data perform time "
                             + "travel for whatever reason, there is a "
                             + "separate <timeShift> configuration option "
                             +
                             "for that purpose.",
                             projectConfigPlus.getOrigin(),
                             source.sourceLocation().getLineNumber(),
                             source.sourceLocation().getColumnNumber() );
            }

            result = false;
        }

        return result;
    }
}
