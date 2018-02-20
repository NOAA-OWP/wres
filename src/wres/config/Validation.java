package wres.config;

import java.io.File;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.xml.bind.ValidationEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.xml.bind.Locatable;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DurationUnit;
import wres.config.generated.Feature;
import wres.config.generated.Format;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.config.generated.TimeScaleConfig;
import wres.config.generated.TimeScaleFunction;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.engine.statistics.metric.config.MetricConfigHelper;
import wres.engine.statistics.metric.config.MetricConfigurationException;
import wres.io.config.ConfigHelper;
import wres.io.config.ProjectConfigPlus;
import wres.util.Strings;


/**
 * Helps validate project configurations at a higher level than parser, with
 * detailed messaging.
 */

public class Validation
{

    private static final Logger LOGGER = LoggerFactory.getLogger( Validation.class );

    /** A message to display for programmers when null project config occurs */
    private static final String NON_NULL = "The ProjectConfigPlus must not be null";

    /** The warning message boilerplate for logger (includes 3 placeholders) */
    private static final String FILE_LINE_COLUMN_BOILERPLATE =
            "In file {}, near line {} and column {}, WRES found an issue with "
            + "the project configuration.";

    private Validation()
    {
        // prevent construction.
    }


    /**
     * Validates a list of {@link ProjectConfigPlus}. Returns true if the
     * projects validate successfully, false otherwise.
     *
     * @param projectConfiggies a list of project configurations to validate
     * @return true if the projects validate successfully, false otherwise
     */

    public static boolean validateProjects( List<ProjectConfigPlus> projectConfiggies )
    {
        boolean validationsPassed = true;

        // Validate all projects, not stopping until all are done
        for ( ProjectConfigPlus projectConfigPlus: projectConfiggies )
        {
            if ( !isProjectValid( projectConfigPlus ) )
            {
                validationsPassed = false;
            }
        }

        return validationsPassed;
    }


    /**
     * Quick validation of the project configuration, will return detailed
     * information to the user regarding issues about the configuration. Strict
     * for now, i.e. return false even on minor xml problems. Does not return on
     * first issue, tries to inform the user of all issues before returning.
     *
     * @param projectConfigPlus the project configuration
     * @return true if no issues were detected, false otherwise
     */

    private static boolean isProjectValid( ProjectConfigPlus projectConfigPlus )
    {
        // Assume valid until demonstrated otherwise
        boolean result = true;

        for ( ValidationEvent ve: projectConfigPlus.getValidationEvents() )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                if ( ve.getLocator() != null )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + " The parser said: {}",
                                 projectConfigPlus.getPath(),
                                 ve.getLocator().getLineNumber(),
                                 ve.getLocator().getColumnNumber(),
                                 ve.getMessage(),
                                 ve.getLinkedException() );
                }
                else
                {
                    LOGGER.warn( "In file {}, WRES found an issue with the "
                                 + "project configuration. The parser said: {}",
                                 projectConfigPlus.getPath(),
                                 ve.getMessage(),
                                 ve.getLinkedException() );
                }
            }

            // Any validation event means we fail.
            result = false;
        }

        // Validate data sources
        result = Validation.areDataSourceConfigsValid( projectConfigPlus )
                 && result;

        // Validate pair section
        result = Validation.isPairConfigValid( projectConfigPlus ) && result;
        
        // Validate metrics section
        result = Validation.isMetricsConfigValid( projectConfigPlus ) && result;        
        
        // Validate outputs section
        result = Validation.isOutputConfigValid( projectConfigPlus ) && result;

        // Validate graphics portion
        result = Validation.isGraphicsPortionOfProjectValid( projectConfigPlus )
                 && result;

        return result;
    }

    /**
     * Validates the metrics portion of the project config.
     * 
     * @param projectConfigPlus the project configuration
     * @return true if the output configuration is valid, false otherwise
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean isMetricsConfigValid( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        // Validate that metric configuration is internally consistent
        boolean result = Validation.isMetricConfigInternallyConsistent( projectConfigPlus );

        // Check that each named metric is consistent with the other configuration
        result = result && Validation.areMetricsConsistentWithOtherConfig( projectConfigPlus );

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

        // Validate that outputs are writeable directories
        return  Validation.areAllOutputPathsWriteableDirectories( projectConfigPlus );
    }

    /**
     * Checks that the metric configuration is internally consistent.
     * 
     * @param projectConfigPlus the project configuration
     * @return true if the metric configuration is internally consistent, false otherwise
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean isMetricConfigInternallyConsistent( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        // Must define one of metric or timeSeriesMetric
        List<MetricConfig> metrics = projectConfigPlus.getProjectConfig().getMetrics().getMetric();
        List<TimeSeriesMetricConfig> timeSeriesMetrics =
                projectConfigPlus.getProjectConfig().getMetrics().getTimeSeriesMetric();
        if( metrics.isEmpty() && timeSeriesMetrics.isEmpty() )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " No metrics are listed for calculation: add a regular metric or time-series metric.",
                             projectConfigPlus,
                             projectConfigPlus.getProjectConfig()
                                              .getMetrics()
                                              .sourceLocation()
                                              .getLineNumber(),
                             projectConfigPlus.getProjectConfig()
                                              .getMetrics()
                                              .sourceLocation()
                                              .getColumnNumber() );
            }
            return false;
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
                         projectConfigPlus.getProjectConfig().getMetrics()
                                          .sourceLocation()
                                          .getLineNumber(),
                         projectConfigPlus.getProjectConfig()
                                          .getMetrics()
                                          .sourceLocation()
                                          .getColumnNumber() );            
        }
        
        // Cannot define specific metrics together with all valid        
        for ( MetricConfig next : metrics )
        {
            //Unnamed metric
            if ( MetricConfigName.ALL_VALID == next.getName() && metrics.size() > 1 )
            {
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + " All valid' metrics cannot be requested alongside named metrics.",
                                 projectConfigPlus,
                                 next.sourceLocation().getLineNumber(),
                                 next.sourceLocation().getColumnNumber() );
                }
                return false;
            }
        }
        return true;
    }

    /**
     * Checks that the metric configuration is consistent with the other configuration.
     *
     * @param projectConfigPlus the project configuration
     * @return true if the metric name is valid
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean areMetricsConsistentWithOtherConfig( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        boolean result = true;

        ProjectConfig config = projectConfigPlus.getProjectConfig();
        List<MetricConfig> metrics = config.getMetrics().getMetric();
        for ( MetricConfig next : metrics )
        {
            // Named metric
            if ( Objects.nonNull( next.getName() ) && MetricConfigName.ALL_VALID != next.getName() )
            {
                try
                {
                    MetricConstants checkMe = MetricConfigHelper.from( next.getName() );

                    // Check that the named metric is consistent with any pooling window configuration
                    if ( projectConfigPlus.getProjectConfig().getPair().getIssuedDatesPoolingWindow() != null && checkMe != null
                         && ! ( checkMe.isInGroup( MetricOutputGroup.SCORE ) ) )
                    {
                        result = false;
                        if ( LOGGER.isWarnEnabled() )
                        {
                            LOGGER.warn( "In file {}, a metric named {} was requested, but is not allowed. "
                                         + "Only verification scores are allowed in "
                                         + "combination with a poolingWindow configuration.",
                                         projectConfigPlus.getPath(),
                                         next.getName() );
                        }
                    }
                    
                    // Check that the CRPS has an explicit baseline
                    if ( checkMe.equals( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE )
                         && config.getInputs().getBaseline() == null )
                    {
                        result = false;
                        if ( LOGGER.isWarnEnabled() )
                        {
                            LOGGER.warn( "In file {}, a metric named {} was requested, which requires an explicit "
                                    + "baseline. Remove this metric or add the required baseline configuration.",
                                         projectConfigPlus.getPath(),
                                         next.getName() );
                        }
                    }

                }
                // Handle the situation where a metric is recognized by the xsd but not by the ConfigMapper. This is
                // unlikely and implies an incomplete implementation of a metric by the system  
                catch ( MetricConfigurationException e )
                {
                    LOGGER.error( "In file {}, a metric named {} was requested, but is not recognized by the system.",
                                  projectConfigPlus.getPath(),
                                  next.getName() );
                    return false;
                }
            }
        }
        return result;
    }

    /**
     * Validates outputs portion of project config have writeable directories.
     *
     * @param projectConfigPlus the project configuration
     * @return true if all have writeable directories, false otherwise
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean areAllOutputPathsWriteableDirectories( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL);

        boolean result = true;

        // No outputs specified
        if ( projectConfigPlus.getProjectConfig()
                              .getOutputs() == null )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( "In file {}, no output configuration was found.",
                             projectConfigPlus.getPath() );
            }

            return false;
        }

        final String PLEASE_UPDATE = "Please update the project configuration "
                                     + "with an existing writeable directory "
                                     + "or create the directory already "
                                     + "specified.";

        for ( DestinationConfig d : projectConfigPlus.getProjectConfig()
                                                     .getOutputs()
                                                     .getDestination() )
        {
            Path destinationPath;
            try
            {
                destinationPath = Paths.get( d.getPath() );
            }
            catch ( InvalidPathException ipe )
            {
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + " The path {} could not be found. "
                                 + PLEASE_UPDATE,
                                 projectConfigPlus.getPath(),
                                 d.sourceLocation().getLineNumber(),
                                 d.sourceLocation().getColumnNumber(),
                                 d.getPath() );
                }

                result = false;
                continue;
            }

            File destinationFile = destinationPath.toFile();

            if ( !destinationFile.canWrite() || !destinationFile.isDirectory() )
            {
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + " The path {} was not a writeable directory."
                                 + " " + PLEASE_UPDATE,
                                 projectConfigPlus.getPath(),
                                 d.sourceLocation().getLineNumber(),
                                 d.sourceLocation().getColumnNumber(),
                                 d.getPath() );
                }

                result = false;
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
            // Check that the plot type is consistent with other configuration
            if ( projectConfig.getPair().getIssuedDatesPoolingWindow() != null
                 && d.getOutputType() != null
                 && d.getOutputType() != OutputTypeSelection.POOLING_WINDOW )
            {
                result = false;
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + " Cannot use poolingWindow configuration with a plot type of {}.",
                                 projectConfigPlus.getPath(),
                                 d.sourceLocation().getLineNumber(),
                                 d.sourceLocation()
                                  .getColumnNumber(),
                                 d.getOutputType() );
                }
            }
            else if ( projectConfig.getPair().getIssuedDatesPoolingWindow() == null
                      && d.getOutputType() != null
                      && d.getOutputType() == OutputTypeSelection.POOLING_WINDOW )
            {
                result = false;
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + " Cannot define a plot type of {} without poolingWindow configuration.",
                                 projectConfigPlus.getPath(),
                                 d.sourceLocation().getLineNumber(),
                                 d.sourceLocation()
                                  .getColumnNumber(),
                                 d.getOutputType() );
                }
            }

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
        Objects.requireNonNull( projectConfigPlus, NON_NULL);
        Objects.requireNonNull( d, NON_NULL);
        Objects.requireNonNull( customString, NON_NULL);

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

        if( !trimmedCustomString.startsWith( BEGIN_TAG )
            && !trimmedCustomString.startsWith( BEGIN_COMMENT ) )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " If custom graphics configuration is "
                             + "provided, please start it with "
                             + BEGIN_TAG,
                             projectConfigPlus.getPath(),
                             nearbyTag.sourceLocation().getLineNumber(),
                             nearbyTag.sourceLocation()
                                      .getColumnNumber() );
            }

            result = false;
        }

        if( !trimmedCustomString.endsWith( END_TAG )
            && !trimmedCustomString.endsWith( END_COMMENT ) )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " If custom graphics configuration is "
                             + "provided, please end it with "
                             + END_TAG,
                             projectConfigPlus.getPath(),
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
             && ConfigHelper.isInstantaneous( aggregationConfig ) )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " In the pair configuration, the aggregation "
                             + "duration provided for pairing is prescriptive "
                             + "so it cannot be 'instant' it needs to be "
                             + "one of the other time units such as 'hour'.";

                LOGGER.warn( msg,
                             projectConfigPlus.getPath(),
                             aggregationConfig.sourceLocation().getLineNumber(),
                             aggregationConfig.sourceLocation()
                                              .getColumnNumber() );
            }

            result = false;
        }

        result = Validation.areFeatureAliasesValid( projectConfigPlus,
                                                    pairConfig )
                 && result;

        result = Validation.areDatesValid ( projectConfigPlus,
                                            pairConfig.getDates() )
                 && result;

        result = Validation.areDatesValid ( projectConfigPlus,
                                            pairConfig.getIssuedDates() )
                 && result;

        result = Validation.isSeasonValid( projectConfigPlus,
                                           pairConfig )
                 && result;

        result = Validation.isDesiredTimeScaleValid( projectConfigPlus,
                                                           pairConfig)
                 && result;

        result = Validation.isPoolingWindowValid( pairConfig)
                && result;
        
        return result;
    }

    private static boolean areFeatureAliasesValid( ProjectConfigPlus projectConfigPlus,
                                                   PairConfig pairConfig )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );
        Objects.requireNonNull( pairConfig, NON_NULL );

        boolean result = true;

        if ( pairConfig.getFeature() != null )
        {
            SortedSet<String> alreadyUsed = new ConcurrentSkipListSet<>();
            for ( Feature f : pairConfig.getFeature() )
            {
                result = Validation.isFeatureAliasValid( projectConfigPlus,
                                                         f,
                                                         alreadyUsed )
                         && result;

                if ( f.getName() != null )
                {
                    alreadyUsed.add( f.getName() );
                }

                if ( f.getAlias() != null )
                {
                    alreadyUsed.addAll( f.getAlias() );
                }
            }
        }

        return result;
    }

    private static boolean areDatesValid( ProjectConfigPlus projectConfigPlus,
                                          DateCondition dates )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );

        boolean result = true;

        if ( dates != null )
        {
            String earliest = dates.getEarliest();
            String latest = dates.getLatest();
            if ( earliest != null )
            {
                result = Validation.isDateStringValid( projectConfigPlus,
                                                       dates,
                                                       earliest )
                         && result;
            }

            if ( latest != null )
            {
                result = Validation.isDateStringValid( projectConfigPlus,
                                                       dates,
                                                       latest )
                         && result;
            }
        }

        return result;
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
                             + date + "' was not able to be converted to an "
                             + "Instant. Please use the ISO8601 format, the UTC"
                             + " zoneOffset, and second-precision, e.g. "
                             + "'2017-11-27 17:36:00Z'.";

                LOGGER.warn( msg,
                             projectConfigPlus.getPath(),
                             locatable.sourceLocation().getLineNumber(),
                             locatable.sourceLocation()
                                      .getColumnNumber() );
            }

            result = false;
        }

        return result;
    }


    /**
     * Validates a given feature's aliases from a projectconfig.
     *
     * Expects caller to create a set of names already existing. If the name of
     * featureAliasConfig is present in that list, consider it invalid.
     *
     * @param projectConfigPlus the project config
     * @param featureConfig the feature with aliases to validate
     * @param stuffAlreadyUsed set of strings already-used as names or aliases
     * @return true when valid
     * @throws NullPointerException when any argument is null
     */

    private static boolean isFeatureAliasValid( ProjectConfigPlus projectConfigPlus,
                                                Feature featureConfig,
                                                SortedSet<String> stuffAlreadyUsed )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL );
        Objects.requireNonNull( featureConfig, NON_NULL );
        Objects.requireNonNull( stuffAlreadyUsed, NON_NULL );

        boolean result = true;

        final String ALREADY_USED = " The lid or alias {} was already used as "
                                    + "an lid or alias earlier. Any and all "
                                    + "aliases for an lid must be specified in "
                                    + "one stanza, e.g. <feature lid=\"{}\">"
                                    + "<alias>{}ONE</alias><alias>{}TWO"
                                    + "</alias></feature>.";

        List<String> aliases = featureConfig.getAlias();

        // There aren't any aliases to validate, therefore valid
        if ( aliases == null || aliases.isEmpty() )
        {
            return result;
        }

        String name = featureConfig.getLocationId();

        if ( Strings.hasValue(name) && name.length() > 0 )
        {
            if ( stuffAlreadyUsed.contains( name ) )
            {
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + ALREADY_USED,
                                 projectConfigPlus.getPath(),
                                 featureConfig.sourceLocation().getLineNumber(),
                                 featureConfig.sourceLocation().getColumnNumber(),
                                 name, name, name, name );
                }

                result = false;
            }
        }
        else
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " The lid represents the name of a location "
                             + " within the actual data. It cannot be missing. "
                             + "Please use the lid found in data, e.g. <name>"
                             + "DRRC2</name>.",
                             projectConfigPlus.getPath(),
                             featureConfig.sourceLocation().getLineNumber(),
                             featureConfig.sourceLocation().getColumnNumber() );
            }

            result = false;
        }

        for ( String alias : aliases )
        {
            if ( stuffAlreadyUsed.contains( alias ) )
            {
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + ALREADY_USED,
                                 projectConfigPlus.getPath(),
                                 featureConfig.sourceLocation().getLineNumber(),
                                 featureConfig.sourceLocation().getColumnNumber(),
                                 alias, name, name, name );
                }

                result = false;
            }

            if ( alias.length() <= 0 )
            {
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + " At least one non-empty <alias> must be "
                                 + "specified in each <featureAlias>. (Feature "
                                 + "aliases as a whole are optional.)",
                                 projectConfigPlus.getPath(),
                                 featureConfig.sourceLocation().getLineNumber(),
                                 featureConfig.sourceLocation().getColumnNumber() );
                }

                result = false;
            }
        }

        return result;
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
            try
            {
                MonthDay.of( season.getEarliestMonth(),
                             season.getEarliestDay() );
            }
            catch ( DateTimeException dte )
            {
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + " The month {} and day {} combination does "
                                 + "not appear to be valid. Please use numeric "
                                 + "month and numeric day, such as 4 for April "
                                 + "and 20 for 20th.",
                                 projectConfigPlus.getPath(),
                                 season.sourceLocation().getLineNumber(),
                                 season.sourceLocation().getColumnNumber(),
                                 season.getEarliestMonth(),
                                 season.getEarliestDay() );
                }
                result = false;
            }

            try
            {
                MonthDay.of( season.getLatestMonth(),
                             season.getLatestDay() );
            }
            catch ( DateTimeException dte )
            {
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + " The month {} and day {} combination does "
                                 + "not appear to be valid. Please use numeric "
                                 + "month and numeric day, such as 8 for August"
                                 + " and 30 for 30th.",
                                 projectConfigPlus.getPath(),
                                 season.sourceLocation().getLineNumber(),
                                 season.sourceLocation().getColumnNumber(),
                                 season.getLatestMonth(),
                                 season.getLatestDay() );
                }
                result = false;
            }
        }

        return result;
    }

    private static boolean isDesiredTimeScaleValid( ProjectConfigPlus projectConfigPlus,
                                                          PairConfig pairConfig )
    {
        TimeScaleConfig aggregationConfig = pairConfig.getDesiredTimeScale();

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
     * Returns true if the time aggregation function associated with the desiredTimeScaleis valid given the time
     * aggregation functions associated with the existingTimeScalefor each source.
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
        if ( desired.equals( TimeScaleFunction.SUM ) )
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
     * @return true if the time aggregation function is valid, given the inputConfig
     */    
    
    private static boolean isDesiredTimeScaleSumValid( ProjectConfigPlus projectConfigPlus,
                                                             TimeScaleConfig inputConfig,
                                                             String helper )
    {
        boolean returnMe = true;
        // Existing aggregation cannot be an instant
        if ( ConfigHelper.isInstantaneous( inputConfig ) )
        {
            returnMe = false;
            String message = " When using a desired time aggregation of "
                             + TimeScaleFunction.SUM
                             + ", the existing time aggregation on the {} cannot be instantaneous.";
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + message,
                             projectConfigPlus.getPath(),
                             inputConfig.sourceLocation().getLineNumber(),
                             inputConfig.sourceLocation().getColumnNumber(),
                             helper );
            }
        }
        
        // Existing function must be a sum
        if ( !inputConfig.getFunction()
                            .equals( TimeScaleFunction.SUM ) )
        {
            returnMe = false;
            String message = " When using a desired time aggregation of "
                    + TimeScaleFunction.SUM
                    + ", the existing time aggregation on the {} must also be a "
                    + TimeScaleFunction.SUM
                    + ".";
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + message,
                             projectConfigPlus.getPath(),
                             inputConfig.sourceLocation().getLineNumber(),
                             inputConfig.sourceLocation().getColumnNumber(),
                             helper );
            }
        }
        return returnMe;
    }    
    
    /**
     * Returns true if the time aggregation period associated with the desiredTimeScaleis valid given the time
     * aggregation periods associated with the existingTimeScalefor each source.
     * 
     * See Redmine issue 40389.
     * 
     * Not all attributes of a valid aggregation can be checked from the configuration alone, but some attributes, 
     * can be checked in advance. Having a valid time aggregation period does not imply that the system actually 
     * supports aggregation to that period.
     * 
     * @param projectConfigPlus the project configuration
     * @param pairConfig the pair configuration
     * @return true if the time aggregation period associated with the desiredTimeScaleis valid
     */

    private static boolean isDesiredTimeScalePeriodValid( ProjectConfigPlus projectConfigPlus,
                                                                PairConfig pairConfig )
    {
        // Only proceed if the desiredTimeScaleis non-null and one or more existingTimeScale
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
        if ( left != null && !ConfigHelper.isInstantaneous( left ) )
        {
            Duration leftExists = Duration.of( left.getPeriod(),
                                               ChronoUnit.valueOf( left.getUnit().toString().toUpperCase() ) );
            returnMe = isDesiredTimeScalePeriodConsistent( projectConfigPlus, desired, leftExists, left, "left" );
        }
        if ( right != null && !ConfigHelper.isInstantaneous( right) )
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
        if ( baseline != null && !ConfigHelper.isInstantaneous( baseline ) )
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
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " The desired time aggregation of the pairs is smaller than the existing time "
                             + "aggregation of the {}: disaggregation is not supported.",
                             projectConfigPlus.getPath(),
                             helper.sourceLocation().getLineNumber(),
                             helper.sourceLocation().getColumnNumber(),
                             helperString );
            }
        }
        // Desired is not an integer multiple of existing
        if ( desired.toMillis() % existing.toMillis() != 0 )
        {
            returnMe = false;
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " The desired time aggregation of the pairs is not an integer multiple of the "
                             + "existing time aggregation of the {}.",
                             projectConfigPlus.getPath(),
                             helper.sourceLocation().getLineNumber(),
                             helper.sourceLocation().getColumnNumber(),
                             helperString );
            }
        }
        return returnMe;
    }

    private static boolean isPoolingWindowValid( PairConfig pairConfig )
    {
        
        //No pooling config
        if( pairConfig.getIssuedDatesPoolingWindow() == null )
        {
            return true;
        }
        
        boolean valid = true;

        PoolingWindowConfig poolingConfig = pairConfig.getIssuedDatesPoolingWindow();
        StringBuilder warning = new StringBuilder();

        if (pairConfig.getIssuedDates() == null || pairConfig.getIssuedDates().getLatest() == null || pairConfig.getIssuedDates().getEarliest() == null)
        {
            valid = false;

            if (warning.length() > 0 )
            {
                warning.append( System.lineSeparator() );
            }

            warning.append("Both an earliest and latest date is required if "
                           + "data pooling is to be used. Please set the "
                           + "earliest and latest issue dates.");
        }

        // Non-null frequency must be >= 1. Otherwise there'd be an infinite loop
        if ( poolingConfig.getFrequency() != null && poolingConfig.getFrequency() < 1 )
        {
            valid = false;
            
            if ( warning.length() > 0 )
            {
                warning.append( System.lineSeparator() );
            }

            warning.append( "A time pooling frequency of " +
                            poolingConfig.getFrequency()
                            +
                            " is not valid; it must be at least 1 in order to "
                            +
                            "move on to the next window." );
        }

        if ( poolingConfig.getPeriod() != null && poolingConfig.getPeriod() < 0 )
        {
            valid = false;

            if ( warning.length() > 0 )
            {
                warning.append( System.lineSeparator() );
            }

            warning.append( "The period of a window for time pooling " +
                            "must be at least 0." );
        }
        
        // TODO: validate time units

        if ( !valid && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( warning.toString() );
        }

        return valid;
    }    
        
    private static boolean areDataSourceConfigsValid( ProjectConfigPlus projectConfigPlus )
    {
        boolean result = true;

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        DataSourceConfig left = projectConfig.getInputs().getLeft();
        DataSourceConfig right = projectConfig.getInputs().getRight();
        DataSourceConfig baseline = projectConfig.getInputs().getBaseline();

        if ( left.getType() == DatasourceType.SINGLE_VALUED_FORECASTS ||
             left.getType() == DatasourceType.ENSEMBLE_FORECASTS)
        {
            // The message is the same whether for period or duration
            if (LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " The left data source cannot be any type of forecast.",
                             projectConfigPlus,
                             left.sourceLocation().getLineNumber(),
                             left.sourceLocation().getColumnNumber() );
            }
        }

        result = Validation.isDataSourceConfigValid( projectConfigPlus,
                                                     left )
                 && result;

        result = Validation.isDataSourceConfigValid( projectConfigPlus,
                                                     right )
                 && result;

        if ( baseline != null )
        {
            result = Validation.isDataSourceConfigValid( projectConfigPlus,
                                                         baseline )
                     && result;
        }

        return result;
    }

    private static boolean isDataSourceConfigValid( ProjectConfigPlus projectConfigPlus,
                                                    DataSourceConfig dataSourceConfig )
    {
        boolean result = true;

        TimeScaleConfig timeAggregation = dataSourceConfig.getExistingTimeScale();

        if ( timeAggregation != null
             && timeAggregation.getUnit() == DurationUnit.NANOS )
        {
            boolean instantMakesSense = true;

            if ( timeAggregation.getPeriod() != 1 )
            {
                instantMakesSense = false;
            }

            if ( timeAggregation.getFrequency() != null
                 && timeAggregation.getFrequency() != 1 )
            {
                instantMakesSense = false;
            }

            // The message is the same whether for period or duration
            if ( !instantMakesSense && LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " When using 'instant' duration, the period (and"
                             + " frequency, if specified) must be 1.",
                             projectConfigPlus,
                             timeAggregation.sourceLocation().getLineNumber(),
                             timeAggregation.sourceLocation().getColumnNumber() );
            }

            result = instantMakesSense && result;
        }

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
                dataSourcesValid =
                        Validation.isDateConfigValid( projectConfigPlus,
                                                      s )
                        && dataSourcesValid;
            }
        }

        result = dataSourcesValid && result;

        return result;
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
                             + "separate <timeShift> configuration option " +
                             "for that purpose.",
                             projectConfigPlus.getPath(),
                             source.sourceLocation().getLineNumber(),
                             source.sourceLocation().getColumnNumber() );
            }

            result = false;
        }

        return result;
    }
}
