package wres.io.config;

import static wres.config.generated.SourceTransformationType.PERSISTENCE;
import static wres.io.project.Project.PairingMode.ROLLING;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.FeaturePlus;
import wres.config.MetricConfigException;
import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.ProjectConfigs;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.config.generated.Format;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.SourceTransformationType;
import wres.config.generated.ProjectConfig.Outputs;
import wres.config.generated.ThresholdFormat;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetric.ThresholdsByMetricBuilder;
import wres.datamodel.time.TimeWindow;
import wres.datamodel.time.generators.TimeWindowGenerator;
import wres.io.data.caching.Features;
import wres.io.project.Project;
import wres.io.reading.commaseparated.CommaSeparatedReader;
import wres.io.utilities.DataScripter;
import wres.io.utilities.NoDataException;
import wres.io.utilities.ScriptBuilder;
import wres.util.Strings;
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

    private static final ConcurrentMap<ProjectConfig, ConcurrentSkipListSet<String>> messages =
            new ConcurrentHashMap<>();

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

    // TODO: Move to Project Details
    // ... or wres-config if useful outside of wres-io
    public static boolean usesUSGSData( ProjectConfig projectConfig )
    {
        for ( DataSourceConfig.Source source : projectConfig.getInputs().getLeft().getSource() )
        {
            if ( source.getFormat() == Format.USGS )
            {
                return true;
            }
        }

        for ( DataSourceConfig.Source source : projectConfig.getInputs().getRight().getSource() )
        {
            if ( source.getFormat() == Format.USGS )
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
                if ( source.getFormat() == Format.USGS )
                {
                    return true;
                }
            }
        }

        return false;
    }

    public static boolean usesS3Data(ProjectConfig projectConfig)
    {
        boolean usesS3 = wres.util.Collections.exists(
                projectConfig.getInputs().getLeft().getSource(),
                source -> source.getFormat() != null &&
                          source.getFormat().equals( Format.S_3 )
        );

        usesS3 = usesS3 || wres.util.Collections.exists(
                projectConfig.getInputs().getRight().getSource(),
                source -> source.getFormat() != null &&
                          source.getFormat().equals( Format.S_3 )
        );

        if (!usesS3 && projectConfig.getInputs().getBaseline() != null)
        {
            usesS3 = wres.util.Collections.exists(
                    projectConfig.getInputs().getBaseline().getSource(),
                    source -> source.getFormat() != null &&
                              source.getFormat().equals( Format.S_3 )
            );
        }

        return usesS3;
    }

    // TODO: Move to wres-config
    public static boolean usesNetCDFData( ProjectConfig projectConfig )
    {
        boolean usesNetcdf = wres.util.Collections.exists(
                projectConfig.getInputs().getLeft().getSource(),
                source -> source.getFormat() != null &&
                          source.getFormat().equals( Format.NET_CDF )
        );

        usesNetcdf = usesNetcdf || wres.util.Collections.exists(
                projectConfig.getInputs().getRight().getSource(),
                source -> source.getFormat() != null &&
                          source.getFormat().equals( Format.NET_CDF )
        );

        if (!usesNetcdf && projectConfig.getInputs().getBaseline() != null)
        {
            usesNetcdf = wres.util.Collections.exists(
                    projectConfig.getInputs().getBaseline().getSource(),
                    source -> source.getFormat() != null &&
                              source.getFormat().equals( Format.NET_CDF )
            );
        }

        return usesNetcdf;
    }
    
    /**
     * Returns true if the input declaration contains one or more source formats that distribute time-series across
     * multiple sources. Currently returns true if any of {@link Format#NET_CDF} or {@link Format#S_3} is found.
     * 
     * TODO: Consider removing this helper when ingest becomes time-series-shaped and the way in which series are 
     * attached to sources no longer matters. Currently, that is the context in which this helper is being used. See
     * #65216. More generally, this helper is unlikely to have value, as source formats and their contents are 
     * orthogonal things, in general, and especially for highly generic formats, like netCDF.
     * 
     * @param dataSourceConfig the data source declaration
     * @return true if the declaration contains source formats that distribute time-series across sources
     * @throws NullPointerException if the input is null
     */

    public static boolean hasSourceFormatWithMultipleSourcesPerSeries( DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( dataSourceConfig );

        return dataSourceConfig.getSource()
                               .stream()
                               .anyMatch( next -> next.getFormat() == Format.NET_CDF
                                                  || next.getFormat() == Format.S_3 );

    }

    public static String getVariableFeatureClause( Feature feature, int variableId, String alias )
            throws SQLException
    {
        StringBuilder clause = new StringBuilder();

        Integer variableFeatureId = Features.getVariableFeatureID( feature, variableId );

        if ( variableFeatureId != null )
        {
            if ( Strings.hasValue( alias ) )
            {
                clause.append( alias ).append( "." );
            }

            clause.append( "variablefeature_id = " ).append( variableFeatureId );
        }

        return clause.toString();
    }

    public static Comparator<Feature> getFeatureComparator()
    {
        return ( feature, t1 ) -> {

            String featureDescription = getFeatureDescription( feature );
            String t1Description = getFeatureDescription( t1 );

            if ( Strings.hasValue( feature.getLocationId() ) &&
                 Strings.hasValue( t1.getLocationId() ) )
            {
                return feature.getLocationId().compareTo( t1.getLocationId() );
            }
            else
            {
                LOGGER.trace( "Either {} and {} have the same location ids or "
                              + "one or both of them don't have one.",
                              featureDescription,
                              t1Description );
            }

            if ( Strings.hasValue( feature.getGageId() ) &&
                 Strings.hasValue( t1.getGageId() ) )
            {
                return feature.getGageId().compareTo( t1.getGageId() );
            }
            else
            {
                LOGGER.trace( "Either {} and {} have the same gage ids or "
                              + "one or both of them don't have one.",
                              featureDescription,
                              t1Description );
            }

            if ( feature.getComid() != null &&
                 t1.getComid() != null )
            {
                return feature.getComid().compareTo( t1.getComid() );
            }
            else
            {
                LOGGER.trace( "Either {} and {} have the same com ids or "
                              + "one or both of them don't have one.",
                              featureDescription,
                              t1Description );
            }

            LOGGER.warn( "A proper comparison couldn't be made between {} and {}."
                         + " Now saying that {} is greater than {}.",
                         featureDescription,
                         t1Description,
                         featureDescription,
                         t1Description );
            return 1;
        };
    }

    public static int getValueCount( Project project,
                                     DataSourceConfig dataSourceConfig,
                                     Feature feature )
            throws SQLException
    {
        Integer variableId;
        String member;

        if ( project.getRight().equals( dataSourceConfig ) )
        {
            variableId = project.getRightVariableID();
            member = Project.RIGHT_MEMBER;
        }
        else if ( project.getLeft().equals( dataSourceConfig ) )
        {
            variableId = project.getLeftVariableID();
            member = Project.LEFT_MEMBER;
        }
        else
        {
            variableId = project.getBaselineVariableID();
            member = Project.BASELINE_MEMBER;
        }

        String variableFeatureClause = ConfigHelper.getVariableFeatureClause( feature, variableId, "" );

        DataScripter script = new DataScripter();

        script.addLine("SELECT COUNT(*)::int");
        if ( ConfigHelper.isForecast( dataSourceConfig ) )
        {
            script.addLine( "FROM wres.TimeSeries TS" );
            script.addLine( "INNER JOIN wres.TimeSeriesValue TSV" );
            script.addTab().addLine("ON TS.timeseries_id = TSV.timeseries_id" );
            script.addLine( "WHERE ", variableFeatureClause );
            script.addTab().addLine( "AND EXISTS (" );
            script.addTab(  2  ).addLine( "SELECT 1" );
            script.addTab(  2  ).addLine( "FROM wres.TimeSeriesSource TSS" );
            script.addTab(  2  ).addLine( "INNER JOIN wres.ProjectSource PS" );
            script.addTab(   3   ).addLine( "ON TSS.source_id = PS.source_id" );
            script.addTab(  2  ).addLine( "WHERE PS.project_id = ", project.getId() );
            script.addTab(   3   ).addLine( "AND PS.member = ", member );
            script.addTab(   3   ).addLine( "AND TSS.timeseries_id = TS.timeseries_id" );
            script.addTab(  2  ).addLine( ");" );
        }
        else
        {
            script.addLine( "FROM wres.Observation O" );
            script.addLine( "WHERE ", variableFeatureClause );
            script.addLine( "    AND EXISTS (" );
            script.addLine( "        SELECT 1" );
            script.addLine( "        FROM wres.ProjectSource PS" );
            script.addLine( "        WHERE PS.project_id = ", project.getId() );
            script.addLine( "            AND PS.member = ", member );
            script.addLine( "            AND PS.source_id = O.source_id" );
            script.addLine( "    );" );
        }

        return script.retrieve( "count" );
    }

    /**
     * Creates a hash for the indicated project configuration based on its
     * specifications and the data it has ingested
     * @param projectConfig The configuration for the project
     * @param leftHashesIngested A collection of the hashes for the left sided
     *                           source data
     * @param rightHashesIngested A collection of the hashes for the right sided
     *                            source data
     * @param baselineHashesIngested A collection of hashes representing the baseline
     *                               source data
     * @return A unique hash code for the project's circumstances
     */
    public static Integer hashProject( final ProjectConfig projectConfig,
                                final List<String> leftHashesIngested,
                                final List<String> rightHashesIngested,
                                final List<String> baselineHashesIngested )
    {
        StringBuilder hashBuilder = new StringBuilder(  );

        DataSourceConfig left = projectConfig.getInputs().getLeft();
        DataSourceConfig right = projectConfig.getInputs().getRight();
        DataSourceConfig baseline = projectConfig.getInputs().getBaseline();

        hashBuilder.append(left.getType().value());

        for ( EnsembleCondition ensembleCondition : left.getEnsemble())
        {
            hashBuilder.append(ensembleCondition.getName());
            hashBuilder.append(ensembleCondition.getMemberId());
            hashBuilder.append(ensembleCondition.getQualifier());
        }

        // Sort for deterministic hash result for same list of ingested
        Collection<String> sortedLeftHashes =
                wres.util.Collections.copyAndSort( leftHashesIngested );

        for ( String leftHash : sortedLeftHashes )
        {
            hashBuilder.append( leftHash );
        }

        hashBuilder.append(left.getVariable().getValue());
        hashBuilder.append(left.getVariable().getUnit());

        hashBuilder.append(right.getType().value());

        for ( EnsembleCondition ensembleCondition : right.getEnsemble())
        {
            hashBuilder.append(ensembleCondition.getName());
            hashBuilder.append(ensembleCondition.getMemberId());
            hashBuilder.append(ensembleCondition.getQualifier());
        }

        // Sort for deterministic hash result for same list of ingested
        Collection<String> sortedRightHashes =
                wres.util.Collections.copyAndSort( rightHashesIngested );

        for ( String rightHash : sortedRightHashes )
        {
            hashBuilder.append( rightHash );
        }

        hashBuilder.append(right.getVariable().getValue());
        hashBuilder.append(right.getVariable().getUnit());

        if (baseline != null)
        {

            hashBuilder.append(baseline.getType().value());

            for ( EnsembleCondition ensembleCondition : baseline.getEnsemble())
            {
                hashBuilder.append(ensembleCondition.getName());
                hashBuilder.append(ensembleCondition.getMemberId());
                hashBuilder.append(ensembleCondition.getQualifier());
            }


            // Sort for deterministic hash result for same list of ingested
            Collection<String> sortedBaselineHashes =
                    wres.util.Collections.copyAndSort( baselineHashesIngested );

            for ( String baselineHash : sortedBaselineHashes )
            {
                hashBuilder.append( baselineHash );
            }

            hashBuilder.append(baseline.getVariable().getValue());
            hashBuilder.append(baseline.getVariable().getUnit());
        }

        for ( Feature feature : projectConfig.getPair()
                                             .getFeature() )
        {
            hashBuilder.append( ConfigHelper.getFeatureDescription( feature ) );
        }

        return hashBuilder.toString().hashCode();
    }

    public static boolean isForecast( DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( dataSourceConfig );

        return dataSourceConfig.getType() == DatasourceType.SINGLE_VALUED_FORECASTS
               || dataSourceConfig.getType() == DatasourceType.ENSEMBLE_FORECASTS;
    }

    public static boolean isSimulation( DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( dataSourceConfig );
        
        return dataSourceConfig.getType() == DatasourceType.SIMULATIONS;
    }

    public static ProjectConfig read( final String path ) throws IOException
    {
        Path actualPath = Paths.get( path );
        ProjectConfigPlus configPlus = ProjectConfigPlus.from( actualPath );
        return configPlus.getProjectConfig();
    }

    /**
     * Returns the "earliest" datetime from given ProjectConfig Conditions
     *
     * TODO: Move to ProjectDetails since it is only used when project details are available
     *
     * @param config the project configuration
     * @return the most narrow "earliest" date, null otherwise
     */
    public static Instant getEarliestDateTimeFromDataSources( ProjectConfig config )
    {
        if ( config.getPair() == null )
        {
            return null;
        }

        String earliest = "";

        if ( config.getPair()
                   .getDates() != null
             && config.getPair()
                      .getDates()
                      .getEarliest() != null )
        {
            try
            {
                earliest = config.getPair()
                                 .getDates()
                                 .getEarliest();
                return Instant.parse( earliest );
            }
            catch ( DateTimeParseException dtpe )
            {
                String messageId = "date_parse_exception_earliest_date";
                if ( LOGGER.isWarnEnabled()
                     && ConfigHelper.messageSendPutIfAbsent( config,
                                                             messageId ) )
                {
                    LOGGER.warn( "Correct the date \"{}\" near line {} column "
                                 + "{} to ISO8601 format such as "
                                 + "\"2017-06-27T16:16:00Z\"",
                                 earliest,
                                 config.getPair()
                                       .getDates()
                                       .sourceLocation()
                                       .getLineNumber(),
                                 config.getPair()
                                       .getDates()
                                       .sourceLocation()
                                       .getColumnNumber() );
                }
                return null;
            }
        }
        else
        {
            String messageId = "no_earliest_date";
            if ( LOGGER.isInfoEnabled() && ConfigHelper.messageSendPutIfAbsent( config, messageId ) )
            {
                LOGGER.info( "No \"earliest\" date found in project. "
                             + "Use <dates earliest=\"yyyy-mm-ddThh:mm:ssZ\" "
                             + "latest=\"yyyy-mm-ddThh:mm:ssZ\" /> "
                             + "under <pair> (near line {} column {} of "
                             + "project file) to specify an earliest date.",
                             config.getPair()
                                   .sourceLocation()
                                   .getLineNumber(),
                             config.getPair()
                                   .sourceLocation()
                                   .getColumnNumber() );
            }
            return null;
        }
    }

    /**
     * Returns the earlier of any "latest" date specified in left or right datasource.
     * If only one date is specified, that one is returned.
     * If no dates for "latest" are specified, null is returned.
     *
     * TODO: Move to ProjectDetails since it is only used when that is available
     *
     * @param config the project configuration
     * @return the most narrow "latest" date, null otherwise.
     */
    public static Instant getLatestDateTimeFromDataSources( ProjectConfig config )
    {
        if ( config.getPair() == null )
        {
            return null;
        }

        String latest = "";

        if ( config.getPair()
                   .getDates() != null
             && config.getPair()
                      .getDates()
                      .getLatest() != null )
        {
            try
            {
                latest = config.getPair()
                               .getDates()
                               .getLatest();
                return Instant.parse( latest );
            }
            catch ( DateTimeParseException dtpe )
            {
                String messageId = "date_parse_exception_latest_date";
                if ( LOGGER.isWarnEnabled()
                     && ConfigHelper.messageSendPutIfAbsent( config,
                                                             messageId ) )
                {
                    LOGGER.warn( "Correct the date \"{}\" after line {} col {} "
                                 + "to ISO8601 format such as "
                                 + "\"2017-06-27T16:16:00Z\"",
                                 latest,
                                 config.getPair()
                                       .getDates()
                                       .sourceLocation()
                                       .getLineNumber(),
                                 config.getPair()
                                       .getDates()
                                       .sourceLocation()
                                       .getColumnNumber() );
                }
                return null;
            }
        }
        else
        {
            String messageId = "no_latest_date";
            if ( LOGGER.isTraceEnabled() && ConfigHelper.messageSendPutIfAbsent( config, messageId ) )
            {
                LOGGER.trace( "No \"latest\" date found in project. Use <dates earliest=\"2017-06-27T16:14:00Z\" latest=\"2017-07-06T11:35:00Z\" />  under <pair> (near line {} col {} of project file) to specify a latest date.",
                             config.getPair()
                                   .sourceLocation()
                                   .getLineNumber(),
                             config.getPair()
                                   .sourceLocation()
                                   .getColumnNumber() );

            }
            return null;
        }
    }

    /**
     * Returns true if the caller is the one who should log a particular message.
     *
     * The exact message is not contained here, just an ad-hoc ID for it,
     * created by the caller.
     *
     * May be too clever, may have a race condition. Rather have race condition
     * than too much locking, this is just for messaging.
     *
     * The idea is that when only one message should appear for the user about
     * a particular validation issue in the configuration (but multiple tasks
     * are able to log this message), the caller first asks this method if it
     * should be the one to log the validation message.
     *
     * @param projectConfig the configuration object to send a message about
     * @param message the identifier for the message to send
     * @return true if the caller should log
     */
    private static boolean messageSendPutIfAbsent( ProjectConfig projectConfig,
                                                   String message )
    {
        // In case we are the first to call regarding a given config:
        ConcurrentSkipListSet<String> possiblyNewSet = new ConcurrentSkipListSet<>();
        possiblyNewSet.add( message );

        ConcurrentSkipListSet<String> theSet = messages.putIfAbsent( projectConfig,
                                                                     possiblyNewSet );
        if ( theSet == null )
        {
            // this call was first to put a set for this config, return true.
            return true;
        }
        // this call was not the first to put a set for this config.
        return theSet.add( message );
    }

    // TODO: Should this move to wres.io.data.caching.Features?
    public static String getFeatureDescription( Feature feature )
    {
        String description = null;

        if ( feature != null )
        {
            if ( feature.getLocationId() != null
                 && !feature.getLocationId()
                            .trim()
                            .isEmpty() )
            {
                description = feature.getLocationId();
            }
            else if ( feature.getGageId() != null
                      && !feature.getGageId()
                                 .trim()
                                 .isEmpty() )
            {
                description = feature.getGageId();
            }
            else if ( feature.getComid() != null )
            {
                description = String.valueOf( feature.getComid() );
            }
            else if ( Strings.hasValue( feature.getName() ) )
            {
                description = feature.getName();
            }
            else if (feature.getCoordinate() != null)
            {
                description = feature.getCoordinate().getLongitude() + " " +
                              feature.getCoordinate().getLatitude();
            }
        }

        return description;
    }

    public static String getFeatureDescription( FeaturePlus featurePlus )
    {
        return ConfigHelper.getFeatureDescription( featurePlus.getFeature() );
    }


    /**
     * Get a comma separated description of a list of features.
     *
     * TODO: Should this move to wres.io.data.caching.Features?
     *
     * @param features the list of features to describe, nonnull
     * @return a description of all features
     * @throws NullPointerException when list of features is null
     */
    public static String getFeaturesDescription( List<Feature> features )
    {
        Objects.requireNonNull( features );
        StringJoiner result = new StringJoiner( ", ", "( ", " )" );

        for ( Feature feature : features )
        {
            String description = ConfigHelper.getFeatureDescription( feature );
            result.add( description );
        }

        return result.toString();
    }

    /**
     * Get all the destinations from a configuration for a particular type.
     * @param config the config to search through
     * @param type the type to look for
     * @return a list of destinations with the type specified
     * @throws NullPointerException when config or type is null
     */

    public static List<DestinationConfig> getDestinationsOfType( ProjectConfig config,
                                                                 DestinationType type )
    {
        Objects.requireNonNull( config, "Config must not be null." );
        Objects.requireNonNull( type, "Type must not be null." );

        List<DestinationConfig> result = new ArrayList<>();

        if ( config.getOutputs() == null
             || config.getOutputs().getDestination() == null )
        {
            LOGGER.debug( "No destinations specified for config {}", config );
            return java.util.Collections.unmodifiableList( result );
        }

        for ( DestinationConfig d : config.getOutputs().getDestination() )
        {
            if ( d.getType() == type )
            {
                result.add( d );
            }
        }

        return java.util.Collections.unmodifiableList( result );
    }

    /**
     * Get all the graphical destinations from a configuration.
     *
     * @param config the config to search through
     * @return a list of graphical destinations
     * @throws NullPointerException when config is null
     */

    public static List<DestinationConfig> getGraphicalDestinations( ProjectConfig config )
    {
        return getDestinationsOfType( config, DestinationType.GRAPHIC );
    }

    /**
     * Get all the numerical destinations from a configuration.
     *
     * @param config the config to search through
     * @return a list of numerical destinations
     * @throws NullPointerException when config is null
     */

    public static List<DestinationConfig> getNumericalDestinations( ProjectConfig config )
    {
        return getDestinationsOfType( config, DestinationType.NUMERIC );
    }

    /**
     * <p>Returns a {@link TimeWindow} from the input configuration using the specified lead time to form the interval
     * on the forecast horizon. The earliest and latest times on the UTC timeline are determined by whichever of the
     * following is available (in this order):</p> 
     * 
     * <ol>
     * <li>The valid times in {@link DateCondition#getEarliest()} and {@link DateCondition#getLatest()}; or</li>
     * <li>The issue times in {@link DateCondition#getEarliest()} and the {@link DateCondition#getLatest()}; or</li>
     * <li>The earliest and latest possible dates on the UTC timeline in valid time, namely {@link Instant#MIN} and 
     * {@link Instant#MAX}, respectively.</li>
     * </ol>
     * 
     * <p>Dates are parsed using {@link Instant#parse(CharSequence)} and an exception is thrown if the dates do not meet 
     * the associated formatting requirement. Validation of dates should be conducted at the earliest 
     * opportunity, which may be well before this point.</p>
     * 
     * TODO: replace this with upfront calculation of the time windows. 
     * See {@link TimeWindowGenerator}
     * 
     * @param project the project configuration
     * @param firstLead the earliest lead time
     * @param lastLead the latest lead time
     * @param sequenceStep the position of the window within a sequence
     * @return a time window 
     * @throws NullPointerException if the config is null
     * @throws DateTimeParseException if the configuration contains dates that cannot be parsed
     * @deprecated
     */
    @Deprecated( forRemoval = true )
    public static TimeWindow getTimeWindow( Project project,
                                            Duration firstLead,
                                            Duration lastLead,
                                            int sequenceStep )
    {
        // TODO: simplify this method, if possible
        Objects.requireNonNull( project );

        Instant earliestReferenceTime = Instant.MIN;
        Instant latestReferenceTime = Instant.MAX;
        Instant earliestValidTime = Instant.MIN;
        Instant latestValidTime = Instant.MAX;
        
        Duration beginningLead;

        // Set the lead durations
        if ( ProjectConfigs.hasTimeSeriesMetrics( project.getProjectConfig() ) )
        {
            beginningLead = firstLead;
        }
        else if ( project.getProjectConfig().getPair().getLeadTimesPoolingWindow() != null )
        {
            PoolingWindowConfig leadPoolingWindow =
                    project.getProjectConfig().getPair().getLeadTimesPoolingWindow();

            beginningLead =
                    lastLead.minus( leadPoolingWindow.getPeriod(),
                                      ChronoUnit.valueOf( leadPoolingWindow.getUnit().toString().toUpperCase() ) );
        }
        else
        {
            beginningLead = lastLead;
        }

        // Set the datetimes
        
        // Rolling sequence
        if ( project.getPairingMode() == ROLLING )
        {
            long frequencyOffset = TimeHelper.unitsToLeadUnits( project.getIssuePoolingWindowUnit(),
                                                                project.getIssuePoolingWindowFrequency() )
                                   * sequenceStep;

            earliestReferenceTime = Instant.parse( project.getEarliestIssueDate() );
            earliestReferenceTime = earliestReferenceTime.plus( frequencyOffset, ChronoUnit.MINUTES );

            if ( project.getIssuePoolingWindowPeriod() > 0 )
            {
                latestReferenceTime = earliestReferenceTime.plus( project.getIssuePoolingWindowPeriod(),
                                                                  ChronoUnit.valueOf( project.getIssuePoolingWindowUnit()
                                                                                             .toUpperCase() ) );
            }
            else
            {
                latestReferenceTime = earliestReferenceTime;
            }
            
            //Valid datetimes available
            if ( project.getEarliestDate() != null && project.getLatestDate() != null )
            {
                earliestValidTime = Instant.parse( project.getEarliestDate() );
                latestValidTime = Instant.parse( project.getLatestDate() );
            }

        }
        // No rolling sequence
        else
        {
            //Valid datetimes available
            if ( project.getEarliestDate() != null && project.getLatestDate() != null )
            {
                earliestValidTime = Instant.parse( project.getEarliestDate() );
                latestValidTime = Instant.parse( project.getLatestDate() );
            }

            //Issue datetimes available
            if ( project.getEarliestIssueDate() != null && project.getLatestIssueDate() != null )
            {
                earliestReferenceTime = Instant.parse( project.getEarliestIssueDate() );
                latestReferenceTime = Instant.parse( project.getLatestIssueDate() );
            }
        }

        return TimeWindow.of( earliestReferenceTime,
                              latestReferenceTime,
                              earliestValidTime,
                              latestValidTime,
                              beginningLead,
                              lastLead );
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
     * Returns true if the sourceConfig from projectConfig is a persistence
     * baseline element.
     * @param projectConfig the project config, not null
     * @param sourceConfig the source config, not null
     * @return true when sourceConfig indicates persistence baseline.
     * @throws NullPointerException when projectConfig or sourceConfig are null
     */

    public static boolean isPersistence( ProjectConfig projectConfig,
                                         DataSourceConfig sourceConfig )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( sourceConfig );

        return projectConfig.getInputs()
                            .getBaseline() != null
               && ConfigHelper.getLeftOrRightOrBaseline( projectConfig,
                                                         sourceConfig )
                              .equals( LeftOrRightOrBaseline.BASELINE )
               && projectConfig.getInputs()
                               .getBaseline()
                               .getTransformation() != null
               && projectConfig.getInputs()
                               .getBaseline()
                               .getTransformation()
                               .equals( PERSISTENCE );
    }


    /**
     * Report if the projectConfig has a persistence baseline
     * @param projectConfig the project config to look at
     * @return true if the projectConfig has persistence baseline, false otherwise
     * @throws NullPointerException when projectConfig or its inputs are null
     */

    public static boolean hasPersistenceBaseline( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( projectConfig.getInputs() );

        return projectConfig.getInputs().getBaseline() != null
               && ConfigHelper.isPersistence( projectConfig,
                                              projectConfig.getInputs().getBaseline() );
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
     * Creates a SQL snippet expected by the ScriptGenerator as part of a where
     * clause, filtering by season specified in the configuration passed in.
     * <br>
     * Caller is responsible for saying which column to compare to.
     * @param project the configuration, non-null
     * @param databaseColumnName the column name with a date or time to use
     * @param timeShift the amount of time to shift, null otherwise
     * @return a where clause sql snippet or empty string if no season
     * @throws NullPointerException when projectDetails or databaseColumnName is null
     * @throws DateTimeException when the values in the season are invalid
     */

    public static String getSeasonQualifier( Project project,
                                             String databaseColumnName,
                                             Duration timeShift )
    {
        Objects.requireNonNull( project, "projectDetails needs to exist" );
        Objects.requireNonNull( databaseColumnName, "databaseColumnName needs to exist" );

        ScriptBuilder script = new ScriptBuilder(  );
        if ( project.specifiesSeason() )
        {
            String dateTemplate = "MAKE_DATE(EXTRACT( YEAR FROM " + databaseColumnName + ")::INTEGER, %d, %d)";
            MonthDay earliestDay;
            MonthDay latestDay;
            boolean daysFlipped = false;

            if ( project.getEarliestDayInSeason().isBefore( project.getLatestDayInSeason()))
            {
                earliestDay = project.getEarliestDayInSeason();
                latestDay = project.getLatestDayInSeason();
            }
            else
            {
                daysFlipped = true;
                latestDay = project.getEarliestDayInSeason();
                earliestDay = project.getLatestDayInSeason();
            }

            String earliestConstraint = String.format( dateTemplate, earliestDay.getMonthValue(), earliestDay.getDayOfMonth());
            String latestConstraint = String.format( dateTemplate, latestDay.getMonthValue(), latestDay.getDayOfMonth() );

            if (timeShift != null)
            {
                earliestConstraint += " INTERVAL '" + timeShift + "'";
                latestConstraint += " INTERVAL '" + timeShift + "'";
            }

            if (daysFlipped)
            {
                script.addTab().addLine("AND ( -- The dates should wrap around the end of the year, ",
                                        "so we're going to check for values before the latest ",
                                        "date and after the earliest");
                script.addTab(  2  ).addLine(databaseColumnName, "::DATE <= ", earliestConstraint,
                                             " -- In the set [1/1, ", earliestDay.getMonthValue(),
                                             "/", earliestDay.getDayOfMonth(), "]");
                script.addTab(  2  ).addLine("OR ", databaseColumnName, "::DATE >= ", latestConstraint,
                                             " -- Or in the set [", latestDay.getMonthValue(),
                                             "/", latestDay.getDayOfMonth(), ", 12/31]");
                script.addTab().addLine(")");
            }
            else
            {
                script.addTab().addLine( "AND ", databaseColumnName + "::DATE >= ", earliestConstraint );
                script.addTab().addLine( "AND ", databaseColumnName, "::DATE <= ", latestConstraint );
            }
        }

        LOGGER.trace( "{}", script );

        return script.toString();
    }

    public static Duration getTimeShift(final DataSourceConfig dataSourceConfig)
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
     * Returns the first instance of the named metric configuration or null if no such configuration exists.
     * 
     * @param projectConfig the project configuration
     * @param metricName the metric name
     * @return the named metric configuration or null
     * @throws NullPointerException if one or both of the inputs are null
     */

    public static MetricConfig getMetricConfigByName( ProjectConfig projectConfig, MetricConfigName metricName )
    {
        Objects.requireNonNull( projectConfig, "Specify a non-null metric configuration as input." );
        Objects.requireNonNull( metricName, "Specify a non-null metric name as input." );

        for ( MetricsConfig next : projectConfig.getMetrics() )
        {
            Optional<MetricConfig> nextConfig =
                    next.getMetric().stream().filter( metric -> metric.getName().equals( metricName ) ).findFirst();
            if ( nextConfig.isPresent() )
            {
                return nextConfig.get();
            }
        }

        return null;
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
        if ( destinationConfig.getDecimalFormat() != null
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
     * <p>Returns the variable identifier from the project configuration. The identifier is one of the following in 
     * order of precedent:</p>
     * 
     * <p>If the variable identifier is required for the left and right:</p>
     * <ol>
     * <li>The label associated with the variable in the left source.</li>
     * <li>The label associated with the variable in the right source.</li>
     * <li>The value associated with the left variable.</li>
     * </ol>
     * 
     * <p>If the variable identifier is required for the baseline:</p>
     * <ol>
     * <li>The label associated with the variable in the baseline source.</li>
     * <li>The value associated with the baseline variable.</li>
     * </ol>
     * 
     * <p>In both cases, the last declaration is always present.</p>
     * 
     * @param projectConfig the project configuration
     * @param isBaseline is true if the variable name is required for the baseline
     * @return the variable identifier
     * @throws IllegalArgumentException if the baseline variable is requested and the input does not contain 
     *            a baseline source
     */

    public static String getVariableIdFromProjectConfig( ProjectConfig projectConfig, boolean isBaseline )
    {
        // Baseline required?
        if ( isBaseline )
        {
            // Has a baseline source
            if ( Objects.nonNull( projectConfig.getInputs().getBaseline() ) )
            {
                // Has a baseline source with a label
                if ( Objects.nonNull( projectConfig.getInputs().getBaseline().getLabel() ) )
                {
                    return projectConfig.getInputs().getBaseline().getVariable().getLabel();
                }
                // Only has a baseline source with a variable value
                return projectConfig.getInputs().getBaseline().getVariable().getValue();
            }
            throw new IllegalArgumentException( "Cannot identify the variable for the baseline as the input project "
                                                + "does not contain a baseline source." );
        }
        // Has a left source with a label 
        if ( Objects.nonNull( projectConfig.getInputs().getLeft().getVariable().getLabel() ) )
        {
            return projectConfig.getInputs().getLeft().getVariable().getLabel();
        }
        // Has a right source with a label
        else if ( Objects.nonNull( projectConfig.getInputs().getRight().getVariable().getLabel() ) )
        {
            return projectConfig.getInputs().getRight().getVariable().getLabel();
        }
        // Has a left source with a variable value
        return projectConfig.getInputs().getLeft().getVariable().getValue();
    }

    /**
     * Returns a path to write from a combination of the {@link DestinationConfig} and the {@link StatisticMetadata}.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @return a path to write
     * @throws NullPointerException if any input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWrite( Path outputDirectory,
                                             DestinationConfig destinationConfig,
                                             StatisticMetadata meta )
            throws IOException
    {
        return ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                  destinationConfig,
                                                  meta,
                                                  (String) null );
    }

    /**
     * Returns a path to write from a combination of the {@link DestinationConfig}, the {@link StatisticMetadata} 
     * associated with the results and a {@link TimeWindow}.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param timeWindow the time window
     * @param leadUnits the time units to use for the lead durations
     * @return a path to write
     * @throws NullPointerException if any input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWrite( Path outputDirectory,
                                             DestinationConfig destinationConfig,
                                             StatisticMetadata meta,
                                             TimeWindow timeWindow,
                                             ChronoUnit leadUnits )
            throws IOException
    {
        Objects.requireNonNull( destinationConfig, "Enter non-null time window to establish a path for writing." );

        Objects.requireNonNull( meta, "Enter non-null metadata to establish a path for writing." );

        Objects.requireNonNull( timeWindow, "Enter a non-null time window  to establish a path for writing." );

        Objects.requireNonNull( leadUnits,
                                "Enter a non-null time unit for the lead durations to establish a path for writing." );

        return ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                  destinationConfig,
                                                  meta,
                                                  TimeHelper.durationToLongUnits( timeWindow.getLatestLeadDuration(),
                                                                                  leadUnits )
                                                        + "_"
                                                        + leadUnits.name().toUpperCase() );
    }

    /**
     * Returns a path to write from a combination of the {@link DestinationConfig}, the {@link StatisticMetadata} 
     * associated with the results and a {@link OneOrTwoThresholds}.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param threshold the threshold
     * @return a path to write
     * @throws NullPointerException if any input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWrite( Path outputDirectory,
                                             DestinationConfig destinationConfig,
                                             StatisticMetadata meta,
                                             OneOrTwoThresholds threshold )
            throws IOException
    {
        Objects.requireNonNull( meta, "Enter non-null metadata to establish a path for writing." );

        Objects.requireNonNull( threshold, "Enter non-null threshold to establish a path for writing." );

        return getOutputPathToWrite( outputDirectory,
                                     destinationConfig,
                                     meta,
                                     threshold.toStringSafe() );
    }

    /**
     * Returns a path to write from a combination of the destination configuration, the input metadata and any 
     * additional string that should be appended to the path (e.g. lead time or threshold). 
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param append an optional string to append to the end of the path, may be null
     * @return a path to write
     * @throws NullPointerException if any required input is null, including the identifier associated 
     *            with the sample metadata
     * @throws IOException if the path cannot be produced
     * @throws ProjectConfigException when the destination configuration is invalid
     */

    public static Path getOutputPathToWrite( Path outputDirectory,
                                             DestinationConfig destinationConfig,
                                             StatisticMetadata meta,
                                             String append )
            throws IOException
    {
        Objects.requireNonNull( destinationConfig,
                                "Enter non-null destination configuration to establish "
                                                   + "a path for writing." );

        Objects.requireNonNull( meta, "Enter non-null metadata to establish a path for writing." );

        Objects.requireNonNull( meta.getSampleMetadata().getIdentifier(),
                                "Enter a non-null identifier for the metadata to establish "
                                                                          + "a path for writing." );


        // Build the path 
        StringJoiner joinElements = new StringJoiner( "_" );
        joinElements.add( meta.getSampleMetadata().getIdentifier().getGeospatialID().toString() )
                    .add( meta.getSampleMetadata().getIdentifier().getVariableID() );

        // Add optional scenario identifier
        if ( meta.getSampleMetadata().getIdentifier().hasScenarioID() )
        {
            joinElements.add( meta.getSampleMetadata().getIdentifier().getScenarioID() );
        }

        // Add the metric name()
        joinElements.add( meta.getMetricID().name() );

        // Add a non-default component name
        if ( meta.hasMetricComponentID() && MetricConstants.MAIN != meta.getMetricComponentID() )
        {
            joinElements.add( meta.getMetricComponentID().name() );
        }

        // Add optional append
        if ( Objects.nonNull( append ) )
        {
            joinElements.add( append );
        }

        // Add extension
        String extension;
        
        // Default graphic extension type
        if( destinationConfig.getType( ) == DestinationType.GRAPHIC )
        {
            extension = ".png";
        }
        // Default numeric extension type
        else if( destinationConfig.getType( ) == DestinationType.NUMERIC )
        {
            extension = ".csv";
        }
        // Specific type
        else
        {
            extension = destinationConfig.getType().name().toLowerCase();
        }
        
        // Derive a sanitized name
        String safeName = URLEncoder.encode( joinElements.toString().replace( " ", "_" ) + extension, "UTF-8" );

        return Paths.get( outputDirectory.toString(), safeName );
    }
    
    /**
     * Returns a formatted file name for writing outputs to a specific time window using the destination 
     * information and other hints provided.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination information
     * @param timeWindow the time window
     * @param output the output to write
     * @param leadUnits the time units to use for the lead durations
     * @return the file name
     * @throws NoDataException if the output is empty
     * @throws NullPointerException if any of the inputs is null
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWriteForOneTimeWindow( Path outputDirectory,
                                                             final DestinationConfig destinationConfig,
                                                             final TimeWindow timeWindow,
                                                             final Collection<DoubleScoreStatistic> output,
                                                             final ChronoUnit leadUnits )
            throws IOException
    {
        Objects.requireNonNull( outputDirectory, "Enter non-null output directory to establish a path for writing." );
        Objects.requireNonNull( destinationConfig, "Enter non-null time window to establish a path for writing." );

        Objects.requireNonNull( output, "Enter non-null output to establish a path for writing." );

        Objects.requireNonNull( timeWindow, "Enter a non-null time window  to establish a path for writing." );

        Objects.requireNonNull( leadUnits,
                                "Enter a non-null time unit for the lead durations to establish a path for writing." );

        if ( output.isEmpty() )
        {
            throw new NoDataException( "No data available to write for "
                                       + timeWindow  + ".");
        }

        StringJoiner filename = new StringJoiner( "_" );

        // Representative metadata for variable and scenario information
        SampleMetadata meta = output.iterator().next().getMetadata().getSampleMetadata();

        filename.add( meta.getIdentifier().getVariableID() );

        // Add optional scenario identifier
        if ( meta.getIdentifier().hasScenarioID() )
        {
            filename.add( meta.getIdentifier().getScenarioID() );
        }

        if ( !timeWindow.getLatestReferenceTime().equals( Instant.MAX ) )
        {
            String lastTime = timeWindow.getLatestReferenceTime().toString();

            // TODO: Format the last time in the style of "20180505T2046"
            // instead of "2018-05-05 20:46:00.000-0000"
            lastTime = lastTime.replaceAll( "-", "" )
                               .replaceAll( ":", "" )
                               .replace( "Z$", "" );

            filename.add( lastTime );
        }

        // Format the duration with the default format
        filename.add( Long.toString( TimeHelper.durationToLongUnits( timeWindow.getLatestLeadDuration(),
                                                                     leadUnits ) ) );
        filename.add( leadUnits.name().toUpperCase() );

        String extension = "";
        if ( destinationConfig.getType() == DestinationType.NETCDF )
        {
            extension = ".nc";
        }

        // Sanitize file name
        String safeName = URLEncoder.encode( filename.toString().replace( " ", "_" ) + extension, "UTF-8" );

        return Paths.get( outputDirectory.toString(), safeName );
    }

    /**
     * Reads all external sources of thresholds from the input configuration and returns a map containing a set of 
     * {@link Threshold} for each {@link FeaturePlus} in the source. If no source is provided, an empty map is
     * returned.
     * 
     * @param projectConfig the project configuration
     * @return the thresholds associated with each feature obtained from a source in the project configuration
     * @throws MetricConfigException if the metric configuration is invalid
     */

    public static Map<FeaturePlus, ThresholdsByMetric>
            readExternalThresholdsFromProjectConfig( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Map<FeaturePlus, ThresholdsByMetric> returnMe = new HashMap<>();

        // Obtain and read thresholds
        List<MetricsConfig> metrics = projectConfig.getMetrics();

        // Obtain any units for non-probability thresholds
        MeasurementUnit units = null;
        if ( Objects.nonNull( projectConfig.getPair() ) && Objects.nonNull( projectConfig.getPair().getUnit() ) )
        {
            units = MeasurementUnit.of( projectConfig.getPair().getUnit() );
        }

        for ( MetricsConfig nextGroup : metrics )
        {

            // Obtain the set of external thresholds to read
            Set<ThresholdsConfig> external = nextGroup.getThresholds()
                                                      .stream()
                                                      .filter( t -> t.getCommaSeparatedValuesOrSource() instanceof ThresholdsConfig.Source )
                                                      .collect( Collectors.toSet() );

            // Iterate the external sources and read them all into the map
            for ( ThresholdsConfig next : external )
            {
                // Add or append
                ConfigHelper.addExternalThresholdsForOneMetricConfigGroup( projectConfig,
                                                                           returnMe,
                                                                           nextGroup,
                                                                           next,
                                                                           units );
            }

        }

        return Collections.unmodifiableMap( returnMe );
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
               && baselineConfig.getTransformation() == SourceTransformationType.PERSISTENCE;
    }
    

    /**
     * Gets the desired time scale associated with the pair declaration, if any.
     * 
     * @param pairConfig the pair declaration
     * @return the desired time scale or null
     */

    public static TimeScale getDesiredTimeScale( PairConfig pairConfig )
    {
        TimeScale returnMe = null;

        if ( Objects.nonNull( pairConfig )
             && Objects.nonNull( pairConfig.getDesiredTimeScale() ) )
        {
            returnMe = TimeScale.of( pairConfig.getDesiredTimeScale() );
        }

        return returnMe;
    }
    
    /**
     * Mutates a map of thresholds, adding the thresholds for one metric configuration group.
     * 
     * @param projectConfig the project configuration
     * @param mutate the map of results to mutate
     * @param group The group of metrics to add the threshold to
     * @param units the optional units associated with the threshold values
     * @throws MetricConfigException if the metric configuration is invalid
     * @throws NullPointerException if any input is null
     */

    private static void
            addExternalThresholdsForOneMetricConfigGroup( ProjectConfig projectConfig,
                                                          Map<FeaturePlus, ThresholdsByMetric> mutate,
                                                          MetricsConfig group,
                                                          ThresholdsConfig thresholdsConfig,
                                                          MeasurementUnit units )
    {

        Objects.requireNonNull( mutate, "Specify a non-null map of thresholds to mutate." );

        Objects.requireNonNull( group, "Specify a non-null configuration group." );

        Objects.requireNonNull( group, "Specify non-null threshold configuration." );

        // Obtain the metrics
        Set<MetricConstants> metrics = DataFactory.getMetricsFromMetricsConfig( group, projectConfig );

        // Obtain the thresholds
        Map<FeaturePlus, ThresholdsByMetric> thresholdsByFeature =
                ConfigHelper.readOneExternalThresholdFromProjectConfig( thresholdsConfig, metrics, units );

        // Iterate the thresholds
        for ( Entry<FeaturePlus, ThresholdsByMetric> nextEntry : thresholdsByFeature.entrySet() )
        {
            // Feature exists in the uber map: mutate it
            if ( mutate.containsKey( nextEntry.getKey() ) )
            {
                ThresholdsByMetric union = mutate.get( nextEntry.getKey() ).unionWithThisStore( nextEntry.getValue() );
                mutate.put( nextEntry.getKey(), union );
            }
            // New feature: add a new map
            else
            {
                mutate.put( nextEntry.getKey(), nextEntry.getValue() );
            }
        }
    }

    /**
     * Reads a {@link ThresholdsConfig} and returns a corresponding {@link Set} of external {@link Threshold}
     * by {@link FeaturePlus}.
     * 
     * @param threshold the threshold configuration
     * @param metrics the metrics to which the threshold applies
     * @param units the optional units associated with the threshold values
     * @return a map of thresholds by feature
     * @throws MetricConfigException if the threshold could not be read
     * @throws NullPointerException if the threshold configuration is null or the metrics are null
     */

    private static Map<FeaturePlus, ThresholdsByMetric>
            readOneExternalThresholdFromProjectConfig( ThresholdsConfig threshold,
                                                       Set<MetricConstants> metrics,
                                                       MeasurementUnit units )
    {

        Objects.requireNonNull( threshold, "Specify non-null threshold configuration." );

        Objects.requireNonNull( threshold, "Specify non-null metrics." );

        Map<FeaturePlus, ThresholdsByMetric> returnMe = new TreeMap<>();

        ThresholdsConfig.Source nextSource = (ThresholdsConfig.Source) threshold.getCommaSeparatedValuesOrSource();

        // Pre-validate path
        if ( Objects.isNull( nextSource.getValue() ) )
        {
            throw new MetricConfigException( threshold, "Specify a non-null path to read for the external "
                                                        + "source of thresholds." );
        }
        // Validate format
        if ( nextSource.getFormat() != ThresholdFormat.CSV )
        {
            throw new MetricConfigException( threshold,
                                             "Unsupported source format for thresholds '"
                                                        + nextSource.getFormat() + "'" );
        }

        // Missing value?
        Double missing = null;

        if ( Objects.nonNull( nextSource.getMissingValue() ) )
        {
            missing = Double.parseDouble( nextSource.getMissingValue() );
        }

        //Path TODO: permit web thresholds. 
        // See #59422
        Path commaSeparated = Paths.get( nextSource.getValue() );

        // Condition: default to greater
        ThresholdConstants.Operator operator = ThresholdConstants.Operator.GREATER;
        if ( Objects.nonNull( threshold.getOperator() ) )
        {
            operator = DataFactory.getThresholdOperator( threshold );
        }

        // Data type: default to left
        ThresholdConstants.ThresholdDataType dataType = ThresholdConstants.ThresholdDataType.LEFT;
        if ( Objects.nonNull( threshold.getApplyTo() ) )
        {
            dataType = DataFactory.getThresholdDataType( threshold.getApplyTo() );
        }
        
        // Threshold type: default to probability
        ThresholdConstants.ThresholdGroup thresholdType = ThresholdConstants.ThresholdGroup.PROBABILITY;
        if ( Objects.nonNull( threshold.getType() ) )
        {
            thresholdType = DataFactory.getThresholdGroup( threshold.getType() );
        }

        // Default to probability
        boolean isProbability = thresholdType == ThresholdConstants.ThresholdGroup.PROBABILITY;

        try
        {
            Map<FeaturePlus, Set<Threshold>> read = CommaSeparatedReader.readThresholds( commaSeparated,
                                                                                         isProbability,
                                                                                         operator,
                                                                                         dataType,
                                                                                         missing,
                                                                                         units );

            // Add the thresholds for each feature
            for ( Entry<FeaturePlus, Set<Threshold>> nextEntry : read.entrySet() )
            {
                ThresholdsByMetricBuilder builder = new ThresholdsByMetricBuilder();
                Map<MetricConstants, Set<Threshold>> thresholds = new EnumMap<>( MetricConstants.class );

                // Add the thresholds for each metric in the group
                metrics.forEach( nextMetric -> thresholds.put( nextMetric, nextEntry.getValue() ) );
                
                // Add to builder
                builder.addThresholds( thresholds, thresholdType );

                // Add to store
                returnMe.put( nextEntry.getKey(), builder.build() );
            }
        }
        catch ( IOException e )
        {
            throw new MetricConfigException( threshold,
                                             "Failed to read the comma separated thresholds "
                                                        + "from '" + commaSeparated + "'.",
                                             e );
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

        // The only incremental type currently supported is DestinationType.NETCDF
        if ( Objects.nonNull( output )
             && output.getDestination().stream().anyMatch( type -> type.getType() == DestinationType.NETCDF ) )
        {
            return Set.of( DestinationType.NETCDF );
        }

        // Return empty set
        return Collections.emptySet();
    }

}
