package wres.io.project;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.MonthDay;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigs;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DesiredTimeScaleConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.DurationUnit;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.config.generated.MetricsConfig;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdType;
import wres.config.generated.TimeScaleFunction;
import wres.datamodel.metadata.MetadataHelper;
import wres.datamodel.metadata.RescalingException;
import wres.datamodel.metadata.TimeScale;
import wres.io.concurrency.Executor;
import wres.io.concurrency.OffsetEvaluator;
import wres.io.config.ConfigHelper;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.util.CalculationException;
import wres.util.FormattedStopwatch;
import wres.util.LRUMap;
import wres.util.TimeHelper;
import wres.util.functional.ExceptionalConsumer;

/**
 * Encapsulates operations involved with interpreting unpredictable data within the
 * project configuration, storing the results of expensive calculations, and forming database
 * statements based solely on the configured specifications
 *
 * TODO: refactor this class and make it immutable, ideally, but otherwise thread-safe. It's also
 * far too large (JBr). See #49511.
 * 
 * TODO: reconcile {@link #getScale()} with {@link #getDesiredTimeScale()}
 * and {@link #getDesiredTimeStep()}
 * The former currently mixes up two different things: time scale and time-step. 
 * Ultimately, we want one pathway to obtain the desired time scale for data 
 * retrieval, validation and metadata propagation. Currently, there are two pathways, 
 * which means that the the validation and metadata may not reflect the data retrieval 
 * reality. See #58715 and #44539-45. (JBr).
 */
public class Project
{
    /**
     * Controls the type of pair retrieval for the project
     *
     * <ul>
     *     <li>
     *         BASIC: Used for simplest case pairing
     *     </li>
     *     <li>
     *          ROLLING: Used when collecting values based on issuance
     *     </li>
     *     <li>
     *         TIME_SERIES: Used when pairing every single time series. Each grouping of pairs in this mode
     *                    is essentially a page of the entire data set
     *     </li>
     *     <li>
     *         BY_TIMESERIES: Used when all pairs in a window belong exclusively to a single time series
     *     </li>
     * </ul>
     */
    public enum PairingMode
    {
        BASIC,
        ROLLING,
        TIME_SERIES,
        BY_TIMESERIES
    }

    private static final Logger LOGGER = LoggerFactory.getLogger( Project.class );

    /**
     * Lock used to protect access to the logic used to determine the left hand scale
     */
    private static final Object LEFT_LEAD_LOCK = new Object();

    /**
     * Lock used to protect access to the logic used to determine the rightt hand scale
     */
    private static final Object RIGHT_LEAD_LOCK = new Object();

    /**
     * The member identifier for left handed data in the database
     */
    public static final String LEFT_MEMBER = "'left'";

    /**
     * The member identifier for right handed data in the database
     */
    public static final String RIGHT_MEMBER = "'right'";

    /**
     * The member identifier for baseline data in the database
     */
    public static final String BASELINE_MEMBER = "'baseline'";

    /**
     * Protects access and generation of the feature collection
     */
    private static final Object FEATURE_LOCK = new Object();

    private Integer projectID = null;
    private final ProjectConfig projectConfig;

    /**
     *  Stores the last possible lead time for each feature.
     *
     *  TODO: refector this class to make it thread-safe,
     *  TODO: #60331 - This can be removed; this is only hit once per feature.
     *  ideally immutable. Synchronizing access and setting the cache to 100 features avoids earlier problems 
     *  with reads and writes both occurring in the same code block with a check-then-act, but this is not a long-term
     *  Solution (JBr). See #49511. 
     */
    private final Map<Feature, Integer> lastLeads = java.util.Collections.synchronizedMap( new LRUMap<>( 100 ) );

    /**
     * Stores the lead unit offset for each feature. The unit is described by TimeHelper.LEAD_RESOLUTION
     *
     * <p>
     * If there is a 6 hour offset at feature x, it means that the lead times
     * used to match with observation data need to be increased by 6 hours to
     * provide a valid match
     * </p>
     */
    private final Map<Feature, Integer> leadOffsets = new ConcurrentSkipListMap<>( ConfigHelper.getFeatureComparator() );

    /**
     * Stores the earliest possible observation date for each feature
     * <p>
     *     Date strings are used because they are plugged right into queries.
     * </p>
     * <b>Used for script building</b>
     */
    private final Map<Feature, String> initialObservationDates = new LRUMap<>( 100 );

    /**
     * Stores the string dates for the first forecast for each feature
     *
     * <p>
     *     Initial dates are stored because they are somewhat expensive to determine and
     *     the parts of the codebase that need them are linked via the ProjectDetails
     * </p>
     * <p>
     *     Date strings are used because they are plugged right into queries.
     * </p>
     * <b>Used for script building</b>
     * <b>Used as part of the TIME_SERIES pairing method</b>
     */
    private final Map<Feature, String> initialForecastDates = new LRUMap<>( 100 );

    /**
     * Stores the number of lead units between each time series for a feature
     *
     * <p>
     *     Lags are stored because they are somewhat expensive to determine and
     *     the parts of the codebase that need them are linked via the ProjectDetails
     * </p>
     * <b>Used as part of the TIME_SERIES pairing method</b>
     */
    private final Map<Feature, Integer> timeSeriesLag = new LRUMap<>( 100 );

    /**
     * Stores the number of basis times pools for each feature
     *
     * <p>
     * Normally, there will be one pool per feature, but configuring an issue
     * times pooling window will possibly create multiple windows. If a feature
     * is deemed to have x pools, then each set of lead times will be evaluated
     * on x different sets of issue times
     * </p>
     */
    private final Map<Feature, Integer> poolCounts = new LRUMap<>( 100 );

    /**
     * The set of all features pertaining to the project
     */
    private Collection<FeatureDetails> features;

    /**
     * The ID for the variable on the left side of the input
     */
    private Integer leftVariableID = null;

    /**
     * The ID for the variable on the right side of the input
     */
    private Integer rightVariableID = null;

    /**
     * The ID for the variable for the baseline
     */
    private Integer baselineVariableID = null;

    /**
     * Guards access to numberOfSeriesToRetrieve
     */
    private static final Object SERIES_AMOUNT_LOCK = new Object();

    /**
     * Details the number of time series to pull at once when gathering by time series
     */
    private Integer numberOfSeriesToRetrieve = null;

    /**
     * Indicates whether or not this project was inserted on upon this
     * execution of the project
     */
    private boolean performedInsert;

    /**
     * The overall hash for the data sources used in the project
     */
    private final int inputCode;

    /**
     * The desired time scale. See {@link #getAndValidateDesiredTimeScale()}.
     * 
     * TODO: reconcile this with {@link #getScale()}, which currently mixes up 
     * two different things: time scale and time-step. Ultimately, we want one pathway
     * to obtain the desired time scale for data retrieval, validation and 
     * metadata propagation. Currently, there are two pathways, which means that the
     * the validation and metadata may not reflect the data retrieval reality. 
     * See #58715 and #44539-45. 
     */
    
    private TimeScale desiredTimeScale;
    
    /**
     * The least common time step associated with all ingested sources.
     * 
     * TODO: reconcile this with {@link #getScale()}, which currently mixes up 
     * two different things: time scale and time-step. 
     */
    
    private Duration leastCommonTimeStep;
    
    /**
     * The desired scale for the project
     *<p>
     * If the configuration doesn't specify the desired scale, the system
     * will determine that data itself
     * </p>
     */
    private DesiredTimeScaleConfig desiredTimeScaleConfig;

    private Boolean leftUsesGriddedData = null;
    private Boolean rightUsesGriddedData = null;
    private Boolean baselineUsesGriddedData = null;

    /**
     * Guards access to the pool counts
     */
    private static final Object POOL_LOCK = new Object();

    public Project( ProjectConfig projectConfig,
                    Integer inputCode )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( inputCode );
        this.projectConfig = projectConfig;
        this.inputCode = inputCode;
    }

    public ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    /**
     * Returns the name of the member that the DataSourceConfig belongs to
     *
     * <p>
     * If the "this.getInputName(this.getRight())" is called, the result
     * should be "ProjectDetails.RIGHT_MEMBER"
     *</p>
     * @param dataSourceConfig A DataSourceConfig from a project configuration
     * @return The name for how the DataSourceConfig relates to the project
     */
    public String getInputName(DataSourceConfig dataSourceConfig)
    {
        String name = null;

        if (dataSourceConfig.equals( this.getLeft() ))
        {
            name = Project.LEFT_MEMBER;
        }
        else if (dataSourceConfig.equals( this.getRight() ))
        {
            name = Project.RIGHT_MEMBER;
        }
        else if (dataSourceConfig.equals( this.getBaseline() ))
        {
            name = Project.BASELINE_MEMBER;
        }

        return name;
    }

    /**
     * Performs operations that are needed for the project to run between ingest and evaluation
     * @throws SQLException if retrieval of data from the database fails
     * @throws IOException if loading fails
     * @throws CalculationException if required calculations could not be completed
     */
    public void prepareForExecution() throws SQLException, IOException, CalculationException
    {
        // Sets and validates the time-scale information from the declaration and ingested sources
        // Also sets the desired time step of the output
        this.setDesiredTimeScaleAndTimeStep();

        if (!ConfigHelper.isSimulation( this.getRight() ) &&
            !ProjectConfigs.hasTimeSeriesMetrics( this.getProjectConfig() ) &&
            !this.usesGriddedData( this.getRight() ))
        {
            this.populateLeadOffsets();
        }
    }

    /**
     * Returns the set of FeaturesDetails for the project. If none have been
     * created yet, then it is evaluated. If there is no specification within
     * the configuration, all locations that have been ingested are retrieved
     * @return A set of all FeatureDetails involved in the project
     * @throws SQLException Thrown if details about the project's features
     * cannot be retrieved from the database
     */
    public Collection<FeatureDetails> getFeatures() throws SQLException
    {
        synchronized ( Project.FEATURE_LOCK )
        {
            if ( this.features == null )
            {
                this.populateFeatures();
            }
            return this.features;
        }
    }
    
    /**
     * Returns the desired time scale associated with the project, which 
     * is either the user-declared time scale (canonically) or the Least 
     * Common Scale determined from the source data. Returns null if the
     * time-scale has not yet been determined. The time-scale is actually
     * computed by {@link #setDesiredTimeScaleAndTimeStep()}.
     * 
     * TODO: reconcile with {@link #getScale()}, which currently mixes up 
     * two different things: time scale and time-step. Ultimately, we want 
     * one pathway to obtain the desired time scale for data retrieval, 
     * validation and metadata propagation. Currently, there are two 
     * pathways, which means that the the validation and metadata may not 
     * reflect the data retrieval reality. See #58715 and #44539-45. (JBr). 
     * 
     * @return the desired time scale or null
     */
    
    public TimeScale getDesiredTimeScale()
    {
        return this.desiredTimeScale;
    }
    
    /**
     * Returns the desired time-step associated with the project. Currently,
     * this is the larger of the following two sources of information:
     * <ol>
     * <li>The least common timestep from all ingested 
     * sources and;</li>
     * <li>The <code>period</code> associated with the <code>desiredTimeScale</code> 
     * from the project declaration, where applicable</li>
     * </ol>
     * 
     * TODO: if, and when, we support a desired time scale that is not
     * equal to the desired time step of the data (e.g. a 24h accumulation 
     * every 6h), refactor this method to return the least common timestep 
     * and refactor any dependencies on this method accordingly. Currently, 
     * we do not allow this and, hence, the returned {@link Duration} is 
     * always the larger of the least common timestep and the period 
     * associated with the <code>desiredTimeScale</code>. 
     * 
     * TODO: eliminate the <code>frequency</code> associated with the 
     * declared time scale information, because this is confusing and is 
     * unrelated to time scale.
     * 
     * TODO: reconcile this method with {@link #getScale()}, which should be
     * deprecated and removed.
     * 
     * @return the desired time step
     */

    public Duration getDesiredTimeStep()
    {
        // A desired time scale is declared
        if ( Objects.nonNull( this.getProjectConfig().getPair().getDesiredTimeScale() ) )
        {
            TimeScale timeScale = TimeScale.of( this.getProjectConfig().getPair().getDesiredTimeScale() );

            // The desired time scale is larger than the least common time step of
            // all ingested sources, so return that as the desired time step
            if ( timeScale.getPeriod().compareTo( this.leastCommonTimeStep ) > 0 )
            {
                return timeScale.getPeriod();
            }
        }

        // Return the least common time-step of all 
        // ingested sources
        return this.leastCommonTimeStep;
    }  
    
    /**
     * <p> Sets and validates the time scale and time step at which evaluation should be conducted.
     * 
     * <p>Determines the "desired time scale" information from the intersection of the declaration and
     * all ingested sources. The "desired time scale" is either declared explicitly (once per declaration) in 
     * the <code>desiredTimeScale</code> or it is determined by the system (see below).
     * 
     * <p>This method must be called post-ingest, when the existing time scale is maximally described,
     * either from the <code>existingTimeScale</code> in the project declaration or the information within the source 
     * (which is canonical). For the same reason, the <code>existingTimeScale</code> is *not* validated separately, but 
     * this method will validate the <code>existingTimeScale</code> insofar as it was used to describe the source in 
     * the database.
     * 
     * <p>When the declaration does not include a <code>desiredTimeScale</code>, an attempt is made to determine 
     * the Least Common Scale (LCS). The LCS is the smallest common multiple of the time scales associated with every 
     * ingested source. If the LCS cannot be determined, an exception is thrown.
     * 
     * </p>The desired time scale is validated against the existing time scale of each source separately. An 
     * exception is thrown if the validation fails for any one source.
     * 
     * <p> Uses the {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, Duration)} for
     * validation.
     * 
     * <p> Uses the {@link TimeScale#getLeastCommonTimeScale(Set)} to determine the LCS.
     * 
     * <p> In addition, it sets the time step for evaluation. Specifically, it sets the {@link #leastCommonTimeStep}
     * associated with all ingested sources using the {@link TimeScale#getLeastCommonDuration(Set)}. See also
     * the {@link #getDesiredTimeStep()}.
     * 
     * @throws SQLException Thrown on failing to obtain the time scale or time step information from the database
     * @throws CalculationException Thrown if no time scale or time step information is discovered
     * @throws RescalingException Thrown if the desired time scale is not deliverable from the existing sources
     */
    private void setDesiredTimeScaleAndTimeStep() throws SQLException, CalculationException
    {
        // The desired time scale
        TimeScale desiredScale = null;

        // Obtain from the declaration if available
        if ( Objects.nonNull( this.getProjectConfig().getPair() )
             && Objects.nonNull( this.getProjectConfig().getPair().getDesiredTimeScale() ) )
        {
            desiredScale = TimeScale.of( this.getProjectConfig().getPair().getDesiredTimeScale() );
        }

        // Obtain the existing time scale and corresponding time step for each ingested source and source type
        Set<Pair<TimeScale, Duration>> leftScalesAndSteps = this.getTimeScaleAndTimeStepForEachProjectSource( this.getLeft() );
        Set<Pair<TimeScale, Duration>> rightScalesAndSteps = this.getTimeScaleAndTimeStepForEachProjectSource( this.getRight() );
        Set<Pair<TimeScale, Duration>> baselineScalesAndSteps = this.getTimeScaleAndTimeStepForEachProjectSource( this.getBaseline() );
        
        // Union of scales and steps
        Set<Pair<TimeScale, Duration>> existingScalesAndSteps = new HashSet<>( leftScalesAndSteps );
        existingScalesAndSteps.addAll( rightScalesAndSteps );
        existingScalesAndSteps.addAll( baselineScalesAndSteps );
        
        // Determine the Least Common Scale if required and available
        if ( Objects.isNull( desiredScale ) )
        {
            LOGGER.debug( "No desired time scale declared. Attempting to substitute the Least Common Scale based on "
                          + "the ingested sources." );

            // The unique existing times scales from which to determined the Least Common Scale
            // Only include non-null time scales
            Set<TimeScale> existingScales =
                    existingScalesAndSteps.stream()
                                          .map( Pair::getLeft )
                                          .filter( Objects::nonNull )
                                          .collect( Collectors.toSet() );

            if ( !existingScales.isEmpty() )
            {
                desiredScale =
                        TimeScale.getLeastCommonTimeScale( java.util.Collections.unmodifiableSet( existingScales ) );
            }
            else
            {
                LOGGER.warn( "Could not determine the desired time scale of the data by analyzing the ingested sources. "
                             + "Assuming that the paired values have matching time scales." );
            }
        }

        // Validate the desired time scale against each existing one        
        if ( Objects.nonNull( desiredScale ) )
        {
            LOGGER.debug( "Found {} distinct time scales across all ingested sources. "
                          + "Validating each against the desired time scale of {}.",
                          existingScalesAndSteps.size(),
                          desiredScale );

            Project.validateTimeScale( Collections.unmodifiableSet( leftScalesAndSteps ),
                                       desiredScale,
                                       LeftOrRightOrBaseline.LEFT );
            Project.validateTimeScale( Collections.unmodifiableSet( rightScalesAndSteps ),
                                       desiredScale,
                                       LeftOrRightOrBaseline.RIGHT );
            Project.validateTimeScale( Collections.unmodifiableSet( baselineScalesAndSteps ),
                                       desiredScale,
                                       LeftOrRightOrBaseline.BASELINE );
        }
        
        LOGGER.debug( "Finished validating the {} existing time scales against the desired time scale.",
                      existingScalesAndSteps.size() );

        // Compute the desired time-step for the pairs, which is the least common duration of 
        // the existing time steps across all ingested sources
        Duration leastCommonStep =
                this.getLeastCommonTimeStepFromSources( leftScalesAndSteps, rightScalesAndSteps, baselineScalesAndSteps );

        LOGGER.debug( "Identified the least common time step associated with all ingested sources: {}.",
                      leastCommonStep );

        // Set the desired scale and least common time step
        this.desiredTimeScale = desiredScale;
        this.leastCommonTimeStep = leastCommonStep;
    }

    /**
     * Validates each of the existing time scales and time steps in the input against the 
     * desired time scale. 
     * 
     * @param existingScalesAndSteps the existing time scale and step information
     * @param desiredTimeScale the desired time scale
     * @param dataType the data type to validate, which helps with messaging
     * @throws a RescalingException if a proposed change of time scale is invalid
     * @throws NullPointerException if any input is null
     */

    private static void validateTimeScale( Set<Pair<TimeScale, Duration>> existingScalesAndSteps,
                                           TimeScale desiredTimeScale,
                                           LeftOrRightOrBaseline dataType )
    {
        Objects.requireNonNull( existingScalesAndSteps );
        
        Objects.requireNonNull( desiredTimeScale );
        
        Objects.requireNonNull( dataType );        
        
        final TimeScale finalDesiredScale = desiredTimeScale;
        // Only check where the existing scale and the time step is known
        existingScalesAndSteps.stream()
                              .filter( p -> Objects.nonNull( p.getLeft() ) && Objects.nonNull( p.getRight() ) )
                              .forEach( pair -> {
                                  try
                                  {
                                      MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( pair.getLeft(),
                                                                                             finalDesiredScale,
                                                                                             pair.getRight(),
                                                                                             dataType.toString() );
                                  }
                                  // Annotate and propagate
                                  catch ( RescalingException e )
                                  {
                                      throw new RescalingException( "Failed to validate the time scale and time step "
                                                                    + "information associated with one or more '"
                                                                    + dataType
                                                                    + "' sources.",
                                                                    e );
                                  }
                              } );
    }
    
    /**
     * Returns the {@link TimeScale} and time-step information for each source associated with the
     * input declaration. The time-step is represented as a {@link Duration}.
     * 
     * @param dataSourceConfig the data source declaration for which time information is required
     * @return the time scale and time-step information for each source
     * @throws SQLException Thrown on failing to obtain the time scale or time step information from the database
     * @throws CalculationException if time step could not be determined from the data
     */

    private Set<Pair<TimeScale, Duration>> getTimeScaleAndTimeStepForEachProjectSource( DataSourceConfig dataSourceConfig )
            throws SQLException, CalculationException
    {
        // Nothing to discover (e.g. if the input is a baseline source and there is no baseline)
        if( Objects.isNull( dataSourceConfig ) )
        {
            return java.util.Collections.emptySet();
        }
        
        Set<Pair<TimeScale,Duration>> existingTimeScales = new HashSet<>(  );
        
        LeftOrRightOrBaseline sourceType =
                ConfigHelper.getLeftOrRightOrBaseline( this.getProjectConfig(), dataSourceConfig );

        // Obtain the existing time scale and corresponding time step for each ingested source
        if ( this.usesGriddedData( dataSourceConfig ) )
        {
            DataScripter scripter;

            // Forecast type
            if ( ConfigHelper.isForecast( dataSourceConfig ) )
            {
                scripter = ProjectScriptGenerator.createGriddedForecastTimeRetriever( this.getId(),
                                                                                      sourceType );

            }
            
            // Observed type
            else
            {
                scripter = ProjectScriptGenerator.createGriddedObservedTimeRetriever( this.getId(),
                                                                                      sourceType );
            }

            existingTimeScales.addAll( this.getTimeScaleAndTimeStep( scripter,
                                                                     dataSourceConfig,
                                                                     sourceType ) );
        }
        // Non-gridded types
        else
        {
            // If right or baseline, check the wres.TimeSeries first, then wres.Observation
            if ( sourceType == LeftOrRightOrBaseline.RIGHT
                 || sourceType == LeftOrRightOrBaseline.BASELINE )
            {
                DataScripter forecast = ProjectScriptGenerator.createForecastTimeRetriever( this.getId(),
                                                                                            this.getFeatures(),
                                                                                            this.getMinimumLead(),
                                                                                            this.getMaximumLead(),
                                                                                            sourceType );
                existingTimeScales.addAll( this.getTimeScaleAndTimeStep( forecast, dataSourceConfig, sourceType ) );

                // Check the wres.Observation instead
                if ( existingTimeScales.isEmpty() )
                {
                    DataScripter observed = ProjectScriptGenerator.createObservedTimeRetriever( this.getId(),
                                                                                                this.getFeatures(),
                                                                                                sourceType );
                    existingTimeScales.addAll( this.getTimeScaleAndTimeStep( observed, dataSourceConfig, sourceType ) );
                }
            }
            // Currently, left sources are always in wres.Observation
            else
            {
                DataScripter observed = ProjectScriptGenerator.createObservedTimeRetriever( this.getId(),
                                                                                            this.getFeatures(),
                                                                                            sourceType );

                existingTimeScales.addAll( this.getTimeScaleAndTimeStep( observed, dataSourceConfig, sourceType ) );
            }

        }

        // No time scales: warn about this
        if ( existingTimeScales.isEmpty() )
        {
            LOGGER.warn( "Could not find the time-scale information for any {} source.", sourceType );
        }
    
        return java.util.Collections.unmodifiableSet( existingTimeScales );
    }

    /**
     * <p>Returns the time step and time scale information from a {@link DataScripter}, which provides an interface to
     * the data in the database, and a {@link DataSourceConfig}, which provides an interface to the declaration of
     * the data in the project.
     * 
     * <p/>When attempting to find the time scale, the database is canonical, but the declaration can augment the
     * database when the time scale information is missing for the data source. If they are both present and they
     * disagree, an exception is thrown.
     *
     * <p>The time step information is not available within the project declaration. If this cannot be determined
     * from the <code>time_step</code> attached to the time-series data, it is evaluated from the raw time-series
     * data using {@link #getValueInterval(DataSourceConfig)}.
     *
     * TODO: consider either removing the <code>time_step</code> from the database or populating it consistently on
     * ingest. See posts #58715-30 #58715-40 for more information.
     *
     * @param data the script that provides the time step and time scale information
     * @param dataSourceConfig the data source configuration
     * @param sourceType the source type to clarify logging
     * @return the pairs of time scale and time step
     * @throws SQLException Thrown on failing to obtain the time scale or time step information from the database
     * @throws CalculationException if the time step could not be determined from the data
     * @throws RescalingException if the time scale information associated with the data and the declaration disagree
     * @throws NullPointerException if either input is null
     */

    private Set<Pair<TimeScale, Duration>>
            getTimeScaleAndTimeStep( DataScripter data,
                                     DataSourceConfig dataSourceConfig,
                                     LeftOrRightOrBaseline sourceType )
                    throws SQLException, CalculationException
    {
        Objects.requireNonNull( data );

        Objects.requireNonNull( dataSourceConfig );

        Set<Pair<TimeScale, Duration>> existingTimeScales;

        // Declared existing time scale to use when the source has no information
        TimeScale declaredExistingTimeScale = null;

        if ( Objects.nonNull( dataSourceConfig.getExistingTimeScale() ) )
        {
            declaredExistingTimeScale = TimeScale.of( dataSourceConfig.getExistingTimeScale() );
        }

        // Consume each result from the wres.timeseries and create a time scale and a time-step for each
        try
        {
            TimeScaleAndTimeStepConsumer consumer =
                    new TimeScaleAndTimeStepConsumer( declaredExistingTimeScale, sourceType );

            // Consume
            data.consume( consumer );

            // Retrieve
            existingTimeScales = consumer.getTimeScalesAndSteps();

            // Log any warnings
            consumer.logAllWarningsOnCompletion();
        }
        // Decorate and propagate
        catch ( SQLException e )
        {
            throw new SQLException( "While attempting to retrieve the "
                                    + "time scale and time step information "
                                    + "for one or more "
                                    + sourceType
                                    + " sources:",
                                    e );
        }

        return finalizeExistingTimeScalesForSources( dataSourceConfig,
                                                     java.util.Collections.unmodifiableSet( existingTimeScales ),
                                                     sourceType );
    }

    /**
     * <p>Finalizes a map of existing time scales for ingested sources. Each <code>null</code> entry indicates a source
     * without a time-step and this is replaced with the global time-step from
     * {@link #getValueInterval(DataSourceConfig)}. If the map is empty, i.e. no sources have any time scale
     * information, and the declaration contains an <code>existingTimeScale</code>, that is added.
     *
     * <p>TODO: the {@link #getValueInterval(DataSourceConfig)} currently returns only one time-step for the first
     * available feature. This method will need to be updated to obtain the time-steps for all features. However, if
     * it does so without returning the time scale information too, then this method will need to add every possible
     * combination of time-step and time scale for each time scale with a <code>null</code> entry. This is overkill,
     * but may be preferable to a more complex implementation of {@link #getValueInterval(DataSourceConfig)} that
     * seeks to retain the pairing with the time scale too, especially since the overhead with checking the different
     * combinations of scale and step should be small. The debug log messages should then be updated too. See #58715-71.
     *
     * @param dataSourceConfig the declared data source
     * @param existingScalesAndSteps the map of existing time scales to augment
     * @param sourceType the source type to clarrify logging
     * @return the finalized pairs of time scale and time step for each source
     * @throws CalculationException if the global time-step is required and cannot be determined
     */

    private Set<Pair<TimeScale, Duration>> finalizeExistingTimeScalesForSources( DataSourceConfig dataSourceConfig,
                                                                                 Set<Pair<TimeScale, Duration>> existingScalesAndSteps,
                                                                                 LeftOrRightOrBaseline sourceType )
            throws CalculationException
    {
        Set<Pair<TimeScale, Duration>> mutableCopy = new HashSet<>();

        // Declared existing time scale to use when the source has no information
        TimeScale declaredExistingTimeScale = null;

        if ( Objects.nonNull( dataSourceConfig.getExistingTimeScale() ) )
        {
            declaredExistingTimeScale = TimeScale.of( dataSourceConfig.getExistingTimeScale() );
        }

        // If the time-step information was missing, analyze the raw data instead
        // TODO: as noted in the method description, the getValueInterval will need to account
        // for time-steps that vary across locations, at which point this will need to be updated to include
        // every possible pair of time-scale and time-step for each pair with a null time-step
        Duration globalTimeStepToReUse = null;
        boolean warn = false;
        for ( Pair<TimeScale, Duration> nextScaleAndStep : existingScalesAndSteps )
        {
            if ( Objects.isNull( nextScaleAndStep.getRight() ) )
            {
                // Compute the global time-step only where required
                if ( Objects.isNull( globalTimeStepToReUse ) )
                {
                    globalTimeStepToReUse = this.getValueIntervalWithLocking( dataSourceConfig );
                }

                warn = true;
                mutableCopy.add( Pair.of( nextScaleAndStep.getLeft(), globalTimeStepToReUse ) );
            }
            else
            {
                mutableCopy.add( nextScaleAndStep );
            }
        }

        // In some cases, the global time-step was used because there was no time_step associated with the source
        if ( warn )
        {
            LOGGER.debug( "The time_step was missing for one or more {} sources in the database. "
                          + "Using an analyzed time-step instead, which is not source-specific.",
                          sourceType );
        }

        // If no scale information was recorded in the database, i.e. nothing returned to consume, then add
        // the existing time scale as a minimum, where provided
        if ( mutableCopy.isEmpty() && Objects.nonNull( declaredExistingTimeScale ) )
        {
            if ( Objects.isNull( globalTimeStepToReUse ) )
            {
                globalTimeStepToReUse = this.getValueIntervalWithLocking( dataSourceConfig );
            }

            LOGGER.debug( "The time_step was missing for all {} sources in the database. "
                          + "Using an analyzed time-step instead, which is not source-specific.",
                          sourceType );

            mutableCopy.add( Pair.of( declaredExistingTimeScale, globalTimeStepToReUse ) );
        }

        return java.util.Collections.unmodifiableSet( mutableCopy );
    }

    /**
     * Wraps the {@link {@link #getValueInterval(DataSourceConfig)}} and performs locking because this method involves
     * mutation. TODO: it would be cleaner if the helper itself handled the locking.
     *
     * @param dataSourceConfig the data source declaration
     * @return the global time-step
     * @throws CalculationException if the global time-step could not be calculated
     * @throws NullPointerException if the input is null
     */

    private Duration getValueIntervalWithLocking( DataSourceConfig dataSourceConfig )
            throws CalculationException
    {
        Objects.requireNonNull( dataSourceConfig );
        
        LeftOrRightOrBaseline sourceType =
                ConfigHelper.getLeftOrRightOrBaseline( this.getProjectConfig(), dataSourceConfig );

        if ( sourceType == LeftOrRightOrBaseline.LEFT )
        {
            synchronized ( LEFT_LEAD_LOCK )
            {
                return this.getValueInterval( dataSourceConfig );
            }
        }
        else
        {
            synchronized ( RIGHT_LEAD_LOCK )
            {
                return this.getValueInterval( dataSourceConfig );
            }
        }
    }
    
    /**
     * Returns the least common time step from the input. Currently, this finds the least
     * value in each source type separately, and then determines the least common value from 
     * these. This avoids problems with missing sources or missing elements within sources,
     * which can lead to irregular time steps. 
     * 
     * TODO: Other approaches may be considered in future, such as the least common 
     * timestep of the modal timestep for each source type. See the discussion in #60032.
     * 
     * @param leftScalesAndSteps steps and scales for the left sources
     * @param rightScalesAndSteps steps and scales for the right sources
     * @param baselineScalesAndSteps steps and scales for the baseline sources
     * @return the least common time step of the least time step in each source
     * @throws NullPointerException if any input is null
     * @throws CalculationException if the least time-step cannot be determined for the left or right sources
     */

    private Duration getLeastCommonTimeStepFromSources( Set<Pair<TimeScale, Duration>> leftScalesAndSteps,
                                                        Set<Pair<TimeScale, Duration>> rightScalesAndSteps,
                                                        Set<Pair<TimeScale, Duration>> baselineScalesAndSteps )
            throws CalculationException
    {
        Objects.requireNonNull( leftScalesAndSteps );
        
        Objects.requireNonNull( rightScalesAndSteps );
        
        Objects.requireNonNull( baselineScalesAndSteps );
        
        TreeSet<Duration> leastLeft = leftScalesAndSteps.stream()
                                                        .map( Pair::getRight )
                                                        .filter( Objects::nonNull )
                                                        .collect( Collectors.toCollection( TreeSet::new ) );
        TreeSet<Duration> leastRight = rightScalesAndSteps.stream()
                                                          .map( Pair::getRight )
                                                          .filter( Objects::nonNull )
                                                          .collect( Collectors.toCollection( TreeSet::new ) );
        TreeSet<Duration> leastBaseline = baselineScalesAndSteps.stream()
                                                                .map( Pair::getRight )
                                                                .filter( Objects::nonNull )
                                                                .collect( Collectors.toCollection( TreeSet::new ) );

        Set<Duration> leastValues = new HashSet<>();

        // Check the left: required
        if ( !leastLeft.isEmpty() )
        {
            leastValues.add( leastLeft.first() );

            if ( leastLeft.size() > 1 )
            {
                LOGGER.warn( "The left sources do not have a consistent time step. "
                             + "Found {} different time steps across all left sources, namely: {}.",
                             leastLeft.size(),
                             leastLeft );
            }
        }
        else
        {
            throw new CalculationException( "Could not find the least time step of any left source." );
        }

        // Check the right: required
        if ( !leastRight.isEmpty() )
        {
            leastValues.add( leastRight.first() );
            if ( leastRight.size() > 1 )
            {
                LOGGER.warn( "The right sources do not have a consistent time step. "
                             + "Found {} different time steps across all right sources, namely: {}.",
                             leastRight.size(),
                             leastRight );
            }
        }
        else
        {
            throw new CalculationException( "Could not find the least time step of any right source." );
        }

        // Check the baseline: optional
        if ( !leastBaseline.isEmpty() )
        {
            leastValues.add( leastBaseline.first() );
            if ( leastBaseline.size() > 1 )
            {
                LOGGER.warn( "The right sources do not have a consistent time step. "
                             + "Found {} different time steps across all right sources, namely: {}.",
                             leastBaseline.size(),
                             leastBaseline );
            }
        }

        return TimeScale.getLeastCommonDuration( java.util.Collections.unmodifiableSet( leastValues ) );
    }
    
    
    /**
     * Loads metadata about all features that the project needs to use
     * @throws SQLException Thrown if metadata about the features could not be loaded from the database
     */
    private void populateFeatures() throws SQLException
    {
        synchronized ( Project.FEATURE_LOCK )
        {
            if ( this.usesGriddedData( this.getRight() ) )
            {
                this.features = Features.getGriddedDetails( this );
            }
            // If there is no indication whatsoever of what to look for, we
            // want to query the database specifically for all locations
            // that have data ingested pertaining to the project.
            //
            // The similar function in wres.io.data.caching.Features cannot be
            // used because it doesn't restrict data based on what lies in the
            // database.
            else if ( this.getProjectConfig().getPair().getFeature() == null ||
                      this.projectConfig.getPair().getFeature().isEmpty() )
            {
                this.features = new HashSet<>();

                DataScripter script = new DataScripter();

                // TODO: it is most likely safe to assume the left will be observations

                // Select all features where...
                script.addLine( "SELECT *" );
                script.addLine( "FROM wres.Feature F" );
                script.addLine( "WHERE EXISTS (" );

                if ( ConfigHelper.isForecast( this.getLeft() ) )
                {
                    // There is at least one value pertaining to a forecasted value
                    // indicated by the left hand configuration
                    script.addTab().addLine( "SELECT 1" );
                    script.addTab().addLine( "FROM wres.TimeSeries TS" );
                    script.addTab().addLine( "INNER JOIN wres.VariableFeature VF" );
                    script.addTab(  2  ).addLine("ON VF.variablefeature_id = TS.variablefeature_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.TimeSeriesSource TSS" );
                    script.addTab( 2 )
                          .addLine( "ON TSS.timeseries_id = TS.timeseries_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ProjectSource PS" );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = TSS.source_id" );
                    script.addTab()
                          .addLine( "WHERE PS.project_id = ", this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'left'" );
                    script.addTab( 2 )
                          .addLine( "AND VF.feature_id = F.feature_id" );
                }
                else
                {
                    // There is at least one observed value pertaining to the
                    // configuration for the left sided data
                    script.addTab().addLine( "SELECT 1" );
                    script.addTab().addLine( "FROM wres.Observation O" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.VariableFeature VF" );
                    script.addTab( 2 )
                          .addLine(
                                  "ON VF.variablefeature_id = O.variablefeature_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ProjectSource PS" );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = O.source_id" );
                    script.addTab()
                          .addLine( "WHERE PS.project_id = ", this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'left'" );
                    script.addTab( 2 )
                          .addLine( "AND VF.feature_id = F.feature_id" );
                }

                script.addTab().addLine( ")" );

                // And...
                script.addTab().addLine( "AND EXISTS (" );

                if ( ConfigHelper.isForecast( this.getRight() ) )
                {
                    // There is at least one value pertaining to a forecasted value
                    // indicated by the right hand configuration
                    script.addTab().addLine( "SELECT 1" );
                    script.addTab().addLine( "FROM wres.TimeSeries TS" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.VariableFeature VF" );
                    script.addTab( 2 )
                          .addLine(
                                  "ON VF.variablefeature_id = TS.variablefeature_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.TimeSeriesSource TSS" );
                    script.addTab( 2 )
                          .addLine( "ON TSS.timeseries_id = TS.timeseries_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ProjectSource PS" );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = TSS.source_id" );
                    script.addTab()
                          .addLine( "WHERE PS.project_id = ", this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'right'" );
                    script.addTab( 2 )
                          .addLine( "AND VF.feature_id = F.feature_id" );
                }
                else
                {
                    // There is at least one observed value pertaining to the
                    // configuration for the right sided data
                    script.addTab().addLine( "SELECT 1" );
                    script.addTab().addLine( "FROM wres.Observation O" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.VariableFeature VF" );
                    script.addTab( 2 )
                          .addLine(
                                  "ON VF.variablefeature_id = O.variablefeature_id" );
                    script.addTab()
                          .addLine( "INNER JOIN wres.ProjectSource PS" );
                    script.addTab( 2 )
                          .addLine( "ON PS.source_id = O.source_id" );
                    script.addTab()
                          .addLine( "WHERE PS.project_id = ", this.getId() );
                    script.addTab( 2 ).addLine( "AND PS.member = 'right'" );
                    script.addTab( 2 )
                          .addLine( "AND VF.feature_id = F.feature_id" );
                }

                script.addLine( ");" );

                try
                {
                    script.consume( feature -> this.features.add( new FeatureDetails(
                            feature ) ) );
                }
                catch ( SQLException e )
                {
                    throw new SQLException(
                            "The features for this project could "
                            + "not be retrieved from the database.",
                            e );
                }
            }
            else
            {
                // Get all features that correspond to the feature configuration
                this.features =
                        Features.getAllDetails( this.getProjectConfig() );
            }
        }
    }

    /**
     * @return The left hand data source configuration
     */
    public DataSourceConfig getLeft()
    {
        return this.projectConfig.getInputs().getLeft();
    }

    /**
     * @return The right hand data source configuration
     */
    public DataSourceConfig getRight()
    {
        return this.projectConfig.getInputs().getRight();
    }

    /**
     * @return The baseline data source configuration
     */
    public DataSourceConfig getBaseline()
    {
        return this.projectConfig.getInputs().getBaseline();
    }

    private Integer getVariableId(DataSourceConfig dataSourceConfig)
            throws SQLException
    {
        final String side = this.getInputName( dataSourceConfig );
        Integer variableId = null;

        if ( Project.LEFT_MEMBER.equals( side ))
        {
            variableId = this.getLeftVariableID();
        }
        else if ( Project.RIGHT_MEMBER.equals( side ))
        {
            variableId = this.getRightVariableID();
        }
        else if ( Project.BASELINE_MEMBER.equals( side ))
        {
            variableId = this.getBaselineVariableID();
        }

        return variableId;
    }

    /**
     * Determines the ID of the left variable
     * @return The ID of the left variable
     * @throws SQLException Thrown if the ID cannot be retrieved from the database
     */
    public Integer getLeftVariableID() throws SQLException
    {
        if (this.leftVariableID == null)
        {
            this.leftVariableID = Variables.getVariableID(this.getLeftVariableName());
        }

        return this.leftVariableID;
    }

    /**
     * @return The name of the left variable
     */
    private String getLeftVariableName()
    {
        return this.getLeft().getVariable().getValue();
    }

    /**
     * Determines the ID of the right variable
     * @return The ID of the right variable
     * @throws SQLException Thrown if the ID cannot be retrieved from the database
     */
    public Integer getRightVariableID() throws SQLException
    {
        if (this.rightVariableID == null)
        {
            this.rightVariableID = Variables.getVariableID( this.getRightVariableName());
        }

        return this.rightVariableID;
    }

    /**
     * @return The name of the right variable
     */
    public String getRightVariableName()
    {
        return this.getRight().getVariable().getValue();
    }

    /**
     * Determines the ID of the baseline variable
     * @return The ID of the baseline variable
     * @throws SQLException Thrown if the ID cannot be retrieved from the database
     */
    public Integer getBaselineVariableID() throws SQLException
    {
        if (this.hasBaseline() && this.baselineVariableID == null)
        {
            this.baselineVariableID = Variables.getVariableID( this.getBaselineVariableName() );
        }

        return this.baselineVariableID;
    }

    /**
     * @return The name of the baseline variable
     */
    private String getBaselineVariableName()
    {
        String name = null;
        if (this.hasBaseline())
        {
            name = this.getBaseline().getVariable().getValue();
        }
        return name;
    }

    /**
     * @return The largest possible value for the data. Double.MAX_VALUE by default.
     */
    public Double getMaximumValue()
    {
        Double maximum = Double.MAX_VALUE;

        if (this.projectConfig.getPair().getValues() != null &&
            this.projectConfig.getPair().getValues().getMaximum() != null)
        {
            maximum = this.projectConfig.getPair().getValues().getMaximum();
        }

        return maximum;
    }

    /**
     * Gets the value that should replace a value that exceeds the maximum allowable value
     * @return The configured default maximum value. null if not configured.
     */
    public Double getDefaultMaximumValue()
    {
        Double maximum = null;

        if (this.projectConfig.getPair().getValues() != null &&
            this.projectConfig.getPair().getValues().getDefaultMaximum() != null)
        {
            maximum = this.projectConfig.getPair().getValues().getDefaultMaximum();
        }

        return maximum;
    }

    /**
     * @return The smallest possible value for the data. -Double.MAX_VALUE by default
     */
    public Double getMinimumValue()
    {
        Double minimum = -Double.MAX_VALUE;

        if (this.projectConfig.getPair().getValues() != null &&
            this.projectConfig.getPair().getValues().getMinimum() != null)
        {
            minimum = this.projectConfig.getPair().getValues().getMinimum();
        }

        return minimum;
    }

    /**
     * Gets the value that should replace a value that falls below the minimum allowable value
     * @return The configured default minimum value. null if not configured.
     */
    public Double getDefaultMinimumValue()
    {
        Double defaultMinimum = null;

        if (this.projectConfig.getPair().getValues() != null &&
                this.projectConfig.getPair().getValues().getDefaultMinimum() != null)
        {
            defaultMinimum = this.projectConfig.getPair().getValues().getDefaultMinimum();
        }

        return defaultMinimum;
    }

    /**
     * @return The earliest possible date for any value. NULL unless specified by
     * the configuration
     */
    public String getEarliestDate()
    {
        String earliestDate = null;

        if (this.projectConfig.getPair().getDates() != null &&
            this.projectConfig.getPair().getDates().getEarliest() != null)
        {
            earliestDate = this.projectConfig.getPair().getDates().getEarliest();
        }

        return earliestDate;
    }

    /**
     * @return The latest possible date for any value. NULL unless specified by
     * the configuration
     */
    public String getLatestDate()
    {
        String latestDate = null;

        if (this.projectConfig.getPair().getDates() != null &&
                this.projectConfig.getPair().getDates().getLatest() != null)
        {
            latestDate = this.projectConfig.getPair().getDates().getLatest();
        }

        return latestDate;
    }

    /**
     * @return The earliest possible issue date for any forecast. NULL unless
     * specified by the configuration
     */
    public String getEarliestIssueDate()
    {
        String earliestDate = null;

        if (this.projectConfig.getPair().getIssuedDates() != null &&
                this.projectConfig.getPair().getIssuedDates().getEarliest() != null)
        {
            earliestDate = this.projectConfig.getPair().getIssuedDates().getEarliest();
        }

        return earliestDate;
    }

    /**
     * @return The latest possible issue date for any forecast. NULL unless
     * specified by the configuration
     */
    public String getLatestIssueDate()
    {
        String latestDate = null;

        if (this.projectConfig.getPair().getIssuedDates() != null &&
            this.projectConfig.getPair().getIssuedDates().getLatest() != null)
        {
            latestDate = this.projectConfig.getPair().getIssuedDates().getLatest();
        }

        return latestDate;
    }

    /**
     * @return Whether or not the project specifies a specific season to evaluate
     */
    public boolean specifiesSeason()
    {
        return this.projectConfig.getPair().getSeason() != null;
    }

    /**
     * @return The earliest possible day in a season. NULL unless specified in
     * the configuration
     */
    public MonthDay getEarliestDayInSeason()
    {
        MonthDay earliest = null;

        PairConfig.Season season = this.projectConfig.getPair().getSeason();

        if (season != null)
        {
            earliest = MonthDay.of(season.getEarliestMonth(), season.getEarliestDay());
        }

        return earliest;
    }

    /**
     * @return The latest possible day in a season. NULL unless specified in the
     * configuration
     */
    public MonthDay getLatestDayInSeason()
    {
        MonthDay latest = null;

        PairConfig.Season season = this.projectConfig.getPair().getSeason();

        if (season != null)
        {
            latest = MonthDay.of(season.getLatestMonth(), season.getLatestDay());
        }

        return latest;
    }

    /**
     * Determines the expected period of lead values to retrieve from the
     * database. If there is a definition provided by a lead times pooling window
     * configuration, that period is used. Otherwise, the period from the scale
     * is used.
     *<p>
     * The period from the scale is taken into account because a range of lead
     * hours is required to scale
     *</p>
     *
     * @return The length of a period of lead time to retrieve from the database
     * @throws CalculationException Thrown if the scale of the data could not be calculated
     */
    public Duration getLeadPeriod() throws CalculationException
    {
        Integer period;

        if (this.projectConfig.getPair().getLeadTimesPoolingWindow() != null &&
            this.projectConfig.getPair().getLeadTimesPoolingWindow().getPeriod() != null)
        {
            period = this.projectConfig.getPair()
                                       .getLeadTimesPoolingWindow()
                                       .getPeriod();
        }
        else
        {
            period = this.getScale().getPeriod();
        }

        return Duration.of(period, this.getLeadUnit());
    }

    /**
     * @return The period of issue times to pull from the database at once. 0
     * unless specified from the database
     */
    public int getIssuePoolingWindowPeriod()
    {
        // Indicates one basis per period
        int period = 0;

        if (this.getIssuePoolingWindow().getPeriod() != null)
        {
            period = this.getIssuePoolingWindow().getPeriod();
        }

        return period;
    }

    /**
     * Determines the unit of time that leads should be queried in. If no lead time
     * pooling window has been configured, the default is that of the expected
     * scale of the data
     *
     * @return The unit of time that leads should be queried in
     * @throws CalculationException Thrown if the scale of the data could not be calculated
     */
    public TemporalUnit getLeadUnit() throws CalculationException
    {
        String unit;

        if (this.projectConfig.getPair().getLeadTimesPoolingWindow() != null)
        {
            unit = this.projectConfig.getPair().getLeadTimesPoolingWindow().getUnit().value();
        }
        else
        {
            unit = this.getScale().getUnit().value();
        }

        // The following ensures that the returned unit may be converted into a ChronoUnit
        if (unit != null)
        {
            unit = unit.toUpperCase();

            if (!unit.endsWith( "S" ))
            {
                unit += "S";
            }

            return ChronoUnit.valueOf( unit );
        }

        return null;
    }

    /**
     * Determines the frequency with which to retrieve the period of leads.
     *<p>
     * If the period is 6 hours, but the frequency is 2 hours, it will evaluate
     * 6 hours of data at a time offset by 2 hours at a time (so leads 1 through 6,
     * 3 through 8, 5 through 10, etc)
     *</p>
     * <p>
     * If a lead time pooling window has been specified, the specified frequency
     * will be used. If no frequency has been specified, the period will be used.
     *</p>
     * <p>
     * If no lead time pooling window has been specified, the specifications from
     * the scale will be used instead
     *</p>
     * <br><br>
     * TODO: Change documentation and names to indicate that this doesn't really involve scale; just the time between values
     * @return The frequency will which to retrieve a period of leads
     * @throws CalculationException Thrown if the scale of the data could not be calculated
     */
    public Duration getLeadFrequency() throws CalculationException
    {
        Integer frequency;

        if (this.projectConfig.getPair().getLeadTimesPoolingWindow() != null)
        {
            if (this.projectConfig.getPair().getLeadTimesPoolingWindow().getFrequency() != null)
            {
                frequency = this.projectConfig.getPair()
                                              .getLeadTimesPoolingWindow()
                                              .getFrequency();
            }
            else
            {
                frequency = this.projectConfig.getPair()
                                              .getLeadTimesPoolingWindow()
                                              .getPeriod();
            }
        }
        else
        {
            frequency = this.getScale().getFrequency();
            if (frequency == null)
            {
                frequency = this.getScale().getPeriod();
            }
        }

        return Duration.of(frequency, this.getLeadUnit());
    }

    /**
     * TODO: Change documentation and names to indicate that this doesn't really involve scale; just the time between values
     * @return Whether or not data should be scaled
     * @throws CalculationException Thrown if the scale of the data could not be calculated
     */
    public boolean shouldScale() throws CalculationException
    {
        return this.getProjectConfig().getPair().getDesiredTimeScale() != null &&
               this.getScale() != null &&
               !TimeScaleFunction.NONE
                       .value().equalsIgnoreCase(this.getScale().getFunction().value());
    }

    /**
     * Determines the time step of the data
     * <p>
     *     If no desired scale has been configured, one is dynamically generated
     * </p>
     *
     * TODO: replace with references to {@link #getDesiredTimeScale()} and {@link #getDesiredTimeStep()}
     * as appropriate.
     *
     * TODO: A unified common time step is not guaranteed; Left hand data from USGS can range
     * from a couple minutes between values to a single value per day, while AHPS data
     * could be all over the place.
     * <br><br>
     * TODO: Change documentation and names to indicate that this doesn't really involve scale; just the time between values
     * @return The expected scale of the data
     * @throws CalculationException Thrown if a common scale between the left and
     * right hand data could not be calculated
     * @deprecated to be replaced with {@link #getDesiredTimeStep()} or {@link #getDesiredTimeScale()} as needed
     */
    @Deprecated(since="1.5", forRemoval=true)
    public DesiredTimeScaleConfig getScale() throws CalculationException
    {
        // TODO: Convert this to a function to determine time step; this doesn't actually have anything to do with scale
        if (this.desiredTimeScaleConfig == null)
        {
            this.desiredTimeScaleConfig = this.projectConfig.getPair().getDesiredTimeScale();

            // If there is an explicit desired time scale but the frequency
            // wasn't set, we need to set the default
            if (this.desiredTimeScaleConfig != null &&
                this.desiredTimeScaleConfig.getFrequency() == null)
            {
                this.desiredTimeScaleConfig = new DesiredTimeScaleConfig(
                        this.desiredTimeScaleConfig.getFunction(),
                        this.desiredTimeScaleConfig.getPeriod(),
                        this.desiredTimeScaleConfig.getUnit(),
                        null,
                        this.desiredTimeScaleConfig.getPeriod());
            }
        }

        if (this.desiredTimeScaleConfig == null && ConfigHelper.isForecast( this.getRight() ))
        {
            Duration commonInterval = this.getDesiredTimeStep();
            this.desiredTimeScaleConfig = new DesiredTimeScaleConfig(
                    TimeScaleFunction.MEAN,
                    (int)commonInterval.toMinutes(),
                    DurationUnit.MINUTES,
                    "Dynamic Scale",
                    (int)commonInterval.toMinutes()
            );
        }
        else if(this.desiredTimeScaleConfig == null)
        {
            this.desiredTimeScaleConfig = new DesiredTimeScaleConfig(
                    TimeScaleFunction.MEAN,
                    60,
                    DurationUnit.fromValue(TimeHelper.LEAD_RESOLUTION.toString().toLowerCase() ),
                    null,
                    60
            );
        }

        return this.desiredTimeScaleConfig;
    }

    /**
     * @return A possible issue dates pooling window configuration
     */
    public PoolingWindowConfig getIssuePoolingWindow()
    {
        return this.projectConfig.getPair().getIssuedDatesPoolingWindow();
    }

    /**
     * Dictates the pooling mode for the project. If an issue times pooling
     * window is established, the pooling mode is "TimeWindowMode.ROLLING",
     * "TimeWindowMode.BASIC" otherwise.
     * @return The pooling mode of the project. Defa
     */
    public PairingMode getPairingMode()
    {
        PairingMode mode = PairingMode.BASIC;

        if ( this.getIssuePoolingWindow() != null )
        {
            mode = PairingMode.ROLLING;
        }
        else if ( ProjectConfigs.hasTimeSeriesMetrics( this.projectConfig ))
        {
            mode = PairingMode.TIME_SERIES;
        }
        else if (this.projectConfig.getPair().isByTimeSeries() != null &&
                 this.projectConfig.getPair().isByTimeSeries())
        {
            mode = PairingMode.BY_TIMESERIES;
        }

        return mode;
    }

    /**
     * @return The temporal unit used for pooling issue times
     */
    public String getIssuePoolingWindowUnit()
    {
        String unit = null;

        if ( this.projectConfig.getPair()
                               .getIssuedDatesPoolingWindow() != null )
        {
            unit = this.projectConfig.getPair()
                                     .getIssuedDatesPoolingWindow()
                                     .getUnit()
                                     .value();
        }

        return unit;
    }

    /**
     * Determines the frequency of issue times pooling windows.
     *
     * <p>
     * The default is 1 in the configured temporal units. If the frequency is
     * configured, the configured frequency is used. If that isn't configured
     * but the the issue times period is greater than 0, that is used instead.
     * </p>
     * @return The frequency with which to pool periods of issue times.
     */
    public int getIssuePoolingWindowFrequency()
    {
        // If there isn't a definition, we want to move a single unit at a time
        int frequency = 1;

        if (this.getIssuePoolingWindow().getFrequency() != null)
        {
            frequency = this.getIssuePoolingWindow().getFrequency();
        }
        else if ( this.getIssuePoolingWindowPeriod() > 0)
        {
            frequency = this.getIssuePoolingWindowPeriod();
        }

        return frequency;
    }

    /**
     * Determines the number of time series that would be prudent to retrieve
     * from the database at once
     * <p>
     *     If small data sets are involved, pulling many series at once is a
     *     good idea. If a large dataset is involved, that may end in retrievals
     *     gathering several megabytes of data per series. If many series are all
     *     loaded within the same task and many tasks are running at once, it
     *     could end up greatly increasing the memory footprint and execution
     *     time of the application.
     * </p>
     * <p>
     *     This value isn't precise and therefore doesn't need to make a huge
     *     effort in doing so. The application can generate a rough estimate of
     *     the size of the data set that will be retrieved, but it cannot
     *     factor in overhead such as IO time on the database server, object
     *     overhead at runtime, or the number of threads that will be
     *     retrieving data all at once.
     * </p>
     * @return The number of Time Series' to return in one window
     * @throws CalculationException Thrown if the ids of ensembles to include or
     * exclude could not be calculated
     * @throws CalculationException Thrown if the estimated size of a time
     * series could not be calculated
     */
    public Integer getNumberOfSeriesToRetrieve() throws CalculationException
    {
        synchronized ( Project.SERIES_AMOUNT_LOCK )
        {
            if ( this.numberOfSeriesToRetrieve == null )
            {

                DataScripter script = new DataScripter();
                script.addLine( "SELECT (" );
                script.addTab().addLine( "(" );
                script.addTab(  2  ).addLine("(COUNT(E.ensemble_id) * 8 + 24) * -- This determines the size of a single row" );
                script.addTab(   3   ).addLine("(  -- This determines the number of expected rows" );
                script.addTab(    4    ).addLine( "SELECT COUNT(*)" );
                script.addTab(    4    ).addLine( "FROM wres.TimeSeriesValue TSV" );
                script.addTab(    4    ).addLine( "INNER JOIN (" );
                script.addTab(     5     ).addLine( "SELECT TS.timeseries_id" );
                script.addTab(     5     ).addLine( "FROM wres.TimeSeries TS" );
                script.addTab(     5     ).addLine( "INNER JOIN wres.TimeSeriesSource TSS" );
                script.addTab(      6      ).addLine( "ON TSS.timeseries_id = TS.timeseries_id" );
                script.addTab(     5     ).addLine( "INNER JOIN wres.ProjectSource PS" );
                script.addTab(      6      ).addLine( "ON PS.source_id = TSS.source_id" );
                script.addTab(     5     ).addLine( "WHERE PS.project_id = ", this.getId() );
                script.addTab(      6      ).addLine( "AND PS.member = ", Project.RIGHT_MEMBER );
                script.addTab(     5     ).addLine( "LIMIT 1" );
                script.addTab(    4    ).addLine( ") AS TS" );
                script.addTab(    4    ).addLine( "ON TS.timeseries_id = TSV.timeseries_id" );
                script.addTab(   3   ).addLine( ")" );
                script.addTab(  2  ).addLine(") / 1000.0)::float AS size     -- We divide by 1000.0 to convert the number to kilobyte scale" );
                script.addLine("-- We Select from ensemble because the number of ensembles affects" );
                script.addLine("--   the number of values returned in the resultant array" );
                script.addLine( "FROM wres.Ensemble E" );
                script.addLine( "WHERE EXISTS (" );
                script.addTab().addLine( "SELECT 1" );
                script.addTab().addLine( "FROM wres.TimeSeries TS" );
                script.addTab().addLine( "INNER JOIN wres.TimeSeriesSource TSS" );
                script.addTab(  2  ).addLine( "ON TSS.timeseries_id = TS.timeseries_id" );
                script.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
                script.addTab(  2  ).addLine( "ON PS.source_id = TSS.source_id" );
                script.addTab().addLine( "WHERE PS.project_id = ", this.getId() );
                script.addTab(  2  ).addLine( "AND PS.member = ", Project.RIGHT_MEMBER );
                script.addTab(  2  ).addLine( "AND TS.ensemble_id = E.ensemble_id" );
                script.addLine( ")" );

                if ( !this.getRight().getEnsemble().isEmpty() )
                {
                    int includeCount = 0;
                    int excludeCount = 0;

                    StringJoiner include = new StringJoiner( ",", "ANY('{", "}'::integer[])");
                    StringJoiner exclude = new StringJoiner(",", "ANY('{", "}'::integer[])");

                    for ( EnsembleCondition condition : this.getRight().getEnsemble())
                    {
                        List<Integer> ids;
                        try
                        {
                            ids = Ensembles.getEnsembleIDs( condition );
                        }
                        catch ( SQLException e )
                        {
                            throw new CalculationException(
                                    "Ensemble IDs needed to determine the size of a single time "
                                    + "series could not be retrieved.",
                                    e
                            );
                        }
                        if ( condition.isExclude() )
                        {
                            excludeCount += ids.size();
                            ids.forEach( id -> exclude.add(id.toString()) );
                        }
                        else
                        {
                            includeCount += ids.size();
                            ids.forEach( id -> include.add(id.toString()) );
                        }
                    }

                    if (includeCount > 0)
                    {
                        script.addTab().addLine( "AND ensemble_id = ", include.toString() );
                    }

                    if (excludeCount > 0)
                    {
                        script.addTab().addLine( "AND NOT ensemble_id = ", exclude.toString() );
                    }
                }

                double timeSeriesSize;
                try
                {
                    timeSeriesSize = script.retrieve( "size" );
                }
                catch ( SQLException e )
                {
                    throw new CalculationException( "The size of a single time series could not be determined.", e );
                }

                // We're going to limit the size of a time series group to 1MB
                final int sizeCap = 1000;

                // The number of series to retrieve will either be 1 or the number
                // of time series that may be retrieved and still fit under the cap.
                // Using Math.max ensures that we'll pull at least one value at a time.
                // If a single time series has an estimated size of 1186KB, we'll
                // only retrieve one time series at a time. If the estimated size is
                // 400KB, we'll bring in two at a time.
                this.numberOfSeriesToRetrieve =
                        ( ( Double ) Math.max( 1.0, sizeCap / timeSeriesSize ) )
                                .intValue();
            }

            return this.numberOfSeriesToRetrieve;
        }
    }

    public boolean usesGriddedData(DataSourceConfig dataSourceConfig)
            throws SQLException
    {
        Boolean usesGriddedData;

        switch ( this.getInputName( dataSourceConfig ) )
        {
            case Project.LEFT_MEMBER:
                usesGriddedData = this.leftUsesGriddedData;
                break;
            case Project.RIGHT_MEMBER:
                usesGriddedData = this.rightUsesGriddedData;
                break;
            default:
                usesGriddedData = this.baselineUsesGriddedData;
        }

        if (usesGriddedData == null)
        {
            DataScripter script = new DataScripter(  );
            script.addLine("SELECT EXISTS (");
            script.addTab().addLine("SELECT 1");
            script.addTab().addLine("FROM wres.ProjectSource PS");
            script.addTab().addLine("INNER JOIN wres.Source S");
            script.addTab(  2  ).addLine("ON PS.source_id = S.source_id");
            script.addTab().addLine("WHERE PS.project_id = ", this.getId());
            script.addTab(  2  ).addLine("AND PS.member = ", this.getInputName( dataSourceConfig ));
            script.addTab(  2  ).addLine("AND S.is_point_data = FALSE");
            script.addLine(") AS uses_gridded_data;");

            usesGriddedData = script.retrieve( "uses_gridded_data" );

            switch ( this.getInputName( dataSourceConfig ) )
            {
                case Project.LEFT_MEMBER:
                    this.leftUsesGriddedData = usesGriddedData;
                    break;
                case Project.RIGHT_MEMBER:
                    this.rightUsesGriddedData = usesGriddedData;
                    break;
                default:
                    this.baselineUsesGriddedData = usesGriddedData;
            }
        }
        return usesGriddedData;
    }

    /**
     * @return The desired unit of measurement for all values
     */
    public String getDesiredMeasurementUnit()
    {
        return String.valueOf(this.projectConfig.getPair().getUnit());
    }

    /**
     * @return A list of all configurations stating where to store pair output
     */
    public List<DestinationConfig> getPairDestinations()
    {
        return ConfigHelper.getDestinationsOfType( this.getProjectConfig(), DestinationType.PAIRS );
    }

    /**
     * Retrieves the offset for lead times to use to pull a valid window for a
     * feature.
     *
     * <p>
     *     All Offsets are cached at once to reduce load times for projects
     *     with many locations
     * </p>
     * @param feature The feature to find the offset for
     * @return The needed offset to match the first valid window with observations
     * @throws IOException Thrown if tasks used to evaluate all lead offsets
     * could not be executed
     * @throws IOException Thrown if the population of all lead offsets is
     * interrupted
     * @throws SQLException Thrown if offsets cannot be populated due to the
     * system being unable to determine what locations to retrieve offsets from.
     * @throws CalculationException Thrown if lead offsets cannot be calculated
     */
    public Integer getLeadOffset(Feature feature)
            throws IOException, SQLException, CalculationException
    {
        if (ConfigHelper.isSimulation( this.getRight() ) ||
                ProjectConfigs.hasTimeSeriesMetrics( this.projectConfig ))
        {
            return 0;
        }
        else if (this.usesGriddedData( this.getRight() ))
        {
            Integer offset = this.getMinimumLead();

            // If the default minimum was hit, return 0 instead.
            if (offset == Integer.MIN_VALUE)
            {
                return 0;
            }

            // We subtract by one time step since this returns an exclusive value where minimum lead is inclusive
            return offset - (int)TimeHelper.unitsToLeadUnits(
                    this.getScale().getUnit().value(),
                    this.getScale().getPeriod()
            );
        }
        else if (this.leadOffsets.isEmpty())
        {
            this.populateLeadOffsets();
        }

        return this.leadOffsets.get( feature );
    }

    /**
     * Caches the lead time offsets for each location in the project
     * <p>
     *     When each offset is retrieved on demand, it adds several seconds to
     *     the evaluation of each location. When there are many locations, this
     *     adds up. When they are all pulled semi-simultaenously, execution
     *     of the evaluation may occur almost immediately upon evaluating its
     *     location.
     * </p>
     * @throws IOException Thrown if the locations with which to evaluate cannot
     * be loaded
     * @throws IOException Thrown if the loading of the actual offset values fails
     * @throws IOException Thrown if the loading of the offset values is interrupted
     * @throws SQLException Thrown if the script used to determine what
     * locations to find offsets for cannot be formed
     * @throws CalculationException Thrown if a common scale between left and right data could be calculated
     */
    private void populateLeadOffsets() throws IOException, SQLException, CalculationException
    {
        FormattedStopwatch timer = null;

        if (LOGGER.isDebugEnabled())
        {
            timer = new FormattedStopwatch();
            timer.start();
        }

        DataScripter script = ProjectScriptGenerator.formVariableFeatureLoadScript(this);

        Map<FeatureDetails.FeatureKey, Future<Integer>> futureOffsets = new LinkedHashMap<>(  );

        try (DataProvider data = script.buffer())
        {
            LOGGER.trace("Variable feature metadata loaded...");

            while (data.next())
            {
                OffsetEvaluator evaluator = new OffsetEvaluator(
                        this,
                        data.getInt( "observation_feature" ),
                        data.getInt("forecast_feature")
                );

                // TODO: Add DataProvider constructor for the key
                FeatureDetails.FeatureKey key = new FeatureDetails.FeatureKey(
                        data.getValue("comid"),
                        data.getValue("lid"),
                        data.getValue("gage_id"),
                        data.getValue("huc"),
                        data.getValue("longitude"),
                        data.getValue("latitude")
                );

                futureOffsets.put( key, Executor.submit( evaluator ));

                LOGGER.trace( "A task has been created to find the offset for {}.", key );
            }
        }
        catch ( SQLException e )
        {
            throw new IOException("Tasks used to evaluate lead hour offsets "
                                  + "could not be created.", e);
        }

        while (!futureOffsets.isEmpty())
        {
            FeatureDetails.FeatureKey key = ( FeatureDetails.FeatureKey)futureOffsets.keySet().toArray()[0];
            Future<Integer> futureOffset = futureOffsets.remove( key );

            // Determine the feature definition for the offset
            Feature feature = new FeatureDetails( key ).toFeature();
            Integer offset;
            try
            {
                LOGGER.trace( "Loading the offset for '{}'", ConfigHelper.getFeatureDescription( feature ));

                // If the current task is the last/only task left to evaluate, wait for the result
                if (futureOffsets.isEmpty())
                {
                    offset = futureOffset.get();
                }
                else
                {
                    // Otherwise, give the offset 500 milliseconds before
                    // quiting and moving on to another task
                    offset = futureOffset.get( 500, TimeUnit.MILLISECONDS );
                }

                // If the returned value doesn't exist, move on
                if (offset == null)
                {
                    continue;
                }
                LOGGER.trace("The offset was: {}", offset);

                // Add the non-null value to the mapping with the feature as the key
                this.leadOffsets.put(
                        feature,
                        offset
                );
                LOGGER.trace( "An offset for {} was loaded!", ConfigHelper.getFeatureDescription( feature ) );
            }
            catch ( InterruptedException e )
            {
                LOGGER.warn( "Population of lead offsets has been interrupted.", e );
                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException e )
            {
                throw new IOException("An error occured while populating the future"
                             + "offsets.", e);
            }
            catch ( TimeoutException e )
            {
                // If the task took more than 500 milliseconds to evaluate, put
                // it back into the queue and move on to the next one
                LOGGER.trace("It took too long to get the offset for '{}'; "
                             + "moving on to another location while we wait "
                             + "for the output on this location",
                             ConfigHelper.getFeatureDescription( feature ));
                futureOffsets.put( key, futureOffset );
            }
        }

        if (timer != null && LOGGER.isDebugEnabled())
        {
            timer.stop();
            LOGGER.debug( "It took {} to get the offsets for all locations.",
                          timer.getFormattedDuration() );
        }
    }

    /**
     * Determines the number of pools for issue date pooling for a feature
     *
     * <p>
     *     If we know that we want to pool data together two issue days at a time,
     *     staggered by a single day, over data spanning four days, this will
     *     tell us that we have three pools to iterate over
     * </p>
     * <p>
     *     Aids in the process of iterating over windows/MetricInputs
     * </p>
     * TODO: Once the logic below is uncommented and tests are updated, move this logic to the wres.io.retrieval.PoolingSampleDataIterator
     * and remove the cache
     * @param feature The feature whose pool count to use
     * @return The number of issue date pools for a feature
     * @throws CalculationException Thrown if the number of issue pools
     * for a set of leads for a feature could not be calculated
     */
    public Integer getIssuePoolCount( final Feature feature) throws CalculationException
    {
        if ( this.getPairingMode() != PairingMode.ROLLING)
        {
            return -1;
        }

        synchronized ( POOL_LOCK )
        {

            if (!this.poolCounts.containsKey( feature ))
            {
                // TODO: Uncomment and remove feature parameter. Will break current system tests
                /*Instant beginning = Instant.parse( this.getEarliestIssueDate());
                Instant end = Instant.parse( this.getLatestIssueDate() );

                Duration jump = Duration.of(
                        this.getIssuePoolingWindowFrequency(),
                        ChronoUnit.valueOf( this.getIssuePoolingWindowUnit().toUpperCase())
                );
                Duration period = Duration.of(
                        this.getIssuePoolingWindowPeriod(),
                        ChronoUnit.valueOf(this.getIssuePoolingWindowUnit().toUpperCase())
                );

                Integer count = 0;


                while (beginning.isBefore( end ) || beginning.equals( end ))
                {
                    LOGGER.info("");
                    LOGGER.info("");
                    LOGGER.info("");
                    LOGGER.info("Pool #{}: {} through {}", count, beginning, beginning.plus( period ));
                    LOGGER.info("");
                    LOGGER.info("");
                    LOGGER.info("");
                    count++;
                    beginning = beginning.plus( jump );
                }

                this.poolCounts.put( feature, count );*/

                try
                {
                    DataScripter counter = ProjectScriptGenerator.formIssuePoolCounter( this, feature );

                    counter.consume( row -> {
                        Integer sampleCount = row.getInt( "window_count" );
                        if (sampleCount != null)
                        {
                            this.poolCounts.put(feature, sampleCount);
                        }
                    } );
                }
                catch ( SQLException e )
                {
                    throw new CalculationException(
                            "The number of value pools revolving around issue times "
                            + "could not be determined for " +
                            ConfigHelper.getFeatureDescription( feature ),
                            e
                    );
                }
            }
        }

        return this.poolCounts.get( feature );
    }

    private String getProjectName()
    {
        return this.projectConfig.getName();
    }

    /**
     * Returns unique identifier for this project's config+data
     * @return The unique ID
     */
    public Integer getInputCode()
    {
        return this.inputCode;
    }

    /**
     * Determines the duration between values in the dataset
     *
     * @param dataSourceConfig The specification for which values to investigate
     * @return The duration between values
     * @throws CalculationException thrown if the frequency for forecast data could not be calculated
     * @throws CalculationException thrown if the frequency for observed data could not be calculated
     * @throws CalculationException thrown if the calculation of the frequency resulted in an
     * impossible value
     */
    private Duration getValueInterval( DataSourceConfig dataSourceConfig) throws CalculationException
    {
        Duration interval;

        if (ConfigHelper.isForecast( dataSourceConfig ))
        {
            interval = this.getForecastInterval( dataSourceConfig );
        }
        else
        {
            interval = this.getObservationInterval( dataSourceConfig );
        }

        if (interval == null)
        {
            throw new CalculationException("The interval for the " +
                                           this.getInputName( dataSourceConfig ) +
                                           " data could either not be determined or was "
                                           + "invalid.");
        }

        return interval;
    }

    /**
     * Retrieves the interval between forecast values from the database
     * @param dataSourceConfig The specification for the data with which to investigate
     * @return The duration between forecast values
     * @throws CalculationException thrown if a feature to use in the calculation could not be found
     * @throws CalculationException thrown if the intersection between feature data and variable data
     * could not be calculated
     * @throws CalculationException thrown if an isolated ensemble to evaluate could not be found
     * @throws CalculationException thrown if the calculation used to determine the frequency of the
     * forecast values encountered an error
     */
    private Duration getForecastInterval( DataSourceConfig dataSourceConfig) throws CalculationException
    {
        // TODO: Forecasts between locations might not be unified.
        // Generalizing the interval for all locations based on a single one could cause miscalculations
        DataScripter script = new DataScripter(  );

        script.addLine("WITH differences AS");
        script.addLine("(");
        script.addLine("    SELECT lead - lag(lead) OVER (ORDER BY TSV.timeseries_id, lead) AS difference");
        script.addLine("    FROM wres.TimeSeriesValue TSV");
        script.addLine("    INNER JOIN wres.TimeSeries TS");
        script.addLine("        ON TS.timeseries_id = TSV.timeseries_id");

        if (this.getMinimumLead() != Integer.MIN_VALUE)
        {
            script.addTab().addLine("WHERE lead > ", this.getMinimumLead());
        }
        else
        {
            script.addTab().addLine("WHERE lead > 0");
        }

        if (this.getMaximumLead() != Integer.MAX_VALUE)
        {
            script.addTab(  2  ).addLine("AND lead <= ", this.getMaximumLead());
        }
        else
        {
            // Set the ceiling to 100 hours. If the maximum lead is 72 hours, then this
            // should not behave much differently than having no clause at all.
            // If real maximum was 2880 hours, 100 will provide a large enough sample
            // size and produce the correct values in a slightly faster fashion.
            // In one data set, leaving this out causes this to take 11.5s .
            // That was even with a subset of the real data (1 month vs 30 years).
            // If we cut it to 100 hours, it now takes 1.6s. Still not great, but
            // much faster
            int ceiling = TimeHelper.durationToLead( Duration.of(100, ChronoUnit.HOURS) );
            script.addTab(  2  ).addLine( "AND lead <= ", ceiling );
        }

        Optional<FeatureDetails> featureDetails;
        try
        {
            featureDetails = this.getFeatures().stream().findFirst();
        }
        catch ( SQLException e )
        {
            throw new CalculationException( "A feature needed to determine a"
                                            + "scale for forecasts could not be loaded.",
                                            e );
        }

        if (featureDetails.isPresent())
        {
            FeatureDetails feature = featureDetails.get();
            Integer variableFeatureId;
            try
            {
                variableFeatureId =
                        Features.getVariableFeatureByFeature( feature, this.getVariableId( dataSourceConfig ) );
            }
            catch ( SQLException e )
            {
                throw new CalculationException( "The location and variable needed to "
                                                + "evaluate an ensemble member for the "
                                                + "scale could not be loaded.",
                                                e );
            }

            Integer arbitraryEnsembleId;
            try
            {
                arbitraryEnsembleId = Ensembles.getSingleEnsembleID( this.getId(), variableFeatureId);
            }
            catch ( SQLException e )
            {
                throw new CalculationException( "An ensemble member used to evaluate the "
                                                + "scale of a forecast could not be loaded.",
                                                e );
            }

            script.addTab(  2  ).addLine("AND TS.variablefeature_id = ", variableFeatureId);
            script.addTab(  2  ).addLine("AND TS.ensemble_id = ", arbitraryEnsembleId);

        }

        script.addTab(  2  ).addLine("AND EXISTS (");
        script.addTab(   3   ).addLine("SELECT 1");
        script.addTab(   3   ).addLine("FROM wres.ProjectSource PS");
        script.addTab(   3   ).addLine("INNER JOIN wres.TimeSeriesSource TSS");
        script.addTab(    4    ).addLine("ON TSS.source_id = PS.source_id");
        script.addTab(   3   ).addLine("WHERE PS.project_id = ", this.getId());
        script.addLine("                AND PS.member = ", this.getInputName( dataSourceConfig ));
        script.addLine("                AND TSV.timeseries_id = TSS.timeseries_id");
        script.addLine("        )");
        script.addLine(")");
        script.addLine("SELECT MIN(difference)::bigint AS interval");
        script.addLine("FROM differences");
        script.addLine("WHERE difference IS NOT NULL");
        script.addLine("    AND difference > 0");

        try
        {
            return Duration.of(script.retrieve( "interval" ), TimeHelper.LEAD_RESOLUTION);
        }
        catch ( SQLException e )
        {
            throw new CalculationException( "The interval between forecast values could not be evaluated.", e );
        }
    }

    /**
     * Retrieves the interval between observation values from the database
     * @param dataSourceConfig The specification for the data with which to investigate
     * @return The duration between observation values
     * @throws CalculationException thrown if a feature to evaluate could not be found
     * @throws CalculationException thrown if the intersection between variable and
     * location data could not be calculated
     * @throws CalculationException thrown if the calculation used to determine the
     * frequency of the observation values encountered an error
     */
    private Duration getObservationInterval( DataSourceConfig dataSourceConfig) throws CalculationException
    {
        // TODO: Observations between locations are often not unified.
        // Generalizing the scale for all locations based on a single one could cause miscalculations
        DataScripter script = new DataScripter(  );

        script.addLine("WITH differences AS");
        script.addLine("(");
        script.addLine("    SELECT AGE(observation_time, (LAG(observation_time) OVER (ORDER BY observation_time)))");
        script.addLine("    FROM wres.Observation O");

        Optional<FeatureDetails> featureDetails;
        try
        {
            featureDetails = this.getFeatures().stream().findFirst();
        }
        catch ( SQLException e )
        {
            throw new CalculationException( "A feature needed to determine a"
                                            + "scale for observations could not be loaded.",
                                            e );
        }

        int tabCount;

        if (featureDetails.isPresent())
        {
            String variablePositionClause;
            try
            {
                variablePositionClause = ConfigHelper.getVariableFeatureClause(
                        featureDetails.get().toFeature(),
                        this.getVariableId( dataSourceConfig ),
                        "O" );
            }
            catch ( SQLException e )
            {
                throw new CalculationException( "The location and variable needed to "
                                                + "determine the scale of observations "
                                                + "could not be loaded.",
                                                e );
            }
            script.addLine("    WHERE ", variablePositionClause);
            tabCount = 3;
            script.addTab(  2  ).add("AND ");

        }
        else
        {
            tabCount = 2;
            script.addTab().add("WHERE ");
        }


        script.addLine("EXISTS (");

        script.addTab(tabCount).addLine("SELECT 1");
        script.addTab(tabCount).addLine("FROM wres.ProjectSource PS");
        script.addTab(tabCount).addLine("WHERE PS.project_id = ", this.getId());
        script.addTab(tabCount).addTab().addLine("AND PS.member = ", this.getInputName( dataSourceConfig ) );
        script.addTab(tabCount).addTab().addLine("AND PS.source_id = O.source_id");
        script.addTab().addLine(")");
        script.addTab().addLine("GROUP BY observation_time");
        script.addLine(")");
        script.addLine("SELECT ( EXTRACT( epoch FROM MIN(age))/60 )::bigint AS interval -- Divide by 60 to convert the seconds to minutes");
        script.addLine("FROM differences");
        script.addLine("WHERE age IS NOT NULL");
        script.addLine("GROUP BY age;");

        try
        {
            return Duration.of(script.retrieve( "interval" ), TimeHelper.LEAD_RESOLUTION);
        }
        catch ( SQLException e )
        {
            throw new CalculationException( "The calculation used to determine the "
                                            + "interval of observational data failed.",
                                            e );
        }
    }

    /**
     * @return Whether or not baseline data is involved in the project
     */
    public boolean hasBaseline()
    {
        return this.getBaseline() != null;
    }

    public Integer getId() {
        return this.projectID;
    }

    private String getIDName() {
        return "project_id";
    }

    protected void setID(Integer id)
    {
        this.projectID = id;
    }

    private DataScripter getInsertSelectStatement()
    {
        DataScripter script = new DataScripter(  );

        script.addLine("WITH new_project AS");
        script.addLine("(");
        script.addTab().addLine("INSERT INTO wres.Project (project_name, input_code)");
        script.addTab().addLine("SELECT ?, ?");

        script.addArgument( this.getProjectName() );
        script.addArgument( this.getInputCode() );

        script.addTab().addLine("WHERE NOT EXISTS (");
        script.addTab(  2  ).addLine("SELECT 1");
        script.addTab(  2  ).addLine("FROM wres.Project P");
        script.addTab(  2  ).addLine("WHERE P.input_code = ?");

        script.addArgument( this.getInputCode() );

        script.addTab().addLine(")");
        script.addTab().addLine("RETURNING project_id");
        script.addLine(")");
        script.addLine("SELECT project_id, TRUE AS wasInserted");
        script.addLine("FROM new_project");
        script.addLine(  );
        script.addLine("UNION");
        script.addLine();
        script.addLine("SELECT project_id, FALSE AS wasInserted");
        script.addLine("FROM wres.Project P");
        script.addLine("WHERE P.input_code = ?;");

        script.addArgument( this.getInputCode() );

        return script;
    }

    public void save() throws SQLException
    {
        DataScripter saveScript = this.getInsertSelectStatement();
        saveScript.setUseTransaction( true );
        saveScript.addTablesToLock( "wres.Project" );

        try (DataProvider data = saveScript.getData())
        {
            this.setID( data.getInt( this.getIDName() ) );
            this.performedInsert = data.getBoolean( "wasInserted" );
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Did I create Project ID {}? {}",
                          this.getId(),
                          this.performedInsert );
        }
    }

    @Override
    public String toString()
    {
        return "Project { Name: " + this.getProjectName() +
               ", Code: " + this.getInputCode() + " }";
    }

    public boolean performedInsert()
    {
        return this.performedInsert;
    }

    /**
     * TODO: #60331 - Remove from Project
     * @param feature The feature to investigate
     * @return The last lead time for the feature available for evaluation
     * @throws CalculationException thrown if the calculation used to determine
     * if gridded data is used encounters an error
     * @throws CalculationException thrown if the calculation used to find the
     * last possible lead for attached gridded data encounters an error
     * @throws CalculationException thrown if the intersection of location and
     * variable data could not be calculated
     * @throws CalculationException thrown if the calculation used to find the
     * last possible lead for attached vector data encountered an error
     * @throws CalculationException thrown if the calculation used to find the
     * last lead for vector data did not return a result
     */
    public Integer getLastLead(Feature feature) throws CalculationException
    {
        boolean leadIsMissing = !this.lastLeads.containsKey( feature );
        Integer lastLead;

        if (leadIsMissing)
        {
            DataScripter script = ProjectScriptGenerator.createLastLeadScript( this, feature );

            try
            {
                lastLead = script.retrieve( "last_lead" );
            }
            catch ( SQLException e )
            {
                throw new CalculationException( "The calculation used to determine "
                                                + "where to stop evaluating " +
                                                ConfigHelper.getFeatureDescription( feature ) +
                                                " failed.",
                                                e);
            }

            if (Objects.isNull( lastLead ))
            {
                String message = "The calculation used to determine when to stop evaluating data for %s returned nothing";
                message = String.format( message, ConfigHelper.getFeatureDescription( feature ) );
                message += System.lineSeparator();
                message += "The offending query was:" + System.lineSeparator();
                message += script.toString();
                throw new CalculationException( message);
            }

            this.lastLeads.put(feature, lastLead);
        }

        return this.lastLeads.get(feature);
    }


    /**
     * TODO: return a {@link Duration}
     * 
     * @return the minimum value specified or a default of Integer.MIN_VALUE
     */
    public int getMinimumLead()
    {
        int result = Integer.MIN_VALUE;

        if ( this.getProjectConfig().getPair() != null
             && this.getProjectConfig().getPair()
                    .getLeadHours() != null
             && this.getProjectConfig().getPair()
                    .getLeadHours()
                    .getMinimum() != null )
        {
            result = (int)TimeHelper.unitsToLeadUnits(
                    ChronoUnit.HOURS.toString(),
                    this.getProjectConfig().getPair().getLeadHours().getMinimum()
            );
        }

        return result;
    }

    /**
     * TODO: return a {@link Duration}
     * 
     * @return the maximum value specified or a default of Integer.MAX_VALUE
     */
    public int getMaximumLead()
    {
        int result = Integer.MAX_VALUE;

        if ( this.getProjectConfig().getPair() != null
             && this.getProjectConfig().getPair()
                    .getLeadHours() != null
             && this.getProjectConfig().getPair()
                    .getLeadHours()
                    .getMaximum() != null )
        {
            result = (int)TimeHelper.unitsToLeadUnits(
                    ChronoUnit.HOURS.toString(),
                    this.getProjectConfig().getPair().getLeadHours().getMaximum()
            );
        }

        return result;
    }

    public Duration getLeftTimeShift()
    {
        return ConfigHelper.getTimeShift(this.getLeft());
    }

    public Duration getRightTimeShift()
    {
        return ConfigHelper.getTimeShift( this.getRight() );
    }

    /**
     * Returns the first date of observation data for the feature, for the given input
     * @param sourceConfig The side of the data whose zero date we are interested in
     * @param feature The feature whose zero date we are interested in
     * @return The first date where a feature contains an observed value for a
     * certain configuration
     * @throws SQLException Thrown if the initial date could not be loaded
     * from the database.
     */
    public String getInitialObservationDate( final DataSourceConfig sourceConfig, final Feature feature) throws SQLException
    {
        synchronized ( this.initialObservationDates )
        {
            if (!this.initialObservationDates.containsKey( feature ))
            {
                DataScripter script = ProjectScriptGenerator.generateInitialObservationDateScript(
                        this,
                        sourceConfig,
                        feature
                );

                this.initialObservationDates.put(feature, script.retrieve( "zero_date" ));
            }
        }

        return this.initialObservationDates.get( feature);
    }

    public String getInitialForecastDate( DataSourceConfig sourceConfig, Feature feature) throws SQLException
    {
        synchronized ( this.initialForecastDates )
        {
            if (!this.initialForecastDates.containsKey( feature ))
            {
                DataScripter script =  ProjectScriptGenerator.generateInitialForecastDateScript(
                        this,
                        sourceConfig,
                        feature
                );

                this.initialForecastDates.put(feature, script.retrieve( "zero_date" ));
            }

            return this.initialForecastDates.get(feature);
        }
    }

    /**
     * Gets and stores the largest leap between two different forecasts in
     * forecast order, measured in the standard lead hour unit
     * <p>
     *     If we have forecasts such as:
     * </p>
     * <table>
     *     <caption>Forecast Dates</caption>
     *     <tr>
     *         <td>1985-01-01 12:00:00</td>
     *     </tr>
     *     <tr>
     *         <td>1985-01-02 12:00:00</td>
     *     </tr>
     *     <tr>
     *         <td>1985-01-03 12:00:00</td>
     *     </tr>
     *     <tr>
     *         <td>1985-01-05 12:00:00</td>
     *     </tr>
     *     <tr>
     *         <td>1985-01-06 12:00:00</td>
     *     </tr>
     *     <tr>
     *         <td>1985-01-09 12:00:00</td>
     *     </tr>
     *     <tr>
     *         <td>1985-01-10 12:00:00</td>
     *     </tr>
     * </table>
     * <p>
     *     We'll find that the greatest gap between two forecasts is three days.
     *     When we go to select data spanning across initialization dates, the
     *     smallest and safest distance is this maximum. If a smaller span is
     *     chosen, you run the risk of generating windows with no data.
     * </p>
     * @param sourceConfig The datasource configuration that dictates what
     *                     type of data to use
     * @param feature The location to find the lag for
     * @return The number of lead hours between forecasts for a feature
     * @throws CalculationException thrown if the intersection between variable
     * and location data could not be calculated
     * @throws CalculationException thrown if the calculation used to determine
     * the typical gap encountered an error
     */
    public Integer getForecastLag(DataSourceConfig sourceConfig, Feature feature) throws CalculationException
    {
        synchronized (this.timeSeriesLag )
        {
            if (!this.timeSeriesLag.containsKey( feature ))
            {
                // This script will tell us the maximum distance between
                // sequential forecasts for a feature for this project.
                // We don't need the intended distance. If we go with the
                // intended distance (say 3 hours), but the user just doesn't
                // have or chooses not to use a forecast, resulting in a gap of
                // 6 hours, we'll encounter an error because we're aren't
                // accounting for that weird gap. By going with the maximum, we
                // ensure that we will always cover that gap.
                DataScripter script = ProjectScriptGenerator.createForecastLagScript( this, sourceConfig, feature );

                try
                {
                    this.timeSeriesLag.put( feature, script.retrieve( "typical_gap" ) );
                }
                catch ( SQLException e )
                {
                    throw new CalculationException( "The calculation used to determine a "
                                                    + "reasonable gap between forecasts failed.",
                                                    e );
                }
            }

            return this.timeSeriesLag.get( feature );
        }
    }

    /**
     * Return <code>true</code> if the project uses probability thresholds, otherwise <code>false</code>.
     * 
     * @return Whether or not the project uses probability thresholds
     */
    public boolean usesProbabilityThresholds()
    {
        // Iterate metrics configuration
        for ( MetricsConfig next : this.projectConfig.getMetrics() )
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

    public int compareTo(Project other)
    {
        return this.getInputCode().compareTo( other.getInputCode() );
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj instanceof Project && this.hashCode() == obj.hashCode();
    }

    @Override
    public int hashCode()
    {
        return this.getInputCode();
    }

    /**
     * Consumes time-scale and time-step information and logs any warnings generated by the process.
     */

    private static class TimeScaleAndTimeStepConsumer implements ExceptionalConsumer<DataProvider, SQLException>
    {

        /**
         * The time scale and time step information consumed.
         */

        private final Set<Pair<TimeScale, Duration>> existingTimeScales = new HashSet<>();

        /**
         * Is <code>true</code> if one or more consumptions generated a warning for a null period.
         */

        private final AtomicBoolean warnOnNullPeriod = new AtomicBoolean();

        /**
         * Is <code>true</code> if one or more consumptions generated a warning for a null function.
         */

        private final AtomicBoolean warnOnNullFunction = new AtomicBoolean();

        /**
         * Is <code>true</code> if one or more consumptions generated a warning for an 
         * {@link TimeScale.TimeScaleFunction#UNKNOWN} function.
         */

        private final AtomicBoolean warnOnUnknownFunction = new AtomicBoolean();

        /**
         * Is <code>true</code> if one or more consumptions generated a warning for an instantaneous
         * time scale according to {@link TimeScale#isInstantaneous()}.
         */

        private final AtomicBoolean warnOnInstantaneous = new AtomicBoolean();

        /**
         * Is <code>true</code> if one or more consumptions generated a warning for null/missing
         * time scale.
         */

        private final AtomicBoolean warnOnNullScale = new AtomicBoolean();

        /**
         * A source type string to help with warnings.
         */

        private LeftOrRightOrBaseline sourceType;

        /**
         * The declared <code>existingTimeScale</code> for the source being consumed.
         */

        private TimeScale declaredExistingTimeScale;

        /**
         * A period to help with warnings.
         */

        private Duration warnPeriod;

        /**
         * A function string to help with warnings.
         */

        private String warnFunctionString;

        /**
         * A time scale to help with warnings.
         */

        private TimeScale warnTimeScale;

        /**
         * Construct with the existing time scale from the declaration and source type.
         * 
         * @param declaredExistingTimeScale the declared existingTimeScale, which may be null
         * @param sourceType a source type string to help with warnings
         */
        private TimeScaleAndTimeStepConsumer( TimeScale declaredExistingTimeScale, LeftOrRightOrBaseline sourceType )
        {
            this.declaredExistingTimeScale = declaredExistingTimeScale;
            this.sourceType = sourceType;
        }

        /**
         * Consumer from the provider.
         * 
         * @param value the data provider
         * @throws SQLException if consumption fails
         * @throws NullPointerException if the input is null
         */

        @Override
        public void accept( DataProvider value ) throws SQLException
        {
            Objects.requireNonNull( value );

            // Time scale and step information
            Duration period = value.getDuration( "scale_period" );
            String functionString = value.getString( "scale_function" );
            Duration timeStep = value.getDuration( "time_step" );

            // Default to the existingTimeScale in the declaration
            if ( Objects.nonNull( declaredExistingTimeScale ) )
            {
                if ( Objects.isNull( period ) )
                {
                    period = declaredExistingTimeScale.getPeriod();
                    this.warnOnNullPeriod.set( true );
                    this.warnPeriod = period;
                }
                // Allow the declaration to augment the functionString
                if ( Objects.isNull( functionString ) )
                {
                    functionString = declaredExistingTimeScale.getFunction().toString();
                    this.warnOnNullFunction.set( true );
                    this.warnFunctionString = functionString;

                }
                // Allow the declaration to override the function ONLY if it is 'UNKNOWN' and differs.
                else if ( functionString.equalsIgnoreCase( "UNKNOWN" )
                          && !functionString.equalsIgnoreCase( declaredExistingTimeScale.getFunction()
                                                                                        .toString() ) )
                {
                    functionString = declaredExistingTimeScale.getFunction().toString();
                    this.warnOnUnknownFunction.set( true );
                    this.warnFunctionString = functionString;
                }
            }

            // Record or log as null
            if ( Objects.isNull( period ) || Objects.isNull( functionString ) )
            {
                this.warnOnNullScale.set( true );
            }
            
            // Create the scale and/or time step information
            this.createTimeStepInformation( period, functionString, timeStep, value );
        }

        /**
        * Uses the input to create the time scale and time step information.
        * 
        * @param period the time scale period
        * @param functionString the time scale function string
        * @param timeStep the time step
        * @param value the time scale and step provider
        */

        private void createTimeStepInformation( Duration period,
                                                String functionString,
                                                Duration timeStep,
                                                DataProvider value )
        {
            TimeScale timeScale = null;
            
            // Time scale information available
            if ( Objects.nonNull( period ) && Objects.nonNull( functionString ) )
            {
                TimeScale.TimeScaleFunction function =
                        TimeScale.TimeScaleFunction.valueOf( functionString.toUpperCase() );

                timeScale = TimeScale.of( period, function );
            }
            
            // Validate time scale if both input and declaration are present
            if ( Objects.nonNull( this.declaredExistingTimeScale )
                 && Objects.nonNull( timeScale )   
                 && ! this.declaredExistingTimeScale.equals( timeScale ) )
            {
                // Report the original time scale function in case the database said UNKNOWN and this was
                // augmented above
                TimeScale timeScaleToReport = timeScale;
                if ( "UNKNOWN".equalsIgnoreCase( value.getString( "scale_function" ) ) )
                {
                    timeScaleToReport = TimeScale.of( period );
                }

                if ( timeScale.isInstantaneous() && this.declaredExistingTimeScale.isInstantaneous() )
                {
                    this.warnOnInstantaneous.set( true );
                    this.warnTimeScale = timeScaleToReport;
                }
                else
                {
                    throw new RescalingException( "The existing time scale information in the project "
                                                  + "declaration is inconsistent with the time scale "
                                                  + "information obtained from one or more "
                                                  + sourceType
                                                  + " sources. Source "
                                                  + "says: "
                                                  + timeScaleToReport
                                                  + ". Declaration says: "
                                                  + this.declaredExistingTimeScale );
                }
            }

            // Store, with possibly null time scale information
            this.existingTimeScales.add( Pair.of( timeScale, timeStep ) );

        }

        /**
         * Returns all time scale and time step information consumed.
         * 
         * @return all time scale and time step information
         */

        private Set<Pair<TimeScale, Duration>> getTimeScalesAndSteps()
        {
            return java.util.Collections.unmodifiableSet( this.existingTimeScales );
        }

        /**
         * Logs all warning messages when consumption is complete.
         */

        private void logAllWarningsOnCompletion()
        {
            // Warn when one or more instances had a null period             
            if ( this.warnOnNullPeriod.get() )
            {
                LOGGER.warn( "The time scale period in the {} source is 'NULL' "
                             + "and the time scale period in the declaration is "
                             + "'{}'. Using the declared period as representative of "
                             + "the source.",
                             this.sourceType,
                             this.warnPeriod );
            }

            // Warn when one or more instances had a null function
            if ( this.warnOnNullFunction.get() )
            {
                LOGGER.warn( "The time scale function in the {} is 'NULL' "
                             + "and the time scale function in the declaration "
                             + "is '{}'. Using the declared function as "
                             + "representative of the source.",
                             this.sourceType,
                             this.warnFunctionString );
            }

            // Warn when one or more instances had an unknown function
            if ( this.warnOnUnknownFunction.get() )
            {
                LOGGER.warn( "The time scale function in the {} source is 'UNKNOWN' "
                             + "and the time scale function in the declaration "
                             + "is '{}'. Using the declared function as "
                             + "representative of the source.",
                             this.sourceType,
                             this.warnFunctionString );
            }

            // Warn when one or more instances differed in time scale, but both 
            // were recognized as instantaneous by TimeScale::isInstantaneous
            if ( this.warnOnInstantaneous.get() )
            {
                // Print the raw period and function for the two time scales
                String warnScaleString = "[" + this.warnTimeScale.getPeriod()
                                         + ","
                                         +
                                         this.warnTimeScale.getFunction()
                                         + "]";
                String declaredScaleString = "[" + this.declaredExistingTimeScale.getPeriod()
                                             + ","
                                             +
                                             this.declaredExistingTimeScale.getFunction()
                                             + "]";

                LOGGER.warn( "The existing time scale information in the project declaration is "
                             + "inconsistent with the time scale information obtained from one "
                             + "or more {} sources, but both are recognized as 'INSTANTANEOUS' "
                             + "and, therefore, allowed. Source says: {}. Declaration says: {}.",
                             this.sourceType,
                             warnScaleString,
                             declaredScaleString );
            }

            // Warn when one or more instances had no time scale information
            if ( this.warnOnNullScale.get() )
            {
                LOGGER.warn( "Could not determine valid time scale information for one or more {}"
                             + " sources. Assuming that the desired time scale is consistent with the "
                             + "time scales of the sources for which information is missing.",
                             this.sourceType );
            }
        }

    }
    
    
}

