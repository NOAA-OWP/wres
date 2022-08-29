package wres.io.project;

import java.time.Duration;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.config.generated.TimeScaleConfig;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesStore;
import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.io.config.ConfigHelper;
import wres.io.ingesting.IngestResult;
import wres.io.project.ProjectUtilities.VariableNames;
import wres.io.reading.DataSource.DataDisposition;
import wres.io.retrieval.DataAccessException;
import wres.io.utilities.NoDataException;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;

/**
 * Provides helpers related to the project declaration in combination with the ingested time-series data.
 * @author James Brown
 */

public class InMemoryProject implements Project
{
    private static final String EXPECTED_LEFT_OR_RIGHT_OR_BASELINE = "': expected LEFT or RIGHT or BASELINE.";

    private static final String UNEXPECTED_CONTEXT = "Unexpected context '";

    private static final Logger LOGGER = LoggerFactory.getLogger( InMemoryProject.class );

    /** Project declaration. */
    private final ProjectConfig projectConfig;

    /** Time-series data. */
    private final TimeSeriesStore timeSeriesStore;

    /** The project identifier. */
    private long projectId;

    /** The measurement unit. */
    private String measurementUnit = null;

    /** The features related to the project. */
    private Set<FeatureTuple> features;

    /** The feature groups related to the project. */
    private Set<FeatureGroup> featureGroups;

    /** The overall hash for the time-series used in the project. */
    private final String hash;

    /** Whether the left data is gridded. */
    private boolean leftUsesGriddedData = false;

    /** Whether the right data is gridded. */
    private boolean rightUsesGriddedData = false;

    /** Whether the baseline data is gridded. */
    private boolean baselineUsesGriddedData = false;

    /** The left-ish variable to evaluate. */
    private String leftVariable;

    /** The right-ish variable to evaluate. */
    private String rightVariable;

    /** The baseline-ish variable to evaluate. */
    private String baselineVariable;

    /** The desired time scale. */
    private TimeScaleOuter desiredTimeScale;

    /**
     * Creates an instance.
     * @param projectConfig the project declaration
     * @param timeSeriesStore the time-series data
     * @param ingestResults the ingest results
     */
    public InMemoryProject( ProjectConfig projectConfig,
                            TimeSeriesStore timeSeriesStore,
                            List<IngestResult> ingestResults )
    {
        Objects.requireNonNull( projectConfig );
        this.timeSeriesStore = timeSeriesStore;
        this.projectConfig = projectConfig;
        this.setUsesGriddedData( ingestResults );
        this.hash = this.getHash( timeSeriesStore );
        this.setFeaturesAndFeatureGroups();
        this.validateEnsembleConditions();
        this.setVariablesToEvaluate();

        if ( this.features.isEmpty() && this.featureGroups.isEmpty() )
        {
            throw new NoDataException( "Failed to identify any features with data on both the left and right sides for "
                                       + "the variables and other declaration supplied. Please check that the "
                                       + "declaration is expected to produce some features with time-series data on "
                                       + "both sides of the pairing." );
        }
    }

    @Override
    public ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    /**
     * @return the measurement unit, which is either the declared unit or the analyzed unit, but possibly null
     * @throws DataAccessException if the measurement unit could not be determined
     * @throws IllegalArgumentException if the project identity is required and undefined
     */

    @Override
    public String getMeasurementUnit()
    {
        // Declared unit available?
        String declaredUnit = this.getProjectConfig()
                                  .getPair()
                                  .getUnit();
        if ( Objects.isNull( this.measurementUnit ) && Objects.nonNull( declaredUnit ) && !declaredUnit.isBlank() )
        {
            this.measurementUnit = declaredUnit;

            LOGGER.debug( "Determined the measurement unit from the project declaration as {}.",
                          this.measurementUnit );
        }

        // Still not available? Then analyze the unit by looking for the most common right-ish unit
        if ( Objects.isNull( this.measurementUnit ) )
        {
            Stream<TimeSeries<?>> concat =
                    Stream.concat( this.timeSeriesStore.getSingleValuedSeries( LeftOrRightOrBaseline.RIGHT ),
                                   this.timeSeriesStore.getEnsembleSeries( LeftOrRightOrBaseline.RIGHT ) );
            Optional<String> mostCommon = concat.map( next -> next.getMetadata().getUnit() )
                                                .collect( Collectors.groupingBy( Function.identity(),
                                                                                 Collectors.counting() ) )
                                                .entrySet()
                                                .stream()
                                                .max( Map.Entry.comparingByValue() )
                                                .map( Map.Entry::getKey );

            if ( !mostCommon.isPresent() )
            {
                throw new DataAccessException( "Failed to determine the most common measurement unit associated with "
                                               + "the right-ish data." );
            }

            this.measurementUnit = mostCommon.get();

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Determined the measurement unit by analyzing the project sources. The analyzed "
                              + "measurement unit is {} and corresponds to the most commonly occurring unit "
                              + "among time-series from {} sources.",
                              this.measurementUnit,
                              LeftOrRightOrBaseline.RIGHT );
            }
        }

        return this.measurementUnit;
    }

    /**
     * Returns the desired time scale. In order of availability, this is:
     * 
     * <ol>
     * <li>The desired time scale provided on construction;</li>
     * <li>The Least Common Scale (LCS) computed from the input data; or</li>
     * <li>The LCS computed from the <code>existingTimeScale</code> provided in the input declaration.</li>
     * </ol>
     * 
     * The LCS is the smallest common multiple of the time scales associated with every ingested dataset for a given 
     * project, variable and feature. The LCS is computed from all sides of a pairing (left, right and baseline) 
     * collectively. 
     * 
     * @return the desired time scale or null if unknown
     * @throws DataAccessException if the existing time scales could not be obtained
     */

    @Override
    public TimeScaleOuter getDesiredTimeScale()
    {
        if ( Objects.nonNull( this.desiredTimeScale ) )
        {
            LOGGER.trace( "Discovered a desired time scale of {}.",
                          this.desiredTimeScale );

            return this.desiredTimeScale;
        }

        // Use the declared time scale
        TimeScaleOuter declaredScale = ConfigHelper.getDesiredTimeScale( this.getProjectConfig()
                                                                             .getPair() );
        if ( Objects.nonNull( declaredScale ) )
        {
            this.desiredTimeScale = declaredScale;

            LOGGER.trace( "Discovered that the desired time scale was declared explicitly as {}.",
                          this.desiredTimeScale );

            return this.desiredTimeScale;
        }

        // Find the Least Common Scale
        Stream<TimeSeries<?>> concat = Stream.concat( this.timeSeriesStore.getSingleValuedSeries(),
                                                      this.timeSeriesStore.getEnsembleSeries() );

        Set<TimeScaleOuter> existingTimeScales = concat.map( TimeSeries::getTimeScale )
                                                       .filter( Objects::nonNull )
                                                       .collect( Collectors.toSet() );

        // Look for the LCS among the ingested sources
        if ( !existingTimeScales.isEmpty() )
        {
            TimeScaleOuter leastCommonScale = TimeScaleOuter.getLeastCommonTimeScale( existingTimeScales );

            this.desiredTimeScale = leastCommonScale;

            LOGGER.trace( "Discovered that the desired time scale was not supplied on construction of the project. "
                          + "Instead, determined the desired time scale from the Least Common Scale of the ingested "
                          + "time-series, which was {}. The existing time scales were: {}.",
                          leastCommonScale,
                          existingTimeScales );

            return this.desiredTimeScale;
        }

        Inputs inputDeclaration = this.getProjectConfig()
                                      .getInputs();

        // Look for the LCS among the declared inputs
        if ( Objects.nonNull( inputDeclaration ) )
        {
            Set<TimeScaleOuter> declaredExistingTimeScales = new HashSet<>();
            TimeScaleConfig leftScaleConfig = inputDeclaration.getLeft().getExistingTimeScale();
            TimeScaleConfig rightScaleConfig = inputDeclaration.getLeft().getExistingTimeScale();

            if ( Objects.nonNull( leftScaleConfig ) )
            {
                declaredExistingTimeScales.add( TimeScaleOuter.of( leftScaleConfig ) );
            }
            if ( Objects.nonNull( rightScaleConfig ) )
            {
                declaredExistingTimeScales.add( TimeScaleOuter.of( rightScaleConfig ) );
            }
            if ( Objects.nonNull( inputDeclaration.getBaseline() )
                 && Objects.nonNull( inputDeclaration.getBaseline().getExistingTimeScale() ) )
            {
                declaredExistingTimeScales.add( TimeScaleOuter.of( inputDeclaration.getBaseline()
                                                                                   .getExistingTimeScale() ) );
            }

            if ( !declaredExistingTimeScales.isEmpty() )
            {
                TimeScaleOuter leastCommonScale = TimeScaleOuter.getLeastCommonTimeScale( declaredExistingTimeScales );

                this.desiredTimeScale = leastCommonScale;

                LOGGER.trace( "Discovered that the desired time scale was not supplied on construction of the project."
                              + " Instead, determined the desired time scale from the Least Common Scale of the "
                              + "declared inputs, which  was {}.",
                              leastCommonScale );

                return this.desiredTimeScale;
            }
        }

        return this.desiredTimeScale;
    }

    /**
     * Returns the set of {@link FeatureTuple} for the project.
     * the configuration, all locations that have been ingested are retrieved
     * @return A set of all feature tuples involved in the project
     */
    @Override
    public Set<FeatureTuple> getFeatures()
    {
        return this.features;
    }

    /**
     * Returns the set of {@link FeatureGroup} for the project.
     * @return A set of all feature groups involved in the project
     */
    @Override
    public Set<FeatureGroup> getFeatureGroups()
    {
        return this.featureGroups;
    }

    /**
     * @param lrb The side of data for which the variable is required
     * @return The declared data source for the specified orientation
     * @throws NullPointerException if the lrb is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    @Override
    public DataSourceConfig getDeclaredDataSource( LeftOrRightOrBaseline lrb )
    {
        Objects.requireNonNull( lrb );

        switch ( lrb )
        {
            case LEFT:
                return this.getLeft();
            case RIGHT:
                return this.getRight();
            case BASELINE:
                return this.getBaseline();
            default:
                throw new IllegalArgumentException( UNEXPECTED_CONTEXT + lrb
                                                    + EXPECTED_LEFT_OR_RIGHT_OR_BASELINE );
        }
    }

    /**
     * @param lrb The side of data for which the variable is required
     * @return The name of the variable for the specified side of data
     * @throws NullPointerException if the lrb is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    @Override
    public String getVariableName( LeftOrRightOrBaseline lrb )
    {
        Objects.requireNonNull( lrb );

        switch ( lrb )
        {
            case LEFT:
                return this.getLeftVariableName();
            case RIGHT:
                return this.getRightVariableName();
            case BASELINE:
                return this.getBaselineVariableName();
            default:
                throw new IllegalArgumentException( UNEXPECTED_CONTEXT + lrb
                                                    + "': expected LEFT or "
                                                    + "RIGHT or BASELINE." );
        }
    }

    /**
     * @param lrb The side of data for which the variable is required
     * @return The name of the declared variable for the specified side of data
     * @throws NullPointerException if the lrb is null
     * @throws IllegalArgumentException if the orientation is unrecognized
     */

    @Override
    public String getDeclaredVariableName( LeftOrRightOrBaseline lrb )
    {
        Objects.requireNonNull( lrb );

        switch ( lrb )
        {
            case LEFT:
                return this.getDeclaredLeftVariableName();
            case RIGHT:
                return this.getDeclaredRightVariableName();
            case BASELINE:
                return this.getDeclaredBaselineVariableName();
            default:
                throw new IllegalArgumentException( UNEXPECTED_CONTEXT + lrb
                                                    + "': expected LEFT or "
                                                    + "RIGHT or BASELINE." );
        }
    }

    /**
     * @return the earliest analysis duration
     */

    @Override
    public Duration getEarliestAnalysisDuration()
    {
        return ConfigHelper.getEarliestAnalysisDuration( this.getProjectConfig() );
    }

    /**
     * @return the latest analysis duration
     */

    @Override
    public Duration getLatestAnalysisDuration()
    {
        return ConfigHelper.getLatestAnalysisDuration( this.getProjectConfig() );
    }

    /**
     * @return the earliest possible day in a season. NULL unless specified in the configuration
     */
    @Override
    public MonthDay getEarliestDayInSeason()
    {
        return ConfigHelper.getEarliestDayInSeason( this.getProjectConfig() );
    }

    /**
     * @return the latest possible day in a season. NULL unless specified in the configuration
     */
    @Override
    public MonthDay getLatestDayInSeason()
    {
        return ConfigHelper.getLatestDayInSeason( this.getProjectConfig() );
    }

    @Override
    public boolean usesGriddedData( DataSourceConfig dataSourceConfig )
    {
        LeftOrRightOrBaseline lrb = ConfigHelper.getLeftOrRightOrBaseline( this.getProjectConfig(), dataSourceConfig );

        switch ( lrb )
        {
            case LEFT:
                return this.leftUsesGriddedData;
            case RIGHT:
                return this.rightUsesGriddedData;
            case BASELINE:
                return this.baselineUsesGriddedData;
            default:
                throw new IllegalArgumentException( "Unrecognized enumeration value in this context, '"
                                                    + lrb
                                                    + "'." );
        }
    }

    /**
     * Sets the status for gridded data.
     * @param ingestResults the ingest results
     */

    private void setUsesGriddedData( List<IngestResult> ingestResults )
    {
        this.leftUsesGriddedData = ingestResults.stream()
                                                .map( IngestResult::getDataSource )
                                                .filter( next -> next.getLeftOrRightOrBaseline() == LeftOrRightOrBaseline.LEFT )
                                                .anyMatch( next -> next.getDisposition() == DataDisposition.NETCDF_GRIDDED );
        this.rightUsesGriddedData = ingestResults.stream()
                                                 .map( IngestResult::getDataSource )
                                                 .filter( next -> next.getLeftOrRightOrBaseline() == LeftOrRightOrBaseline.RIGHT )
                                                 .anyMatch( next -> next.getDisposition() == DataDisposition.NETCDF_GRIDDED );
        this.baselineUsesGriddedData = ingestResults.stream()
                                                    .map( IngestResult::getDataSource )
                                                    .filter( next -> next.getLeftOrRightOrBaseline() == LeftOrRightOrBaseline.BASELINE )
                                                    .anyMatch( next -> next.getDisposition() == DataDisposition.NETCDF_GRIDDED );

        LOGGER.debug( "Set the status of gridded data to left={}, right={}, baseline={}.",
                      this.leftUsesGriddedData,
                      this.rightUsesGriddedData,
                      this.baselineUsesGriddedData );
    }

    /**
     * Returns unique identifier for this project's dataset
     * @return The unique ID
     */
    @Override
    public String getHash()
    {
        return this.hash;
    }

    /**
     * @return Whether or not baseline data is involved in the project
     */
    @Override
    public boolean hasBaseline()
    {
        return this.getBaseline() != null;
    }

    /**
     * @return Whether or not there is a generated baseline
     */
    @Override
    public boolean hasGeneratedBaseline()
    {
        return ConfigHelper.hasGeneratedBaseline( this.getBaseline() );
    }

    /**
     * @return the project identity
     */

    @Override
    public long getId()
    {
        return this.projectId;
    }

    /**
     * Return <code>true</code> if the project uses probability thresholds, otherwise <code>false</code>.
     * 
     * @return Whether or not the project uses probability thresholds
     */
    @Override
    public boolean hasProbabilityThresholds()
    {
        return ConfigHelper.hasProbabilityThresholds( this.getProjectConfig() );
    }

    /**
     * Saves the project.
     * @return true if this call resulted in the project being saved, false otherwise
     * @throws DataAccessException if the save fails for any reason
     */
    @Override
    public boolean save()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return "Project { Name: " + this.getProjectName()
               +
               ", Code: "
               + this.getHash()
               + " }";
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj instanceof InMemoryProject && this.hashCode() == obj.hashCode();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getHash() );
    }

    /**
     * Checks that the union of ensemble conditions will select some data, otherwise throws an exception.
     * 
     * @throws NoDataException if the conditions select no data
     * @throws DataAccessException if one or more ensemble conditions could not be evaluated
     */

    private void validateEnsembleConditions()
    {
        DataSourceConfig left = this.getProjectConfig().getInputs().getLeft();
        DataSourceConfig right = this.getProjectConfig().getInputs().getRight();
        DataSourceConfig baseline = this.getProjectConfig().getInputs().getBaseline();

        // Show all errors at once rather than drip-feeding
        List<String> failed = new ArrayList<>();
        List<String> failedLeft = this.getInvalidEnsembleConditions( LeftOrRightOrBaseline.LEFT, left );
        List<String> failedRight = this.getInvalidEnsembleConditions( LeftOrRightOrBaseline.RIGHT, right );
        List<String> failedBaseline = this.getInvalidEnsembleConditions( LeftOrRightOrBaseline.BASELINE, baseline );

        failed.addAll( failedLeft );
        failed.addAll( failedRight );
        failed.addAll( failedBaseline );

        if ( !failed.isEmpty() )
        {
            throw new NoDataException( "Of the filters that were defined for ensemble names, "
                                       + failed.size()
                                       + " of those filters did not select any data. Fix the declared filters to "
                                       + "ensure that each filter selects some data. The invalid filters are: "
                                       + failed
                                       + "." );
        }
    }

    /**
     * Sets the variables to evaluate. Begins by looking at the declaration. If it cannot find a declared variable for 
     * any particular left/right/baseline context, it looks at the data instead. If there is more than one possible 
     * name and it does not exactly match the name identified for the other side of the pairing, then an exception is 
     * thrown because declaration is require to disambiguate. Otherwise, it chooses the single variable name and warns 
     * about the assumption made when using the data to disambiguate.
     * 
     * @throws DataAccessException if the variable information could not be determined from the data
     */

    private void setVariablesToEvaluate()
    {
        // The set of possibilities to validate
        Set<String> leftNames = new HashSet<>();
        Set<String> rightNames = new HashSet<>();
        Set<String> baselineNames = new HashSet<>();

        boolean leftAuto = false;
        boolean rightAuto = false;
        boolean baselineAuto = false;

        // Left declared?
        if ( Objects.nonNull( this.getLeft().getVariable() ) )
        {
            String name = this.getLeft().getVariable().getValue();
            leftNames.add( name );
        }
        // No, look at data
        else
        {
            Set<String> names = this.getVariableNameByInspectingData( LeftOrRightOrBaseline.LEFT );
            leftNames.addAll( names );
            leftAuto = true;
        }

        // Right declared?
        if ( Objects.nonNull( this.getRight().getVariable() ) )
        {
            String name = this.getRight().getVariable().getValue();
            rightNames.add( name );
        }
        // No, look at data
        else
        {
            Set<String> names = this.getVariableNameByInspectingData( LeftOrRightOrBaseline.RIGHT );
            rightNames.addAll( names );
            rightAuto = true;
        }

        // Baseline declared?
        if ( this.hasBaseline() )
        {
            if ( Objects.nonNull( this.getBaseline().getVariable() ) )
            {
                String name = this.getBaseline().getVariable().getValue();
                baselineNames.add( name );
            }
            // No, look at data
            else
            {
                Set<String> names = this.getVariableNameByInspectingData( LeftOrRightOrBaseline.BASELINE );
                baselineNames.addAll( names );
                baselineAuto = true;
            }
        }

        LOGGER.debug( "While looking for variable names to evaluate, discovered {} on the LEFT side, {} on the RIGHT "
                      + "side and {} on the BASELINE side. LEFT autodetected: {}, RIGHT autodetected: {}, BASELINE "
                      + "auto-detected: {}.",
                      leftNames,
                      rightNames,
                      baselineNames,
                      leftAuto,
                      rightAuto,
                      baselineAuto );

        VariableNames variableNames = ProjectUtilities.getVariableNames( this.getProjectConfig(),
                                                                         Collections.unmodifiableSet( leftNames ),
                                                                         Collections.unmodifiableSet( rightNames ),
                                                                         Collections.unmodifiableSet( baselineNames ) );

        this.leftVariable = variableNames.getLeftVariableName();
        this.rightVariable = variableNames.getRightVariableName();
        this.baselineVariable = variableNames.getBaselineVariableName();

        ProjectUtilities.validateVariableNames( this.getDeclaredLeftVariableName(),
                                                this.getDeclaredRightVariableName(),
                                                this.getDeclaredBaselineVariableName(),
                                                this.getLeftVariableName(),
                                                this.getRightVariableName(),
                                                this.getBaselineVariableName(),
                                                this.hasBaseline() );
    }

    /**
     * Returns the hash value of the project based on the time-series data.
     * @param store the store of time-series data
     * @return the hash
     */

    private String getHash( TimeSeriesStore store )
    {
        Stream<TimeSeries<?>> series = Stream.concat( store.getSingleValuedSeries(), store.getEnsembleSeries() );

        return Integer.toString( series.collect( Collectors.toList() )
                                       .hashCode() );
    }

    /**
     * Determines the possible variable names by inspecting the data.
     * 
     * @param lrb the context
     * @return the possible variable names
     * @throws DataAccessException if the variable information could not be determined from the data
     * @throws ProjectConfigException if declaration is required to disambiguate the variable name
     */

    private Set<String> getVariableNameByInspectingData( LeftOrRightOrBaseline lrb )
    {
        Stream<TimeSeries<?>> series = Stream.concat( this.timeSeriesStore.getSingleValuedSeries( lrb ),
                                                      this.timeSeriesStore.getEnsembleSeries( lrb ) );

        return series.map( next -> next.getMetadata().getVariableName() ).collect( Collectors.toSet() );
    }

    /**
     * Checks for any invalid ensemble conditions and returns a string representation of the invalid conditions.
     * 
     * @param lrb the orientation of the source
     * @param config the source configuration whose ensemble conditions should be validated
     * @return a string representation of the invalid conditions 
     * @throws DataAccessException if one or more ensemble conditions could not be evaluated
     */

    private List<String> getInvalidEnsembleConditions( LeftOrRightOrBaseline lrb,
                                                       DataSourceConfig config )
    {
        List<String> failed = new ArrayList<>();

        if ( Objects.nonNull( config ) && !config.getEnsemble().isEmpty() )
        {
            List<EnsembleCondition> conditions = config.getEnsemble();
            for ( EnsembleCondition condition : conditions )
            {
                Stream<TimeSeries<Ensemble>> series = this.timeSeriesStore.getEnsembleSeries( lrb );
                String name = condition.getName();

                boolean dataExists = series.flatMap( next -> next.getEvents().stream() )
                                           .map( next -> next.getValue().getLabels() )
                                           .flatMap( labels -> Arrays.stream( labels.getLabels() ) )
                                           .anyMatch( name::equals );

                if ( !dataExists )
                {
                    ToStringBuilder builder =
                            new ToStringBuilder( condition,
                                                 ToStringStyle.SHORT_PREFIX_STYLE ).append( "orientation", lrb )
                                                                                   .append( "name",
                                                                                            condition.getName() )
                                                                                   .append( "exclude",
                                                                                            condition.isExclude() );

                    failed.add( builder.toString() );
                }

            }
        }

        return Collections.unmodifiableList( failed );
    }

    /**
     * Builds a set of gridded feature tuples. Assumes that all dimensions have the same tuple (i.e., cannot currently
     * pair grids with different features. Feature groupings are also not supported.
     * 
     * @return a set of gridded feature tuples
     */

    private Set<FeatureTuple> getGriddedFeatureTuples()
    {
        LOGGER.debug( "Getting details of intersecting features for gridded data." );

        Stream<TimeSeries<?>> leftSeries =
                Stream.concat( this.timeSeriesStore.getSingleValuedSeries( LeftOrRightOrBaseline.LEFT ),
                               this.timeSeriesStore.getEnsembleSeries( LeftOrRightOrBaseline.LEFT ) );

        Set<FeatureKey> griddedFeatures = leftSeries.map( next -> next.getMetadata()
                                                                      .getFeature() )
                                                    .collect( Collectors.toSet() );

        Set<FeatureTuple> featureTuples = new HashSet<>();

        for ( FeatureKey nextFeature : griddedFeatures )
        {
            Geometry geometry = MessageFactory.parse( nextFeature );
            GeometryTuple geoTuple = null;
            if ( this.hasBaseline() )
            {
                geoTuple = MessageFactory.getGeometryTuple( geometry, geometry, geometry );
            }
            else
            {
                geoTuple = MessageFactory.getGeometryTuple( geometry, geometry, null );
            }

            FeatureTuple featureTuple = FeatureTuple.of( geoTuple );
            featureTuples.add( featureTuple );
        }

        return Collections.unmodifiableSet( featureTuples );
    }

    /**
     * Sets the features and feature groups.
     * @throws DataAccessException if the features and/or feature groups could not be set
     */

    private void setFeaturesAndFeatureGroups()
    {
        LOGGER.debug( "Setting the features and feature groups for project {}.", this.getId() );

        // Gridded features?
        // Yes
        if ( this.usesGriddedData( this.getRight() ) )
        {
            Set<FeatureTuple> griddedTuples = this.getGriddedFeatureTuples();

            this.features = Collections.unmodifiableSet( griddedTuples );
            Set<FeatureGroup> fGroups = ProjectUtilities.getFeatureGroups( this.features,
                                                                           Set.of(),
                                                                           this.getProjectConfig()
                                                                               .getPair(),
                                                                           this.getId() );
            this.featureGroups = fGroups;

            LOGGER.debug( "Finished setting the features for project {}. Discovered {} gridded features.",
                          this.getId(),
                          griddedTuples.size() );
        }
        else
        {
            Stream<TimeSeries<?>> leftSeries =
                    Stream.concat( this.timeSeriesStore.getSingleValuedSeries( LeftOrRightOrBaseline.LEFT ),
                                   this.timeSeriesStore.getEnsembleSeries( LeftOrRightOrBaseline.LEFT ) );

            Stream<TimeSeries<?>> rightSeries =
                    Stream.concat( this.timeSeriesStore.getSingleValuedSeries( LeftOrRightOrBaseline.RIGHT ),
                                   this.timeSeriesStore.getEnsembleSeries( LeftOrRightOrBaseline.RIGHT ) );

            Stream<TimeSeries<?>> baselineSeries =
                    Stream.concat( this.timeSeriesStore.getSingleValuedSeries( LeftOrRightOrBaseline.BASELINE ),
                                   this.timeSeriesStore.getEnsembleSeries( LeftOrRightOrBaseline.BASELINE ) );

            Map<String, FeatureKey> leftFeatures =
                    leftSeries.collect( Collectors.toMap( next -> next.getMetadata()
                                                                      .getFeature()
                                                                      .getName(),
                                                          next -> next.getMetadata()
                                                                      .getFeature(),
                                                          ( a, b ) -> a ) );

            Map<String, FeatureKey> rightFeatures =
                    rightSeries.collect( Collectors.toMap( next -> next.getMetadata()
                                                                       .getFeature()
                                                                       .getName(),
                                                           next -> next.getMetadata()
                                                                       .getFeature(),
                                                           ( a, b ) -> a ) );

            Map<String, FeatureKey> baselineFeatures =
                    baselineSeries.collect( Collectors.toMap( next -> next.getMetadata()
                                                                          .getFeature()
                                                                          .getName(),
                                                              next -> next.getMetadata()
                                                                          .getFeature(),
                                                              ( a, b ) -> a ) );

            PairConfig pairConfig = this.getProjectConfig()
                                        .getPair();

            // Create the singletons
            List<Feature> declaredSingletons = pairConfig.getFeature();

            Set<FeatureTuple> singletons = this.getFeatureTuplesFromDeclaredFeatures( declaredSingletons,
                                                                                      leftFeatures,
                                                                                      rightFeatures,
                                                                                      baselineFeatures );


            // Create the feature groups
            List<Feature> groupedFeatures = pairConfig.getFeatureGroup()
                                                      .stream()
                                                      .flatMap( next -> next.getFeature().stream() )
                                                      .collect( Collectors.toList() );

            Set<FeatureTuple> groupedTuples = this.getFeatureTuplesFromDeclaredFeatures( groupedFeatures,
                                                                                         leftFeatures,
                                                                                         rightFeatures,
                                                                                         baselineFeatures );

            Set<FeatureGroup> fGroups = ProjectUtilities.getFeatureGroups( singletons,
                                                                           groupedTuples,
                                                                           pairConfig,
                                                                           this.getId() );

            this.featureGroups = Collections.unmodifiableSet( fGroups );
            this.features = Collections.unmodifiableSet( singletons );

            LOGGER.debug( "Finished setting the feature groups for project {}. Discovered {} feature groups: {}.",
                          this.getId(),
                          this.featureGroups.size(),
                          this.featureGroups );
        }
    }

    /**
     * @param features the declared features
     * @param leftFeatures the left feature names against feature keys
     * @param rightFeatures the right feature names against feature keys
     * @param baselineFeatures the baseline feature names against feature keys
     * @return the feature tuples
     */

    private Set<FeatureTuple> getFeatureTuplesFromDeclaredFeatures( List<Feature> features,
                                                                    Map<String, FeatureKey> leftFeatures,
                                                                    Map<String, FeatureKey> rightFeatures,
                                                                    Map<String, FeatureKey> baselineFeatures )
    {
        // No features declared?
        if ( features.isEmpty() )
        {
            return this.getFeaturesWhenNoneDeclared( leftFeatures, rightFeatures, baselineFeatures );
        }

        // Declared features
        Set<FeatureTuple> featureTuples = new HashSet<>();
        for ( Feature next : features )
        {
            String leftName = next.getLeft();
            String rightName = next.getRight();
            String baselineName = next.getBaseline();

            if ( leftFeatures.containsKey( leftName ) && rightFeatures.containsKey( rightName ) )
            {
                FeatureKey left = leftFeatures.get( leftName );
                FeatureKey right = rightFeatures.get( rightName );
                FeatureKey baseline = baselineFeatures.get( baselineName );
                GeometryTuple geometryTuple = MessageFactory.getGeometryTuple( left, right, baseline );
                FeatureTuple featureTuple = FeatureTuple.of( geometryTuple );
                featureTuples.add( featureTuple );
            }
        }

        return Collections.unmodifiableSet( featureTuples );
    }

    /**
     * @param leftFeatures the left feature names against feature keys
     * @param rightFeatures the right feature names against feature keys
     * @param baselineFeatures the baseline feature names against feature keys
     * @return the feature tuples when no features are declared
     */

    private Set<FeatureTuple> getFeaturesWhenNoneDeclared( Map<String, FeatureKey> leftFeatures,
                                                           Map<String, FeatureKey> rightFeatures,
                                                           Map<String, FeatureKey> baselineFeatures )
    {
        LOGGER.debug( "No features were declared. Attempting to correlate left/right/baseline features by common "
                      + "feature name, else one feature on all sides." );

        Set<FeatureTuple> featureTuples = new HashSet<>();

        for ( Map.Entry<String, FeatureKey> nextFeature : leftFeatures.entrySet() )
        {
            String name = nextFeature.getKey();
            FeatureKey left = nextFeature.getValue();
            FeatureKey right = rightFeatures.get( name );
            FeatureKey baseline = baselineFeatures.get( name );

            // Still no correlated feature, what about a unique feature?
            if ( Objects.isNull( right ) && rightFeatures.size() == 1 )
            {
                right = rightFeatures.values()
                                     .iterator()
                                     .next();
                LOGGER.debug( "Assuming that {} and {} are correlated features to evaluate.", left, right );
            }
            if ( Objects.isNull( baseline ) && baselineFeatures.size() == 1 )
            {
                baseline = baselineFeatures.values()
                                           .iterator()
                                           .next();
                LOGGER.debug( "Assuming that {} and {} are correlated features to evaluate.", left, baseline );
            }

            if ( Objects.nonNull( right ) )
            {
                GeometryTuple geometryTuple = MessageFactory.getGeometryTuple( left, right, baseline );
                FeatureTuple featureTuple = FeatureTuple.of( geometryTuple );
                featureTuples.add( featureTuple );
            }
            else
            {
                LOGGER.debug( "Failed to correlate left feature {} with a right feature.", left );
            }
        }

        return Collections.unmodifiableSet( featureTuples );
    }

    /**
     * @return the project name
     */
    private String getProjectName()
    {
        return this.projectConfig.getName();
    }

    /**
     * @see #getDeclaredLeftVariableName()
     * @return The name of the left variable or null if determined from the data and the data has yet to be inspected
     */
    private String getLeftVariableName()
    {
        if ( Objects.isNull( this.leftVariable ) )
        {
            return this.getDeclaredLeftVariableName();
        }

        return this.leftVariable;
    }

    /**
     * @return The name of the right variable or null if determined from the data and the data has yet to be inspected
     */
    private String getRightVariableName()
    {
        if ( Objects.isNull( this.rightVariable ) )
        {
            return this.getDeclaredRightVariableName();
        }

        return this.rightVariable;
    }

    /**
     * @return The name of the baseline variable or null if determined from the data and the data has yet to be 
     *            inspected
     */
    private String getBaselineVariableName()
    {
        if ( Objects.isNull( this.baselineVariable ) )
        {
            return this.getDeclaredBaselineVariableName();
        }

        return this.baselineVariable;
    }

    /**
     * @see #getLeftVariableName()
     * @return The declared left variable name or null if undeclared
     */
    private String getDeclaredLeftVariableName()
    {
        return ConfigHelper.getVariableName( this.getLeft() );
    }

    /**
     * @see #getRightVariableName()
     * @return The declared right variable name or null if undeclared
     */
    private String getDeclaredRightVariableName()
    {
        return ConfigHelper.getVariableName( this.getRight() );
    }

    /**
     * @see #getBaselineVariableName()
     * @return The declared baseline variable name or null if undeclared
     */
    private String getDeclaredBaselineVariableName()
    {
        String variableName = null;

        if ( this.hasBaseline() )
        {
            variableName = ConfigHelper.getVariableName( this.getBaseline() );
        }

        return variableName;
    }

    /**
     * @return The left hand data source configuration
     */
    private DataSourceConfig getLeft()
    {
        return this.projectConfig.getInputs()
                                 .getLeft();
    }

    /**
     * @return The right hand data source configuration
     */
    private DataSourceConfig getRight()
    {
        return this.projectConfig.getInputs()
                                 .getRight();
    }

    /**
     * @return The baseline data source configuration
     */
    private DataSourceConfig getBaseline()
    {
        return this.projectConfig.getInputs()
                                 .getBaseline();
    }
}

