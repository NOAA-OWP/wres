package wres.io.project;

import java.time.Duration;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.TimeScale;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesStore;
import wres.datamodel.types.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.Feature;
import wres.io.NoProjectDataException;
import wres.io.ingesting.IngestResult;
import wres.io.project.ProjectUtilities.VariableNames;
import wres.reading.DataSource.DataDisposition;
import wres.io.retrieving.DataAccessException;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Provides helpers related to the project declaration in combination with the ingested time-series data.
 * @author James Brown
 */

public class InMemoryProject implements Project
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( InMemoryProject.class );

    /** Reused string. */
    private static final String UNRECOGNIZED_DATASET_ORIENTATION_IN_THIS_CONTEXT =
            "Unrecognized dataset orientation in this context: ";

    /** Project declaration. */
    private final EvaluationDeclaration declaration;

    /** Time-series data. */
    private final TimeSeriesStore timeSeriesStore;

    /** The overall hash for the time-series used in the project. */
    private final String hash;

    /** The measurement unit. */
    private String measurementUnit = null;

    /** The features related to the project. */
    private Set<FeatureTuple> features;

    /** The feature groups related to the project. */
    private Set<FeatureGroup> featureGroups;

    /** The singleton feature groups for which statistics should not be published, if any. */
    private Set<FeatureGroup> doNotPublish;

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
     * @param declaration the project declaration
     * @param timeSeriesStore the time-series data
     * @param ingestResults the ingest results
     */
    public InMemoryProject( EvaluationDeclaration declaration,
                            TimeSeriesStore timeSeriesStore,
                            List<IngestResult> ingestResults )
    {
        Objects.requireNonNull( declaration );
        this.timeSeriesStore = timeSeriesStore;
        this.declaration = declaration;
        this.setUsesGriddedData( ingestResults );
        this.hash = this.getHash( timeSeriesStore );
        this.setFeaturesAndFeatureGroups();
        this.validateEnsembleConditions();
        this.setVariablesToEvaluate();

        if ( this.features.isEmpty() && this.featureGroups.isEmpty() )
        {
            throw new NoProjectDataException(
                    "Failed to identify any features with data on all required sides (left, right "
                    + "and, when declared, baseline) for the variables and other declaration "
                    + "supplied. Please check that the declaration is expected to produce some "
                    + "features with time-series data on both sides of the pairing." );
        }
    }

    @Override
    public EvaluationDeclaration getDeclaration()
    {
        return this.declaration;
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
        String declaredUnit = this.getDeclaration()
                                  .unit();
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
                    Stream.concat( this.timeSeriesStore.getSingleValuedSeries( DatasetOrientation.RIGHT ),
                                   this.timeSeriesStore.getEnsembleSeries( DatasetOrientation.RIGHT ) );
            Optional<String> mostCommon = concat.map( next -> next.getMetadata().getUnit() )
                                                .collect( Collectors.groupingBy( Function.identity(),
                                                                                 Collectors.counting() ) )
                                                .entrySet()
                                                .stream()
                                                .max( Map.Entry.comparingByValue() )
                                                .map( Map.Entry::getKey );

            if ( mostCommon.isEmpty() )
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
                              DatasetOrientation.RIGHT );
            }
        }

        return this.measurementUnit;
    }

    /**
     * Returns the desired timescale. In order of availability, this is:
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
     * @return the desired timescale or null if unknown
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

        // Use the declared timescale
        TimeScale declaredScale = this.getDeclaration()
                                      .timeScale();
        if ( Objects.nonNull( declaredScale ) )
        {
            this.desiredTimeScale = TimeScaleOuter.of( declaredScale.timeScale() );

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

        // Look for the LCS among the declared inputs
        Set<TimeScaleOuter> declaredExistingTimeScales = DeclarationUtilities.getSourceTimeScales( declaration )
                                                                             .stream()
                                                                             .map( TimeScaleOuter::of )
                                                                             .collect( Collectors.toUnmodifiableSet() );

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

        return this.desiredTimeScale;
    }

    @Override
    public Set<FeatureTuple> getFeatures()
    {
        if ( Objects.isNull( this.features ) )
        {
            throw new IllegalStateException( "The features have not been set." );
        }

        return this.features;
    }

    @Override
    public Set<FeatureGroup> getFeatureGroups()
    {
        if ( Objects.isNull( this.featureGroups ) )
        {
            throw new IllegalStateException( "The feature groups have not been set." );
        }

        return this.featureGroups;
    }

    @Override
    public Set<FeatureGroup> getFeatureGroupsForWhichStatisticsShouldNotBePublished()
    {
        if ( Objects.isNull( this.doNotPublish ) )
        {
            throw new IllegalStateException( "The feature groups used only for summary statistics have not been set." );
        }

        return this.doNotPublish;
    }

    @Override
    public String getVariableName( DatasetOrientation orientation )
    {
        Objects.requireNonNull( orientation );

        return switch ( orientation )
        {
            case LEFT -> this.getLeftVariableName();
            case RIGHT -> this.getRightVariableName();
            case BASELINE -> this.getBaselineVariableName();
            default -> throw new IllegalStateException( UNRECOGNIZED_DATASET_ORIENTATION_IN_THIS_CONTEXT
                                                        + orientation );
        };
    }

    @Override
    public SortedSet<String> getEnsembleLabels( DatasetOrientation orientation )
    {
        Objects.requireNonNull( orientation );

        Stream<TimeSeries<Ensemble>> series = this.timeSeriesStore.getEnsembleSeries( orientation );
        List<Ensemble> ensembles = series.flatMap( n -> n.getEvents()
                                                         .stream()
                                                         .map( Event::getValue ) )
                                         .toList();

        SortedSet<String> labels = ensembles.stream()
                                            .flatMap( e -> Arrays.stream( e.getLabels().getLabels() ) )
                                            .collect( Collectors.toCollection( TreeSet::new ) );

        // Return some default labels
        if ( labels.isEmpty() )
        {
            int memberCount = ensembles.stream()
                                       .mapToInt( Ensemble::size )
                                       .max()
                                       .orElse( 0 );
            if ( memberCount > 0 )
            {
                labels = ProjectUtilities.getSeries( memberCount );
            }
        }

        SortedSet<String> unmodifiable = Collections.unmodifiableSortedSet( labels );

        return ProjectUtilities.filter( unmodifiable, this.getDeclaration(), orientation );
    }

    @Override
    public Duration getEarliestAnalysisDuration()
    {
        return DeclarationUtilities.getEarliestAnalysisDuration( this.getDeclaration() );
    }

    @Override
    public Duration getLatestAnalysisDuration()
    {
        return DeclarationUtilities.getLatestAnalysisDuration( this.getDeclaration() );
    }

    @Override
    public MonthDay getStartOfSeason()
    {
        return DeclarationUtilities.getStartOfSeason( this.getDeclaration() );
    }

    @Override
    public MonthDay getEndOfSeason()
    {
        return DeclarationUtilities.getEndOfSeason( this.getDeclaration() );
    }

    @Override
    public boolean usesGriddedData( DatasetOrientation orientation )
    {
        return switch ( orientation )
        {
            case LEFT -> this.leftUsesGriddedData;
            case RIGHT -> this.rightUsesGriddedData;
            case BASELINE -> this.baselineUsesGriddedData;
            default -> throw new IllegalStateException( UNRECOGNIZED_DATASET_ORIENTATION_IN_THIS_CONTEXT
                                                        + orientation );
        };
    }

    /**
     * Sets the status for gridded data.
     * @param ingestResults the ingest results
     */

    private void setUsesGriddedData( List<IngestResult> ingestResults )
    {
        this.leftUsesGriddedData = ingestResults.stream()
                                                .map( IngestResult::getDataSource )
                                                .filter( next -> next.getDatasetOrientation()
                                                                 == DatasetOrientation.LEFT )
                                                .anyMatch( next -> next.getDisposition()
                                                                   == DataDisposition.NETCDF_GRIDDED );
        this.rightUsesGriddedData = ingestResults.stream()
                                                 .map( IngestResult::getDataSource )
                                                 .filter( next -> next.getDatasetOrientation()
                                                                  == DatasetOrientation.RIGHT )
                                                 .anyMatch( next -> next.getDisposition()
                                                                    == DataDisposition.NETCDF_GRIDDED );
        this.baselineUsesGriddedData = ingestResults.stream()
                                                    .map( IngestResult::getDataSource )
                                                    .filter( next -> next.getDatasetOrientation()
                                                                     == DatasetOrientation.BASELINE )
                                                    .anyMatch( next -> next.getDisposition()
                                                                       == DataDisposition.NETCDF_GRIDDED );

        LOGGER.debug( "Set the status of gridded data to left={}, right={}, baseline={}.",
                      this.leftUsesGriddedData,
                      this.rightUsesGriddedData,
                      this.baselineUsesGriddedData );
    }

    @Override
    public String getHash()
    {
        return this.hash;
    }

    @Override
    public boolean hasBaseline()
    {
        return this.getBaseline() != null;
    }

    @Override
    public boolean hasGeneratedBaseline()
    {
        return DeclarationUtilities.hasGeneratedBaseline( this.getBaseline() );
    }

    @Override
    public long getId()
    {
        return 0;
    }

    @Override
    public boolean hasProbabilityThresholds()
    {
        return DeclarationUtilities.hasProbabilityThresholds( this.getDeclaration() );
    }

    @Override
    public boolean isUpscalingLenient( DatasetOrientation orientation )
    {
        return ProjectUtilities.isUpscalingLenient( orientation,
                                                    this.getDeclaration()
                                                        .timeScale(),
                                                    this.getDeclaration()
                                                        .rescaleLenience() );
    }

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
     * @throws NoProjectDataException if the conditions select no data
     * @throws DataAccessException if one or more ensemble conditions could not be evaluated
     */

    private void validateEnsembleConditions()
    {
        // Show all errors at once rather than drip-feeding
        Dataset left = this.getLeft();
        List<String> failedLeft = this.getInvalidEnsembleConditions( DatasetOrientation.LEFT, left );
        List<String> failed = new ArrayList<>( failedLeft );
        Dataset right = this.getRight();
        List<String> failedRight = this.getInvalidEnsembleConditions( DatasetOrientation.RIGHT, right );
        failed.addAll( failedRight );

        if ( this.hasBaseline() )
        {
            Dataset baseline = this.getBaseline()
                                   .dataset();
            List<String> failedBaseline = this.getInvalidEnsembleConditions( DatasetOrientation.BASELINE, baseline );
            failed.addAll( failedBaseline );
        }

        if ( !failed.isEmpty() )
        {
            throw new NoProjectDataException( "Of the filters that were defined for ensemble names, "
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
     * thrown because declaration is required to disambiguate. Otherwise, it chooses the single variable name and warns
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
        String leftName = this.getVariableName( DatasetOrientation.LEFT );
        if ( Objects.nonNull( leftName ) )
        {
            leftNames.add( leftName );
        }
        // No, look at data
        else
        {
            Set<String> names = this.getVariableNameByInspectingData( DatasetOrientation.LEFT );
            leftNames.addAll( names );
            leftAuto = true;
        }

        // Right declared?
        String rightName = this.getVariableName( DatasetOrientation.RIGHT );
        if ( Objects.nonNull( rightName ) )
        {
            rightNames.add( rightName );
        }
        // No, look at data
        else
        {
            Set<String> names = this.getVariableNameByInspectingData( DatasetOrientation.RIGHT );
            rightNames.addAll( names );
            rightAuto = true;
        }

        // Baseline declared?
        if ( this.hasBaseline() )
        {
            String baselineName = this.getVariableName( DatasetOrientation.BASELINE );
            if ( Objects.nonNull( baselineName ) )
            {
                baselineNames.add( baselineName );
            }
            // No, look at data
            else
            {
                Set<String> names = this.getVariableNameByInspectingData( DatasetOrientation.BASELINE );
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

        VariableNames variableNames = ProjectUtilities.getVariableNames( this.getDeclaration(),
                                                                         Collections.unmodifiableSet( leftNames ),
                                                                         Collections.unmodifiableSet( rightNames ),
                                                                         Collections.unmodifiableSet( baselineNames ) );

        this.leftVariable = variableNames.leftVariableName();
        this.rightVariable = variableNames.rightVariableName();
        this.baselineVariable = variableNames.baselineVariableName();

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

        return Integer.toString( series.toList()
                                       .hashCode() );
    }

    /**
     * Determines the possible variable names by inspecting the data.
     *
     * @param orientation the context
     * @return the possible variable names
     * @throws DataAccessException if the variable information could not be determined from the data
     */

    private Set<String> getVariableNameByInspectingData( DatasetOrientation orientation )
    {
        Stream<TimeSeries<?>> series = Stream.concat( this.timeSeriesStore.getSingleValuedSeries( orientation ),
                                                      this.timeSeriesStore.getEnsembleSeries( orientation ) );

        return series.map( next -> next.getMetadata().getVariableName() ).collect( Collectors.toSet() );
    }

    /**
     * Checks for any invalid ensemble conditions and returns a string representation of the invalid conditions.
     *
     * @param orientation the orientation of the dataset
     * @param dataset the dataset whose ensemble conditions should be validated
     * @return a string representation of the invalid conditions 
     * @throws DataAccessException if one or more ensemble conditions could not be evaluated
     */

    private List<String> getInvalidEnsembleConditions( DatasetOrientation orientation,
                                                       Dataset dataset )
    {
        List<String> failed = new ArrayList<>();

        EnsembleFilter filter = dataset.ensembleFilter();
        if ( Objects.nonNull( filter ) )
        {
            for ( String name : filter.members() )
            {
                Stream<TimeSeries<Ensemble>> series = this.timeSeriesStore.getEnsembleSeries( orientation );
                boolean dataExists = series.flatMap( next -> next.getEvents()
                                                                 .stream() )
                                           .map( next -> next.getValue()
                                                             .getLabels() )
                                           .flatMap( labels -> Arrays.stream( labels.getLabels() ) )
                                           .anyMatch( name::equals );

                if ( !dataExists )
                {
                    ToStringBuilder builder =
                            new ToStringBuilder( ToStringStyle.SHORT_PREFIX_STYLE ).append( "orientation",
                                                                                            orientation )
                                                                                   .append( "name", name )
                                                                                   .append( "exclude",
                                                                                            filter.exclude() );

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
                Stream.concat( this.timeSeriesStore.getSingleValuedSeries( DatasetOrientation.LEFT ),
                               this.timeSeriesStore.getEnsembleSeries( DatasetOrientation.LEFT ) );

        Set<Feature> griddedFeatures = leftSeries.map( next -> next.getMetadata()
                                                                   .getFeature() )
                                                 .collect( Collectors.toSet() );

        Set<FeatureTuple> featureTuples = new HashSet<>();

        for ( Feature nextFeature : griddedFeatures )
        {
            Geometry geometry = MessageFactory.parse( nextFeature );
            GeometryTuple geoTuple;
            if ( this.hasBaseline() )
            {
                geoTuple = wres.statistics.MessageFactory.getGeometryTuple( geometry, geometry, geometry );
            }
            else
            {
                geoTuple = wres.statistics.MessageFactory.getGeometryTuple( geometry, geometry, null );
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
        if ( this.usesGriddedData( DatasetOrientation.RIGHT ) )
        {
            Set<FeatureTuple> griddedTuples = this.getGriddedFeatureTuples();

            this.features = griddedTuples;
            ProjectUtilities.FeatureGroupsPlus groups = ProjectUtilities.getFeatureGroups( this.features,
                                                                                           Set.of(),
                                                                                           this.getDeclaration(),
                                                                                           this.getId() );
            this.featureGroups = groups.featureGroups();
            this.doNotPublish = groups.doNotPublish();

            LOGGER.debug( "Finished setting the features for project {}. Discovered {} gridded features.",
                          this.getId(),
                          griddedTuples.size() );
        }
        else
        {
            Stream<TimeSeries<?>> leftSeries =
                    Stream.concat( this.timeSeriesStore.getSingleValuedSeries( DatasetOrientation.LEFT ),
                                   this.timeSeriesStore.getEnsembleSeries( DatasetOrientation.LEFT ) );

            Stream<TimeSeries<?>> rightSeries =
                    Stream.concat( this.timeSeriesStore.getSingleValuedSeries( DatasetOrientation.RIGHT ),
                                   this.timeSeriesStore.getEnsembleSeries( DatasetOrientation.RIGHT ) );

            Stream<TimeSeries<?>> baselineSeries =
                    Stream.concat( this.timeSeriesStore.getSingleValuedSeries( DatasetOrientation.BASELINE ),
                                   this.timeSeriesStore.getEnsembleSeries( DatasetOrientation.BASELINE ) );

            Map<String, Feature> leftFeaturesWithData =
                    leftSeries.collect( Collectors.toMap( next -> next.getMetadata()
                                                                      .getFeature()
                                                                      .getName(),
                                                          next -> next.getMetadata()
                                                                      .getFeature(),
                                                          ( a, b ) -> a ) );

            Map<String, Feature> rightFeaturesWithData =
                    rightSeries.collect( Collectors.toMap( next -> next.getMetadata()
                                                                       .getFeature()
                                                                       .getName(),
                                                           next -> next.getMetadata()
                                                                       .getFeature(),
                                                           ( a, b ) -> a ) );

            Map<String, Feature> baselineFeaturesWithData =
                    baselineSeries.collect( Collectors.toMap( next -> next.getMetadata()
                                                                          .getFeature()
                                                                          .getName(),
                                                              next -> next.getMetadata()
                                                                          .getFeature(),
                                                              ( a, b ) -> a ) );

            EvaluationDeclaration innerDeclaration = this.getDeclaration();

            // Get the declared singletons
            Set<GeometryTuple> declaredSingletons = this.getDeclaredFeatures();
            // Correlate with those from the times-series data
            Set<FeatureTuple> singletons = this.getCorrelatedFeatures( declaredSingletons,
                                                                       leftFeaturesWithData,
                                                                       rightFeaturesWithData,
                                                                       baselineFeaturesWithData );

            // Get the feature tuples within feature groups
            Set<GeometryTuple> groupedFeatures = this.getDeclaredFeatureGroups()
                                                     .stream()
                                                     .flatMap( next -> next.getGeometryTuplesList()
                                                                           .stream() )
                                                     .collect( Collectors.toSet() );

            // Correlate with those from the time-series data
            Set<FeatureTuple> groupedTuples = this.getCorrelatedFeatures( groupedFeatures,
                                                                          leftFeaturesWithData,
                                                                          rightFeaturesWithData,
                                                                          baselineFeaturesWithData );

            // Filter the singleton features against any spatial mask, unless there is gridded data, which is masked
            // upfront. Do this before forming the groups, which include singleton groups
            if ( !this.usesGriddedData( DatasetOrientation.RIGHT ) )
            {
                singletons = ProjectUtilities.filterFeatures( singletons, this.getDeclaration()
                                                                              .spatialMask() );
            }

            ProjectUtilities.FeatureGroupsPlus groups = ProjectUtilities.getFeatureGroups( singletons,
                                                                                           groupedTuples,
                                                                                           innerDeclaration,
                                                                                           this.getId() );
            Set<FeatureGroup> innerFeatureGroups = groups.featureGroups();

            // Filter the multi-group features against any spatial mask, unless there is gridded data, which is masked
            // upfront
            if ( !this.usesGriddedData( DatasetOrientation.RIGHT ) )
            {
                innerFeatureGroups = ProjectUtilities.filterFeatureGroups( innerFeatureGroups, this.getDeclaration()
                                                                                                   .spatialMask() );
            }

            // Filter the features and feature groups against any spatial mask
            this.features = Collections.unmodifiableSet( singletons );
            this.featureGroups = Collections.unmodifiableSet( innerFeatureGroups );
            this.doNotPublish = groups.doNotPublish();

            LOGGER.debug( "Finished setting the feature groups for project {}. Discovered {} feature groups: {}.",
                          this.getId(),
                          this.featureGroups.size(),
                          this.featureGroups );
        }
    }

    /**
     * @return the declared features
     */
    private Set<GeometryTuple> getDeclaredFeatures()
    {
        if ( Objects.isNull( this.getDeclaration()
                                 .features() ) )
        {
            return Set.of();
        }

        return this.getDeclaration()
                   .features()
                   .geometries();
    }

    /**
     * @return the declared feature groups
     */
    private Set<GeometryGroup> getDeclaredFeatureGroups()
    {
        if ( Objects.isNull( this.getDeclaration()
                                 .featureGroups() ) )
        {
            return Set.of();
        }

        return this.getDeclaration()
                   .featureGroups()
                   .geometryGroups();
    }

    /**
     * Attempts to correlate the declared feature tuples with the oriented features that have data, returning the
     * feature tuples with data.
     * @param features the declared features
     * @param leftFeatures the left feature names against feature keys
     * @param rightFeatures the right feature names against feature keys
     * @param baselineFeatures the baseline feature names against feature keys
     * @return the feature tuples
     */

    private Set<FeatureTuple> getCorrelatedFeatures( Set<GeometryTuple> features,
                                                     Map<String, Feature> leftFeatures,
                                                     Map<String, Feature> rightFeatures,
                                                     Map<String, Feature> baselineFeatures )
    {
        // No features declared?
        if ( features.isEmpty() )
        {
            return this.getFeaturesWhenNoneDeclared( leftFeatures, rightFeatures, baselineFeatures );
        }

        // Iterate the declared features and look for matching features by name that have data
        Set<FeatureTuple> featureTuples = new HashSet<>();
        for ( GeometryTuple next : features )
        {
            String leftName = next.getLeft()
                                  .getName();
            String rightName = next.getRight()
                                   .getName();
            String baselineName = next.getBaseline()
                                      .getName();

            if ( leftFeatures.containsKey( leftName )
                 && rightFeatures.containsKey( rightName )
                 // #126628
                 && ( !this.hasBaseline()
                      || baselineFeatures.containsKey( baselineName ) ) )
            {
                Feature left = leftFeatures.get( leftName );
                Feature right = rightFeatures.get( rightName );
                Feature baseline = baselineFeatures.get( baselineName );
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

    private Set<FeatureTuple> getFeaturesWhenNoneDeclared( Map<String, Feature> leftFeatures,
                                                           Map<String, Feature> rightFeatures,
                                                           Map<String, Feature> baselineFeatures )
    {
        LOGGER.debug( "No features were declared. Attempting to correlate left/right/baseline features by common "
                      + "feature name, else one feature on all sides." );

        Set<FeatureTuple> featureTuples = new HashSet<>();

        for ( Map.Entry<String, Feature> nextFeature : leftFeatures.entrySet() )
        {
            String name = nextFeature.getKey();
            Feature left = nextFeature.getValue();
            Feature right = rightFeatures.get( name );
            Feature baseline = baselineFeatures.get( name );

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
        return this.getDeclaration()
                   .label();
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
        return DeclarationUtilities.getVariableName( this.getLeft() );
    }

    /**
     * @see #getRightVariableName()
     * @return The declared right variable name or null if undeclared
     */
    private String getDeclaredRightVariableName()
    {
        return DeclarationUtilities.getVariableName( this.getRight() );
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
            variableName = DeclarationUtilities.getVariableName( this.getBaseline()
                                                                     .dataset() );
        }

        return variableName;
    }

    /**
     * @return The left hand data source configuration
     */
    private Dataset getLeft()
    {
        return this.getDeclaration()
                   .left();
    }

    /**
     * @return The right hand data source configuration
     */
    private Dataset getRight()
    {
        return this.getDeclaration()
                   .right();
    }

    /**
     * @return The baseline data source configuration
     */
    private BaselineDataset getBaseline()
    {
        return this.getDeclaration()
                   .baseline();
    }
}

