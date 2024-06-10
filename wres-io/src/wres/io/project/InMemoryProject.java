package wres.io.project;

import java.time.Duration;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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

import net.jcip.annotations.Immutable;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.CovariateDataset;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.TimeScale;
import wres.config.yaml.components.Variable;
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
import wres.config.yaml.VariableNames;
import wres.reading.DataSource.DataDisposition;
import wres.io.retrieving.DataAccessException;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * An implementation of the {@link Project} for an evaluation performed using a database.
 * @author James Brown
 */
@Immutable
public class InMemoryProject implements Project
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( InMemoryProject.class );

    /** A default project identifier. */
    private static final long DEFAULT_PROJECT_ID = 0;

    /** Project declaration. */
    private final EvaluationDeclaration declaration;

    /** Time-series data. */
    private final TimeSeriesStore timeSeriesStore;

    /** The overall hash for the time-series used in the project. */
    private final String hash;

    /** The measurement unit. */
    private final String measurementUnit;

    /** The features related to the project. */
    private final Set<FeatureTuple> features;

    /** The covariate features by variable name. **/
    private final Map<String, Set<Feature>> covariateFeatures;

    /** The feature groups related to the project. */
    private final Set<FeatureGroup> featureGroups;

    /** The singleton feature groups for which statistics should not be published, if any. */
    private final Set<FeatureGroup> doNotPublish;

    /** Whether the left data is gridded. */
    private final boolean leftUsesGriddedData;

    /** Whether the right data is gridded. */
    private final boolean rightUsesGriddedData;

    /** Whether the baseline data is gridded. */
    private final boolean baselineUsesGriddedData;

    /** Whether the covariates use gridded data (all covariates must be the same). */
    private final boolean covariatesUseGriddedData;

    /** The desired timescale. */
    private final TimeScaleOuter desiredTimeScale;

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

        // Set the gridded data status of each dataset orientation
        this.leftUsesGriddedData = this.getUsesGriddedData( ingestResults, DatasetOrientation.LEFT );
        this.rightUsesGriddedData = this.getUsesGriddedData( ingestResults, DatasetOrientation.RIGHT );
        this.baselineUsesGriddedData = this.getUsesGriddedData( ingestResults, DatasetOrientation.BASELINE );
        this.covariatesUseGriddedData = this.getUsesGriddedData( ingestResults, DatasetOrientation.COVARIATE );

        this.hash = this.getHash( timeSeriesStore );
        this.measurementUnit = this.getAnalyzedMeasurementUnit( declaration, timeSeriesStore );

        FeatureSets featureSets = this.getFeaturesAndFeatureGroups( declaration,
                                                                    this.leftUsesGriddedData
                                                                    || this.rightUsesGriddedData,
                                                                    timeSeriesStore );

        this.features = featureSets.features();
        this.featureGroups = featureSets.featureGroups();
        this.doNotPublish = featureSets.doNotPublish();

        this.desiredTimeScale = this.getDesiredTimeScale( declaration, timeSeriesStore );

        // Get the variable name to evaluate
        VariableNames variableNames = this.getVariablesToEvaluate( declaration, timeSeriesStore );

        // Interpolate and set the declaration
        this.declaration = ProjectUtilities.interpolate( declaration,
                                                         ingestResults,
                                                         variableNames,
                                                         this.measurementUnit,
                                                         this.desiredTimeScale,
                                                         this.features,
                                                         this.featureGroups );

        // Set the covariate features
        this.covariateFeatures = this.getCovariateFeatures( this.declaration.covariates(),
                                                            timeSeriesStore,
                                                            this.features );

        // Validate any ensemble conditions in the declaration
        this.validateEnsembleConditions( timeSeriesStore, this.declaration );

        // Validate the project
        ProjectUtilities.validate( this );

        LOGGER.info( "Project validation and metadata loading is complete." );
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
    public Set<Feature> getCovariateFeatures( String variableName )
    {
        Objects.requireNonNull( variableName );

        return this.covariateFeatures.get( variableName );
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
    public Variable getLeftVariable()
    {
        return this.getLeft()
                   .variable();
    }

    @Override
    public Variable getRightVariable()
    {
        return this.getRight()
                   .variable();
    }

    @Override
    public Variable getBaselineVariable()
    {
        Variable variable = null;

        if ( this.hasBaseline() )
        {
            variable = this.getBaseline()
                           .dataset()
                           .variable();
        }

        return variable;
    }

    @Override
    public Dataset getCovariateDataset( String variableName )
    {
        return ProjectUtilities.getCovariateDatset( this.declaration, variableName );
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
            case COVARIATE -> this.covariatesUseGriddedData;
        };
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
        return DEFAULT_PROJECT_ID;
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
     * @param timeSeriesStore the time-series data store
     * @param declaration the project declaration
     * @throws NoProjectDataException if the conditions select no data
     * @throws DataAccessException if one or more ensemble conditions could not be evaluated
     */

    private void validateEnsembleConditions( TimeSeriesStore timeSeriesStore,
                                             EvaluationDeclaration declaration )
    {
        // Show all errors at once rather than drip-feeding
        Dataset left = declaration.left();
        List<String> failedLeft = this.getInvalidEnsembleConditions( timeSeriesStore, DatasetOrientation.LEFT, left );
        List<String> failed = new ArrayList<>( failedLeft );
        Dataset right = declaration.right();
        List<String> failedRight = this.getInvalidEnsembleConditions( timeSeriesStore, DatasetOrientation.RIGHT,
                                                                      right );
        failed.addAll( failedRight );

        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            Dataset baseline = declaration.baseline()
                                          .dataset();
            List<String> failedBaseline = this.getInvalidEnsembleConditions( timeSeriesStore,
                                                                             DatasetOrientation.BASELINE,
                                                                             baseline );
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
     * @param declaration the project declaration
     * @param timeSeriesStore the time-series data store
     * @return the variable names
     * @throws DataAccessException if the variable information could not be determined from the data
     */

    private VariableNames getVariablesToEvaluate( EvaluationDeclaration declaration,
                                                  TimeSeriesStore timeSeriesStore )
    {
        // The set of possibilities to validate
        Set<String> leftNames = this.getVariableNameByInspectingData( timeSeriesStore,
                                                                      DatasetOrientation.LEFT );
        Set<String> rightNames = this.getVariableNameByInspectingData( timeSeriesStore,
                                                                       DatasetOrientation.RIGHT );
        Set<String> baselineNames = this.getVariableNameByInspectingData( timeSeriesStore,
                                                                          DatasetOrientation.BASELINE );
        Set<String> covariateNames = this.getVariableNameByInspectingData( timeSeriesStore,
                                                                           DatasetOrientation.COVARIATE );

        LOGGER.debug( "While looking for variable names to evaluate, discovered {} on the LEFT side, {} on the RIGHT "
                      + "side, {} on the BASELINE side and {} on the COVARIATE side.",
                      leftNames,
                      rightNames,
                      baselineNames,
                      covariateNames );

        return ProjectUtilities.getVariableNames( declaration,
                                                  leftNames,
                                                  rightNames,
                                                  baselineNames,
                                                  covariateNames );
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
     * @param timeSeriesStore the time-series data store
     * @param orientation the context
     * @return the possible variable names
     * @throws DataAccessException if the variable information could not be determined from the data
     */

    private Set<String> getVariableNameByInspectingData( TimeSeriesStore timeSeriesStore,
                                                         DatasetOrientation orientation )
    {
        Stream<TimeSeries<?>> series = Stream.concat( timeSeriesStore.getSingleValuedSeries( orientation ),
                                                      timeSeriesStore.getEnsembleSeries( orientation ) );
        return series.map( next -> next.getMetadata()
                                       .getVariableName() )
                     .collect( Collectors.toSet() );
    }

    /**
     * Checks for any invalid ensemble conditions and returns a string representation of the invalid conditions.
     *
     * @param timeSeriesStore  the time-series data store
     * @param orientation the orientation of the dataset
     * @param dataset the dataset whose ensemble conditions should be validated
     * @return a string representation of the invalid conditions 
     * @throws DataAccessException if one or more ensemble conditions could not be evaluated
     */

    private List<String> getInvalidEnsembleConditions( TimeSeriesStore timeSeriesStore,
                                                       DatasetOrientation orientation,
                                                       Dataset dataset )
    {
        List<String> failed = new ArrayList<>();

        EnsembleFilter filter = dataset.ensembleFilter();
        if ( Objects.nonNull( filter ) )
        {
            for ( String name : filter.members() )
            {
                Stream<TimeSeries<Ensemble>> series = timeSeriesStore.getEnsembleSeries( orientation );
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
     * @param timeSeriesStore the time-series data store
     * @param hasBaseline whether the evaluation has a baseline dataset
     * @return a set of gridded feature tuples
     */

    private Set<FeatureTuple> getGriddedFeatureTuples( TimeSeriesStore timeSeriesStore,
                                                       boolean hasBaseline )
    {
        LOGGER.debug( "Getting details of intersecting features for gridded data." );

        Stream<TimeSeries<?>> leftSeries =
                Stream.concat( timeSeriesStore.getSingleValuedSeries( DatasetOrientation.LEFT ),
                               timeSeriesStore.getEnsembleSeries( DatasetOrientation.LEFT ) );

        Set<Feature> griddedFeatures = leftSeries.map( next -> next.getMetadata()
                                                                   .getFeature() )
                                                 .collect( Collectors.toSet() );

        Set<FeatureTuple> featureTuples = new HashSet<>();

        for ( Feature nextFeature : griddedFeatures )
        {
            Geometry geometry = MessageFactory.parse( nextFeature );
            GeometryTuple geoTuple;
            if ( hasBaseline )
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
     * @param declaration the project declaration
     * @param gridded whether the evaluation uses gridded data
     * @param timeSeriesStore the time-series data store
     * @throws DataAccessException if the features and/or feature groups could not be set
     */

    private FeatureSets getFeaturesAndFeatureGroups( EvaluationDeclaration declaration,
                                                     boolean gridded,
                                                     TimeSeriesStore timeSeriesStore )
    {
        LOGGER.debug( "Setting the features and feature groups for in memory project." );

        Set<FeatureTuple> featuresInner;
        Set<FeatureGroup> featureGroupsInner;
        Set<FeatureGroup> doNotPublishInner;

        boolean hasBaseline = DeclarationUtilities.hasBaseline( declaration );

        // Gridded features?
        // Yes
        if ( gridded )
        {
            Set<FeatureTuple> griddedTuples = this.getGriddedFeatureTuples( timeSeriesStore, hasBaseline );

            featuresInner = griddedTuples;
            ProjectUtilities.FeatureSets groups = ProjectUtilities.getFeatureGroups( featuresInner,
                                                                                     Set.of(),
                                                                                     declaration,
                                                                                     DEFAULT_PROJECT_ID );
            featureGroupsInner = groups.featureGroups();
            doNotPublishInner = groups.doNotPublish();

            LOGGER.debug( "Finished setting the features for in-memory project. Discovered {} gridded features.",
                          griddedTuples.size() );
        }
        else
        {
            Stream<TimeSeries<?>> leftSeries =
                    Stream.concat( timeSeriesStore.getSingleValuedSeries( DatasetOrientation.LEFT ),
                                   timeSeriesStore.getEnsembleSeries( DatasetOrientation.LEFT ) );

            Stream<TimeSeries<?>> rightSeries =
                    Stream.concat( timeSeriesStore.getSingleValuedSeries( DatasetOrientation.RIGHT ),
                                   timeSeriesStore.getEnsembleSeries( DatasetOrientation.RIGHT ) );

            Stream<TimeSeries<?>> baselineSeries =
                    Stream.concat( timeSeriesStore.getSingleValuedSeries( DatasetOrientation.BASELINE ),
                                   timeSeriesStore.getEnsembleSeries( DatasetOrientation.BASELINE ) );

            Map<String, Set<Feature>> leftFeaturesWithData =
                    leftSeries.map( t -> t.getMetadata()
                                          .getFeature() )
                              .collect( Collectors.groupingBy( Feature::getName, Collectors.toSet() ) );

            Map<String, Set<Feature>> rightFeaturesWithData =
                    rightSeries.map( t -> t.getMetadata()
                                           .getFeature() )
                               .collect( Collectors.groupingBy( Feature::getName, Collectors.toSet() ) );

            Map<String, Set<Feature>> baselineFeaturesWithData =
                    baselineSeries.map( t -> t.getMetadata()
                                              .getFeature() )
                                  .collect( Collectors.groupingBy( Feature::getName, Collectors.toSet() ) );

            // Get the declared singletons
            Set<GeometryTuple> declaredSingletons = this.getDeclaredFeatures( declaration );

            // Correlate with those from the times-series data
            Set<FeatureTuple> singletons = this.getCorrelatedFeatures( declaredSingletons,
                                                                       leftFeaturesWithData,
                                                                       rightFeaturesWithData,
                                                                       baselineFeaturesWithData,
                                                                       hasBaseline );

            // Get the feature tuples within feature groups
            Set<GeometryTuple> groupedFeatures = this.getDeclaredFeatureGroups( declaration )
                                                     .stream()
                                                     .flatMap( next -> next.getGeometryTuplesList()
                                                                           .stream() )
                                                     .collect( Collectors.toSet() );

            // Correlate with those from the time-series data
            Set<FeatureTuple> groupedTuples = this.getCorrelatedFeatures( groupedFeatures,
                                                                          leftFeaturesWithData,
                                                                          rightFeaturesWithData,
                                                                          baselineFeaturesWithData,
                                                                          hasBaseline );

            // Filter the singleton features against any spatial mask, unless there is gridded data, which is masked
            // upfront. Do this before forming the groups, which include singleton groups
            singletons = ProjectUtilities.filterFeatures( singletons, declaration.spatialMask() );

            ProjectUtilities.FeatureSets groups = ProjectUtilities.getFeatureGroups( singletons,
                                                                                     groupedTuples,
                                                                                     declaration,
                                                                                     DEFAULT_PROJECT_ID );
            Set<FeatureGroup> innerFeatureGroups = groups.featureGroups();

            // Filter the multi-group features against any spatial mask, unless there is gridded data, which is masked
            // upfront
            innerFeatureGroups =
                    ProjectUtilities.filterFeatureGroups( innerFeatureGroups, declaration.spatialMask() );

            // Filter the features and feature groups against any spatial mask
            featuresInner = Collections.unmodifiableSet( singletons );
            featureGroupsInner = Collections.unmodifiableSet( innerFeatureGroups );
            doNotPublishInner = groups.doNotPublish();

            LOGGER.debug( "Finished setting the feature groups for in-memory project. Discovered {} feature groups: {}.",
                          featureGroupsInner.size(),
                          featureGroupsInner );
        }

        if ( featuresInner.isEmpty()
             && featureGroupsInner.isEmpty() )
        {
            throw new NoProjectDataException( "Failed to identify any geographic features with data on all required "
                                              + "sides (left, right and, when declared, baseline) for the variables "
                                              + "and other declaration supplied. Please check that the declaration is "
                                              + "expected to produce some features with time-series data on both sides "
                                              + "of the pairing." );
        }

        return new FeatureSets( featuresInner, featureGroupsInner, doNotPublishInner );
    }

    /**
     * @return the declared features
     */
    private Set<GeometryTuple> getDeclaredFeatures( EvaluationDeclaration declaration )
    {
        if ( Objects.isNull( declaration.features() ) )
        {
            return Set.of();
        }

        return declaration.features()
                          .geometries();
    }

    /**
     * @return the declared feature groups
     */
    private Set<GeometryGroup> getDeclaredFeatureGroups( EvaluationDeclaration declaration )
    {
        if ( Objects.isNull( declaration.featureGroups() ) )
        {
            return Set.of();
        }

        return declaration.featureGroups()
                          .geometryGroups();
    }

    /**
     * Attempts to correlate the declared feature tuples with the oriented features that have data, returning the
     * feature tuples with data.
     * @param features the declared features
     * @param leftFeatures the left feature names against feature keys
     * @param rightFeatures the right feature names against feature keys
     * @param baselineFeatures the baseline feature names against feature keys
     * @param hasBaseline whether the evaluation has a baseline dataset
     * @return the feature tuples
     */

    private Set<FeatureTuple> getCorrelatedFeatures( Set<GeometryTuple> features,
                                                     Map<String, Set<Feature>> leftFeatures,
                                                     Map<String, Set<Feature>> rightFeatures,
                                                     Map<String, Set<Feature>> baselineFeatures,
                                                     boolean hasBaseline )
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
                 && ( !hasBaseline
                      || baselineFeatures.containsKey( baselineName ) ) )
            {
                Set<Feature> left = leftFeatures.get( leftName );
                Set<Feature> right = rightFeatures.get( rightName );
                Set<Feature> baseline = baselineFeatures.get( baselineName );

                Set<FeatureTuple> tuples = this.getFeatureTuplesFromFeatures( left, right, baseline );
                featureTuples.addAll( tuples );
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

    private Set<FeatureTuple> getFeaturesWhenNoneDeclared( Map<String, Set<Feature>> leftFeatures,
                                                           Map<String, Set<Feature>> rightFeatures,
                                                           Map<String, Set<Feature>> baselineFeatures )
    {
        LOGGER.debug( "No features were declared. Attempting to correlate left/right/baseline features by common "
                      + "feature name, else one feature on all sides." );

        Set<FeatureTuple> featureTuples = new HashSet<>();

        for ( Map.Entry<String, Set<Feature>> nextFeature : leftFeatures.entrySet() )
        {
            String name = nextFeature.getKey();
            Set<Feature> left = nextFeature.getValue();
            Set<Feature> right = rightFeatures.get( name );
            Set<Feature> baseline = baselineFeatures.get( name );

            // Still no correlated feature, what about a unique feature?
            if ( Objects.isNull( right )
                 && rightFeatures.size() == 1 )
            {
                right = rightFeatures.values()
                                     .iterator()
                                     .next();
                LOGGER.debug( "Assuming that {} and {} are correlated features to evaluate.", left, right );
            }
            if ( Objects.isNull( baseline )
                 && baselineFeatures.size() == 1 )
            {
                baseline = baselineFeatures.values()
                                           .iterator()
                                           .next();
                LOGGER.debug( "Assuming that {} and {} are correlated features to evaluate.", left, baseline );
            }

            if ( Objects.nonNull( right ) )
            {
                Set<FeatureTuple> tuples = this.getFeatureTuplesFromFeatures( left, right, baseline );
                featureTuples.addAll( tuples );
            }
            else
            {
                LOGGER.debug( "Failed to correlate left feature {} with a right feature.", left );
            }
        }

        return Collections.unmodifiableSet( featureTuples );
    }

    /**
     * Gets the possible feature tuple combinations from the supplied features.
     * @param left the left-ish features
     * @param right the right-ish features
     * @param baseline the baseline-ish features, possibly null
     * @return the feature tuples
     */
    private Set<FeatureTuple> getFeatureTuplesFromFeatures( Set<Feature> left,
                                                            Set<Feature> right,
                                                            Set<Feature> baseline )
    {
        Set<FeatureTuple> featureTuples = new HashSet<>();

        for ( Feature nextLeft : left )
        {
            for ( Feature nextRight : right )
            {
                if ( Objects.nonNull( baseline ) )
                {
                    for ( Feature nextBaseline : baseline )
                    {
                        GeometryTuple geometryTuple = MessageFactory.getGeometryTuple( nextLeft,
                                                                                       nextRight,
                                                                                       nextBaseline );
                        FeatureTuple featureTuple = FeatureTuple.of( geometryTuple );
                        featureTuples.add( featureTuple );
                    }
                }
                else
                {
                    GeometryTuple geometryTuple = MessageFactory.getGeometryTuple( nextLeft, nextRight, null );
                    FeatureTuple featureTuple = FeatureTuple.of( geometryTuple );
                    featureTuples.add( featureTuple );
                }
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

    /**
     * Determines whether the ingest results for the prescribed orientation use gridded data.
     * @param ingestResults the ingest results
     * @param orientation the orientation
     * @return whether the ingest results use gridded data
     * @throws IllegalStateException if there is a mixture of gridded and non-gridded datasets
     */
    private boolean getUsesGriddedData( List<IngestResult> ingestResults, DatasetOrientation orientation )
    {
        Set<Boolean> usesGridded = ingestResults.stream()
                                                .map( IngestResult::getDataSource )
                                                .filter( next -> next.getDatasetOrientation()
                                                                 == orientation )
                                                .map( next -> next.getDisposition()
                                                              == DataDisposition.NETCDF_GRIDDED )
                                                .collect( Collectors.toSet() );

        if ( usesGridded.size() > 1 )
        {
            throw new IllegalStateException( "Discovered multiple covariates of which some are gridded and others are "
                                             + "not, which is not supported." );
        }

        boolean gridded = usesGridded.contains( Boolean.TRUE );

        LOGGER.debug( "Set the status of gridded data for {} to {}.", orientation, gridded );

        return gridded;
    }

    /**
     * @param declaration the project declaration
     * @param timeSeriesStore the time-series data store
     * @return the measurement unit, which is either the declared unit or the analyzed unit, but possibly null
     * @throws DataAccessException if the measurement unit could not be determined
     * @throws IllegalArgumentException if the project identity is required and undefined
     */

    private String getAnalyzedMeasurementUnit( EvaluationDeclaration declaration,
                                               TimeSeriesStore timeSeriesStore )
    {
        String innerMeasurementUnit = null;

        // Declared unit available?
        String declaredUnit = declaration.unit();
        if ( Objects.nonNull( declaredUnit )
             && !declaredUnit.isBlank() )
        {
            innerMeasurementUnit = declaredUnit;

            LOGGER.debug( "Determined the measurement unit from the project declaration as {}.",
                          innerMeasurementUnit );
        }

        // Still not available? Then analyze the unit by looking for the most common right-ish unit
        if ( Objects.isNull( innerMeasurementUnit ) )
        {
            Stream<TimeSeries<?>> concat =
                    Stream.concat( timeSeriesStore.getSingleValuedSeries( DatasetOrientation.RIGHT ),
                                   timeSeriesStore.getEnsembleSeries( DatasetOrientation.RIGHT ) );
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

            innerMeasurementUnit = mostCommon.get();

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Determined the measurement unit by analyzing the project sources. The analyzed "
                              + "measurement unit is {} and corresponds to the most commonly occurring unit "
                              + "among time-series from {} sources.",
                              innerMeasurementUnit,
                              DatasetOrientation.RIGHT );
            }
        }

        return innerMeasurementUnit;
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
     * @param declaration the project declaration
     * @param timeSeriesStore the time-series data store
     * @return the desired timescale or null if unknown
     * @throws DataAccessException if the existing time scales could not be obtained
     */

    private TimeScaleOuter getDesiredTimeScale( EvaluationDeclaration declaration,
                                                TimeSeriesStore timeSeriesStore )
    {
        TimeScaleOuter innerTimeScale = null;

        // Use the declared timescale
        TimeScale declaredScale = declaration.timeScale();
        if ( Objects.nonNull( declaredScale ) )
        {
            innerTimeScale = TimeScaleOuter.of( declaredScale.timeScale() );

            LOGGER.trace( "Discovered that the desired time scale was declared explicitly as {}.",
                          innerTimeScale );

            return innerTimeScale;
        }

        // Find the Least Common Scale
        Stream<TimeSeries<?>> concat = Stream.concat( timeSeriesStore.getSingleValuedSeries(),
                                                      timeSeriesStore.getEnsembleSeries() );

        Set<TimeScaleOuter> existingTimeScales = concat.map( TimeSeries::getTimeScale )
                                                       .filter( Objects::nonNull )
                                                       .collect( Collectors.toSet() );

        // Look for the LCS among the ingested sources
        if ( !existingTimeScales.isEmpty() )
        {
            TimeScaleOuter leastCommonScale = TimeScaleOuter.getLeastCommonTimeScale( existingTimeScales );

            innerTimeScale = leastCommonScale;

            LOGGER.trace( "Discovered that the desired time scale was not supplied on construction of the project. "
                          + "Instead, determined the desired time scale from the Least Common Scale of the ingested "
                          + "time-series, which was {}. The existing time scales were: {}.",
                          leastCommonScale,
                          existingTimeScales );

            return innerTimeScale;
        }

        // Look for the LCS among the declared inputs
        Set<TimeScaleOuter> declaredExistingTimeScales = DeclarationUtilities.getSourceTimeScales( declaration )
                                                                             .stream()
                                                                             .map( TimeScaleOuter::of )
                                                                             .collect( Collectors.toUnmodifiableSet() );

        if ( !declaredExistingTimeScales.isEmpty() )
        {
            TimeScaleOuter leastCommonScale = TimeScaleOuter.getLeastCommonTimeScale( declaredExistingTimeScales );

            innerTimeScale = leastCommonScale;

            LOGGER.trace( "Discovered that the desired time scale was not supplied on construction of the project."
                          + " Instead, determined the desired time scale from the Least Common Scale of the "
                          + "declared inputs, which  was {}.",
                          leastCommonScale );

            return innerTimeScale;
        }

        return innerTimeScale;
    }

    /**
     * Checks that each covariate has some data for the feature names associated with it. Otherwise, the feature
     * authority of the covariate may need to be declared explicitly.
     *
     * @param covariates the declared covariates
     * @param timeSeriesStore the time-series data store
     * @param projectFeatures the project features
     * @return the covariate features by variable name
     * @throws DataAccessException if the data could not be accessed
     * @throws NoProjectDataException if no features could be correlated with time-series data
     */

    private Map<String, Set<Feature>> getCovariateFeatures( List<CovariateDataset> covariates,
                                                            TimeSeriesStore timeSeriesStore,
                                                            Set<FeatureTuple> projectFeatures )
    {
        Map<String, Set<Feature>> covariateFeaturesInner = new HashMap<>();

        for ( CovariateDataset covariate : covariates )
        {
            Stream<TimeSeries<Double>> covariateSeries =
                    timeSeriesStore.getSingleValuedSeries( DatasetOrientation.COVARIATE );

            Objects.requireNonNull( covariate.dataset(), "Expected a covariate dataset." );
            Objects.requireNonNull( covariate.dataset()
                                             .variable(), "Expected a covariate variable." );
            Objects.requireNonNull( covariate.dataset()
                                             .variable()
                                             .name(), "Expected a covariate variable name." );

            String covariateName = covariate.dataset()
                                            .variable()
                                            .name();

            Objects.requireNonNull( covariate.featureNameOrientation(), "Could not find the orientation of the "
                                                                        + "feature names associated with the covariate "
                                                                        + "dataset whose variable name is '"
                                                                        + covariateName
                                                                        + "'." );

            Set<Feature> ingestedFeatures = covariateSeries.filter( c -> Objects.equals( covariateName, c.getMetadata()
                                                                                                         .getVariableName() ) )
                                                           .map( c -> c.getMetadata()
                                                                       .getFeature() )
                                                           .collect( Collectors.toUnmodifiableSet() );

            Set<Feature> matchingFeatures = ProjectUtilities.covariateFeaturesSelectSomeData( covariate,
                                                                                              projectFeatures,
                                                                                              ingestedFeatures );

            covariateFeaturesInner.put( covariateName, matchingFeatures );
        }

        return Collections.unmodifiableMap( covariateFeaturesInner );
    }

    /**
     * Small collection of geographic faetures to use in different contexts.
     * @param features the singleton features
     * @param featureGroups the feature groups
     * @param doNotPublish the feature groups whose raw statistics should not be published
     */
    private record FeatureSets( Set<FeatureTuple> features,
                                Set<FeatureGroup> featureGroups,
                                Set<FeatureGroup> doNotPublish ) {}
}