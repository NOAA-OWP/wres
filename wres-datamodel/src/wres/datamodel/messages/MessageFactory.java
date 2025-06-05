package wres.datamodel.messages;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Timestamp;

import wres.config.yaml.components.CovariateDataset;
import wres.datamodel.types.Ensemble;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.StatisticsStore;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.Values;
import wres.config.yaml.components.Variable;
import wres.config.MetricConstants;
import wres.config.MetricConstants.StatisticType;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.Covariate;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Pool.EnsembleAverageType;
import wres.statistics.generated.Season;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.Pairs;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.Pairs.TimeSeriesOfPairs;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.ValueFilter;
import wres.statistics.generated.Evaluation.DefaultData;

/**
 * A factory class for mapping between canonical (protobuf) representations and corresponding Java representations,
 * which often provide extra behavior. The "parse" methods provide a direct (one-to-one) translation in each direction
 * and the "get" methods involve an indirect translation or one-to-many/many-to-one translation.
 *
 * @see MessageUtilities
 * @author James Brown
 */

public class MessageFactory
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MessageFactory.class );

    /** Re-used string. */
    private static final String SET_THE_BASELINE_DATASET_NAME_TO = "Set the baseline dataset name to {}.";

    /**
     * Creates a collection of {@link wres.statistics.generated.Statistics} by pool from a
     * {@link wres.datamodel.statistics.StatisticsStore}. The statistics are returned in a guaranteed order,
     * corresponding to the natural order of the pool boundaries.
     *
     * @param store the project statistics
     * @return the statistics in the natural order of their corresponding pools
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if any input is null
     * @throws InterruptedException if the statistics could not be retrieved from the project
     */

    public static List<Statistics> getStatistics( StatisticsStore store )
            throws InterruptedException
    {
        Objects.requireNonNull( store );

        List<StatisticsStore> decomposedStatistics = MessageFactory.getStatisticsPerPool( store );

        List<Statistics> returnMe = new ArrayList<>();

        for ( StatisticsStore next : decomposedStatistics )
        {
            Statistics statistics = MessageFactory.parseOnePool( next );

            // Do not add empty statistics
            if ( Objects.nonNull( statistics ) )
            {
                returnMe.add( statistics );
            }
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Builds a pool from the input, some of which may be missing. The pool is assigned the default identifier.
     *
     * @param featureGroup the feature group
     * @param timeWindow the time window
     * @param timeScale the time scale
     * @param thresholds the thresholds
     * @param isBaselinePool is true if the pool refers to pairs of left and baseline data, otherwise left and right
     * @return the pool
     */

    public static Pool getPool( FeatureGroup featureGroup,
                                TimeWindowOuter timeWindow,
                                TimeScaleOuter timeScale,
                                OneOrTwoThresholds thresholds,
                                boolean isBaselinePool )
    {
        return MessageFactory.getPool( featureGroup, timeWindow, timeScale, thresholds, isBaselinePool, 0 );
    }

    /**
     * Builds a pool from the input, some of which may be missing.
     *
     * @param featureGroup the feature group
     * @param timeWindow the time window
     * @param timeScale the time scale
     * @param thresholds the thresholds
     * @param isBaselinePool is true if the pool refers to pairs of left and baseline data, otherwise left and right
     * @param poolId the pool identifier
     * @return the pool
     */

    public static Pool getPool( FeatureGroup featureGroup,
                                TimeWindowOuter timeWindow,
                                TimeScaleOuter timeScale,
                                OneOrTwoThresholds thresholds,
                                boolean isBaselinePool,
                                long poolId )
    {
        return MessageFactory.getPool( featureGroup,
                                       timeWindow,
                                       timeScale,
                                       thresholds,
                                       isBaselinePool,
                                       poolId,
                                       EnsembleAverageType.NONE );
    }

    /**
     * Builds a pool from the input, some of which may be missing.
     *
     * @param featureGroup the feature group
     * @param timeWindow the time window
     * @param timeScale the time-scale
     * @param thresholds the thresholds
     * @param isBaselinePool is true if the pool refers to pairs of left and baseline data, otherwise left and right
     * @param poolId the pool identifier
     * @param ensembleAverageType the ensemble average type
     * @return the pool
     */

    public static Pool getPool( FeatureGroup featureGroup,
                                TimeWindowOuter timeWindow,
                                TimeScaleOuter timeScale,
                                OneOrTwoThresholds thresholds,
                                boolean isBaselinePool,
                                long poolId,
                                EnsembleAverageType ensembleAverageType )
    {
        Pool.Builder poolBuilder = Pool.newBuilder()
                                       .setIsBaselinePool( isBaselinePool )
                                       .setPoolId( poolId );

        // Feature tuple
        if ( Objects.nonNull( featureGroup ) )
        {
            // If there is a natural ordering, preserve it, because the tuples are messaged as a list
            List<GeometryTuple> geoTuples = featureGroup.getFeatures()
                                                        .stream()
                                                        .map( MessageFactory::parse )
                                                        .toList();

            poolBuilder.addAllGeometryTuples( geoTuples );

            GeometryGroup.Builder geometryBuilder = GeometryGroup.newBuilder()
                                                                 .addAllGeometryTuples( geoTuples );

            // Region name?
            if ( Objects.nonNull( featureGroup.getName() ) )
            {
                poolBuilder.setRegionName( featureGroup.getName() );
                geometryBuilder.setRegionName( featureGroup.getName() );
            }

            poolBuilder.setGeometryGroup( geometryBuilder );

            LOGGER.debug( "While creating sample metadata, populated the pool with a feature group of {}.",
                          featureGroup );
        }

        // Time window
        if ( Objects.nonNull( timeWindow ) )
        {
            wres.statistics.generated.TimeWindow window = MessageFactory.parse( timeWindow );
            poolBuilder.setTimeWindow( window );

            LOGGER.debug( "While creating sample metadata, populated the pool with a time window of {}.",
                          timeWindow );
        }

        // Time scale
        if ( Objects.nonNull( timeScale ) )
        {
            wres.statistics.generated.TimeScale scale = MessageFactory.parse( timeScale );
            poolBuilder.setTimeScale( scale );

            LOGGER.debug( "While creating sample metadata, populated the pool with a time scale of "
                          + "{}.",
                          timeScale );
        }

        // Thresholds
        if ( Objects.nonNull( thresholds ) )
        {
            wres.statistics.generated.Threshold event = MessageFactory.parse( thresholds.first() );
            poolBuilder.setEventThreshold( event );

            if ( thresholds.hasTwo() )
            {
                wres.statistics.generated.Threshold decision = MessageFactory.parse( thresholds.second() );
                poolBuilder.setDecisionThreshold( decision );
            }

            LOGGER.debug( "While creating pool metadata, populated the pool with a threshold of {}.",
                          thresholds );
        }

        // Ensemble average type
        if ( Objects.nonNull( ensembleAverageType ) )
        {
            poolBuilder.setEnsembleAverageType( ensembleAverageType );

            LOGGER.debug( "While creating pool metadata, populated the pool with an ensemble average type of {}.",
                          ensembleAverageType );
        }

        return poolBuilder.build();
    }

    /**
     * Returns a {@link Pairs} from a {@link Pool}.
     *
     * @param pairs The pairs
     * @return a pairs message
     */

    public static Pairs
    getTimeSeriesOfEnsemblePairs( wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> pairs )
    {
        Objects.requireNonNull( pairs );

        Pairs.Builder builder = Pairs.newBuilder();

        for ( TimeSeries<Pair<Double, Ensemble>> nextSeries : pairs.get() )
        {
            TimeSeriesOfPairs.Builder series = TimeSeriesOfPairs.newBuilder();

            // Add the reference times
            Map<ReferenceTimeType, Instant> times = nextSeries.getReferenceTimes();
            for ( Map.Entry<ReferenceTimeType, Instant> nextEntry : times.entrySet() )
            {
                ReferenceTimeType nextType = nextEntry.getKey();
                Instant nextTime = nextEntry.getValue();
                ReferenceTime nextRef =
                        ReferenceTime.newBuilder()
                                     .setReferenceTimeType( nextType )
                                     .setReferenceTime( Timestamp.newBuilder()
                                                                 .setSeconds( nextTime.getEpochSecond() )
                                                                 .setNanos( nextTime.getNano() ) )
                                     .build();
                series.addReferenceTimes( nextRef );
            }

            // Add the events
            for ( Event<Pair<Double, Ensemble>> nextEvent : nextSeries.getEvents() )
            {
                wres.statistics.generated.Pairs.Pair.Builder nextPair =
                        wres.statistics.generated.Pairs.Pair.newBuilder();

                nextPair.setValidTime( Timestamp.newBuilder()
                                                .setSeconds( nextEvent.getTime().getEpochSecond() )
                                                .setNanos( nextEvent.getTime().getNano() ) )
                        .addLeft( nextEvent.getValue().getLeft() );

                for ( double nextRight : nextEvent.getValue().getRight().getMembers() )
                {
                    nextPair.addRight( nextRight );
                }

                series.addPairs( nextPair );
            }

            builder.addTimeSeries( series );
        }

        return builder.build();
    }

    /**
     * Creates a geometry tuple from the input.
     *
     * @param left the left feature, required
     * @param right the right feature, required
     * @param baseline the baseline feature, optional
     * @return the geometry tuple
     * @throws NullPointerException if either the left or right input is null
     */

    public static GeometryTuple getGeometryTuple( Feature left, Feature right, Feature baseline )
    {
        Objects.requireNonNull( left );
        Objects.requireNonNull( right );

        GeometryTuple.Builder builder = GeometryTuple.newBuilder()
                                                     .setLeft( left.getGeometry() )
                                                     .setRight( right.getGeometry() );

        if ( Objects.nonNull( baseline ) )
        {
            builder.setBaseline( baseline.getGeometry() );
        }

        return builder.build();
    }

    /**
     * Creates a geometry group from the input.
     *
     * @param name the group name, optional
     * @param features the features, required
     * @return the geometry tuple
     * @throws NullPointerException if the features are null
     */

    public static GeometryGroup getGeometryGroup( String name, Set<FeatureTuple> features )
    {
        Objects.requireNonNull( features );

        // Use a predictable order because the GeometryGroup contains a list
        List<FeatureTuple> sorted = new ArrayList<>( new TreeSet<>( features ) );
        List<GeometryTuple> geometries = sorted.stream()
                                               .map( FeatureTuple::getGeometryTuple )
                                               .toList();

        GeometryGroup.Builder builder = GeometryGroup.newBuilder()
                                                     .addAllGeometryTuples( geometries );

        if ( Objects.nonNull( name ) )
        {
            builder.setRegionName( name );
        }

        return builder.build();
    }

    /**
     * Creates a geometry group from the input.
     *
     * @param features the features, required
     * @return the geometry tuple
     * @throws NullPointerException if the features are null
     */

    public static GeometryGroup getGeometryGroup( Set<FeatureTuple> features )
    {
        return MessageFactory.getGeometryGroup( null, features );
    }

    /**
     * Creates a geometry group from the input.
     *
     * @param name the group name, optional
     * @param singleton the singleton feature, required
     * @return the geometry tuple
     * @throws NullPointerException if the singleton is null
     */

    public static GeometryGroup getGeometryGroup( String name, FeatureTuple singleton )
    {
        return MessageFactory.getGeometryGroup( name, Collections.singleton( singleton ) );
    }

    /**
     * Creates a geometry group from the input.
     *
     * @param singleton the singleton feature, required
     * @return the geometry tuple
     * @throws NullPointerException if the singleton is null
     */

    public static GeometryGroup getGeometryGroup( FeatureTuple singleton )
    {
        return MessageFactory.getGeometryGroup( null, Collections.singleton( singleton ) );
    }

    /**
     * Creates an evaluation for messaging from a declared evaluation.
     *
     * @param evaluation the evaluation declaration
     * @return an evaluation for messaging
     * @throws NullPointerException if the evaluation is null
     */

    public static Evaluation parse( EvaluationDeclaration evaluation )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( evaluation.left() );
        Objects.requireNonNull( evaluation.right() );

        Evaluation.Builder builder = Evaluation.newBuilder();

        // Add the dataset information
        MessageFactory.addDatasets( evaluation, builder );

        // Add the measurement unit
        String unitString = evaluation.unit();
        if ( Objects.nonNull( unitString ) )
        {
            builder.setMeasurementUnit( unitString );
            LOGGER.debug( "Set the measurement unit to {}.", unitString );
        }

        // Set the season
        wres.config.yaml.components.Season season = evaluation.season();
        if ( Objects.nonNull( season ) )
        {
            Season innerSeason = season.canonical();
            builder.setSeason( innerSeason );
            LOGGER.debug( "Set the season to: {}.", season );
        }

        // Set the value filter
        Values values = evaluation.values();
        if ( Objects.nonNull( values ) )
        {
            ValueFilter innerValues = values.canonical();
            builder.setValueFilter( innerValues );
            LOGGER.debug( "Set the value filter to: {}.", values );
        }

        // Set the ensemble members to filter
        MessageFactory.addEnsembleMemberFilters( evaluation, builder );

        // Set the outputs
        wres.config.yaml.components.Formats formats = evaluation.formats();
        if ( Objects.nonNull( formats ) )
        {
            Outputs innerOutputs = formats.outputs();
            builder.setOutputs( innerOutputs );
            LOGGER.debug( "Set the outputs to: {}.", formats );
        }

        // Set the covariates
        List<Covariate> covariates = MessageFactory.parse( evaluation.covariates() );
        builder.addAllCovariates( covariates );

        return builder.build();
    }

    /**
     * Creates a {@link wres.statistics.generated.TimeScale} from a {@link wres.datamodel.scale.TimeScaleOuter}.
     *
     * @param timeScale the time-scale from which to create a message
     * @return the message
     */

    public static TimeScale parse( wres.datamodel.scale.TimeScaleOuter timeScale )
    {
        Objects.requireNonNull( timeScale );

        return timeScale.getTimeScale();
    }

    /**
     * Creates a {@link wres.statistics.generated.TimeWindow} from a {@link wres.datamodel.time.TimeWindowOuter}.
     *
     * @param timeWindow the time window from which to create a message
     * @return the message
     */

    public static TimeWindow parse( wres.datamodel.time.TimeWindowOuter timeWindow )
    {
        Objects.requireNonNull( timeWindow );

        return timeWindow.getTimeWindow();
    }

    /**
     * Creates a {@link wres.statistics.generated.Threshold} from a 
     * {@link wres.datamodel.thresholds.ThresholdOuter}.
     *
     * @param threshold the threshold from which to create a message
     * @return the message
     */

    public static Threshold parse( wres.datamodel.thresholds.ThresholdOuter threshold )
    {
        Objects.requireNonNull( threshold );

        return threshold.getThreshold();
    }

    /**
     * Creates a {@link wres.statistics.generated.DoubleScoreStatistic} from a
     * {@link wres.datamodel.statistics.DoubleScoreStatisticOuter}.
     *
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static DoubleScoreStatistic parse( wres.datamodel.statistics.DoubleScoreStatisticOuter statistic )
    {
        Objects.requireNonNull( statistic );

        return statistic.getStatistic();
    }

    /**
     * Creates a {@link wres.statistics.generated.DurationScoreStatistic} from a 
     * {@link wres.datamodel.statistics.DurationScoreStatisticOuter}.
     *
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static DurationScoreStatistic parse( wres.datamodel.statistics.DurationScoreStatisticOuter statistic )
    {
        Objects.requireNonNull( statistic );

        return statistic.getStatistic();
    }

    /**
     * Creates a {@link wres.statistics.generated.DiagramStatistic} from a 
     * {@link wres.datamodel.statistics.DiagramStatisticOuter}.
     *
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static DiagramStatistic parse( wres.datamodel.statistics.DiagramStatisticOuter statistic )
    {
        Objects.requireNonNull( statistic );

        return statistic.getStatistic();
    }

    /**
     * Creates a {@link wres.statistics.generated.DurationDiagramStatistic} from a
     * {@link wres.datamodel.statistics.DurationDiagramStatisticOuter} composed of timing
     * errors.
     *
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static DurationDiagramStatistic parse( wres.datamodel.statistics.DurationDiagramStatisticOuter statistic )
    {
        Objects.requireNonNull( statistic );

        return statistic.getStatistic();
    }

    /**
     * Creates a {@link wres.statistics.generated.DiagramStatistic} from a 
     * {@link wres.datamodel.statistics.BoxplotStatisticOuter}.
     *
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static BoxplotStatistic parse( wres.datamodel.statistics.BoxplotStatisticOuter statistic )
    {
        Objects.requireNonNull( statistic );

        return statistic.getStatistic();
    }

    /**
     * Creates a {@link wres.statistics.generated.GeometryTuple} from a {@link wres.datamodel.space.FeatureTuple}.
     *
     * @param featureTuple the feature tuple from which to create a message
     * @return the message
     */

    public static GeometryTuple parse( FeatureTuple featureTuple )
    {
        Objects.requireNonNull( featureTuple );

        return featureTuple.getGeometryTuple();
    }

    /**
     * Creates a {@link wres.statistics.generated.Geometry} from a {@link wres.datamodel.space.Feature}.
     *
     * @param featureKey the feature key from which to create a message
     * @return the message
     */

    public static Geometry parse( Feature featureKey )
    {
        Objects.requireNonNull( featureKey );

        Geometry.Builder builder = Geometry.newBuilder();

        if ( Objects.nonNull( featureKey.getName() ) )
        {
            builder.setName( featureKey.getName() );
        }

        if ( Objects.nonNull( featureKey.getDescription() ) )
        {
            builder.setDescription( featureKey.getDescription() );
        }

        if ( Objects.nonNull( featureKey.getWkt() ) )
        {
            builder.setWkt( featureKey.getWkt() );
        }

        if ( Objects.nonNull( featureKey.getSrid() ) )
        {
            builder.setSrid( featureKey.getSrid() );
        }

        return builder.build();
    }

    /**
     * Creates a {@link wres.datamodel.space.FeatureTuple} from a {@link wres.statistics.generated.GeometryTuple}.
     *
     * @param location the location from which to create a message
     * @return the message
     */

    public static FeatureTuple parse( GeometryTuple location )
    {
        Objects.requireNonNull( location );

        return FeatureTuple.of( location );
    }

    /**
     * Copies the covariates from an evaluation declaration to the evaluation builder.
     * @param covariates the covariates
     * @return the canonical covariates
     */
    public static List<Covariate> parse( List<CovariateDataset> covariates )
    {
        Objects.requireNonNull( covariates );

        List<Covariate> mapped = new ArrayList<>();

        for ( CovariateDataset covariate : covariates )
        {
            Objects.requireNonNull( covariate.dataset()
                                             .variable()
                                             .name() );

            Covariate.Builder covariateBuilder = Covariate.newBuilder()
                                                          .setVariableName( covariate.dataset()
                                                                                     .variable()
                                                                                     .name() );

            if ( Objects.nonNull( covariate.minimum() ) )
            {
                covariateBuilder.setMinimumInclusiveValue( covariate.minimum() );
            }

            if ( Objects.nonNull( covariate.maximum() ) )
            {
                covariateBuilder.setMaximumInclusiveValue( covariate.maximum() );
            }

            mapped.add( covariateBuilder.build() );

            LOGGER.debug( "Added a covariate with a variable name of '{}', a minimum value of {}, and a maximum value "
                          + "of {}.",
                          covariate.dataset()
                                   .variable()
                                   .name(),
                          covariate.minimum(),
                          covariate.maximum() );
        }

        return Collections.unmodifiableList( mapped );
    }

    /**
     * Creates a collection of {@link wres.statistics.generated.Statistics} by pool from a
     * {@link wres.datamodel.statistics.StatisticsStore}.
     *
     * @param project the project statistics
     * @param pairs the optional pairs
     * @return the statistics messages
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if the input is null
     * @throws InterruptedException if the statistics could not be retrieved from the project
     */

    static Statistics parseOnePool( StatisticsStore project,
                                    wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> pairs )
            throws InterruptedException
    {
        Statistics prototype = MessageFactory.parseOnePool( project );

        Statistics.Builder statistics = Statistics.newBuilder( prototype );

        if ( Objects.nonNull( pairs ) )
        {
            Pool prototypeSample = statistics.getPool();
            Pool.Builder sampleBuilder = Pool.newBuilder( prototypeSample );
            sampleBuilder.setPairs( MessageFactory.getTimeSeriesOfEnsemblePairs( pairs ) );
            statistics.setPool( sampleBuilder );
        }

        return statistics.build();
    }

    /**
     * Creates a {@link wres.statistics.generated.Statistics} from a 
     * {@link wres.datamodel.statistics.StatisticsStore}.
     *
     * @param onePool the pool-shaped statistics
     * @return the statistics message or null
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if the input is null
     * @throws InterruptedException if the statistics could not be retrieved from the project
     */

    static Statistics parseOnePool( StatisticsStore onePool )
            throws InterruptedException
    {
        Objects.requireNonNull( onePool );

        Statistics.Builder statistics = Statistics.newBuilder();

        // Set the minimum sample size
        statistics.setMinimumSampleSize( onePool.getMinimumSampleSize() );

        List<PoolMetadata> metadatas = new ArrayList<>();

        boolean added = false;

        // Add the double scores
        if ( onePool.hasStatistic( StatisticType.DOUBLE_SCORE ) )
        {
            List<wres.datamodel.statistics.DoubleScoreStatisticOuter> doubleScores = onePool.getDoubleScoreStatistics();
            doubleScores.forEach( next -> {
                statistics.addScores( MessageFactory.parse( next ) );
                metadatas.add( next.getPoolMetadata() );
            } );

            added = true;
        }

        // Add the diagrams
        if ( onePool.hasStatistic( StatisticType.DIAGRAM ) )
        {
            List<wres.datamodel.statistics.DiagramStatisticOuter> diagrams = onePool.getDiagramStatistics();
            diagrams.forEach( next -> {
                statistics.addDiagrams( MessageFactory.parse( next ) );
                metadatas.add( next.getPoolMetadata() );
            } );

            added = true;
        }

        // Add the boxplots per pool
        if ( onePool.hasStatistic( StatisticType.BOXPLOT_PER_POOL ) )
        {
            List<wres.datamodel.statistics.BoxplotStatisticOuter> boxplots = onePool.getBoxPlotStatisticsPerPool();
            boxplots.forEach( next -> {
                statistics.addOneBoxPerPool( MessageFactory.parse( next ) );
                metadatas.add( next.getPoolMetadata() );
            } );

            added = true;
        }

        // Add the boxplots per pair
        if ( onePool.hasStatistic( StatisticType.BOXPLOT_PER_PAIR ) )
        {
            List<wres.datamodel.statistics.BoxplotStatisticOuter> boxplots = onePool.getBoxPlotStatisticsPerPair();
            boxplots.forEach( next -> {
                statistics.addOneBoxPerPair( MessageFactory.parse( next ) );
                metadatas.add( next.getPoolMetadata() );
            } );

            added = true;
        }

        // Add the duration scores
        if ( onePool.hasStatistic( StatisticType.DURATION_SCORE ) )
        {
            List<wres.datamodel.statistics.DurationScoreStatisticOuter> durationScores =
                    onePool.getDurationScoreStatistics();
            durationScores.forEach( next -> {
                statistics.addDurationScores( MessageFactory.parse( next ) );
                metadatas.add( next.getPoolMetadata() );
            } );

            added = true;
        }

        // Add the duration diagrams with instant/duration pairs
        if ( onePool.hasStatistic( StatisticType.DURATION_DIAGRAM ) )
        {
            List<wres.datamodel.statistics.DurationDiagramStatisticOuter> durationDiagrams =
                    onePool.getDurationDiagramStatistics();
            durationDiagrams.forEach( next -> {
                statistics.addDurationDiagrams( MessageFactory.parse( next ) );
                metadatas.add( next.getPoolMetadata() );
            } );

            added = true;
        }

        List<PoolMetadata> unmodifiableMetadatas = Collections.unmodifiableList( metadatas );
        PoolMetadata metadata = PoolSlicer.unionOf( unmodifiableMetadatas );

        // Return null
        if ( !added )
        {
            LOGGER.debug( "Discovered an empty pool of statistics for {}. Returning null.",
                          metadata.getPoolDescription() );

            return null;
        }

        statistics.setPool( metadata.getPoolDescription() );

        // TODO: set the pool metadata for the baseline pool, which requires that the metadata is available in the
        // wrapped statistics. Currently, any description of the baseline pool information is relying on the evaluation
        // metadata, rather than the pool metadata. Note that, when an evaluation includes separate statistics for a
        // baseline, then the statistics for the baseline pool are in the slot for the main/predicted pool

        return statistics.build();
    }

    /**
     * Decomposes the input into pools. A pool contains a single set of space, time and threshold dimensions. The
     * statistics are returned in the natural order of the pools.
     *
     * @param statisticsStore the project statistics
     * @return the decomposed statistics in natural order
     * @throws InterruptedException if the statistics could not be retrieved from the project
     * @throws NullPointerException if the input is null
     */

    private static List<StatisticsStore> getStatisticsPerPool( StatisticsStore statisticsStore )
            throws InterruptedException
    {
        Objects.requireNonNull( statisticsStore );

        Map<PoolBoundaries, StatisticsStore.Builder> mappedStatistics = new TreeMap<>();

        // Double scores
        if ( statisticsStore.hasStatistic( StatisticType.DOUBLE_SCORE ) )
        {
            List<DoubleScoreStatisticOuter> statistics = statisticsStore.getDoubleScoreStatistics();
            MessageFactory.addDoubleScoreStatisticsToPool( statistics, mappedStatistics );
        }

        // Duration scores
        if ( statisticsStore.hasStatistic( StatisticType.DURATION_SCORE ) )
        {
            List<wres.datamodel.statistics.DurationScoreStatisticOuter> statistics =
                    statisticsStore.getDurationScoreStatistics();
            MessageFactory.addDurationScoreStatisticsToPool( statistics, mappedStatistics );
        }

        // Diagrams
        if ( statisticsStore.hasStatistic( StatisticType.DIAGRAM ) )
        {
            List<wres.datamodel.statistics.DiagramStatisticOuter> statistics = statisticsStore.getDiagramStatistics();
            MessageFactory.addDiagramStatisticsToPool( statistics, mappedStatistics );
        }

        // Duration diagrams
        if ( statisticsStore.hasStatistic( StatisticType.DURATION_DIAGRAM ) )
        {
            List<DurationDiagramStatisticOuter> statistics =
                    statisticsStore.getDurationDiagramStatistics();
            MessageFactory.addDurationDiagramStatisticsToPool( statistics, mappedStatistics );
        }

        // Box plots per pair
        if ( statisticsStore.hasStatistic( StatisticType.BOXPLOT_PER_PAIR ) )
        {
            List<wres.datamodel.statistics.BoxplotStatisticOuter> statistics =
                    statisticsStore.getBoxPlotStatisticsPerPair();
            MessageFactory.addBoxPlotStatisticsToPool( statistics, mappedStatistics, false );
        }

        // Box plots statistics per pool
        if ( statisticsStore.hasStatistic( StatisticType.BOXPLOT_PER_POOL ) )
        {
            List<wres.datamodel.statistics.BoxplotStatisticOuter> statistics =
                    statisticsStore.getBoxPlotStatisticsPerPool();
            MessageFactory.addBoxPlotStatisticsToPool( statistics, mappedStatistics, true );
        }

        List<StatisticsStore> returnMe = new ArrayList<>();

        // Build the per-pool statistics
        for ( StatisticsStore.Builder builder : mappedStatistics.values() )
        {
            StatisticsStore statistics = builder.setMinimumSampleSize( statisticsStore.getMinimumSampleSize() )
                                                .build();
            returnMe.add( statistics );
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Creates pool boundaries from metadata.
     *
     * @param metadata the metadata
     * @return the pool boundaries
     * @throws IllegalArgumentException if the pool does not contain precisely one geometry
     */

    private static PoolBoundaries getPoolBoundaries( PoolMetadata metadata )
    {
        Objects.requireNonNull( metadata );

        wres.datamodel.time.TimeWindowOuter window = metadata.getTimeWindow();
        wres.datamodel.thresholds.OneOrTwoThresholds thresholds = metadata.getThresholds();

        SortedSet<wres.datamodel.space.FeatureTuple> features =
                metadata.getPoolDescription()
                        .getGeometryGroup()
                        .getGeometryTuplesList()
                        .stream()
                        .map( MessageFactory::parse )
                        .collect( Collectors.toCollection( TreeSet::new ) );

        return new PoolBoundaries( Collections.unmodifiableSortedSet( features ),
                                   window,
                                   thresholds,
                                   metadata.getPoolDescription()
                                           .getIsBaselinePool() );
    }

    /**
     * Class the helps to organize statistics by pool boundaries within a map.
     *
     * @author James Brown
     */

    private record PoolBoundaries( SortedSet<FeatureTuple> features,
                                   TimeWindowOuter window,
                                   OneOrTwoThresholds thresholds,
                                   boolean isBaselinePool ) implements Comparable<PoolBoundaries>
    {
        @Override
        public int compareTo( PoolBoundaries o )
        {
            int compare = this.window()
                              .compareTo( o.window() );

            if ( compare != 0 )
            {
                return compare;
            }

            compare = this.thresholds()
                          .compareTo( o.thresholds() );

            if ( compare != 0 )
            {
                return compare;
            }

            compare = Boolean.compare( this.isBaselinePool(), o.isBaselinePool() );

            if ( compare != 0 )
            {
                return compare;
            }

            compare = Integer.compare( this.features()
                                           .size(),
                                       o.features()
                                        .size() );

            if ( compare != 0 )
            {
                return compare;
            }

            Iterator<FeatureTuple> internal = this.features()
                                                  .iterator();
            Iterator<FeatureTuple> external = o.features()
                                               .iterator();
            while ( internal.hasNext() )
            {
                compare = internal.next()
                                  .compareTo( external.next() );

                if ( compare != 0 )
                {
                    return compare;
                }
            }

            return 0;
        }
    }

    /**
     * Adds the new statistics to the map.
     *
     * @param statistics the statistics to add
     * @param mappedStatistics the existing statistics which the new statistics should be added
     * @throws NullPointerException if the input is null
     */

    private static void addDoubleScoreStatisticsToPool( List<DoubleScoreStatisticOuter> statistics,
                                                        Map<PoolBoundaries, StatisticsStore.Builder> mappedStatistics )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( DoubleScoreStatisticOuter next : statistics )
        {
            PoolMetadata metadata = next.getPoolMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsStore.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsStore.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<DoubleScoreStatisticOuter>> future = CompletableFuture.completedFuture( List.of( next ) );
            another.addDoubleScoreStatistics( future );
        }
    }

    /**
     * Adds the new statistics to the map.
     *
     * @param statistics the statistics to add
     * @param mappedStatistics the existing statistics which the new statistics should be added
     * @throws NullPointerException if the input is null
     */

    private static void
    addDurationScoreStatisticsToPool( List<wres.datamodel.statistics.DurationScoreStatisticOuter> statistics,
                                      Map<PoolBoundaries, StatisticsStore.Builder> mappedStatistics )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( wres.datamodel.statistics.DurationScoreStatisticOuter next : statistics )
        {
            PoolMetadata metadata = next.getPoolMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsStore.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsStore.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<wres.datamodel.statistics.DurationScoreStatisticOuter>> future =
                    CompletableFuture.completedFuture( List.of( next ) );
            another.addDurationScoreStatistics( future );
        }
    }

    /**
     * Adds the new statistics to the map.
     *
     * @param statistics the statistics to add
     * @param mappedStatistics the existing statistics which the new statistics should be added
     * @param perPool is true if the statistics are per pool, false for per pair (per pool)
     * @throws NullPointerException if the input is null
     */

    private static void addBoxPlotStatisticsToPool( List<wres.datamodel.statistics.BoxplotStatisticOuter> statistics,
                                                    Map<PoolBoundaries, StatisticsStore.Builder> mappedStatistics,
                                                    boolean perPool )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( wres.datamodel.statistics.BoxplotStatisticOuter next : statistics )
        {
            PoolMetadata metadata = next.getPoolMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsStore.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsStore.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<wres.datamodel.statistics.BoxplotStatisticOuter>> future =
                    CompletableFuture.completedFuture( List.of( next ) );
            if ( perPool )
            {
                another.addBoxPlotStatisticsPerPool( future );
            }
            else
            {
                another.addBoxPlotStatisticsPerPair( future );
            }
        }
    }

    /**
     * Adds the new statistics to the map.
     *
     * @param statistics the statistics to add
     * @param mappedStatistics the existing statistics which the new statistics should be added
     * @throws NullPointerException if the input is null
     */

    private static void addDiagramStatisticsToPool( List<wres.datamodel.statistics.DiagramStatisticOuter> statistics,
                                                    Map<PoolBoundaries, StatisticsStore.Builder> mappedStatistics )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( wres.datamodel.statistics.DiagramStatisticOuter next : statistics )
        {
            PoolMetadata metadata = next.getPoolMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsStore.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsStore.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<wres.datamodel.statistics.DiagramStatisticOuter>> future =
                    CompletableFuture.completedFuture( List.of( next ) );
            another.addDiagramStatistics( future );
        }
    }

    /**
     * Adds the new statistics to the map.
     *
     * @param statistics the statistics to add
     * @param mappedStatistics the existing statistics which the new statistics should be added
     * @throws NullPointerException if the input is null
     */

    private static void addDurationDiagramStatisticsToPool( List<DurationDiagramStatisticOuter> statistics,
                                                            Map<PoolBoundaries, StatisticsStore.Builder> mappedStatistics )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( DurationDiagramStatisticOuter next : statistics )
        {
            PoolMetadata metadata = next.getPoolMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsStore.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsStore.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<DurationDiagramStatisticOuter>> future =
                    CompletableFuture.completedFuture( List.of( next ) );
            another.addDurationDiagramStatistics( future );
        }
    }

    /**
     * Copies dataset information from an evaluation declaration to the evaluation builder.
     * @param evaluation the evaluation declaration
     * @param builder the messaging evaluation builder
     */
    private static void addDatasets( EvaluationDeclaration evaluation, Evaluation.Builder builder )
    {
        int metricCount = evaluation.metrics()
                                    .size();

        Dataset left = evaluation.left();
        Dataset right = evaluation.right();
        BaselineDataset baseline = evaluation.baseline();

        builder.setMetricCount( metricCount );
        LOGGER.debug( "Set the metric count to {}.", metricCount );

        // Dataset names for left/right
        String leftDataName = left.label();
        if ( Objects.nonNull( leftDataName ) )
        {
            builder.setLeftDataName( leftDataName );
            LOGGER.debug( "Set the left dataset name to {}.", leftDataName );
        }

        String rightDataName = right.label();
        if ( Objects.nonNull( rightDataName ) )
        {
            builder.setRightDataName( rightDataName );
            LOGGER.debug( "Set the right dataset name to {}.", rightDataName );
        }

        // Variable names for left/right
        Variable leftVariable = left.variable();
        if ( Objects.nonNull( leftVariable )
             && Objects.nonNull( leftVariable.getPreferredName() ) )
        {
            String leftVariableName = leftVariable.getPreferredName();
            builder.setLeftVariableName( leftVariableName );
            LOGGER.debug( "Set the left variable name to {}.", leftVariableName );
        }

        Variable rightVariable = right.variable();
        if ( Objects.nonNull( rightVariable )
             && Objects.nonNull( rightVariable.getPreferredName() ) )
        {
            String rightVariableName = rightVariable.getPreferredName();
            builder.setRightVariableName( rightVariableName );
            LOGGER.debug( "Set the right variable name to {}.", rightVariableName );
        }

        // Baselines
        builder.setDefaultBaseline( DefaultData.OBSERVED_CLIMATOLOGY );

        LOGGER.debug( "Set the default baseline to {}.",
                      DefaultData.OBSERVED_CLIMATOLOGY );

        // Explicit baseline
        if ( Objects.nonNull( baseline ) )
        {
            Dataset baselineDataset = baseline.dataset();
            String baselineDataName = baselineDataset.label();
            if ( Objects.nonNull( baselineDataName ) )
            {
                builder.setBaselineDataName( baselineDataName );
                LOGGER.debug( SET_THE_BASELINE_DATASET_NAME_TO, baselineDataName );
            }
            else if ( Objects.nonNull( baseline.generatedBaseline() ) )
            {
                String methodName = baseline.generatedBaseline()
                                            .method()
                                            .name();
                builder.setBaselineDataName( methodName );
                LOGGER.debug( SET_THE_BASELINE_DATASET_NAME_TO, methodName );
            }

            // Variable name
            Variable baselineVariable = baselineDataset.variable();
            if ( Objects.nonNull( baselineVariable )
                 && Objects.nonNull( baselineVariable.getPreferredName() ) )
            {
                String baselineVariableName = baselineVariable.getPreferredName();
                builder.setBaselineVariableName( baselineVariableName );
                LOGGER.debug( "Set the baseline variable name to {}.", baselineVariableName );
            }
        }
        // No explicit baseline, but possibly implicit baseline via skill metrics
        // In this case, the default is always climatology
        else if ( evaluation.metrics()
                            .stream()
                            .map( Metric::name )
                            .anyMatch( MetricConstants::isSkillMetric ) )
        {
            String baselineName = DefaultData.OBSERVED_CLIMATOLOGY.toString()
                                                                  .replace( "_", " " );
            builder.setBaselineDataName( baselineName );

            LOGGER.debug( SET_THE_BASELINE_DATASET_NAME_TO, baselineName );
        }
    }

    /**
     * Copies the ensemble filters from an evaluation declaration to the evaluation builder.
     * @param evaluation the evaluation declaration
     * @param builder the messaging evaluation builder
     */
    private static void addEnsembleMemberFilters( EvaluationDeclaration evaluation, Evaluation.Builder builder )
    {
        EnsembleFilter leftFilter = evaluation.left()
                                              .ensembleFilter();
        Set<String> members = new TreeSet<>();

        // Determine whether any filters exclude members
        boolean leftExclude = false;
        boolean rightExclude = false;
        boolean baselineExclude = false;
        if ( Objects.nonNull( leftFilter ) )
        {
            leftExclude = leftFilter.exclude();
            if ( !leftFilter.exclude() )
            {
                members.addAll( leftFilter.members() );
            }
        }
        EnsembleFilter rightFilter = evaluation.right()
                                               .ensembleFilter();
        if ( Objects.nonNull( rightFilter ) )
        {
            rightExclude = rightFilter.exclude();
            if ( !rightFilter.exclude() )
            {
                members.addAll( rightFilter.members() );
            }
        }

        if ( Objects.nonNull( evaluation.baseline() ) )
        {
            EnsembleFilter baselineFilter = evaluation.baseline()
                                                      .dataset()
                                                      .ensembleFilter();
            if ( Objects.nonNull( baselineFilter ) )
            {
                baselineExclude = baselineFilter.exclude();
                if ( !baselineExclude )
                {
                    members.addAll( baselineFilter.members() );
                }
            }
        }

        builder.addAllEnsembleMemberSubset( members );
        LOGGER.debug( "Set the ensemble members to: {}", members );

        if ( LOGGER.isWarnEnabled() && leftExclude || rightExclude || baselineExclude )
        {
            LOGGER.warn( "Discovered ensemble members to exclude from the evaluation, but these will not appear in "
                         + "the statistics metadata, which only records the subset of members to include." );
        }
    }

    /**
     * Do not construct.
     */

    private MessageFactory()
    {
    }

}
