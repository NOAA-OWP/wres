package wres.datamodel.messages;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import wres.config.ProjectConfigPlus;
import wres.config.ProjectConfigs;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.DoubleBoundsType;
import wres.config.generated.Feature;
import wres.config.generated.GraphicalType;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.DataFactory;
import wres.datamodel.Ensemble;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.StatisticsStore;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.Csv2Format;
import wres.statistics.generated.Outputs.CsvFormat;
import wres.statistics.generated.Outputs.GraphicFormat;
import wres.statistics.generated.Outputs.NumericFormat;
import wres.statistics.generated.Outputs.GraphicFormat.DurationUnit;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;
import wres.statistics.generated.Outputs.NetcdfFormat;
import wres.statistics.generated.Outputs.PngFormat;
import wres.statistics.generated.Outputs.ProtobufFormat;
import wres.statistics.generated.Outputs.SvgFormat;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;
import wres.statistics.generated.Pool;
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
 * @author James Brown
 */

public class MessageFactory
{

    private static final Logger LOGGER = LoggerFactory.getLogger( MessageFactory.class );

    /**
     * Creates a collection of {@link wres.statistics.generated.Statistics} by pool from a
     * {@link wres.datamodel.statistics.StatisticsStore}.
     *
     * @param project the project statistics
     * @return the statistics message
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if any input is null
     * @throws InterruptedException if the statistics could not be retrieved from the project
     */

    public static Collection<Statistics> getStatistics( StatisticsStore project )
            throws InterruptedException
    {
        Objects.requireNonNull( project );

        Collection<StatisticsStore> decomposedStatistics = MessageFactory.getStatisticsPerPool( project );

        Collection<Statistics> returnMe = new ArrayList<>();

        for ( StatisticsStore next : decomposedStatistics )
        {
            Statistics statistics = MessageFactory.parseOnePool( next );

            // Do not add empty statistics
            if ( Objects.nonNull( statistics ) )
            {
                returnMe.add( statistics );
            }
        }

        return Collections.unmodifiableCollection( returnMe );
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
                                                        .collect( Collectors.toList() );
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
            Map<wres.datamodel.time.ReferenceTimeType, Instant> times = nextSeries.getReferenceTimes();
            for ( Map.Entry<wres.datamodel.time.ReferenceTimeType, Instant> nextEntry : times.entrySet() )
            {
                wres.datamodel.time.ReferenceTimeType nextType = nextEntry.getKey();
                Instant nextTime = nextEntry.getValue();
                ReferenceTime nextRef =
                        ReferenceTime.newBuilder()
                                     .setReferenceTimeType( ReferenceTimeType.valueOf( nextType.toString() ) )
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
     * Returns the graphical destinations associated with the outputs message.
     * 
     * @param outputs the outputs message
     * @return the graphical destinations
     * @throws NullPointerException if the input is null 
     */

    public static Set<DestinationType> getGraphicsTypes( Outputs outputs )
    {
        Objects.requireNonNull( outputs );

        Set<DestinationType> returnMe = new HashSet<>();

        if ( outputs.hasPng() )
        {
            returnMe.add( DestinationType.PNG );
            returnMe.add( DestinationType.GRAPHIC );
        }

        if ( outputs.hasSvg() )
        {
            returnMe.add( DestinationType.SVG );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Uncovers a set of declared formats from a description of the outputs.
     * 
     * @param outputs the outputs that declare the formats to write
     * @return the declared formats to write
     */

    public static Set<Format> getDeclaredFormats( Outputs outputs )
    {
        return wres.statistics.MessageUtilities.getDeclaredFormats( outputs );
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

    public static GeometryTuple getGeometryTuple( FeatureKey left, FeatureKey right, FeatureKey baseline )
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
     * Creates a geometry tuple from the input.
     * 
     * @param left the left geometry, required
     * @param right the right geometry, required
     * @param baseline the baseline geometry, optional
     * @return the geometry tuple
     * @throws NullPointerException if either the left or right input is null
     */

    public static GeometryTuple getGeometryTuple( Geometry left, Geometry right, Geometry baseline )
    {
        Objects.requireNonNull( left );
        Objects.requireNonNull( right );

        GeometryTuple.Builder builder = GeometryTuple.newBuilder()
                                                     .setLeft( left )
                                                     .setRight( right );

        if ( Objects.nonNull( baseline ) )
        {
            builder.setBaseline( baseline );
        }

        return builder.build();
    }  
    
    /**
     * Creates a geometry group from the input.
     * 
     * @param groupName the group name
     * @param singleton the single geometry tuple
     * @return the geometry group
     * @throws NullPointerException if the singleton is null
     */

    public static GeometryGroup getGeometryGroup( String groupName, GeometryTuple singleton )
    {
        Objects.requireNonNull( singleton );

        GeometryGroup.Builder builder = GeometryGroup.newBuilder()
                                                     .addGeometryTuples( singleton );

        if ( Objects.nonNull( groupName ) )
        {
            builder.setRegionName( groupName );
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

        Set<GeometryTuple> geometries = features.stream()
                                                .map( FeatureTuple::getGeometryTuple )
                                                .collect( Collectors.toSet() );

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
     * Creates a geometry from the input.
     * @param name the name, optional
     * @param description the description, optional
     * @param srid the spatial reference id, optional
     * @param wkt the well-known text string, optional
     * @return the geometry
     */

    public static Geometry getGeometry( String name,
                                        String description,
                                        Integer srid,
                                        String wkt )
    {
        Geometry.Builder builder = Geometry.newBuilder();

        if ( Objects.nonNull( name ) )
        {
            builder.setName( name );
        }

        if ( Objects.nonNull( description ) )
        {
            builder.setDescription( description );
        }

        if ( Objects.nonNull( srid ) )
        {
            builder.setSrid( srid );
        }

        if ( Objects.nonNull( wkt ) )
        {
            builder.setWkt( wkt );
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Created a new geometry with name '{}', description '{}', srid '{}', and wkt '{}'.",
                          name,
                          description,
                          srid,
                          wkt );
        }

        return builder.build();
    }

    /**
     * Creates a geometry from the input.
     * @param name the name
     * @return geometry
     */

    public static Geometry getGeometry( String name )
    {
        return MessageFactory.getGeometry( name, null, null, null );
    }

    /**
     * Creates a {@link wres.statistics.generated.TimeWindow} from the input. Times on the lower and upper bounds 
     * default to {@link Instant#MIN} and {@link Instant#MAX}, respectively. Durations on the lower and upper bounds 
     * default to {@link TimeWindowOuter#DURATION_MIN} and {@link TimeWindowOuter#DURATION_MAX}, respectively.
     * 
     * @param earliestReferenceTime the earliest reference time, optional
     * @param latestReferenceTime the latest reference time, optional
     * @param earliestValidTime the earliest valid time, optional
     * @param latestValidTime the latest valid time, optional 
     * @param earliestLead the earliest lead time, optional
     * @param latestLead the latest lead time, optional
     * @return the time window
     */

    public static TimeWindow getTimeWindow( Instant earliestReferenceTime,
                                            Instant latestReferenceTime,
                                            Instant earliestValidTime,
                                            Instant latestValidTime,
                                            java.time.Duration earliestLead,
                                            java.time.Duration latestLead )
    {
        Instant earliestR = Instant.MIN;
        Instant latestR = Instant.MAX;
        Instant earliestV = Instant.MIN;
        Instant latestV = Instant.MAX;
        java.time.Duration earliestL = TimeWindowOuter.DURATION_MIN;
        java.time.Duration latestL = TimeWindowOuter.DURATION_MAX;

        if ( Objects.nonNull( earliestReferenceTime ) )
        {
            earliestR = earliestReferenceTime;
        }

        if ( Objects.nonNull( latestReferenceTime ) )
        {
            latestR = latestReferenceTime;
        }

        if ( Objects.nonNull( earliestValidTime ) )
        {
            earliestV = earliestValidTime;
        }

        if ( Objects.nonNull( latestValidTime ) )
        {
            latestV = latestValidTime;
        }

        if ( Objects.nonNull( earliestLead ) )
        {
            earliestL = earliestLead;
        }

        if ( Objects.nonNull( latestLead ) )
        {
            latestL = latestLead;
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Created a new time window with an earliest reference time of {}, a latest reference time "
                          + "of {}, an earliest valid time of {}, a latest valid time of {}, an earliest lead duration "
                          + "of {} and a latest lead duration of {}.",
                          earliestR,
                          latestR,
                          earliestV,
                          latestV,
                          earliestL,
                          latestL );
        }

        return TimeWindow.newBuilder()
                         .setEarliestReferenceTime( MessageFactory.parse( earliestR ) )
                         .setLatestReferenceTime( MessageFactory.parse( latestR ) )
                         .setEarliestValidTime( MessageFactory.parse( earliestV ) )
                         .setLatestValidTime( MessageFactory.parse( latestV ) )
                         .setEarliestLeadDuration( MessageFactory.parse( earliestL ) )
                         .setLatestLeadDuration( MessageFactory.parse( latestL ) )
                         .build();
    }

    /**
     * Creates a {@link wres.statistics.generated.TimeWindow} from the input. Times on the lower and upper bounds 
     * default to {@link Instant#MIN} and {@link Instant#MAX}, respectively.
     * 
     * @param earliestValidTime the earliest valid time, optional
     * @param latestValidTime the latest valid time, optional
     * @return the time window
     */

    public static TimeWindow getTimeWindow( Instant earliestValidTime,
                                            Instant latestValidTime )
    {
        return MessageFactory.getTimeWindow( null, null, earliestValidTime, latestValidTime, null, null );
    }

    /**
     * Creates a {@link wres.statistics.generated.TimeWindow} from the input. Durations on the lower and upper bounds 
     * default to {@link TimeWindowOuter#DURATION_MIN} and {@link TimeWindowOuter#DURATION_MAX}, respectively.
     * 
     * @param earliestLead the earliest lead time, optional
     * @param latestLead the latest lead time, optional
     * @return the time window
     */

    public static TimeWindow getTimeWindow( java.time.Duration earliestLead,
                                            java.time.Duration latestLead )
    {
        return MessageFactory.getTimeWindow( null, null, null, null, earliestLead, latestLead );
    }

    /**
     * Creates a {@link wres.statistics.generated.TimeWindow} from the input. Times on the lower and upper bounds 
     * default to {@link Instant#MIN} and {@link Instant#MAX}, respectively.
     * 
     * @param earliestReferenceTime the earliest reference time, optional
     * @param latestReferenceTime the latest reference time, optional
     * @param earliestValidTime the earliest valid time, optional
     * @param latestValidTime the latest valid time, optional
     * @return the time window
     */

    public static TimeWindow getTimeWindow( Instant earliestReferenceTime,
                                            Instant latestReferenceTime,
                                            Instant earliestValidTime,
                                            Instant latestValidTime )
    {
        return MessageFactory.getTimeWindow( earliestReferenceTime,
                                             latestReferenceTime,
                                             earliestValidTime,
                                             latestValidTime,
                                             null,
                                             null );
    }

    /**
     * Creates a {@link wres.statistics.generated.TimeWindow} from the input. Times on the lower and upper bounds 
     * default to {@link Instant#MIN} and {@link Instant#MAX}, respectively. Durations on the lower and upper bounds 
     * default to {@link TimeWindowOuter#DURATION_MIN} and {@link TimeWindowOuter#DURATION_MAX}, respectively.
     * 
     * @param earliestReferenceTime the earliest reference time, optional
     * @param latestReferenceTime the latest reference time, optional
     * @param lead the earliest and latest lead time, optional
     * @return the time window
     */

    public static TimeWindow getTimeWindow( Instant earliestReferenceTime,
                                            Instant latestReferenceTime,
                                            java.time.Duration lead )
    {
        return MessageFactory.getTimeWindow( earliestReferenceTime, latestReferenceTime, null, null, lead, lead );
    }

    /**
     * Creates a {@link wres.statistics.generated.TimeWindow} from the input. Times on the lower and upper bounds 
     * default to {@link Instant#MIN} and {@link Instant#MAX}, respectively. Durations on the lower and upper bounds 
     * default to {@link TimeWindowOuter#DURATION_MIN} and {@link TimeWindowOuter#DURATION_MAX}, respectively.
     * 
     * @param earliestReferenceTime the earliest reference time, optional
     * @param latestReferenceTime the latest reference time, optional
     * @param earliestLead the earliest lead time, optional
     * @param latestLead the latest lead time, optional
     * @return the time window
     */

    public static TimeWindow getTimeWindow( Instant earliestReferenceTime,
                                            Instant latestReferenceTime,
                                            java.time.Duration earliestLead,
                                            java.time.Duration latestLead )
    {
        return MessageFactory.getTimeWindow( earliestReferenceTime,
                                             latestReferenceTime,
                                             null,
                                             null,
                                             earliestLead,
                                             latestLead );
    }

    /**
     * Creates an empty {@link wres.statistics.generated.TimeWindow} in which the times on the lower and upper bounds 
     * default to {@link Instant#MIN} and {@link Instant#MAX}, respectively, and the durations on the lower and upper 
     * bounds default to {@link TimeWindowOuter#DURATION_MIN} and {@link TimeWindowOuter#DURATION_MAX}, respectively.
     * 
     * @return the empty time window
     */

    public static TimeWindow getTimeWindow()
    {
        return MessageFactory.getTimeWindow( null,
                                             null,
                                             null,
                                             null,
                                             null,
                                             null );
    }

    /**
     * Creates an evaluation from a project declaration.
     * 
     * @param projectConfigPlus the project declaration plus graphics strings
     * @return an evaluation
     * @throws NullPointerException if the project is null
     */

    public static Evaluation parse( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus );

        Evaluation.Builder builder = Evaluation.newBuilder();

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        // Add the inputs and default baseline, as needed
        MessageFactory.addInputs( projectConfig.getInputs(), builder );
        MessageFactory.addDefaultBaseline( projectConfig, builder );

        // Populate the evaluation from the supplied information as reasonably as possible
        if ( Objects.nonNull( projectConfig.getPair() ) && Objects.nonNull( projectConfig.getPair().getUnit() ) )
        {
            builder.setMeasurementUnit( projectConfig.getPair().getUnit() );

            LOGGER.debug( "Populated the evaluation with a measurement unit of {}.",
                          projectConfig.getPair().getUnit() );
        }

        if ( Objects.nonNull( projectConfig.getPair() ) && Objects.nonNull( projectConfig.getPair().getSeason() ) )
        {
            Season season = MessageFactory.parse( projectConfig.getPair().getSeason() );
            builder.setSeason( season );

            LOGGER.debug( "Populated the evaluation with a season of {}.",
                          season );
        }

        if ( Objects.nonNull( projectConfig.getPair() ) && Objects.nonNull( projectConfig.getPair().getValues() ) )
        {
            ValueFilter filter = MessageFactory.parse( projectConfig.getPair().getValues() );
            builder.setValueFilter( filter );

            LOGGER.debug( "Populated the evaluation with a value filter of {}.",
                          projectConfig.getPair().getValues() );
        }

        if ( Objects.nonNull( projectConfig.getOutputs() ) )
        {
            MessageFactory.addOutputs( projectConfigPlus, builder );

            LOGGER.debug( "Populated the evaluation with outputs of {}.",
                          builder.getOutputs() );
        }

        return builder.build();
    }

    /**
     * Creates a {@link Season} message from a {@link wres.config.generated.PairConfig.Season}.
     * 
     * @param season the declared season
     * @return the season message
     */

    public static Season parse( wres.config.generated.PairConfig.Season season )
    {
        Objects.requireNonNull( season );

        return Season.newBuilder()
                     .setStartDay( season.getEarliestDay() )
                     .setStartMonth( season.getEarliestMonth() )
                     .setEndDay( season.getLatestDay() )
                     .setEndMonth( season.getLatestMonth() )
                     .build();
    }

    /**
     * Creates a {@link ValueFilter} message from a {@link DoubleBoundsType}.
     * 
     * @param filter the declared value filter
     * @return the value filter message
     */

    public static ValueFilter parse( DoubleBoundsType filter )
    {
        Objects.requireNonNull( filter );

        ValueFilter.Builder filterBuilder = ValueFilter.newBuilder();

        if ( Objects.nonNull( filter.getMinimum() ) )
        {
            filterBuilder.setMinimumInclusiveValue( filter.getMinimum() );
        }

        if ( Objects.nonNull( filter.getMaximum() ) )
        {
            filterBuilder.setMaximumInclusiveValue( filter.getMaximum() );
        }

        return filterBuilder.build();
    }

    /**
     * Creates a {@link wres.statistics.generated.TimeScale} from a {@link wres.datamodel.scale.TimeScaleOuter}.
     * 
     * @param timeScale the time scale from which to create a message
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
     * Creates a {@link java.time.Duration} from a {@link Duration}.
     *
     * @param duration the duration to parse
     * @return the duration
     */

    public static java.time.Duration parse( Duration duration )
    {
        Objects.requireNonNull( duration );

        return java.time.Duration.ofSeconds( duration.getSeconds(), duration.getNanos() );
    }

    /**
     * Creates a {@link java.time.Duration} from a {@link Duration}.
     *
     * @param duration the duration to parse
     * @return the duration
     */

    public static Duration parse( java.time.Duration duration )
    {
        Objects.requireNonNull( duration );

        return Duration.newBuilder()
                       .setSeconds( duration.getSeconds() )
                       .setNanos( duration.getNano() )
                       .build();
    }

    /**
     * Creates a {@link java.time.Instant} from a {@link Timestamp}.
     *
     * @param timeStamp the time stamp to parse
     * @return the instant
     */

    public static java.time.Instant parse( Timestamp timeStamp )
    {
        Objects.requireNonNull( timeStamp );

        return java.time.Instant.ofEpochSecond( timeStamp.getSeconds(), timeStamp.getNanos() );
    }

    /**
     * Creates a {@link Timestamp} from a {@link java.time.Instant}.
     *
     * @param instant the instant to parse
     * @return the time stamp
     */

    public static Timestamp parse( java.time.Instant instant )
    {
        Objects.requireNonNull( instant );

        return Timestamp.newBuilder()
                        .setSeconds( instant.getEpochSecond() )
                        .setNanos( instant.getNano() )
                        .build();
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

        return statistic.getData();
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

        return statistic.getData();
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

        return statistic.getData();
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

        return statistic.getData();
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

        return statistic.getData();
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
     * Creates a {@link wres.statistics.generated.GeometryTuple} from a {@link Feature}.
     * 
     * @param feature a declared feature from which to build the instance, not null
     * @return the message
     */

    public static GeometryTuple parse( Feature feature )
    {
        Objects.requireNonNull( feature );

        Geometry left = MessageFactory.getGeometry( feature.getLeft() );
        Geometry right = MessageFactory.getGeometry( feature.getRight() );
        GeometryTuple.Builder builder = GeometryTuple.newBuilder()
                                                     .setLeft( left )
                                                     .setRight( right );

        if ( Objects.nonNull( feature.getBaseline() ) )
        {
            Geometry baseline = MessageFactory.getGeometry( feature.getBaseline() );
            builder.setBaseline( baseline );
        }

        return builder.build();
    }

    /**
     * Creates a {@link wres.statistics.generated.Geometry} from a {@link wres.datamodel.space.FeatureKey}.
     * 
     * @param featureKey the feature key from which to create a message
     * @return the message
     */

    public static Geometry parse( FeatureKey featureKey )
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
     * Creates a {@link Format} from a {@link DestinationType}.
     * 
     * @param destinationType the destination type
     * @return the format
     */

    public static Format parse( DestinationType destinationType )
    {
        switch ( destinationType )
        {
            case GRAPHIC:
                return Format.PNG;
            case NUMERIC:
                return Format.CSV;
            default:
                return Format.valueOf( destinationType.name() );
        }
    }

    /**
     * Returns <code>true</code> if the outputs contains graphics type, otherwise <code>false</code>.
     * 
     * @param outputs the outputs message
     * @return true if graphics are required
     * @throws NullPointerException if the input is null 
     */

    public static boolean hasGraphicsTypes( Outputs outputs )
    {
        return !MessageFactory.getGraphicsTypes( outputs ).isEmpty();
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

        PoolMetadata metadata = PoolMetadata.of();

        boolean added = false;

        // Add the double scores
        if ( onePool.hasStatistic( StatisticType.DOUBLE_SCORE ) )
        {
            List<wres.datamodel.statistics.DoubleScoreStatisticOuter> doubleScores = onePool.getDoubleScoreStatistics();
            doubleScores.forEach( next -> statistics.addScores( MessageFactory.parse( next ) ) );

            // Because the input is a single pool
            metadata = doubleScores.get( 0 ).getMetadata();
            added = true;
        }

        // Add the diagrams
        if ( onePool.hasStatistic( StatisticType.DIAGRAM ) )
        {
            List<wres.datamodel.statistics.DiagramStatisticOuter> diagrams = onePool.getDiagramStatistics();
            diagrams.forEach( next -> statistics.addDiagrams( MessageFactory.parse( next ) ) );

            // Because the input is a single pool
            metadata = diagrams.get( 0 ).getMetadata();
            added = true;
        }

        // Add the boxplots per pool
        if ( onePool.hasStatistic( StatisticType.BOXPLOT_PER_POOL ) )
        {
            List<wres.datamodel.statistics.BoxplotStatisticOuter> boxplots = onePool.getBoxPlotStatisticsPerPool();
            boxplots.forEach( next -> statistics.addOneBoxPerPool( MessageFactory.parse( next ) ) );

            // Because the input is a single pool
            metadata = boxplots.get( 0 ).getMetadata();
            added = true;
        }

        // Add the boxplots per pair
        if ( onePool.hasStatistic( StatisticType.BOXPLOT_PER_PAIR ) )
        {
            List<wres.datamodel.statistics.BoxplotStatisticOuter> boxplots = onePool.getBoxPlotStatisticsPerPair();
            boxplots.forEach( next -> statistics.addOneBoxPerPair( MessageFactory.parse( next ) ) );
            metadata = boxplots.get( 0 ).getMetadata();
            added = true;
        }

        // Add the duration scores
        if ( onePool.hasStatistic( StatisticType.DURATION_SCORE ) )
        {
            List<wres.datamodel.statistics.DurationScoreStatisticOuter> durationScores =
                    onePool.getDurationScoreStatistics();
            durationScores.forEach( next -> statistics.addDurationScores( MessageFactory.parse( next ) ) );

            // Because the input is a single pool
            metadata = durationScores.get( 0 ).getMetadata();
            added = true;
        }

        // Add the duration diagrams with instant/duration pairs
        if ( onePool.hasStatistic( StatisticType.DURATION_DIAGRAM ) )
        {
            List<wres.datamodel.statistics.DurationDiagramStatisticOuter> durationDiagrams =
                    onePool.getInstantDurationPairStatistics();
            durationDiagrams.forEach( next -> statistics.addDurationDiagrams( MessageFactory.parse( next ) ) );
            metadata = durationDiagrams.get( 0 ).getMetadata();
            added = true;
        }

        // Return null
        if ( !added )
        {
            LOGGER.debug( "Discovered an empty pool of statistics for {}. Returning null.",
                          metadata.getPool() );

            return null;
        }

        // Set the pool information
        if ( metadata.getPool().getIsBaselinePool() )
        {
            statistics.setBaselinePool( metadata.getPool() );
        }
        else
        {
            statistics.setPool( metadata.getPool() );
        }

        return statistics.build();
    }

    /**
     * Decomposes the input into pools. A pool contains a single set of space, time and threshold dimensions.
     *
     * @param project the project statistics
     * @return the decomposed statistics
     * @throws InterruptedException if the statistics could not be retrieved from the project
     * @throws NullPointerException if the input is null
     */

    private static Collection<StatisticsStore> getStatisticsPerPool( StatisticsStore project )
            throws InterruptedException
    {
        Objects.requireNonNull( project );

        Map<PoolBoundaries, StatisticsStore.Builder> mappedStatistics = new HashMap<>();

        // Double scores
        if ( project.hasStatistic( StatisticType.DOUBLE_SCORE ) )
        {
            List<DoubleScoreStatisticOuter> statistics = project.getDoubleScoreStatistics();
            MessageFactory.addDoubleScoreStatisticsToPool( statistics, mappedStatistics );
        }

        // Duration scores
        if ( project.hasStatistic( StatisticType.DURATION_SCORE ) )
        {
            List<wres.datamodel.statistics.DurationScoreStatisticOuter> statistics =
                    project.getDurationScoreStatistics();
            MessageFactory.addDurationScoreStatisticsToPool( statistics, mappedStatistics );
        }

        // Diagrams
        if ( project.hasStatistic( StatisticType.DIAGRAM ) )
        {
            List<wres.datamodel.statistics.DiagramStatisticOuter> statistics = project.getDiagramStatistics();
            MessageFactory.addDiagramStatisticsToPool( statistics, mappedStatistics );
        }

        // Box plots per pair
        if ( project.hasStatistic( StatisticType.BOXPLOT_PER_PAIR ) )
        {
            List<wres.datamodel.statistics.BoxplotStatisticOuter> statistics = project.getBoxPlotStatisticsPerPair();
            MessageFactory.addBoxPlotStatisticsToPool( statistics, mappedStatistics, false );
        }

        // Box plots statistics per pool
        if ( project.hasStatistic( StatisticType.BOXPLOT_PER_POOL ) )
        {
            List<wres.datamodel.statistics.BoxplotStatisticOuter> statistics = project.getBoxPlotStatisticsPerPool();
            MessageFactory.addBoxPlotStatisticsToPool( statistics, mappedStatistics, true );
        }

        // Box plots statistics per pool
        if ( project.hasStatistic( StatisticType.DURATION_DIAGRAM ) )
        {
            List<DurationDiagramStatisticOuter> statistics =
                    project.getInstantDurationPairStatistics();
            MessageFactory.addPairedStatisticsToPool( statistics, mappedStatistics );
        }

        Collection<StatisticsStore> returnMe = new ArrayList<>();

        // Build the per-pool statistics
        for ( StatisticsStore.Builder builder : mappedStatistics.values() )
        {
            StatisticsStore statistics = builder.setMinimumSampleSize( project.getMinimumSampleSize() )
                                                .build();
            returnMe.add( statistics );
        }

        return Collections.unmodifiableCollection( returnMe );
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

        Set<wres.datamodel.space.FeatureTuple> features = metadata.getPool()
                                                                  .getGeometryGroup()
                                                                  .getGeometryTuplesList()
                                                                  .stream()
                                                                  .map( MessageFactory::parse )
                                                                  .collect( Collectors.toSet() );

        return new PoolBoundaries( features, window, thresholds, metadata.getPool().getIsBaselinePool() );
    }

    /**
     * Class the helps to organize statistics by pool boundaries within a map.
     *
     * @author James Brown
     */

    private static class PoolBoundaries
    {
        private final OneOrTwoThresholds thresholds;
        private final wres.datamodel.time.TimeWindowOuter window;
        private final Set<wres.datamodel.space.FeatureTuple> features;
        private final boolean isBaselinePool;

        private PoolBoundaries( Set<wres.datamodel.space.FeatureTuple> features,
                                wres.datamodel.time.TimeWindowOuter window,
                                OneOrTwoThresholds thresholds,
                                boolean isBaselinePool )
        {
            this.features = features;
            this.window = window;
            this.thresholds = thresholds;
            this.isBaselinePool = isBaselinePool;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( o == this )
            {
                return true;
            }

            if ( ! ( o instanceof PoolBoundaries ) )
            {
                return false;
            }

            PoolBoundaries input = (PoolBoundaries) o;

            return Objects.equals( this.features, input.features ) && Objects.equals( this.window, input.window )
                   && Objects.equals( this.thresholds, input.thresholds )
                   && this.isBaselinePool == input.isBaselinePool;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( this.features, this.window, this.thresholds, this.isBaselinePool );
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
            PoolMetadata metadata = next.getMetadata();
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
            PoolMetadata metadata = next.getMetadata();
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
            PoolMetadata metadata = next.getMetadata();
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
            PoolMetadata metadata = next.getMetadata();
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

    private static void addPairedStatisticsToPool( List<DurationDiagramStatisticOuter> statistics,
                                                   Map<PoolBoundaries, StatisticsStore.Builder> mappedStatistics )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( DurationDiagramStatisticOuter next : statistics )
        {
            PoolMetadata metadata = next.getMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsStore.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsStore.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<DurationDiagramStatisticOuter>> future =
                    CompletableFuture.completedFuture( List.of( next ) );
            another.addInstantDurationPairStatistics( future );
        }
    }

    /**
     * Adds the inputs declaration to an evaluation builder.
     * 
     * @param project the project declaration
     * 
     * @return an evaluation
     * @throws NullPointerException if the project is null
     */

    private static void addInputs( Inputs inputs, Evaluation.Builder builder )
    {
        Objects.requireNonNull( builder );

        if ( Objects.isNull( inputs ) )
        {
            LOGGER.debug( "No inputs were discovered." );

            return;
        }

        if ( Objects.nonNull( inputs.getLeft() )
             && Objects.nonNull( inputs.getLeft().getVariable() ) )
        {
            String variableName = ProjectConfigs.getVariableIdFromDataSourceConfig( inputs.getLeft() );
            builder.setLeftVariableName( variableName );

            LOGGER.debug( "Populated the evaluation with a left variable name of {}.",
                          variableName );
        }

        if ( Objects.nonNull( inputs.getRight() )
             && Objects.nonNull( inputs.getRight().getVariable() ) )
        {
            String variableName = ProjectConfigs.getVariableIdFromDataSourceConfig( inputs.getRight() );
            builder.setRightVariableName( variableName );

            LOGGER.debug( "Populated the evaluation with a right variable name of {}.",
                          variableName );
        }

        if ( Objects.nonNull( inputs.getBaseline() )
             && Objects.nonNull( inputs.getBaseline().getVariable() ) )
        {
            String variableName = ProjectConfigs.getVariableIdFromDataSourceConfig( inputs.getBaseline() );
            builder.setBaselineVariableName( variableName );

            LOGGER.debug( "Populated the evaluation with a baseline variable name of {}.",
                          variableName );
        }

        if ( Objects.nonNull( inputs.getLeft() )
             && Objects.nonNull( inputs.getLeft().getLabel() ) )
        {
            String name = inputs.getLeft().getLabel();
            builder.setLeftDataName( name );

            LOGGER.debug( "Populated the evaluation with a left source name of {}.",
                          name );
        }

        if ( Objects.nonNull( inputs.getRight() )
             && Objects.nonNull( inputs.getRight().getLabel() ) )
        {
            String name = inputs.getRight().getLabel();
            builder.setRightDataName( name );

            LOGGER.debug( "Populated the evaluation with a right source name of {}.",
                          name );
        }

        if ( Objects.nonNull( inputs.getBaseline() )
             && Objects.nonNull( inputs.getBaseline().getLabel() ) )
        {
            String name = inputs.getBaseline().getLabel();
            builder.setBaselineDataName( name );

            LOGGER.debug( "Populated the evaluation with a baseline source name of {}.",
                          name );
        }
    }

    /**
     * Adds a default baseline, as requried by the project declaration.
     * 
     * @param projectConfig the project declaration
     * @return an evaluation
     * @throws NullPointerException if the project is null
     */

    private static void addDefaultBaseline( ProjectConfig projectConfig, Evaluation.Builder builder )
    {
        Objects.requireNonNull( builder );

        if ( Objects.nonNull( projectConfig.getMetrics() ) )
        {
            List<MetricsConfig> declaredMetrics = projectConfig.getMetrics();
            Set<MetricConstants> metrics = declaredMetrics.stream()
                                                          .flatMap( a -> DataFactory.getMetricsFromMetricsConfig( a,
                                                                                                                  projectConfig )
                                                                                    .stream() )
                                                          .collect( Collectors.toSet() );

            builder.setMetricCount( metrics.size() );

            LOGGER.debug( "Populated the evaluation with a metric count of {}.",
                          metrics.size() );

            // Add a default baseline
            builder.setDefaultBaseline( DefaultData.OBSERVED_CLIMATOLOGY );

            // Set the baseline name as the default where no baseline is defined and skill metrics are requested
            if ( metrics.stream().anyMatch( MetricConstants::isSkillMetric )
                 && Objects.isNull( projectConfig.getInputs().getBaseline() ) )
            {
                builder.setBaselineDataName( DefaultData.OBSERVED_CLIMATOLOGY.toString()
                                                                             .replace( "_", " " ) );
            }

            LOGGER.debug( "Populated the default baseline with {}.",
                          DefaultData.OBSERVED_CLIMATOLOGY );
        }
    }

    /**
     * Creates a {@link wres.statistics.generated.Outputs} from a {@link ProjectConfigPlus}.
     * 
     * @param projectConfigPlus the project declaration plus graphics string from which to create a message
     * @param evaluation the evaluation builder
     */

    private static void addOutputs( ProjectConfigPlus projectConfigPlus, Evaluation.Builder evaluation )
    {
        Objects.requireNonNull( projectConfigPlus );
        Objects.requireNonNull( projectConfigPlus.getProjectConfig().getOutputs() );

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();
        ProjectConfig.Outputs outputs = projectConfig.getOutputs();

        Outputs.Builder builder = Outputs.newBuilder();

        wres.config.generated.DurationUnit durationFormat = outputs.getDurationFormat();

        boolean issuedDatesPools = Objects.nonNull( projectConfig.getPair() )
                                   && Objects.nonNull( projectConfig.getPair().getIssuedDatesPoolingWindow() );

        boolean validDatesPools = Objects.nonNull( projectConfig.getPair() )
                                  && Objects.nonNull( projectConfig.getPair().getValidDatesPoolingWindow() );

        // Iterate the destinations
        for ( DestinationConfig destination : outputs.getDestination() )
        {
            MessageFactory.addDestination( destination,
                                           durationFormat,
                                           builder,
                                           issuedDatesPools,
                                           validDatesPools );
        }

        evaluation.setOutputs( builder );
    }

    /**
     * Adds a destination to an outputs builder.
     * @param destination the destination
     * @param durationFormat the optional duration format
     * @param builder the builder
     * @param issuedDatesPools is true if there are issued dates pools
     * @param validDatesPools is true if there are valid dates pools
     * @throws IllegalArgumentException if the declared format does not map to a recognized type
     */
    private static void addDestination( DestinationConfig destination,
                                        wres.config.generated.DurationUnit durationFormat,
                                        Outputs.Builder builder,
                                        boolean issuedDatesPools,
                                        boolean validDatesPools )
    {
        DestinationType destinationType = destination.getType();

        // Graphic formats
        if ( ProjectConfigs.isGraphicsType( destinationType ) )
        {
            MessageFactory.addGraphicsDestination( destination,
                                                   durationFormat,
                                                   builder,
                                                   issuedDatesPools,
                                                   validDatesPools );
        }
        // Numeric formats
        else
        {
            MessageFactory.addNumericDestination( destination, builder );
        }
    }

    /**
     * Adds a graphics destination to an outputs builder.
     * @param destination the destination
     * @param durationFormat the optional duration format
     * @param builder the builder
     * @param issuedDatesPools is true if there are issued dates pools
     * @param validDatesPools is true if there are valid dates pools
     * @throws IllegalArgumentException if the declared format does not map to a recognized type
     */

    private static void addGraphicsDestination( DestinationConfig destination,
                                                wres.config.generated.DurationUnit durationFormat,
                                                Outputs.Builder builder,
                                                boolean issuedDatesPools,
                                                boolean validDatesPools )
    {
        DestinationType destinationType = destination.getType();

        GraphicFormat generalOptions = MessageFactory.getGeneralGraphicOptions( destination,
                                                                                durationFormat,
                                                                                issuedDatesPools,
                                                                                validDatesPools );

        if ( destinationType == DestinationType.PNG || destinationType == DestinationType.GRAPHIC )
        {
            builder.setPng( PngFormat.newBuilder()
                                     .setOptions( generalOptions ) );
        }
        else if ( destinationType == DestinationType.SVG )
        {
            builder.setSvg( SvgFormat.newBuilder()
                                     .setOptions( generalOptions ) );
        }
        else
        {
            throw new IllegalArgumentException( "While creating an evaluation description, encountered an "
                                                + "unrecognized format '"
                                                + destinationType
                                                + "'." );
        }
    }

    /**
     * Adds a numeric destination to an outputs message.
     * @param destination the destination
     * @param builder the builder
     * @throws IllegalArgumentException if the declared format does not map to a recognized type
     */

    private static void addNumericDestination( DestinationConfig destination,
                                               Outputs.Builder builder )
    {
        DestinationType destinationType = destination.getType();

        NumericFormat.Builder generalOptions = NumericFormat.newBuilder();

        if ( Objects.nonNull( destination.getDecimalFormat() ) )
        {
            generalOptions.setDecimalFormat( destination.getDecimalFormat() );
        }

        if ( destinationType == DestinationType.CSV || destinationType == DestinationType.NUMERIC )
        {
            builder.setCsv( CsvFormat.newBuilder()
                                     .setOptions( generalOptions ) );
        }
        else if ( destinationType == DestinationType.PROTOBUF )
        {
            builder.setProtobuf( ProtobufFormat.newBuilder() );
        }
        else if ( destinationType == DestinationType.NETCDF || destinationType == DestinationType.NETCDF_2 )
        {
            builder.setNetcdf( NetcdfFormat.newBuilder() );
        }
        else if ( destinationType == DestinationType.CSV2 )
        {
            builder.setCsv2( Csv2Format.newBuilder() );
        }
        else if ( destinationType == DestinationType.PAIRS )
        {
            LOGGER.debug( "Encountered a declaration that requires {}. This destination is not currently mapped "
                          + "to a message format type.",
                          destinationType );
        }
        else
        {
            throw new IllegalArgumentException( "While creating an evaluation description, encountered an "
                                                + "unrecognized format '"
                                                + destinationType
                                                + "'." );
        }
    }

    /**
     * Returns the general graphic format options that apply to all graphics.
     * @param destination the destination
     * @param durationFormat the optional duration format
     * @param issuedDatesPools is true if there are issued dates pools
     * @param validDatesPools is true if there are valid dates pools
     * @return the graphic format options
     */
    private static GraphicFormat getGeneralGraphicOptions( DestinationConfig destination,
                                                           wres.config.generated.DurationUnit durationFormat,
                                                           boolean issuedDatesPools,
                                                           boolean validDatesPools )
    {
        GraphicalType graphics = destination.getGraphical();

        GraphicFormat.Builder generalOptions = GraphicFormat.newBuilder();

        // Graphics options available?
        if ( Objects.nonNull( graphics ) )
        {
            if ( Objects.nonNull( graphics.getHeight() ) )
            {
                generalOptions.setHeight( graphics.getHeight() );
            }

            if ( Objects.nonNull( graphics.getWidth() ) )
            {
                generalOptions.setWidth( graphics.getWidth() );
            }

            // Add any metrics to ignore
            for ( MetricConfigName ignore : graphics.getSuppressMetric() )
            {
                generalOptions.addIgnore( MetricName.valueOf( ignore.name() ) );
            }
        }

        if ( Objects.nonNull( durationFormat ) )
        {
            generalOptions.setLeadUnit( DurationUnit.valueOf( durationFormat.name() ) );
        }

        if ( issuedDatesPools )
        {
            generalOptions.setShape( GraphicShape.ISSUED_DATE_POOLS );
        }
        else if ( validDatesPools )
        {
            generalOptions.setShape( GraphicShape.VALID_DATE_POOLS );
        }
        else if ( Objects.nonNull( destination.getOutputType() ) )
        {
            GraphicShape shape = GraphicShape.valueOf( destination.getOutputType().name() );

            LOGGER.debug( "Detected a shape of {} for the graphics output format in destination {}.",
                          shape,
                          destination );

            generalOptions.setShape( shape );
        }
        else
        {
            generalOptions.setShape( GraphicShape.LEAD_THRESHOLD );
        }

        return generalOptions.build();
    }

    /**
     * Do not construct.
     */

    private MessageFactory()
    {
    }

}
