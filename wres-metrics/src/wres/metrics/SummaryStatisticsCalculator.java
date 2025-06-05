package wres.metrics;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.eclipse.collections.api.list.primitive.MutableDoubleList;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;

import wres.statistics.MessageUtilities;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Statistics;

/**
 * <p>Accepts raw statistics as they are computed and, on request, closes the instance to further input and calculates the
 * summary statistics nominated on construction. Optionally, filters the supplied statistics with a filter provided on
 * construction, ignoring any statistics that do not meet the filter condition. It is assumed that each blob of
 * statistics has a common structure for each supplied metric. For example, if one blob of statistics contains a
 * reliability diagram with five bins, then all other reliability diagrams provided to the same instance must contain
 * five bins. However, some of the blobs of statistics may contain no reliability diagrams.
 *
 * <p>Implementation notes:
 *
 * <p>This is an in-memory implementation, not backed by a database and is, therefore, constrained by system memory.
 * Callers should use the APIs advertised by this class, rather than the class itself, as that will allow the
 * implementation to be swapped for a database variant, when a database schema is available. See #45466.
 *
 * @author James Brown
 */

@ThreadSafe
public class SummaryStatisticsCalculator implements Supplier<List<Statistics>>, Predicate<Statistics>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( SummaryStatisticsCalculator.class );

    /** Filters durations. */
    private static final UnaryOperator<List<Duration>> DURATION_FILTER =
            durations -> durations.stream()
                                  .filter( Objects::nonNull )
                                  .toList();

    /** The cached samples of score statistics. */
    private final Map<MetricNames, MutableDoubleList> doubleScores;

    /** The cached sample of diagram statistics with each column containing one sample and each row containing all
     * samples for one index of the diagram component. */
    private final Map<MetricNames, List<MutableDoubleList>> diagrams;

    /** The cached sample of duration score statistics. */
    private final Map<MetricNames, List<Duration>> durationScores;

    /** The cached sample of duration diagram statistics. */
    private final Map<MetricName, Map<Instant, List<Duration>>> durationDiagrams;

    /** Template builders used to create summary statistics for double scores. */
    private final Map<MetricName, DoubleScoreStatistic.Builder> doubleScoreTemplates;

    /** Template builders used to create summary statistics for duration scores. */
    private final Map<MetricName, DurationScoreStatistic.Builder> durationScoreTemplates;

    /** Template builders used to create summary statistics for diagrams. */
    private final Map<MetricName, DiagramStatistic.Builder> diagramTemplates;

    /** Template builders used to create summary statistics for duration diagrams. */
    private final Map<MetricName, DurationDiagramStatistic.Builder> durationDiagramTemplates;

    /** Whether this instance has been completed and is read-only. */
    private final AtomicBoolean isComplete;

    /** A filter supplied on construction. */
    private final Predicate<Statistics> filter;

    /** The scalar summary statistics to calculate on completion. */
    private final Set<ScalarSummaryStatisticFunction> scalarStatistics;

    /** The diagram summary statistics to calculate on completion. */
    private final Set<DiagramSummaryStatisticFunction> diagramStatistics;

    /** The box plot summary statistics to calculate on completion. */
    private final Set<BoxplotSummaryStatisticFunction> boxplotStatistics;

    /** The calculated summary statistics. */
    private final List<Statistics> statistics;

    /** Time units for duration statistics. */
    private final ChronoUnit timeUnit;

    /** Locks the creation of new diagram row slots within the {@link #diagrams}. */
    private final ReentrantLock diagramRowSlotCreationLock = new ReentrantLock();

    /** A transformer that aggregates the raw statistics metadata to reflect the dimension over which the summary
     * statistics were calculated. For example, when summarizing over features, the feature metadata should be
     * aggregated to include all features registered with the calculator. Consumes the existing metadata in the first
     * argument and uses the new metadata in the second argument to generate updated metadata. */
    private final BinaryOperator<Statistics> metadataAggregator;

    /** The superset of statistics added to this instance. Contains only the first occurrence of each metric. */
    @GuardedBy( "nominalStatisticsLock" )
    private Statistics nominal = null;

    /** A lock to allow foratomic updating of the {@link #nominal} statistics. */
    private final ReentrantLock nominalStatisticsLock = new ReentrantLock();

    /**
     * Creates an instance.
     * @param scalarStatistics the scalar summary statistics to calculate
     * @param diagramStatistics the diagram summary statistics to calculate
     * @param boxplotStatistics the box plot summary statistics to calculate
     * @param filter an optional filter
     * @param metadataTransformer a transformer that adapts the statistics metadata to reflect the summary performed
     * @param timeUnits the optional time units to use for duration statistics
     * @return an instance
     * @throws IllegalArgumentException if all lists of statistics are null or empty
     */
    public static SummaryStatisticsCalculator of( Set<ScalarSummaryStatisticFunction> scalarStatistics,
                                                  Set<DiagramSummaryStatisticFunction> diagramStatistics,
                                                  Set<BoxplotSummaryStatisticFunction> boxplotStatistics,
                                                  Predicate<Statistics> filter,
                                                  BinaryOperator<Statistics> metadataTransformer,
                                                  ChronoUnit timeUnits )
    {
        return new SummaryStatisticsCalculator( scalarStatistics,
                                                diagramStatistics,
                                                boxplotStatistics,
                                                filter,
                                                metadataTransformer,
                                                timeUnits );
    }

    /**
     * Closes the calculator to further additions via {@link #test(Statistics)}} and returns a {@link Statistics} for
     * each summary statistic requested on construction.
     *
     * @return the summary statistics
     */
    @Override
    public List<Statistics> get()
    {
        if ( !this.isComplete.getAndSet( true ) )
        {
            this.calculateSummaryStatistics();
        }

        return Collections.unmodifiableList( this.statistics );
    }

    /**
     * Adds the supplied statistics to the internal store.
     * @param statistics the statistics
     * @throws NullPointerException if the statistics is null
     * @throws IllegalArgumentException if this instance has been completed and no further statistics are expected
     * @return whether the statistics were accepted into the store based on a filter supplied on construction
     */

    @Override
    public boolean test( Statistics statistics )
    {
        Objects.requireNonNull( statistics );

        if ( this.isComplete.get() )
        {
            throw new IllegalArgumentException( "No further statistics can be added, as this instance has been marked "
                                                + "complete." );
        }

        // Filter statistics that do not meet the filter supplied on construction
        if ( !this.filter.test( statistics ) )
        {
            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Rejected these statistics as they do not meet the filter: {}.", statistics );
            }

            return false;
        }

        // Remove any statistics that do not support summary statistics
        statistics = this.filter( statistics );

        // Update the nominal statistics metadata
        this.updateStatisticsMetadata( statistics );

        // Update the templates that store the raw statistics to include any novel statistics in the current blob
        this.updateStatisticsTemplates( statistics );

        // Add the double score statistics
        this.addDoubleScores( statistics );
        // Add the duration score statistics
        this.addDurationScores( statistics );
        // Add the diagrams
        this.addDiagrams( statistics );
        // Add the duration diagrams
        this.addDurationDiagrams( statistics );

        LOGGER.debug( "Added statistics in thread {}.",
                      Thread.currentThread()
                            .getName() );

        return true;
    }

    /**
     * Updates the summary statistics metadata to reflect the latest raw statistics.
     *
     * @param latest the latest raw statistics
     */

    private void updateStatisticsMetadata( Statistics latest )
    {
        try
        {
            this.nominalStatisticsLock.lock();

            // Set the nominal statistics whose metadata will be used when reporting summary statistics
            if ( Objects.isNull( this.nominal ) )
            {
                this.nominal = latest.toBuilder()
                                     .clearScores()
                                     .clearDurationScores()
                                     .clearDiagrams()
                                     .clearDurationDiagrams()
                                     .clearOneBoxPerPair()
                                     .clearOneBoxPerPool()
                                     .build();

                // Apply metadata adaptation to the first instance in case any unconditional adaptations are requested,
                // such as removing event threshold values. For example, event threshold values may be removed
                // unconditionally, and this should occur even when the summary statistics are produced for a sample
                // size of one
                this.metadataAggregator.apply( this.nominal, this.nominal );

                LOGGER.debug( "Set the nominal statistics metadata for summary statistics calculator {} to: {}",
                              this,
                              this.nominal );
            }

            // Transform the metadata to reflect the new information supplied
            this.nominal = this.metadataAggregator.apply( this.nominal, latest );

            LOGGER.debug( "Transformed the nominal statistics metadata for summary statistics calculation: {}.",
                          this.nominal );
        }
        finally
        {
            this.nominalStatisticsLock.unlock();
        }
    }

    /**
     * Updates the templates for the summary statistics.
     *
     * @param rawStatistics the raw statistics to use
     */

    private void updateStatisticsTemplates( Statistics rawStatistics )
    {
        rawStatistics.getScoresList()
                     .forEach( n -> this.doubleScoreTemplates.putIfAbsent( n.getMetric()
                                                                            .getName(),
                                                                           n.toBuilder() ) );
        rawStatistics.getDurationScoresList()
                     .forEach( n -> this.durationScoreTemplates.putIfAbsent( n.getMetric()
                                                                              .getName(),
                                                                             n.toBuilder() ) );
        rawStatistics.getDiagramsList()
                     .forEach( n -> this.diagramTemplates.putIfAbsent( n.getMetric()
                                                                        .getName(),
                                                                       n.toBuilder() ) );
        rawStatistics.getDurationDiagramsList()
                     .forEach( n -> this.durationDiagramTemplates.putIfAbsent( n.getMetric()
                                                                                .getName(),
                                                                               n.toBuilder() ) );
    }

    /**
     * Add the double score statistics to the internal store.
     * @param source the raw statistics
     */

    private void addDoubleScores( Statistics source )
    {
        List<DoubleScoreStatistic> scoresList = source.getScoresList();

        for ( DoubleScoreStatistic score : scoresList )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            for ( DoubleScoreStatistic.DoubleScoreStatisticComponent component : score.getStatisticsList() )
            {
                MetricNames name = new MetricNames( metricName,
                                                    component.getMetric()
                                                             .getName(),
                                                    null );

                MutableDoubleList samples = this.getOrAddDoubleScoreSlot( name );
                double scoreValue = component.getValue();
                samples.add( scoreValue );
            }
        }
    }

    /**
     * Adds a double score slot for the named metric, as needed, and returns the slot.
     * @param name the named metric
     * @return the slot
     */
    private MutableDoubleList getOrAddDoubleScoreSlot( MetricNames name )
    {
        // Add a thread-safe mutable list
        this.doubleScores.putIfAbsent( name, new DoubleArrayList().asSynchronized() );
        return this.doubleScores.get( name );
    }

    /**
     * Add the duration score statistics to the internal store.
     * @param source the raw statistics
     */

    private void addDurationScores( Statistics source )
    {
        List<DurationScoreStatistic> scoresList = source.getDurationScoresList();

        for ( DurationScoreStatistic score : scoresList )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            for ( DurationScoreStatistic.DurationScoreStatisticComponent component : score.getStatisticsList() )
            {
                MetricNames name = new MetricNames( metricName,
                                                    component.getMetric()
                                                             .getName(),
                                                    null );

                List<Duration> samples = this.getOrAddDurationScoreSlot( name );
                Duration nextScore = MessageUtilities.getDuration( component.getValue() );
                samples.add( nextScore );
            }
        }
    }

    /**
     * Adds a duration score slot for the named metric, as needed, and returns the slot.
     * @param name the named metric
     * @return the slot
     */
    private List<Duration> getOrAddDurationScoreSlot( MetricNames name )
    {
        // Add a thread-safe mutable list
        this.durationScores.putIfAbsent( name, new FastList<Duration>().asSynchronized() );
        return this.durationScores.get( name );
    }

    /**
     * Add the diagram statistics to the internal store.
     * @param source the raw statistics
     */

    private void addDiagrams( Statistics source )
    {
        List<DiagramStatistic> diagramsList = source.getDiagramsList();

        for ( DiagramStatistic diagram : diagramsList )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();
            for ( DiagramStatistic.DiagramStatisticComponent component : diagram.getStatisticsList() )
            {
                MetricNames name = new MetricNames( metricName,
                                                    component.getMetric()
                                                             .getName(),
                                                    component.getName() );
                List<MutableDoubleList> samples = this.getOrAddDiagramSlot( name );

                int valuesCount = component.getValuesCount();
                for ( int i = 0; i < valuesCount; i++ )
                {
                    MutableDoubleList row = this.getOrAddDiagramRowSlot( samples, i );
                    double value = component.getValues( i );
                    row.add( value );
                }
            }
        }
    }

    /**
     * Adds a diagram slot for the named metric, as needed, and returns the slot.
     * @param name the named metric
     * @return the slot
     */
    private List<MutableDoubleList> getOrAddDiagramSlot( MetricNames name )
    {
        // Add a thread-safe list (of lists)
        this.diagrams.putIfAbsent( name, new FastList<MutableDoubleList>().asSynchronized() );
        return this.diagrams.get( name );
    }

    /**
     * Adds a slot for a diagram row, as needed, and returns the slot.
     * @param slots the slots
     * @param slotIndex the slot index required
     * @return the diagram row slot
     */
    private MutableDoubleList getOrAddDiagramRowSlot( List<MutableDoubleList> slots, int slotIndex )
    {
        try
        {
            // Lock to ensure that the check on list size, creation and addition of a new list are all atomic. Even
            // though the outer list is a thread-safe variant, this entire sequence needs to be atomic otherwise
            // multiple threads could enter the "add list" step below (the multiple lists then being added atomically,
            // but duplicated)
            this.diagramRowSlotCreationLock.lock();

            if ( slotIndex < slots.size() )
            {
                return slots.get( slotIndex );
            }

            // Add a thread-safe list
            MutableDoubleList newSlot = new DoubleArrayList().asSynchronized();
            slots.add( newSlot );

            return newSlot;
        }
        finally
        {
            this.diagramRowSlotCreationLock.unlock();
        }
    }

    /**
     * Add the duration diagram statistics to the internal store.
     * @param source the raw statistics
     */

    private void addDurationDiagrams( Statistics source )
    {
        List<DurationDiagramStatistic> diagramsList = source.getDurationDiagramsList();

        for ( DurationDiagramStatistic diagram : diagramsList )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();

            Map<Instant, List<Duration>> samples = this.getOrAddDurationDiagramSlot( metricName );

            int valuesCount = diagram.getStatisticsCount();
            for ( int i = 0; i < valuesCount; i++ )
            {
                DurationDiagramStatistic.PairOfInstantAndDuration statistic = diagram.getStatistics( i );
                Instant instant = MessageUtilities.getInstant( statistic.getTime() );
                Duration duration = MessageUtilities.getDuration( statistic.getDuration() );

                // Create a thread safe list to add, if needed
                List<Duration> newList = new FastList<Duration>().asSynchronized();
                List<Duration> durations = samples.putIfAbsent( instant, newList );

                // Slot just created, get it
                if ( Objects.isNull( durations ) )
                {
                    durations = samples.get( instant );
                }

                durations.add( duration );
            }
        }
    }

    /**
     * Adds a duration diagram slot for the named metric, as needed, and returns the slot.
     * @param name the named metric
     * @return the slot
     */
    private Map<Instant, List<Duration>> getOrAddDurationDiagramSlot( MetricName name )
    {
        this.durationDiagrams.putIfAbsent( name, new ConcurrentHashMap<>() );
        return this.durationDiagrams.get( name );
    }

    /**
     * Calculates the summary statistics on request. Can be called only once, which is guarded by {@link #isComplete}.
     */

    private void calculateSummaryStatistics()
    {
        if ( Objects.isNull( this.nominal ) )
        {
            LOGGER.debug( "No statistics were added from which to calculate summary statistics." );
            return;
        }

        LOGGER.debug( "Calculating {} scalar summary statistics: {}.",
                      this.scalarStatistics.size(),
                      this.scalarStatistics );
        LOGGER.debug( "Calculating {} diagram summary statistics: {}.",
                      this.diagramStatistics.size(),
                      this.diagramStatistics );
        LOGGER.debug( "Calculating {} box plot summary statistics: {}.",
                      this.boxplotStatistics.size(),
                      this.boxplotStatistics );

        // Calculate the scalar summary statistics
        for ( ScalarSummaryStatisticFunction summaryStatistic : this.scalarStatistics )
        {
            Statistics.Builder builder = this.nominal.toBuilder()
                                                     .setSummaryStatistic( summaryStatistic.statistic() );

            // Set the summary statistic for the double scores, sorted by metric name
            List<DoubleScoreStatistic.Builder> scoreBuilders
                    = this.doubleScoreTemplates.values()
                                               .stream()
                                               .sorted( Comparator.comparing( a -> a.getMetric()
                                                                                    .getName() ) )
                                               .toList();
            scoreBuilders.forEach( builder::addScores );
            scoreBuilders = builder.getScoresBuilderList();
            this.calculateDoubleScoreSummaryStatistic( scoreBuilders, summaryStatistic );

            // Set the summary statistic for the duration scores, sorted by metric name
            List<DurationScoreStatistic.Builder> durationScoreBuilders =
                    this.durationScoreTemplates.values()
                                               .stream()
                                               .sorted( Comparator.comparing( a -> a.getMetric()
                                                                                    .getName() ) )
                                               .toList();
            durationScoreBuilders.forEach( builder::addDurationScores );
            durationScoreBuilders = builder.getDurationScoresBuilderList();
            this.calculateDurationScoreSummaryStatistics( durationScoreBuilders, summaryStatistic );

            // Set the summary statistic for the diagrams, sorted by metric name
            List<DiagramStatistic.Builder> diagramBuilders =
                    this.diagramTemplates.values()
                                         .stream()
                                         .sorted( Comparator.comparing( a -> a.getMetric()
                                                                              .getName() ) )
                                         .toList();
            diagramBuilders.forEach( builder::addDiagrams );
            diagramBuilders = builder.getDiagramsBuilderList();
            this.calculateDiagramSummaryStatistics( diagramBuilders, summaryStatistic );

            // Set the summary statistic for the duration diagrams, sorted by metric name
            List<DurationDiagramStatistic.Builder> durationDiagramBuilders =
                    this.durationDiagramTemplates.values()
                                                 .stream()
                                                 .sorted( Comparator.comparing( a -> a.getMetric()
                                                                                      .getName() ) )
                                                 .toList();
            durationDiagramBuilders.forEach( builder::addDurationDiagrams );
            durationDiagramBuilders = builder.getDurationDiagramsBuilderList();
            this.calculateDurationDiagramSummaryStatistics( durationDiagramBuilders, summaryStatistic );

            // Add the summary statistic
            this.statistics.add( builder.build() );
        }

        // Calculate the diagram summary statistics for scores
        for ( DiagramSummaryStatisticFunction diagramStatistic : this.diagramStatistics )
        {
            Statistics.Builder builder = this.nominal.toBuilder()
                                                     .setSummaryStatistic( diagramStatistic.statistic() );

            List<DiagramStatistic> diags = this.calculateDiagramStatisticForDoubleScores( diagramStatistic );
            builder.addAllDiagrams( diags );
            this.statistics.add( builder.build() );
        }

        // Calculate the box plot statistics for scores
        for ( BoxplotSummaryStatisticFunction boxplotStatistic : this.boxplotStatistics )
        {
            Statistics.Builder builder = this.nominal.toBuilder()
                                                     .setSummaryStatistic( boxplotStatistic.statistic() );

            List<BoxplotStatistic> boxes = this.calculateBoxplotStatisticForDoubleScores( boxplotStatistic );

            if ( !boxes.isEmpty() )
            {
                builder.addAllOneBoxPerPool( boxes );
                this.statistics.add( builder.build() );
            }
        }

        // Calculate the diagram summary statistics for duration scores
        for ( DiagramSummaryStatisticFunction diagramStatistic : this.diagramStatistics )
        {
            Statistics.Builder builder = this.nominal.toBuilder()
                                                     .setSummaryStatistic( diagramStatistic.statistic() );

            List<DiagramStatistic> diags = this.calculateDiagramStatisticForDurationScores( diagramStatistic );
            builder.addAllDiagrams( diags );
            this.statistics.add( builder.build() );
        }

        // Calculate the box plot statistics for duration scores
        for ( BoxplotSummaryStatisticFunction boxplotStatistic : this.boxplotStatistics )
        {
            Statistics.Builder builder = this.nominal.toBuilder()
                                                     .setSummaryStatistic( boxplotStatistic.statistic() );

            List<BoxplotStatistic> boxes = this.calculateBoxplotStatisticForDurationScores( boxplotStatistic );

            if ( !boxes.isEmpty() )
            {
                builder.addAllOneBoxPerPool( boxes );
                this.statistics.add( builder.build() );
            }
        }

        LOGGER.debug( "Finished setting the summary statistics." );
    }

    /**
     * Calculates a summary statistics for the double scores.
     * @param builders the builders to update
     * @param summaryStatistic the summary statistic to calculate
     */

    private void calculateDoubleScoreSummaryStatistic( List<DoubleScoreStatistic.Builder> builders,
                                                       ScalarSummaryStatisticFunction summaryStatistic )
    {
        for ( DoubleScoreStatistic.Builder score : builders )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            List<DoubleScoreStatistic.DoubleScoreStatisticComponent.Builder> componentBuilders =
                    score.getStatisticsBuilderList();
            for ( DoubleScoreStatistic.DoubleScoreStatisticComponent.Builder component : componentBuilders )
            {
                MetricNames name = new MetricNames( metricName,
                                                    component.getMetric()
                                                             .getName(),
                                                    null );
                MutableDoubleList samples = this.doubleScores.get( name );
                double[] raw = samples.toArray();
                double statisticValue = summaryStatistic.applyAsDouble( raw );
                component.setValue( statisticValue );
            }
        }
    }

    /**
     * Calculates the summary statistic for the duration scores.
     * @param builders the builders to update
     * @param summaryStatistic the summary statistic
     */

    private void calculateDurationScoreSummaryStatistics( List<DurationScoreStatistic.Builder> builders,
                                                          ScalarSummaryStatisticFunction summaryStatistic )
    {
        Function<Duration[], Duration> durationStatistic =
                FunctionFactory.ofDurationFromUnivariateFunction( summaryStatistic );

        for ( DurationScoreStatistic.Builder score : builders )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            List<DurationScoreStatistic.DurationScoreStatisticComponent.Builder> componentBuilders =
                    score.getStatisticsBuilderList();
            for ( DurationScoreStatistic.DurationScoreStatisticComponent.Builder component : componentBuilders )
            {
                MetricNames name = new MetricNames( metricName,
                                                    component.getMetric()
                                                             .getName(), null );
                List<Duration> samples = this.durationScores.get( name );

                // Filter any null values and sort. Null values can occur if the pairs were empty, for example,
                // and no duration errors were produced. Empty pairs can occur when slicing a realization by
                // threshold.
                samples = DURATION_FILTER.apply( samples );
                Duration[] durations = samples.toArray( new Duration[0] );

                Duration statisticValue = durationStatistic.apply( durations );
                com.google.protobuf.Duration statisticProto = MessageUtilities.getDuration( statisticValue );
                component.setValue( statisticProto );
            }
        }
    }

    /**
     * Calculates the summary statistic for diagrams.
     * @param builders the builders to update
     * @param summaryStatistic the summary statistic
     */

    private void calculateDiagramSummaryStatistics( List<DiagramStatistic.Builder> builders,
                                                    ScalarSummaryStatisticFunction summaryStatistic )
    {
        for ( DiagramStatistic.Builder diagram : builders )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();
            List<DiagramStatistic.DiagramStatisticComponent.Builder> componentBuilders =
                    diagram.getStatisticsBuilderList();
            for ( DiagramStatistic.DiagramStatisticComponent.Builder component : componentBuilders )
            {
                MetricNames name = new MetricNames( metricName,
                                                    component.getMetric()
                                                             .getName(),
                                                    component.getName() );
                List<MutableDoubleList> samples = this.diagrams.get( name );
                component.clearValues();

                for ( MutableDoubleList nextSamples : samples )
                {
                    double[] nextSampleArray = nextSamples.toArray();
                    double statisticValue = summaryStatistic.applyAsDouble( nextSampleArray );
                    component.addValues( statisticValue );
                }
            }
        }
    }

    /**
     * Sets the quantiles for the duration diagram statistics.
     * @param builders the builders to update
     * @param summaryStatistic the summary statistic
     */

    private void calculateDurationDiagramSummaryStatistics( List<DurationDiagramStatistic.Builder> builders,
                                                            ScalarSummaryStatisticFunction summaryStatistic )
    {
        Function<Duration[], Duration> durationStatistic =
                FunctionFactory.ofDurationFromUnivariateFunction( summaryStatistic );

        for ( DurationDiagramStatistic.Builder diagram : builders )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();

            Map<Instant, List<Duration>> samples = this.durationDiagrams.get( metricName );

            int componentCount = diagram.getStatisticsCount();

            for ( int i = 0; i < componentCount; i++ )
            {
                DurationDiagramStatistic.PairOfInstantAndDuration.Builder pair = diagram.getStatisticsBuilder( i );
                Instant instant = MessageUtilities.getInstant( pair.getTime() );
                List<Duration> sample = samples.get( instant );

                // Filter any null values and sort. Null values can occur if the pairs were empty, for example,
                // and no duration errors were produced. Empty pairs can occur when slicing a realization by
                // threshold.
                sample = DURATION_FILTER.apply( sample );

                Duration[] durations = sample.toArray( new Duration[0] );

                Duration statisticValue = durationStatistic.apply( durations );
                com.google.protobuf.Duration statisticProto = MessageUtilities.getDuration( statisticValue );
                pair.setDuration( statisticProto );
            }
        }
    }

    /**
     * Calculates the diagram statistic for all double scores.
     * @param diagram the diagram statistic
     * @return the diagram statistics, one for each double score
     */
    private List<DiagramStatistic> calculateDiagramStatisticForDoubleScores( DiagramSummaryStatisticFunction diagram )
    {
        List<DiagramStatistic> diagramList = new ArrayList<>();
        for ( Map.Entry<MetricNames, MutableDoubleList> nextScore : this.doubleScores.entrySet() )
        {
            MetricNames name = nextScore.getKey();
            String nameString = name.metricName()
                                    .toString();
            String componentNameString = name.componentName()
                                             .toString();
            MutableDoubleList scores = nextScore.getValue();
            double[] rawScores = scores.toArray();
            DoubleScoreStatistic.Builder b = this.doubleScoreTemplates.get( name.metricName() );

            String unitString = b.getStatistics( 0 )
                                 .getMetric()
                                 .getUnits();
            Map<SummaryStatisticComponentName, String> names =
                    Map.of( SummaryStatisticComponentName.METRIC_NAME, nameString,
                            SummaryStatisticComponentName.METRIC_COMPONENT_NAME, componentNameString,
                            SummaryStatisticComponentName.METRIC_UNIT, unitString );
            DiagramStatistic nextDiagram = diagram.apply( names, rawScores );
            diagramList.add( nextDiagram );
        }

        return Collections.unmodifiableList( diagramList );
    }

    /**
     * Calculates the diagram statistic for all duration scores.
     * @param diagram the diagram statistic
     * @return the diagram statistics, one for each duration score
     */
    private List<DiagramStatistic> calculateDiagramStatisticForDurationScores( DiagramSummaryStatisticFunction diagram )
    {
        List<DiagramStatistic> diagramList = new ArrayList<>();
        for ( Map.Entry<MetricNames, List<Duration>> nextScore : this.durationScores.entrySet() )
        {
            MetricNames name = nextScore.getKey();
            String nameString = name.metricName()
                                    .toString();
            String componentNameString = name.componentName()
                                             .toString();

            List<Duration> scores = nextScore.getValue();
            Duration[] rawScores = scores.toArray( new Duration[0] );
            Map<SummaryStatisticComponentName, String> names =
                    Map.of( SummaryStatisticComponentName.METRIC_NAME, nameString,
                            SummaryStatisticComponentName.METRIC_COMPONENT_NAME, componentNameString,
                            SummaryStatisticComponentName.METRIC_UNIT, this.timeUnit.name() );
            BiFunction<Map<SummaryStatisticComponentName, String>, Duration[], DiagramStatistic>
                    durationDiagram = FunctionFactory.ofDurationDiagramFromUnivariateFunction( diagram, this.timeUnit );
            DiagramStatistic nextDiagram = durationDiagram.apply( names, rawScores );
            diagramList.add( nextDiagram );
        }

        return Collections.unmodifiableList( diagramList );
    }

    /**
     * Calculates the box plot statistic for all double scores.
     * @param boxplot the box plot statistic
     * @return the box plot statistics, one for each double score
     */
    private List<BoxplotStatistic> calculateBoxplotStatisticForDoubleScores( BoxplotSummaryStatisticFunction boxplot )
    {
        List<BoxplotStatistic> boxplotList = new ArrayList<>();
        for ( Map.Entry<MetricNames, MutableDoubleList> nextScore : this.doubleScores.entrySet() )
        {
            MetricNames name = nextScore.getKey();
            String nameString = name.metricName()
                                    .toString();

            MutableDoubleList scores = nextScore.getValue();
            double[] rawScores = scores.toArray();
            DoubleScoreStatistic.Builder b = this.doubleScoreTemplates.get( name.metricName() );

            String unitString = b.getStatistics( 0 )
                                 .getMetric()
                                 .getUnits();
            Map<SummaryStatisticComponentName, String> names =
                    Map.of( SummaryStatisticComponentName.METRIC_NAME, nameString,
                            SummaryStatisticComponentName.METRIC_COMPONENT_NAME, name.componentName()
                                                                                     .name(),
                            SummaryStatisticComponentName.METRIC_UNIT, unitString );

            BoxplotStatistic nextBoxplot = boxplot.apply( names, rawScores );
            boxplotList.add( nextBoxplot );
        }

        return Collections.unmodifiableList( boxplotList );
    }

    /**
     * Calculates the box plot statistic for all duration scores.
     * @param boxplot the box plot statistic
     * @return the box plot statistics, one for each duration score
     */
    private List<BoxplotStatistic> calculateBoxplotStatisticForDurationScores( BoxplotSummaryStatisticFunction boxplot )
    {
        List<BoxplotStatistic> boxplotList = new ArrayList<>();
        for ( Map.Entry<MetricNames, List<Duration>> nextScore : this.durationScores.entrySet() )
        {
            MetricNames name = nextScore.getKey();
            String nameString = name.metricName()
                                    .toString();
            String metricComponentName = name.componentName()
                                             .toString();

            List<Duration> scores = nextScore.getValue();
            Duration[] rawScores = scores.toArray( new Duration[0] );
            double[] rawScoresDecimal = FunctionFactory.ofDecimalDurations( rawScores, this.timeUnit );

            String unitString = this.timeUnit.toString()
                                             .toUpperCase();
            Map<SummaryStatisticComponentName, String> names =
                    Map.of( SummaryStatisticComponentName.METRIC_NAME, nameString,
                            SummaryStatisticComponentName.METRIC_COMPONENT_NAME, metricComponentName,
                            SummaryStatisticComponentName.METRIC_UNIT, unitString );

            BoxplotStatistic nextBoxplot = boxplot.apply( names, rawScoresDecimal );
            boxplotList.add( nextBoxplot );
        }

        return Collections.unmodifiableList( boxplotList );
    }

    /**
     * Removes any statistics that do not support summary statistics.
     *
     * @param statistics the statistics to filter
     * @return the filtered statistics
     */

    private Statistics filter( Statistics statistics )
    {
        // Clear all statistics first, retaining the metadata
        Statistics.Builder builder = statistics.toBuilder()
                                               .clearScores()
                                               .clearDurationScores()
                                               .clearDiagrams()
                                               .clearDurationDiagrams()
                                               .clearOneBoxPerPair()
                                               .clearOneBoxPerPool();

        // Filter scores
        List<DoubleScoreStatistic> scoreList =
                statistics.getScoresList()
                          .stream()
                          .filter( n -> MetricConstants.valueOf( n.getMetric()
                                                                  .getName()
                                                                  .name() )
                                                       .isSamplingUncertaintyAllowed() )
                          .toList();
        builder.addAllScores( scoreList );

        // Filter duration scores
        List<DurationScoreStatistic> durationScoreList =
                statistics.getDurationScoresList()
                          .stream()
                          .filter( n -> MetricConstants.valueOf( n.getMetric()
                                                                  .getName()
                                                                  .name() )
                                                       .isSamplingUncertaintyAllowed() )
                          .toList();
        builder.addAllDurationScores( durationScoreList );

        // Filter diagrams
        List<DiagramStatistic> diagramList =
                statistics.getDiagramsList()
                          .stream()
                          .filter( n -> MetricConstants.valueOf( n.getMetric()
                                                                  .getName()
                                                                  .name() )
                                                       .isSamplingUncertaintyAllowed() )
                          .toList();
        builder.addAllDiagrams( diagramList );

        // Filter duration diagrams
        List<DurationDiagramStatistic> durationDiagramList =
                statistics.getDurationDiagramsList()
                          .stream()
                          .filter( n -> MetricConstants.valueOf( n.getMetric()
                                                                  .getName()
                                                                  .name() )
                                                       .isSamplingUncertaintyAllowed() )
                          .toList();
        builder.addAllDurationDiagrams( durationDiagramList );

        // Filter box plots per pool
        List<BoxplotStatistic> boxplotPerPoolList =
                statistics.getOneBoxPerPoolList()
                          .stream()
                          .filter( n -> MetricConstants.valueOf( n.getMetric()
                                                                  .getName()
                                                                  .name() )
                                                       .isSamplingUncertaintyAllowed() )
                          .toList();
        builder.addAllOneBoxPerPool( boxplotPerPoolList );

        // Filter box plots per pair
        List<BoxplotStatistic> boxplotPerPairList =
                statistics.getOneBoxPerPairList()
                          .stream()
                          .filter( n -> MetricConstants.valueOf( n.getMetric()
                                                                  .getName()
                                                                  .name() )
                                                       .isSamplingUncertaintyAllowed() )
                          .toList();
        builder.addAllOneBoxPerPair( boxplotPerPairList );

        return builder.build();
    }

    /**
     * Creates an instance.
     * @param scalarStatistics the scalar summary statistics to calculate
     * @param diagramStatistics the diagram summary statistics to calculate
     * @param boxplotStatistics the box plot summary statistics to calculate
     * @param filter an optional filter
     * @param metadataAggregator a transformer that adapts the statistics metadata to reflect the summary performed
     * @param timeUnits the optional time units to use for duration statistics
     * @throws IllegalArgumentException if all lists of statistics are null or empty
     * @throws NullPointerException if the metadata transformer is null
     */
    private SummaryStatisticsCalculator( Set<ScalarSummaryStatisticFunction> scalarStatistics,
                                         Set<DiagramSummaryStatisticFunction> diagramStatistics,
                                         Set<BoxplotSummaryStatisticFunction> boxplotStatistics,
                                         Predicate<Statistics> filter,
                                         BinaryOperator<Statistics> metadataAggregator,
                                         ChronoUnit timeUnits )
    {
        Objects.requireNonNull( metadataAggregator );

        // Replace null with empty lists
        if ( Objects.isNull( scalarStatistics ) )
        {
            scalarStatistics = Set.of();
        }

        if ( Objects.isNull( diagramStatistics ) )
        {
            diagramStatistics = Set.of();
        }

        if ( Objects.isNull( boxplotStatistics ) )
        {
            boxplotStatistics = Set.of();
        }

        // No statistics to compute?
        if ( scalarStatistics.isEmpty()
             && diagramStatistics.isEmpty()
             && boxplotStatistics.isEmpty() )
        {
            throw new IllegalArgumentException( "Provide one or more summary statistics to calculate." );
        }

        // Set the time unit for duration statistics
        if ( Objects.nonNull( timeUnits ) )
        {
            this.timeUnit = timeUnits;
        }
        else
        {
            this.timeUnit = ChronoUnit.HOURS;
        }

        if ( Objects.isNull( filter ) )
        {
            this.filter = in -> true;
        }
        else
        {
            this.filter = filter;
        }

        this.metadataAggregator = metadataAggregator;
        this.scalarStatistics = scalarStatistics;
        this.diagramStatistics = diagramStatistics;
        this.boxplotStatistics = boxplotStatistics;
        this.isComplete = new AtomicBoolean();
        this.statistics = new ArrayList<>();

        // Create the containers for the raw statistics
        this.doubleScores = new ConcurrentHashMap<>();
        this.diagrams = new ConcurrentHashMap<>();
        this.durationScores = new ConcurrentHashMap<>();
        this.durationDiagrams = new ConcurrentHashMap<>();

        // Create the containers for the summary statistic templates
        this.doubleScoreTemplates = new ConcurrentHashMap<>();
        this.diagramTemplates = new ConcurrentHashMap<>();
        this.durationScoreTemplates = new ConcurrentHashMap<>();
        this.durationDiagramTemplates = new ConcurrentHashMap<>();
    }

    /**
     * The fully qualified name of a score whose sample quantiles must be estimated.
     * @param metricName the metric name
     * @param componentName the metric component name
     * @param qualifier the qualifier to use when the same component name is repeated
     */
    private record MetricNames( MetricName metricName,
                                MetricName componentName,
                                String qualifier ) {}
}
