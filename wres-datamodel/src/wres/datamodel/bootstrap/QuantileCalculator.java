package wres.datamodel.bootstrap;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.datamodel.MissingValues;
import wres.datamodel.Slicer;
import wres.statistics.MessageFactory;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Statistics;

/**
 * Accepts sample quantiles as they are computed and, once all expected samples have been received, calculates the
 * quantiles of the sampling distribution that were requested on construction. Once calculated, the object becomes
 * "read only" so that no further samples can be added and {@link #get()} returns the calculated quantiles.
 *
 * @author James Brown
 */
@ThreadSafe
public class QuantileCalculator implements Supplier<List<Statistics>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( QuantileCalculator.class );

    /** Filters durations. */
    private static final UnaryOperator<Duration[]> DURATION_FILTER = durations -> Arrays.stream( durations )
                                                                                        .filter( Objects::nonNull )
                                                                                        .toArray( Duration[]::new );

    /** The cached samples of score statistics. */
    private final Map<DoubleScoreName, double[]> doubleScores;

    /** The cached sample of diagram statistics with each column containing one sample and each row containing all
     * samples for one index of the diagram component. */
    private final Map<DiagramName, double[][]> diagrams;

    /** The cached sample of duration score statistics. */
    private final Map<DurationScoreName, Duration[]> durationScores;

    /** The cached sample of duration diagram statistics. */
    private final Map<MetricName, Map<Instant, Duration[]>> durationDiagrams;

    /** The nominal values of the statistics. */
    private final Statistics nominal;

    /** The sample count. */
    private final int sampleCount;

    /** The current sample index for which statistics have been supplied, but not necessarily finalized. */
    private final AtomicInteger sampleIndexStarted;

    /** The current sample index for which statistics have been supplied and finalized. */
    private final AtomicInteger sampleIndexCompleted;

    /** Whether this instance has been completed and is read-only. */
    private final AtomicBoolean hasQuantiles;

    /** The probabilities corresponding to the required quantile values. */
    private final SortedSet<Double> probabilities;

    /** The quantiles, which cannot be read unless {@link #hasQuantiles} is {@code true}. */
    private final List<Statistics> quantiles;

    /**
     * Creates an instance.
     * @param nominal the nominal values of the statistics
     * @param sampleCount the expected sample count
     * @param probabilities the probabilities for the quantile values
     * @param addNominal is true to add the nominal sample to the store, false otherwise
     * @return an instance
     * @throws NullPointerException if the statistics or quantileValues are null
     * @throws IllegalArgumentException if the sampleCount is less than 1 or the quantileValues are invalid
     */
    public static QuantileCalculator of( Statistics nominal,
                                         int sampleCount,
                                         SortedSet<Double> probabilities,
                                         boolean addNominal )
    {
        return new QuantileCalculator( nominal, sampleCount, probabilities, addNominal );
    }

    /**
     * Returns a {@link Statistics} for each quantile value requested on construction. This cannot be requested until
     * all expected samples have been received and the quantiles calculated.
     *
     * @return the quantiles
     * @throws IllegalArgumentException if the quantiles have not been calculated
     */
    @Override
    public List<Statistics> get()
    {
        if ( !this.hasQuantiles.get() )
        {
            throw new IllegalArgumentException( "The sample quantiles have not yet been calculated. Only "
                                                + this.sampleIndexCompleted.get()
                                                + " of the expected "
                                                + this.sampleCount
                                                + " samples have been registered with this quantile calculator, "
                                                + this
                                                + "." );
        }

        return Collections.unmodifiableList( this.quantiles );
    }

    /**
     * Adds a sample statistic to the internal store.
     * @param statistics the sample
     * @throws NullPointerException if the statistics is null
     * @throws IllegalArgumentException if this instance has been completed and no further statistics are expected
     */

    public void add( Statistics statistics )
    {
        Objects.requireNonNull( statistics );

        int index = this.sampleIndexStarted.getAndIncrement();

        if ( index + 1 > this.sampleCount )
        {
            throw new IllegalArgumentException( "Already received the expected number of samples from which to compute "
                                                + "quantiles and cannot accept any more: "
                                                + this.sampleCount
                                                + "." );
        }

        // Add the double score statistics
        this.addDoubleScores( statistics.getScoresList(), index );
        // Add the duration score statistics
        this.addDurationScores( statistics.getDurationScoresList(), index );
        // Add the diagrams
        this.addDiagrams( statistics.getDiagramsList(), index );
        // Add the duration diagrams
        this.addDurationDiagrams( statistics.getDurationDiagramsList(), index );

        LOGGER.debug( "Added sample statistics for index {} of {} in thread {}.",
                      index + 1,
                      this.sampleCount,
                      Thread.currentThread()
                            .getName() );

        // If multiple threads are incrementing statistics, only the last one should succeed in setting the quantiles
        if ( this.sampleIndexCompleted.getAndIncrement() == this.sampleCount - 1 )
        {
            this.setQuantiles();
        }
    }

    /**
     * Add the double score statistics at the prescribed index.
     * @param doubleScores the double scores
     * @param index the index
     */

    private void addDoubleScores( List<DoubleScoreStatistic> doubleScores, int index )
    {
        for ( DoubleScoreStatistic score : doubleScores )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            for ( DoubleScoreStatistic.DoubleScoreStatisticComponent component : score.getStatisticsList() )
            {
                DoubleScoreName name = new DoubleScoreName( metricName,
                                                            component.getMetric()
                                                                     .getName() );
                double[] samples = this.doubleScores.get( name );
                // Check the slot exists
                this.validateSlot( samples, name );
                samples[index] = component.getValue();
            }
        }
    }

    /**
     * Add the duration score statistics at the prescribed index.
     * @param durationScores the duration scores
     * @param index the index
     */

    private void addDurationScores( List<DurationScoreStatistic> durationScores, int index )
    {
        for ( DurationScoreStatistic score : durationScores )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            for ( DurationScoreStatistic.DurationScoreStatisticComponent component : score.getStatisticsList() )
            {
                DurationScoreName name = new DurationScoreName( metricName,
                                                                component.getMetric()
                                                                         .getName() );

                Duration[] samples = this.durationScores.get( name );
                // Check the slot exists
                this.validateSlot( samples, name );
                samples[index] = MessageFactory.parse( component.getValue() );
            }
        }
    }

    /**
     * Add the double score statistics at the prescribed index.
     * @param diagrams the double scores
     * @param index the index
     */

    private void addDiagrams( List<DiagramStatistic> diagrams, int index )
    {
        for ( DiagramStatistic diagram : diagrams )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();
            for ( DiagramStatistic.DiagramStatisticComponent component : diagram.getStatisticsList() )
            {
                DiagramName name = new DiagramName( metricName,
                                                    component.getMetric()
                                                             .getName(),
                                                    component.getName() );
                double[][] samples = this.diagrams.get( name );
                // Check the slot exists
                this.validateSlot( samples, name );
                int valuesCount = component.getValuesCount();
                for ( int i = 0; i < valuesCount; i++ )
                {
                    samples[i][index] = component.getValues( i );
                }
            }
        }
    }

    /**
     * Add the duration diagram statistics at the prescribed index.
     * @param diagrams the duraiton diagrams
     * @param index the index
     */

    private void addDurationDiagrams( List<DurationDiagramStatistic> diagrams, int index )
    {
        for ( DurationDiagramStatistic diagram : diagrams )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();

            Map<Instant, Duration[]> samples = this.durationDiagrams.get( metricName );
            // Check the slot exists
            this.validateSlot( samples, metricName );
            int valuesCount = diagram.getStatisticsCount();
            for ( int i = 0; i < valuesCount; i++ )
            {
                DurationDiagramStatistic.PairOfInstantAndDuration statistic = diagram.getStatistics( i );
                Instant instant = MessageFactory.parse( statistic.getTime() );
                Duration duration = MessageFactory.parse( statistic.getDuration() );
                Duration[] durations = samples.get( instant );

                // Slot doesn't exist, but this is a special case where mutation is allowed because the reference times
                // present in duration diagrams can vary with each resample, depending on how thresholds slice their
                // corresponding time-series
                if ( Objects.isNull( durations ) )
                {
                    durations = new Duration[this.sampleCount];
                    samples.put( instant, durations );
                }

                durations[index] = duration;
            }
        }
    }

    /**
     * Throws an exception in encountering an empty slot for a statistic received at runtime.
     * @param slot the slot data to check for nullity
     * @param name the name of the statistic
     */
    private void validateSlot( Object slot, Object name )
    {
        if ( Objects.isNull( slot ) )
        {
            throw new ResamplingException( "Encountered an internal error when conducting quantile "
                                           + "estimation. Could not find a slot in the quantile calculator "
                                           + "for a supplied statistic: "
                                           + name
                                           + ". This probably occurred because the quantile calculator was not "
                                           + "instantiated with all required statistics." );
        }
    }

    /**
     * Calculates the quantiles once all expected samples are available.
     */

    private void setQuantiles()
    {
        boolean sort = true;
        for ( double nextQuantile : this.probabilities )
        {
            Statistics.Builder builder = this.nominal.toBuilder();
            builder.setSampleQuantile( nextQuantile );

            // Set the quantile for the double scores
            List<DoubleScoreStatistic.Builder> scoreBuilders = builder.getScoresBuilderList();
            this.setDoubleScoreQuantiles( scoreBuilders, nextQuantile, sort );

            // Set the quantiles for the duration scores
            List<DurationScoreStatistic.Builder> durationScoreBuilders = builder.getDurationScoresBuilderList();
            this.setDurationScoreQuantiles( durationScoreBuilders, nextQuantile, sort );

            // Set the quantiles for the diagrams
            List<DiagramStatistic.Builder> diagramBuilders = builder.getDiagramsBuilderList();
            this.setDiagramQuantiles( diagramBuilders, nextQuantile, sort );

            // Set the quantiles for the diagrams
            List<DurationDiagramStatistic.Builder> durationDiagramBuilders = builder.getDurationDiagramsBuilderList();
            this.setDurationDiagramQuantiles( durationDiagramBuilders, nextQuantile, sort );

            // Add the quantile
            this.quantiles.add( builder.build() );

            // Sort on the first quantile only
            sort = false;
        }

        // Finally, indicate that the quantiles are set
        this.hasQuantiles.set( true );

        LOGGER.debug( "Finished setting the quantiles for the quantile calculator. This calculator is now read only." );
    }

    /**
     * Sets the quantiles for the double scores.
     * @param builders the builders to update
     * @param probability the probability associated with the quantile
     * @param sort whether to sort the samples
     */

    private void setDoubleScoreQuantiles( List<DoubleScoreStatistic.Builder> builders,
                                          double probability,
                                          boolean sort )
    {
        for ( DoubleScoreStatistic.Builder score : builders )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            List<DoubleScoreStatistic.DoubleScoreStatisticComponent.Builder> componentBuilders =
                    score.getStatisticsBuilderList();
            for ( DoubleScoreStatistic.DoubleScoreStatisticComponent.Builder component : componentBuilders )
            {
                DoubleScoreName name = new DoubleScoreName( metricName,
                                                            component.getMetric()
                                                                     .getName() );
                double[] samples = this.doubleScores.get( name );

                // Sort
                if ( sort )
                {
                    Arrays.sort( samples );
                }

                DoubleUnaryOperator quantileFunction = Slicer.getQuantileFunction( samples );
                double quantile = quantileFunction.applyAsDouble( probability );
                component.setValue( quantile );
            }
        }
    }

    /**
     * Sets the quantiles for the duration scores.
     * @param builders the builders to update
     * @param probability the probability associated with the quantile
     * @param sort whether to sort the samples
     */

    private void setDurationScoreQuantiles( List<DurationScoreStatistic.Builder> builders,
                                            double probability,
                                            boolean sort )
    {
        for ( DurationScoreStatistic.Builder score : builders )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            List<DurationScoreStatistic.DurationScoreStatisticComponent.Builder> componentBuilders =
                    score.getStatisticsBuilderList();
            for ( DurationScoreStatistic.DurationScoreStatisticComponent.Builder component : componentBuilders )
            {
                DurationScoreName name = new DurationScoreName( metricName,
                                                                component.getMetric()
                                                                         .getName() );
                Duration[] samples = this.durationScores.get( name );

                // Filter any null values and sort. Null values can occur if the pairs were empty, for example,
                // and no duration errors were produced. Empty pairs can occur when slicing a realization by
                // threshold.
                samples = DURATION_FILTER.apply( samples );

                // Sort
                if ( sort )
                {
                    Arrays.sort( samples );
                }

                Duration quantile = this.getDurationQuantile( samples, probability );
                com.google.protobuf.Duration quantileProto = MessageFactory.parse( quantile );
                component.setValue( quantileProto );
            }
        }
    }

    /**
     * Sets the quantiles for the double scores.
     * @param builders the builders to update
     * @param probability the probability associated with the quantile
     * @param sort whether to sort the samples
     */

    private void setDiagramQuantiles( List<DiagramStatistic.Builder> builders,
                                      double probability,
                                      boolean sort )
    {
        for ( DiagramStatistic.Builder diagram : builders )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();
            List<DiagramStatistic.DiagramStatisticComponent.Builder> componentBuilders =
                    diagram.getStatisticsBuilderList();
            for ( DiagramStatistic.DiagramStatisticComponent.Builder component : componentBuilders )
            {
                DiagramName name = new DiagramName( metricName,
                                                    component.getMetric()
                                                             .getName(),
                                                    component.getName() );
                double[][] samples = this.diagrams.get( name );

                // Sort
                if ( sort )
                {
                    for ( double[] sample : samples )
                    {
                        Arrays.sort( sample );
                    }
                }

                int componentCount = samples.length;

                for ( int i = 0; i < componentCount; i++ )
                {
                    DoubleUnaryOperator quantileFunction = Slicer.getQuantileFunction( samples[i] );
                    double quantile = quantileFunction.applyAsDouble( probability );
                    component.setValues( i, quantile );
                }
            }
        }
    }

    /**
     * Sets the quantiles for the duration diagram statistics.
     * @param builders the builders to update
     * @param probability the probability associated with the quantile
     * @param sort whether to sort the samples
     */

    private void setDurationDiagramQuantiles( List<DurationDiagramStatistic.Builder> builders,
                                              double probability,
                                              boolean sort )
    {
        for ( DurationDiagramStatistic.Builder diagram : builders )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();

            Map<Instant, Duration[]> samples = this.durationDiagrams.get( metricName );

            int componentCount = diagram.getStatisticsCount();

            for ( int i = 0; i < componentCount; i++ )
            {
                DurationDiagramStatistic.PairOfInstantAndDuration.Builder pair = diagram.getStatisticsBuilder( i );
                Instant instant = MessageFactory.parse( pair.getTime() );
                Duration[] sample = samples.get( instant );

                // Filter any null values and sort. Null values can occur if the pairs were empty, for example,
                // and no duration errors were produced. Empty pairs can occur when slicing a realization by
                // threshold.
                sample = DURATION_FILTER.apply( sample );

                // Sort
                if ( sort )
                {
                    Arrays.sort( sample );
                }

                Duration quantile = this.getDurationQuantile( sample, probability );
                com.google.protobuf.Duration quantileProto = MessageFactory.parse( quantile );
                pair.setDuration( quantileProto );
            }
        }
    }

    /**
     * Calculates the prescribed duration quantile from the sorted input and quantile value.
     *
     * @param sortedDurations the sorted durations
     * @param probability the probability associated with the quantile
     * @return the estimated quantile
     */

    private Duration getDurationQuantile( Duration[] sortedDurations, double probability )
    {
        // Single item
        if ( sortedDurations.length == 1 )
        {
            return sortedDurations[0];
        }

        // Estimate the position
        double pos = probability * ( sortedDurations.length + 1.0 );
        // Lower bound
        if ( pos < 1.0 )
        {
            return sortedDurations[0];
        }
        // Upper bound
        else if ( pos >= sortedDurations.length )
        {
            return sortedDurations[sortedDurations.length - 1];
        }
        // Contained: use linear interpolation in seconds
        else
        {
            double floorPos = Math.floor( pos );
            int intPos = ( int ) floorPos;
            Duration lower = sortedDurations[intPos - 1];
            Duration upper = sortedDurations[intPos];

            BigDecimal lowerDecimal = BigDecimal.valueOf( lower.getSeconds() );
            BigDecimal upperDecimal = BigDecimal.valueOf( upper.getSeconds() );

            BigDecimal difDecimal = BigDecimal.valueOf( pos )
                                              .subtract( BigDecimal.valueOf( floorPos ) );
            BigDecimal result = lowerDecimal.add( difDecimal.multiply( upperDecimal.subtract( lowerDecimal ) ) );

            long seconds = result.longValue();

            return Duration.ofSeconds( seconds );
        }
    }

    /**
     * Creates an instance.
     * @param nominal the nominal values of the statistics
     * @param sampleCount the expected sample count
     * @param probabilities the probabilities for the quantile values
     * @param addNominal is true to add the nominal sample to the store, false otherwise
     * @throws NullPointerException if the statistics or quantileValues are null
     * @throws IllegalArgumentException if the sampleCount is less than 1 or the quantileValues are invalid
     */
    private QuantileCalculator( Statistics nominal,
                                int sampleCount,
                                SortedSet<Double> probabilities,
                                boolean addNominal )
    {
        Objects.requireNonNull( nominal );
        Objects.requireNonNull( probabilities );

        // Valid sample count?
        if ( sampleCount < 1 )
        {
            throw new IllegalArgumentException( "At least one sample is required to compute quantiles: "
                                                + sampleCount
                                                + "." );
        }

        // Some quantiles?
        if ( probabilities.isEmpty() )
        {
            throw new IllegalArgumentException( "Expected at least one quantile value." );
        }

        // Quantile values are valid?
        if ( probabilities.stream()
                          .anyMatch( n -> n <= 0 || n >= 1 ) )
        {
            throw new IllegalArgumentException( "One of more of the supplied quantiles is invalid. The quantiles must "
                                                + "be greater than zero and less than one: "
                                                + probabilities
                                                + "." );
        }

        // One more sample if the nominal statistics should be added
        if ( addNominal )
        {
            sampleCount = sampleCount + 1;
        }

        // Filter the nominal statistics for statistics that support sampling uncertainty estimation
        this.nominal = this.filter( nominal );
        this.sampleCount = sampleCount;
        this.probabilities = Collections.unmodifiableSortedSet( probabilities );
        this.sampleIndexStarted = new AtomicInteger();
        this.sampleIndexCompleted = new AtomicInteger();
        this.hasQuantiles = new AtomicBoolean();
        this.quantiles = new ArrayList<>();

        // Create the slots for the sample statistics
        this.doubleScores = this.createDoubleScoreSlots( this.nominal.getScoresList(),
                                                         this.sampleCount );
        this.diagrams = this.createDiagramSlots( this.nominal.getDiagramsList(),
                                                 this.sampleCount );
        this.durationScores = this.createDurationScoreSlots( this.nominal.getDurationScoresList(),
                                                             this.sampleCount );
        this.durationDiagrams = this.createDurationDiagramSlots( this.nominal.getDurationDiagramsList(),
                                                                 this.sampleCount );

        if ( LOGGER.isDebugEnabled() )
        {
            Set<Object> names = new HashSet<>( this.doubleScores.keySet() );
            names.addAll( this.diagrams.keySet() );
            names.addAll( this.durationScores.keySet() );
            names.addAll( this.durationDiagrams.keySet() );
            LOGGER.debug( "Created a quantile calculator with a sample count of {}, probabilities of {} and slots for "
                          + "the following metrics: {}.",
                          sampleCount,
                          probabilities,
                          names );
        }

        // Add the nominal statistics
        if ( addNominal )
        {
            this.add( nominal );
        }
    }

    /**
     * Removes any statistics that do not support sampling uncertainty estimation.
     *
     * @param statistics the statistics to filter
     * @return the filtered statistics
     */

    private Statistics filter( Statistics statistics )
    {
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
     * Creates the slots for the double scores.
     * @param scores the nominal double score statistics
     * @param sampleCount the sample count
     * @return the double score samples, to fill
     */

    private Map<DoubleScoreName, double[]> createDoubleScoreSlots( List<DoubleScoreStatistic> scores,
                                                                   int sampleCount )
    {
        Map<DoubleScoreName, double[]> slots = new HashMap<>();
        for ( DoubleScoreStatistic score : scores )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            MetricConstants nextMetric = MetricConstants.valueOf( metricName.name() );

            // Supports sampling uncertainty?
            if ( nextMetric.isSamplingUncertaintyAllowed() )
            {
                List<DoubleScoreStatistic.DoubleScoreStatisticComponent> components = score.getStatisticsList();
                for ( DoubleScoreStatistic.DoubleScoreStatisticComponent component : components )
                {
                    DoubleScoreName name = new DoubleScoreName( metricName,
                                                                component.getMetric()
                                                                         .getName() );
                    double[] placeholders = new double[sampleCount];

                    // Fill with missings
                    Arrays.fill( placeholders, MissingValues.DOUBLE );

                    slots.put( name, placeholders );
                }
            }
        }

        return Collections.unmodifiableMap( slots );
    }

    /**
     * Creates the slots for the diagrams.
     * @param diagramStatistics the nominal diagram statistics
     * @param sampleCount the sample count
     * @return the diagram samples, to fill
     */

    private Map<DiagramName, double[][]> createDiagramSlots( List<DiagramStatistic> diagramStatistics,
                                                             int sampleCount )
    {
        Map<DiagramName, double[][]> slots = new HashMap<>();
        for ( DiagramStatistic diagram : diagramStatistics )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();
            MetricConstants nextMetric = MetricConstants.valueOf( metricName.name() );

            // Supports sampling uncertainty?
            if ( nextMetric.isSamplingUncertaintyAllowed() )
            {
                List<DiagramStatistic.DiagramStatisticComponent> components = diagram.getStatisticsList();
                for ( DiagramStatistic.DiagramStatisticComponent component : components )
                {
                    DiagramName name = new DiagramName( metricName,
                                                        component.getMetric()
                                                                 .getName(),
                                                        component.getName() );

                    int valueCount = component.getValuesCount();
                    double[][] placeholders = new double[valueCount][sampleCount];

                    // Fill with missings
                    for ( double[] next : placeholders )
                    {
                        Arrays.fill( next, MissingValues.DOUBLE );
                    }

                    slots.put( name, placeholders );
                }
            }
        }

        return Collections.unmodifiableMap( slots );
    }

    /**
     * Creates the slots for the duration scores.
     * @param scores the nominal duration score statistics
     * @param sampleCount the sample count
     * @return the empty duration score samples, to fill
     */

    private Map<DurationScoreName, Duration[]> createDurationScoreSlots( List<DurationScoreStatistic> scores,
                                                                         int sampleCount )
    {
        Map<DurationScoreName, Duration[]> slots = new HashMap<>();
        for ( DurationScoreStatistic score : scores )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            MetricConstants nextMetric = MetricConstants.valueOf( metricName.name() );

            // Supports sampling uncertainty?
            if ( nextMetric.isSamplingUncertaintyAllowed() )
            {
                List<DurationScoreStatistic.DurationScoreStatisticComponent> components = score.getStatisticsList();
                for ( DurationScoreStatistic.DurationScoreStatisticComponent component : components )
                {
                    DurationScoreName name = new DurationScoreName( metricName,
                                                                    component.getMetric()
                                                                             .getName() );
                    Duration[] placeholders = new Duration[sampleCount];
                    slots.put( name, placeholders );
                }
            }
        }

        return Collections.unmodifiableMap( slots );
    }

    /**
     * Creates the slots for the duration diagrams.
     * @param diagramStatistics the nominal duration diagram statistics
     * @param sampleCount the sample count
     * @return the duration diagram samples, to fill
     */

    private Map<MetricName, Map<Instant, Duration[]>> createDurationDiagramSlots( List<DurationDiagramStatistic> diagramStatistics,
                                                                                  int sampleCount )
    {
        Map<MetricName, Map<Instant, Duration[]>> slots = new EnumMap<>( MetricName.class );
        for ( DurationDiagramStatistic diagram : diagramStatistics )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();
            MetricConstants nextMetric = MetricConstants.valueOf( metricName.name() );

            // Supports sampling uncertainty?
            if ( nextMetric.isSamplingUncertaintyAllowed() )
            {
                List<DurationDiagramStatistic.PairOfInstantAndDuration> values = diagram.getStatisticsList();
                Map<Instant, Duration[]> pairs = new ConcurrentHashMap<>();
                for ( DurationDiagramStatistic.PairOfInstantAndDuration pair : values )
                {
                    Instant instant = MessageFactory.parse( pair.getTime() );
                    Duration[] durations = new Duration[sampleCount];
                    pairs.put( instant, durations );
                }

                // Must remain mutable because structure is not fixed at instantiation time
                slots.put( metricName, pairs );
            }
        }

        return Collections.unmodifiableMap( slots );
    }

    /**
     * The fully qualified name of a score whose sample quantiles must be estimated.
     * @param metricName the metric name
     * @param metricComponentName the metric component name
     */
    private record DoubleScoreName( MetricName metricName,
                                    DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName metricComponentName ) {}

    /**
     * The fully qualified name of a diagram whose sample quantiles must be estimated.
     * @param metricName the metric name
     * @param metricComponentName the metric component name
     * @param qualifier the qualifier to use when the same component name is repeated
     */
    private record DiagramName( MetricName metricName,
                                DiagramMetric.DiagramMetricComponent.DiagramComponentName metricComponentName,
                                String qualifier ) {}

    /**
     * The fully qualified name of a duration score whose sample quantiles must be estimated.
     * @param metricName the metric name
     * @param metricComponentName the metric component name
     */
    private record DurationScoreName( MetricName metricName,
                                      DurationScoreMetric.DurationScoreMetricComponent.ComponentName metricComponentName ) {}

}