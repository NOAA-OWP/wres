package wres.io.pooling;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DatasourceType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesCrossPairer;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.project.Project;
import wres.io.retrieval.CachingRetriever;
import wres.io.retrieval.DataAccessException;
import wres.io.retrieval.RetrieverFactory;

/**
 * Generates a collection of {@link PoolSupplier} that contain the pools for a particular evaluation.
 * 
 * @author James Brown
 */

public class PoolsGenerator<L, R> implements Supplier<List<Supplier<Pool<TimeSeries<Pair<L, R>>>>>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( PoolsGenerator.class );

    /** The project for which pools are required. */
    private final Project project;

    /** The pool requests. */
    private final List<PoolRequest> poolRequests;

    /** A factory to create project-relevant retrievers. */
    private final RetrieverFactory<L, R> retrieverFactory;

    /** The upscaler for left-ish values. */
    private final TimeSeriesUpscaler<L> leftUpscaler;

    /** The upscaler for right-ish values. */
    private final TimeSeriesUpscaler<R> rightUpscaler;

    /** The pairer, which admits finite value pairs. */
    private final TimeSeriesPairer<L, R> pairer;

    /** An optional cross-pairer to use common pairs (by time) for the main and baseline pairs. */
    private final TimeSeriesCrossPairer<L, R> crossPairer;

    /** A transformer for left-ish values. */
    private final UnaryOperator<TimeSeries<L>> leftTransformer;

    /** A transformer of right-ish values that can take into account the event as a whole. */
    private final UnaryOperator<TimeSeries<R>> rightTransformer;

    /** A transformer of baseline-ish values that can take into account the event as a whole. */
    private final UnaryOperator<TimeSeries<R>> baselineTransformer;

    /** A function that filters left-ish time-series. */
    private final Predicate<TimeSeries<L>> leftFilter;

    /** A function that filters right-ish time-series. */
    private final Predicate<TimeSeries<R>> rightFilter;

    /** A function that filters baseline-ish time-series. */
    private final Predicate<TimeSeries<R>> baselineFilter;

    /** A mapper to map between left-ish climate values and double values. TODO: propagate left-ish data for 
     * climatology, rather than transforming it upfront. */
    private final ToDoubleFunction<L> climateMapper;

    /** The admissible values for the left-ish data associated with climatology. */
    private final Predicate<L> climateAdmissibleValue;

    /** An optional generator for baseline data (e.g., persistence or climatology). */
    private final Function<Set<FeatureKey>, UnaryOperator<TimeSeries<R>>> baselineGenerator;

    /** The pool suppliers. */
    private final List<Supplier<Pool<TimeSeries<Pair<L, R>>>>> pools;

    @Override
    public List<Supplier<Pool<TimeSeries<Pair<L, R>>>>> get()
    {
        return this.pools;
    }

    /**
     * A builder for the {@link PoolsGenerator}.
     */

    static class Builder<L, R>
    {
        /** The project for which pools are required. */
        private Project project;

        /** The pool requests. */
        private List<PoolRequest> poolRequests = new ArrayList<>();

        /** A factory to create project-relevant retrievers. */
        private RetrieverFactory<L, R> retrieverFactory;

        /**  A function to support pairing of left and right data. */
        private TimeSeriesPairer<L, R> pairer;

        /** An optional cross-pairer to use common pairs (by time) for the main and baseline pairs. */
        private TimeSeriesCrossPairer<L, R> crossPairer;

        /** A function to upscale left data. */
        private TimeSeriesUpscaler<L> leftUpscaler;

        /** A function to upscale right data. */
        private TimeSeriesUpscaler<R> rightUpscaler;

        /** A transformer that applies value constraints to left-ish values. */
        private UnaryOperator<TimeSeries<L>> leftTransformer = in -> in;

        /** A transformer for right-ish values that can take into account the encapsulating event. */
        private UnaryOperator<TimeSeries<R>> rightTransformer = in -> in;

        /** A transformer for baseline-ish values that can take into account the encapsulating event. */
        private UnaryOperator<TimeSeries<R>> baselineTransformer = in -> in;

        /** A function that filters left-ish time-series. */
        private Predicate<TimeSeries<L>> leftFilter = in -> true;

        /** A function that filters right-ish time-series. */
        private Predicate<TimeSeries<R>> rightFilter = in -> true;

        /** A function that filters baseline-ish time-series. */
        private Predicate<TimeSeries<R>> baselineFilter = in -> true;

        /** A mapper to map between left-ish climate values and double values. */
        private ToDoubleFunction<L> climateMapper;

        /** The admissible values for the left-ish data associated with climatology. */
        private Predicate<L> climateAdmissibleValue;

        /** An optional generator for baseline data (e.g., persistence or climatology). */
        private Function<Set<FeatureKey>, UnaryOperator<TimeSeries<R>>> baselineGenerator;

        /**
         * @param project the project
         * @return the builder
         */
        Builder<L, R> setProject( Project project )
        {
            this.project = project;

            return this;
        }

        /**
         * @param poolRequests the pool requests
         * @return the builder
         */
        Builder<L, R> setPoolRequests( List<PoolRequest> poolRequests )
        {
            if ( Objects.nonNull( poolRequests ) )
            {
                this.poolRequests.addAll( poolRequests );
            }

            return this;
        }

        /**
         * @param retrieverFactory the retriever factory
         * @return the builder
         */
        Builder<L, R> setRetrieverFactory( RetrieverFactory<L, R> retrieverFactory )
        {
            this.retrieverFactory = retrieverFactory;

            return this;
        }

        /**
         * @param pairer the pairer
         * @return the builder
         */
        Builder<L, R> setPairer( TimeSeriesPairer<L, R> pairer )
        {
            this.pairer = pairer;

            return this;
        }

        /**
         * @param crossPairer the cross-pairer
         * @return the builder
         */
        Builder<L, R> setCrossPairer( TimeSeriesCrossPairer<L, R> crossPairer )
        {
            this.crossPairer = crossPairer;

            return this;
        }

        /**
         * @param leftUpscaler the upscaler for left values
         * @return the builder
         */
        Builder<L, R> setLeftUpscaler( TimeSeriesUpscaler<L> leftUpscaler )
        {
            this.leftUpscaler = leftUpscaler;

            return this;
        }

        /**
         * @param rightUpscaler the upscaler for right values
         * @return the builder
         */
        Builder<L, R> setRightUpscaler( TimeSeriesUpscaler<R> rightUpscaler )
        {
            this.rightUpscaler = rightUpscaler;

            return this;
        }

        /**
         * @param climatologyAdmissibleValue the admissible value constraint on climatology
         * @return the builder
         */
        Builder<L, R> setClimateAdmissibleValue( Predicate<L> climatologyAdmissibleValue )
        {
            this.climateAdmissibleValue = climatologyAdmissibleValue;

            return this;
        }

        /**
         * @param leftTransformer the left transformer
         * @return the builder
         */
        Builder<L, R> setLeftTransformer( UnaryOperator<TimeSeries<L>> leftTransformer )
        {
            this.leftTransformer = leftTransformer;

            return this;
        }

        /**
         * @param rightTransformer the right transformer, which may consider the encapsulating event
         * @return the builder
         */
        Builder<L, R> setRightTransformer( UnaryOperator<TimeSeries<R>> rightTransformer )
        {
            this.rightTransformer = rightTransformer;

            return this;
        }

        /**
         * @param baselineTransformer the baseline transformer, which may consider the encapsulating event
         * @return the builder
         */
        Builder<L, R> setBaselineTransformer( UnaryOperator<TimeSeries<R>> baselineTransformer )
        {
            this.baselineTransformer = baselineTransformer;

            return this;
        }

        /**
         * @param leftFilter the filter for left-style data
         * @return the builder
         */
        Builder<L, R> setLeftFilter( Predicate<TimeSeries<L>> leftFilter )
        {
            this.leftFilter = leftFilter;

            return this;
        }

        /**
         * @param rightFilter the filter for right-style data
         * @return the builder
         */
        Builder<L, R> setRightFilter( Predicate<TimeSeries<R>> rightFilter )
        {
            this.rightFilter = rightFilter;

            return this;
        }

        /**
         * @param baselineFilter the filter for baseline-style data
         * @return the builder
         */
        Builder<L, R> setBaselineFilter( Predicate<TimeSeries<R>> baselineFilter )
        {
            this.baselineFilter = baselineFilter;

            return this;
        }

        /**
         * @param climateMapper the climateMapper to set
         * @return the builder
         */
        Builder<L, R> setClimateMapper( ToDoubleFunction<L> climateMapper )
        {
            this.climateMapper = climateMapper;

            return this;
        }

        /**
         * @param baselineGenerator the baselineGenerator to set
         * @return the builder
         */
        Builder<L, R> setBaselineGenerator( Function<Set<FeatureKey>, UnaryOperator<TimeSeries<R>>> baselineGenerator )
        {
            this.baselineGenerator = baselineGenerator;

            return this;
        }

        /**
         * Builds an instance.
         * 
         * @return an instance
         */

        PoolsGenerator<L, R> build()
        {
            return new PoolsGenerator<>( this );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder a builder
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the declaration is inconsistent with the type of pool expected
     */

    private PoolsGenerator( Builder<L, R> builder )
    {
        // Set then validate
        this.project = builder.project;
        this.retrieverFactory = builder.retrieverFactory;
        this.poolRequests = Collections.unmodifiableList( new ArrayList<>( builder.poolRequests ) );
        this.baselineGenerator = builder.baselineGenerator;
        this.pairer = builder.pairer;
        this.leftUpscaler = builder.leftUpscaler;
        this.rightUpscaler = builder.rightUpscaler;
        this.climateAdmissibleValue = builder.climateAdmissibleValue;
        this.leftTransformer = builder.leftTransformer;
        this.rightTransformer = builder.rightTransformer;
        this.baselineTransformer = builder.baselineTransformer;
        this.leftFilter = builder.leftFilter;
        this.rightFilter = builder.rightFilter;
        this.baselineFilter = builder.baselineFilter;
        this.climateMapper = builder.climateMapper;
        this.crossPairer = builder.crossPairer;

        String messageStart = "Cannot build the pool generator: ";

        Objects.requireNonNull( this.project, messageStart + "the project is missing." );
        Objects.requireNonNull( this.retrieverFactory, messageStart + "the retriever factory is missing." );

        Objects.requireNonNull( this.pairer, messageStart + "the pairer is missing." );
        Objects.requireNonNull( this.leftUpscaler, messageStart + "the upscaler for left values is missing" );
        Objects.requireNonNull( this.rightUpscaler, messageStart + "the upscaler for right values is missing." );
        Objects.requireNonNull( this.leftTransformer, messageStart + "add a transformer for the left data." );
        Objects.requireNonNull( this.rightTransformer, messageStart + "add a transformer for the right data." );
        Objects.requireNonNull( this.baselineTransformer, messageStart + "add a transformer for the baseline data." );
        Objects.requireNonNull( this.leftFilter, messageStart + "add a filter for the left data." );
        Objects.requireNonNull( this.rightFilter, messageStart + "add a filter for the right data." );
        Objects.requireNonNull( this.baselineFilter, messageStart + "add a filter for the baseline data." );

        // If adding a baseline, baseline metadata is needed. If not, it should not be supplied
        if ( this.getPoolRequests().isEmpty() )
        {
            throw new IllegalArgumentException( messageStart + "cannot create pools with zero pool requests. Add one "
                                                + "or more pool requests and try again." );
        }

        // A baseline generator should be supplied if there is a baseline to generate, otherwise not
        if ( Objects.nonNull( this.baselineGenerator ) != this.project.hasGeneratedBaseline() )
        {
            throw new IllegalArgumentException( messageStart
                                                + " a baseline generator should be supplied when required, "
                                                + "otherwise it should not be supplied." );
        }

        // Create the pools
        this.pools = this.createPools();
    }

    /**
     * Produces a collection of pools.
     * 
     * @return a collection of pools
     * @throws NullPointerException if any input is null
     * @throws PoolCreationException if the pools cannot be created for any other reason
     */

    private List<Supplier<Pool<TimeSeries<Pair<L, R>>>>> createPools()
    {
        LOGGER.debug( "Creating pool suppliers for {} pools requests: {}.",
                      this.getPoolRequests().size(),
                      this.getPoolRequests() );

        ProjectConfig projectConfig = this.getProject()
                                          .getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();

        // Create the common builder
        PoolSupplier.Builder<L, R> builder = new PoolSupplier.Builder<>();
        builder.setLeftUpscaler( this.getLeftUpscaler() )
               .setRightUpscaler( this.getRightUpscaler() )
               .setPairer( this.getPairer() )
               .setCrossPairer( this.getCrossPairer() )
               .setProjectDeclaration( projectConfig )
               .setLeftTransformer( this.getLeftTransformer() )
               .setRightTransformer( this.getRightTransformer() )
               .setBaselineTransformer( this.getBaselineTransformer() )
               .setLeftFilter( this.getLeftFilter() )
               .setRightFilter( this.getRightFilter() )
               .setBaselineFilter( this.getBaselineFilter() );

        // Obtain the desired time scale and set it
        TimeScaleOuter desiredTimeScale = this.getProject()
                                              .getDesiredTimeScale();
        builder.setDesiredTimeScale( desiredTimeScale );

        // Set the frequency associated with the pairs, if declared
        this.setFrequency( pairConfig, builder );

        // Get a left-ish retriever for every pool in order to promote re-use across pools via caching. May consider
        // doing this for other sides of data in future, but left-ish data is the priority because this is very 
        // often re-used. Only need this collection if there isn't a climatological source that spans all pools
        Map<TimeWindowOuter, Supplier<Stream<TimeSeries<L>>>> leftRetrievers = new HashMap<>();

        // Create the time windows, iterate over them and create the retrievers 
        try
        {
            // Climatological data required?
            Supplier<Stream<TimeSeries<L>>> climatologySupplier = null;
            if ( this.getProject()
                     .hasProbabilityThresholds()
                 || this.getProject()
                        .hasGeneratedBaseline() )
            {
                Set<FeatureKey> leftFeatures = this.getFeatures( FeatureTuple::getLeft );
                climatologySupplier = this.getRetrieverFactory()
                                          .getClimatologyRetriever( leftFeatures );

                // Get the climatology at an appropriate scale and with any transformations required and add to the 
                // builder, but retain the existing scale for the main supplier, as that may be re-used for left data, 
                // and left data is rescaled with respect to right data
                Supplier<Stream<TimeSeries<L>>> climatologyAtScale =
                        this.getClimatologyAtDesiredTimeScale( climatologySupplier,
                                                               this.getLeftUpscaler(),
                                                               desiredTimeScale,
                                                               this.getLeftTransformer(),
                                                               this.getClimateAdmissibleValue() );

                // Cache the upscaled climatology, even if the raw climatology is itself cached because the upscaling
                // is potentially expensive and there is no need to repeat it on every call to the supplier.
                climatologyAtScale = CachingRetriever.of( climatologyAtScale );

                builder.setClimatology( climatologyAtScale, this.getClimateMapper() );
            }
            // No climatology, so populate the collection of per-pool left-ish retrievers
            else
            {
                leftRetrievers = this.getLeftRetrievers( this.getPoolRequests(),
                                                         inputsConfig.getLeft()
                                                                     .getType() );
            }

            List<PoolSupplier<L, R>> returnMe = new ArrayList<>();

            // Create the retrievers for each time window
            for ( PoolRequest nextPool : this.getPoolRequests() )
            {
                TimeWindowOuter nextWindow = nextPool.getMetadata()
                                                     .getTimeWindow();

                Set<FeatureKey> rightFeatures = this.getFeatures( nextPool.getMetadata(),
                                                                  FeatureTuple::getRight );

                Supplier<Stream<TimeSeries<R>>> rightSupplier = this.getRetrieverFactory()
                                                                    .getRightRetriever( rightFeatures,
                                                                                        nextWindow );

                builder.setRight( rightSupplier );
                builder.setMetadata( nextPool.getMetadata() );

                // Add left data, using the climatology supplier first if one exists
                Supplier<Stream<TimeSeries<L>>> leftSupplier = climatologySupplier;

                if ( Objects.isNull( leftSupplier ) )
                {
                    leftSupplier = leftRetrievers.get( nextWindow );
                }

                builder.setLeft( leftSupplier );

                // Set baseline if needed
                if ( nextPool.hasBaseline() )
                {
                    builder.setBaselineMetadata( nextPool.getMetadataForBaseline() );

                    // Generated baseline?
                    if ( this.getProject().hasGeneratedBaseline() )
                    {
                        builder.setBaselineGenerator( this.getBaselineGenerator() );
                    }
                    // Data-source baseline
                    else
                    {
                        Set<FeatureKey> baselineFeatures = this.getFeatures( nextPool.getMetadata(),
                                                                             FeatureTuple::getBaseline );

                        Supplier<Stream<TimeSeries<R>>> baselineSupplier = this.getRetrieverFactory()
                                                                               .getBaselineRetriever( baselineFeatures,
                                                                                                      nextWindow );

                        builder.setBaseline( baselineSupplier );
                    }
                }

                returnMe.add( builder.build() );
            }

            LOGGER.debug( "Created pool suppliers for {} pool requests: {}.",
                          this.getPoolRequests().size(),
                          this.getPoolRequests() );

            return Collections.unmodifiableList( returnMe );
        }
        catch ( DataAccessException | ProjectConfigException e )
        {
            throw new PoolCreationException( "While attempting to create pool suppliers:", e );
        }
    }

    /**
     * Builds a left-ish retriever for each {@link TimeWindowOuter} in the input. Uses the minimum number of retrievers 
     * necessary to avoid retrieving the same data from an underlying source (e.g., database) more than once. In order,
     * to achieve this, each unique retriever is wrapped inside a {@link CachingRetriever} and re-used as many times 
     * as there are common time-windows, ignoring any lead duration bounds. The returned map has a comparator that 
     * ignores the lead duration bounds, which allows for transparent use by the caller against the original time 
     * windows. De-duplication only happens for datasets that are {@link DatasourceType#OBSERVATIONS} or 
     * {@link DatasourceType#SIMULATIONS}.
     * 
     * @param poolRequest the pool requests
     * @param type the type of data
     * @return a left-ish retriever for each time-window
     */

    private Map<TimeWindowOuter, Supplier<Stream<TimeSeries<L>>>> getLeftRetrievers( List<PoolRequest> poolRequests,
                                                                                     DatasourceType type )
    {
        RetrieverFactory<L, R> factory = this.getRetrieverFactory();

        Set<TimeWindowOuter> timeWindows = poolRequests.stream()
                                                       .map( next -> next.getMetadata().getTimeWindow() )
                                                       .collect( Collectors.toSet() );

        Set<FeatureKey> features = poolRequests.stream()
                                               .flatMap( next -> next.getMetadata().getFeatureTuples().stream() )
                                               .map( FeatureTuple::getLeft )
                                               .collect( Collectors.toSet() );

        // Observations or simulations? Then de-duplicate if possible.
        if ( type == DatasourceType.OBSERVATIONS || type == DatasourceType.SIMULATIONS )
        {
            // Find the union of the time windows, bearing in mind that lead durations can influence the valid 
            // datetimes for observation selection
            TimeWindowOuter unionWindow = TimeWindowOuter.unionOf( timeWindows );
            Supplier<Stream<TimeSeries<L>>> leftRetriever = factory.getLeftRetriever( features, unionWindow );
            Supplier<Stream<TimeSeries<L>>> cachingRetriever = CachingRetriever.of( leftRetriever );

            // Build a retriever for each unique time window (ignoring lead durations via the comparator)
            Map<TimeWindowOuter, Supplier<Stream<TimeSeries<L>>>> returnMe = new TreeMap<>();
            timeWindows.forEach( nextWindow -> returnMe.put( nextWindow, cachingRetriever ) );

            // Log any de-duplication that was achieved
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While creating pools for {} pools, de-duplicated the retrievers of {} data from {} to {} "
                              + "using the union of time windows across all pools, which is {}.",
                              this.getPoolRequests().size(),
                              LeftOrRightOrBaseline.LEFT,
                              timeWindows.size(),
                              1,
                              unionWindow );
            }

            return Collections.unmodifiableMap( returnMe );
        }
        // Other datasets: do not attempt to de-duplicate for now
        else
        {
            Map<TimeWindowOuter, Supplier<Stream<TimeSeries<L>>>> returnMe = new HashMap<>();

            timeWindows.forEach( next -> returnMe.put( next, factory.getLeftRetriever( features, next ) ) );

            return Collections.unmodifiableMap( returnMe );
        }
    }

    /**
     * Returns the upscaler.
     * 
     * @return the upscaler
     */

    private TimeSeriesUpscaler<L> getLeftUpscaler()
    {
        return this.leftUpscaler;
    }

    /**
     * Returns the upscaler.
     * 
     * @return the upscaler
     */

    private TimeSeriesUpscaler<R> getRightUpscaler()
    {
        return this.rightUpscaler;
    }

    /**
     * Returns the pairer.
     * 
     * @return the pairer
     */

    private TimeSeriesPairer<L, R> getPairer()
    {
        return this.pairer;
    }

    /**
     * Returns the cross pairer.
     * 
     * @return the cross pairer
     */

    private TimeSeriesCrossPairer<L, R> getCrossPairer()
    {
        return this.crossPairer;
    }

    /**
     * Returns the project.
     * 
     * @return the project
     */

    private Project getProject()
    {
        return this.project;
    }

    /**
     * @return the transformer for left values
     */

    private UnaryOperator<TimeSeries<L>> getLeftTransformer()
    {
        return this.leftTransformer;
    }

    /**
     * @return the transformer for right values
     */

    private UnaryOperator<TimeSeries<R>> getRightTransformer()
    {
        return this.rightTransformer;
    }

    /**
     * @return the transformer for baseline values
     */

    private UnaryOperator<TimeSeries<R>> getBaselineTransformer()
    {
        return this.baselineTransformer;
    }

    /**
     * @return the filter for left-ish data
     */

    private Predicate<TimeSeries<L>> getLeftFilter()
    {
        return this.leftFilter;
    }

    /**
     * @return the filter for right-ish data
     */

    private Predicate<TimeSeries<R>> getRightFilter()
    {
        return this.rightFilter;
    }

    /**
     * @return the filter for baseline-ish data
     */

    private Predicate<TimeSeries<R>> getBaselineFilter()
    {
        return this.baselineFilter;
    }

    /**
     * Returns the retriever factory.
     * 
     * @return the retriever factory
     */

    private RetrieverFactory<L, R> getRetrieverFactory()
    {
        return this.retrieverFactory;
    }

    /**
     * Returns the pool requests.
     * 
     * @return the pool requests
     */

    private List<PoolRequest> getPoolRequests()
    {
        return this.poolRequests;
    }

    /**
     * Returns a feature-specific baseline generator, if any.
     * 
     * @return the baseline generator
     */

    private Function<Set<FeatureKey>, UnaryOperator<TimeSeries<R>>> getBaselineGenerator()
    {
        return this.baselineGenerator;
    }

    /**
     * Returns the admissible value guard for climatology.
     * 
     * @return the admissible value guard for climatology
     */

    private Predicate<L> getClimateAdmissibleValue()
    {
        return this.climateAdmissibleValue;
    }

    /**
     * Returns the mapper from left-ish climate values to double values.
     * 
     * @return the climate mapper
     */

    private ToDoubleFunction<L> getClimateMapper()
    {
        return this.climateMapper;
    }

    /**
     * @param metadata
     * @param featureGetter the feature-getter
     * @return the features from the metadata using the prescribed feature-getter
     * @throws NullPointerException if the metadata is null
     */

    private Set<FeatureKey> getFeatures( PoolMetadata metadata,
                                         Function<FeatureTuple, FeatureKey> featureGetter )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( featureGetter );

        return metadata.getFeatureTuples()
                       .stream()
                       .map( featureGetter )
                       .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * @param featureGetter the feature-getter
     * @return the features using the prescribed feature-getter
     * @throws NullPointerException if the metadata is null
     */

    private Set<FeatureKey> getFeatures( Function<FeatureTuple, FeatureKey> featureGetter )
    {
        Objects.requireNonNull( featureGetter );
        return this.getPoolRequests()
                   .stream()
                   .flatMap( next -> next.getMetadata().getFeatureTuples().stream() )
                   .map( featureGetter )
                   .collect( Collectors.toSet() );

    }

    /**
     * Sets the frequency of the pairs, if declared.
     * 
     * @param pairConfig the pair declaration
     * @param builder the builder whose frequency should be set
     */

    private void setFrequency( PairConfig pairConfig,
                               PoolSupplier.Builder<L, R> builder )
    {
        // Obtain from the declaration if available
        if ( Objects.nonNull( pairConfig )
             && Objects.nonNull( pairConfig.getDesiredTimeScale() )
             && Objects.nonNull( pairConfig.getDesiredTimeScale().getFrequency() ) )
        {
            ChronoUnit unit = ChronoUnit.valueOf( pairConfig.getDesiredTimeScale()
                                                            .getUnit()
                                                            .value()
                                                            .toUpperCase() );

            Duration frequency = Duration.of( pairConfig.getDesiredTimeScale().getFrequency(), unit );

            builder.setFrequencyOfPairs( frequency );
        }
    }

    /**
     * Returns a climatological data supply at the desired time scale.
     * 
     * @param climatologySupplier the raw data supplier
     * @param upscaler the upscaler
     * @param desiredTimeScale the desired time scale
     * @param transformer an optional transformer
     * @param admissibleValue an admissible value constraint
     * @return a climatological supply at the desired time scale
     */

    private Supplier<Stream<TimeSeries<L>>>
            getClimatologyAtDesiredTimeScale( Supplier<Stream<TimeSeries<L>>> climatologySupplier,
                                              TimeSeriesUpscaler<L> upscaler,
                                              TimeScaleOuter desiredTimeScale,
                                              UnaryOperator<TimeSeries<L>> transformer,
                                              Predicate<L> admissibleValue )
    {
        // Defer rescaling until retrieval time
        return () -> {
            List<TimeSeries<L>> climData = climatologySupplier.get()
                                                              .collect( Collectors.toList() );

            TimeSeriesMetadata existingMetadata = null;

            List<TimeSeries<L>> returnMe = new ArrayList<>();

            for ( TimeSeries<L> next : climData )
            {
                TimeSeries.Builder<L> builder = new TimeSeries.Builder<>();

                TimeSeries<L> nextSeries = next;
                TimeScaleOuter nextScale = nextSeries.getMetadata()
                                                     .getTimeScale();

                // Upscale?
                if ( this.shouldUpscaleClimatology( nextScale, desiredTimeScale ) )
                {
                    if ( Objects.isNull( upscaler ) )
                    {
                        throw new IllegalArgumentException( "The climatological time-series "
                                                            + nextSeries.hashCode()
                                                            + " needed upscaling from "
                                                            + nextScale
                                                            + " to "
                                                            + desiredTimeScale
                                                            + " but no upscaler was provided." );
                    }

                    String desiredUnit = this.getProject()
                                             .getMeasurementUnit();

                    nextSeries = upscaler.upscale( nextSeries, desiredTimeScale, desiredUnit )
                                         .getTimeSeries();

                    LOGGER.debug( "Upscaled the climatological time-series {} from {} to {}.",
                                  nextSeries.hashCode(),
                                  nextScale,
                                  desiredTimeScale );

                }

                // Transform?
                if ( Objects.nonNull( transformer ) )
                {
                    nextSeries = transformer.apply( nextSeries );
                }

                // Filter inadmissible values. Do this LAST because a transformer may produce 
                // non-finite values
                nextSeries = TimeSeriesSlicer.filter( nextSeries, admissibleValue );

                existingMetadata = nextSeries.getMetadata();
                builder.addEvents( nextSeries.getEvents() );

                TimeSeriesMetadata metadata =
                        new TimeSeriesMetadata.Builder( existingMetadata ).setTimeScale( desiredTimeScale )
                                                                          .build();
                builder.setMetadata( metadata );

                TimeSeries<L> climatologyAtScale = builder.build();

                returnMe.add( climatologyAtScale );
            }

            LOGGER.debug( "Created {} climatological time-series.", returnMe.size() );

            return returnMe.stream();
        };
    }

    /**
     * @param existingTimeScale the existing time scale
     * @param desiredTimeScale the desired time scale
     * @return true if both scales are not null and the desired time scale has a larger period
     */

    private boolean shouldUpscaleClimatology( TimeScaleOuter existingTimeScale, TimeScaleOuter desiredTimeScale )
    {
        return Objects.nonNull( existingTimeScale ) && Objects.nonNull( desiredTimeScale )
               && ! existingTimeScale.equalsOrInstantaneous( desiredTimeScale );
    }

}
