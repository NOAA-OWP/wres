package wres.pipeline.pooling;

import java.time.Duration;
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

import wres.config.yaml.DeclarationException;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.Climatology;
import wres.datamodel.baselines.BaselineGenerator;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.RescaledTimeSeriesPlusValidation;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesCrossPairer;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.project.Project;
import wres.io.retrieving.CachingRetriever;
import wres.io.retrieving.CachingSupplier;
import wres.io.retrieving.DataAccessException;
import wres.io.retrieving.RetrieverFactory;

/**
 * Generates a collection of {@link PoolSupplier} that contain the pools for a particular evaluation.
 *
 * @author James Brown
 */

public class PoolsGenerator<L, R, B> implements Supplier<List<Supplier<Pool<TimeSeries<Pair<L, R>>>>>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( PoolsGenerator.class );

    /** The project for which pools are required. */
    private final Project project;

    /** The pool requests. */
    private final List<PoolRequest> poolRequests;

    /** A factory to create project-relevant retrievers. */
    private final RetrieverFactory<L, R, B> retrieverFactory;

    /** The upscaler for left-ish values. */
    private final TimeSeriesUpscaler<L> leftUpscaler;

    /** The upscaler for right-ish values. */
    private final TimeSeriesUpscaler<R> rightUpscaler;

    /** A function to upscale baseline data. */
    private final TimeSeriesUpscaler<R> baselineUpscaler;

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

    /** The time shift for left-ish valid times. */
    private final Duration leftTimeShift;

    /** The time shift for right-ish valid times. */
    private final Duration rightTimeShift;

    /** The time shift for baseline-ish valid times. */
    private final Duration baselineTimeShift;

    /** The pair frequency. */
    private final Duration pairFrequency;

    /** A mapper to map between left-ish climate values and double values. TODO: propagate left-ish data for 
     * climatology, rather than transforming it upfront. */
    private final ToDoubleFunction<L> climateMapper;

    /** The admissible values for the left-ish data associated with climatology. */
    private final Predicate<L> climateAdmissibleValue;

    /** An optional generator for baseline data (e.g., persistence or climatology). */
    private final Function<Set<Feature>, BaselineGenerator<R>> baselineGenerator;

    /** A shim to map from a baseline-ish dataset to a right-ish dataset.
     * TODO: remove this shim when pools support different types of right and baseline data. */
    private final Function<TimeSeries<B>,TimeSeries<R>> baselineShim;

    @Override
    public List<Supplier<Pool<TimeSeries<Pair<L, R>>>>> get()
    {
        return this.createPools();
    }

    /**
     * A builder for the {@link PoolsGenerator}.
     * @param <L> the left-ish data type
     * @param <R> the right-ish data type
     * @param <B> the baseline-ish data type
     */
    static class Builder<L, R, B>
    {
        /** The project for which pools are required. */
        private Project project;

        /** The pool requests. */
        private final List<PoolRequest> poolRequests = new ArrayList<>();

        /** A factory to create project-relevant retrievers. */
        private RetrieverFactory<L, R, B> retrieverFactory;

        /**  A function to support pairing of left and right data. */
        private TimeSeriesPairer<L, R> pairer;

        /** An optional cross-pairer to use common pairs (by time) for the main and baseline pairs. */
        private TimeSeriesCrossPairer<L, R> crossPairer;

        /** A function to upscale left data. */
        private TimeSeriesUpscaler<L> leftUpscaler;

        /** A function to upscale right data. */
        private TimeSeriesUpscaler<R> rightUpscaler;

        /** A function to upscale baseline data. */
        private TimeSeriesUpscaler<R> baselineUpscaler;

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

        /** The time offset to apply to the left-ish valid times. */
        private Duration leftTimeShift = Duration.ZERO;

        /** The time offset to apply to the right-ish valid times. */
        private Duration rightTimeShift = Duration.ZERO;

        /** The time offset to apply to the baseline-ish valid times. */
        private Duration baselineTimeShift = Duration.ZERO;

        /** The pair frequency. */
        private Duration pairFrequency = null;

        /** A mapper to map between left-ish climate values and double values. */
        private ToDoubleFunction<L> climateMapper;

        /** The admissible values for the left-ish data associated with climatology. */
        private Predicate<L> climateAdmissibleValue;

        /** An optional generator for baseline data (e.g., persistence or climatology). */
        private Function<Set<Feature>, BaselineGenerator<R>> baselineGenerator;

        /** A shim to map from a baseline-ish dataset to a right-ish dataset. */
        private Function<TimeSeries<B>,TimeSeries<R>> baselineShim;

        /**
         * @param project the project
         * @return the builder
         */
        Builder<L, R, B> setProject( Project project )
        {
            this.project = project;

            return this;
        }

        /**
         * @param poolRequests the pool requests
         * @return the builder
         */
        Builder<L, R, B> setPoolRequests( List<PoolRequest> poolRequests )
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
        Builder<L, R, B> setRetrieverFactory( RetrieverFactory<L, R, B> retrieverFactory )
        {
            this.retrieverFactory = retrieverFactory;

            return this;
        }

        /**
         * @param pairer the pairer
         * @return the builder
         */
        Builder<L, R, B> setPairer( TimeSeriesPairer<L, R> pairer )
        {
            this.pairer = pairer;

            return this;
        }

        /**
         * @param crossPairer the cross-pairer
         * @return the builder
         */
        Builder<L, R, B> setCrossPairer( TimeSeriesCrossPairer<L, R> crossPairer )
        {
            this.crossPairer = crossPairer;

            return this;
        }

        /**
         * @param leftUpscaler the upscaler for left values
         * @return the builder
         */
        Builder<L, R, B> setLeftUpscaler( TimeSeriesUpscaler<L> leftUpscaler )
        {
            this.leftUpscaler = leftUpscaler;

            return this;
        }

        /**
         * @param rightUpscaler the upscaler for right values
         * @return the builder
         */
        Builder<L, R, B> setRightUpscaler( TimeSeriesUpscaler<R> rightUpscaler )
        {
            this.rightUpscaler = rightUpscaler;

            return this;
        }

        /**
         * @param baselineUpscaler the upscaler for baseline values
         * @return the builder
         */
        Builder<L, R, B> setBaselineUpscaler( TimeSeriesUpscaler<R> baselineUpscaler )
        {
            this.baselineUpscaler = baselineUpscaler;

            return this;
        }

        /**
         * @param climatologyAdmissibleValue the admissible value constraint on climatology
         * @return the builder
         */
        Builder<L, R, B> setClimateAdmissibleValue( Predicate<L> climatologyAdmissibleValue )
        {
            this.climateAdmissibleValue = climatologyAdmissibleValue;

            return this;
        }

        /**
         * @param leftTransformer the left transformer
         * @return the builder
         */
        Builder<L, R, B> setLeftTransformer( UnaryOperator<TimeSeries<L>> leftTransformer )
        {
            this.leftTransformer = leftTransformer;

            return this;
        }

        /**
         * @param rightTransformer the right transformer, which may consider the encapsulating event
         * @return the builder
         */
        Builder<L, R, B> setRightTransformer( UnaryOperator<TimeSeries<R>> rightTransformer )
        {
            this.rightTransformer = rightTransformer;

            return this;
        }

        /**
         * @param baselineTransformer the baseline transformer, which may consider the encapsulating event
         * @return the builder
         */
        Builder<L, R, B> setBaselineTransformer( UnaryOperator<TimeSeries<R>> baselineTransformer )
        {
            this.baselineTransformer = baselineTransformer;

            return this;
        }

        /**
         * @param leftFilter the filter for left-style data
         * @return the builder
         */
        Builder<L, R, B> setLeftFilter( Predicate<TimeSeries<L>> leftFilter )
        {
            this.leftFilter = leftFilter;

            return this;
        }

        /**
         * @param rightFilter the filter for right-style data
         * @return the builder
         */
        Builder<L, R, B> setRightFilter( Predicate<TimeSeries<R>> rightFilter )
        {
            this.rightFilter = rightFilter;

            return this;
        }

        /**
         * @param baselineFilter the filter for baseline-style data
         * @return the builder
         */
        Builder<L, R, B> setBaselineFilter( Predicate<TimeSeries<R>> baselineFilter )
        {
            this.baselineFilter = baselineFilter;

            return this;
        }

        /**
         * @param leftTimeShift the time shift for left-ish valid times
         * @return the builder
         */
        Builder<L, R, B> setLeftTimeShift( Duration leftTimeShift )
        {
            if ( Objects.nonNull( leftTimeShift ) )
            {
                this.leftTimeShift = leftTimeShift;
            }

            return this;
        }

        /**
         * @param rightTimeShift the time shift for right-ish valid times
         * @return the builder
         */
        Builder<L, R, B> setRightTimeShift( Duration rightTimeShift )
        {
            if ( Objects.nonNull( rightTimeShift ) )
            {
                this.rightTimeShift = rightTimeShift;
            }

            return this;
        }

        /**
         * @param baselineTimeShift the time shift for baseline-ish valid times
         * @return the builder
         */
        Builder<L, R, B> setBaselineTimeShift( Duration baselineTimeShift )
        {
            if ( Objects.nonNull( baselineTimeShift ) )
            {
                this.baselineTimeShift = baselineTimeShift;
            }

            return this;
        }

        /**
         * @param pairFrequency the pair frequency
         * @return the builder
         */
        Builder<L, R, B> setPairFrequency( Duration pairFrequency )
        {
            this.pairFrequency = pairFrequency;

            return this;
        }

        /**
         * @param climateMapper the climateMapper to set
         * @return the builder
         */
        Builder<L, R, B> setClimateMapper( ToDoubleFunction<L> climateMapper )
        {
            this.climateMapper = climateMapper;

            return this;
        }

        /**
         * @param baselineGenerator the baselineGenerator to set
         * @return the builder
         */
        Builder<L, R, B> setBaselineGenerator( Function<Set<Feature>, BaselineGenerator<R>> baselineGenerator )
        {
            this.baselineGenerator = baselineGenerator;

            return this;
        }

        /**
         * Sets the baseline shim to map from baseline-ish to right-ish data.
         * @param baselineShim the baseline shim
         * @return the builder
         */

        Builder<L,R,B> setBaselineShim( Function<TimeSeries<B>,TimeSeries<R>> baselineShim )
        {
            this.baselineShim = baselineShim;

            return this;
        }

        /**
         * Builds an instance.
         *
         * @return an instance
         */

        PoolsGenerator<L, R, B> build()
        {
            return new PoolsGenerator<>( this );
        }
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

        TimeScaleOuter desiredTimeScale = this.getProject()
                                              .getDesiredTimeScale();

        // Create the common builder
        PoolSupplier.Builder<L, R, B> builder =
                new PoolSupplier.Builder<L, R, B>()
                        .setLeftUpscaler( this.getLeftUpscaler() )
                        .setRightUpscaler( this.getRightUpscaler() )
                        .setBaselineUpscaler( this.getBaselineUpscaler() )
                        .setPairer( this.getPairer() )
                        .setCrossPairer( this.getCrossPairer() )
                        .setLeftTimeShift( this.getLeftTimeShift() )
                        .setRightTimeShift( this.getRightTimeShift() )
                        .setBaselineTimeShift( this.getBaselineTimeShift() )
                        .setLeftTransformer( this.getLeftTransformer() )
                        .setRightTransformer( this.getRightTransformer() )
                        .setBaselineTransformer( this.getBaselineTransformer() )
                        .setLeftFilter( this.getLeftFilter() )
                        .setRightFilter( this.getRightFilter() )
                        .setBaselineFilter( this.getBaselineFilter() )
                        .setPairFrequency( this.getPairFrequency() )
                        .setBaselineShim( this.getBaselineShim() )
                        .setDesiredTimeScale( desiredTimeScale );

        // Get a left-ish retriever for every pool in order to promote re-use across pools via caching. May consider
        // doing this for other sides of data in future, but left-ish data is the priority because this is very 
        // often re-used. Only need this collection if there isn't a climatological source that spans all pools
        Map<TimeWindowOuter, Supplier<Stream<TimeSeries<L>>>> leftRetrievers = new HashMap<>();

        // Create the time windows, iterate over them and create the retrievers 
        try
        {
            // Climatological data required? If there are probability thresholds, yes.
            Supplier<Stream<TimeSeries<L>>> climatologySupplier = null;
            if ( this.getProject()
                     .hasProbabilityThresholds() )
            {
                Set<Feature> leftFeatures = this.getFeatures( FeatureTuple::getLeft );
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

                // Create the climatology abstraction from the left-ish data
                Supplier<Climatology> climatology = this.createClimatology( climatologyAtScale,
                                                                            this.getClimateMapper() );

                // Create a caching supplier to re-use the result once the data has been pulled
                Supplier<Climatology> cachedClimatology = CachingSupplier.of( climatology );
                builder.setClimatology( cachedClimatology );
            }
            // No climatology, so populate the collection of per-pool left-ish retrievers
            else
            {
                leftRetrievers = this.getLeftRetrievers( this.getPoolRequests(),
                                                         this.getProject()
                                                             .getDeclaredDataset( DatasetOrientation.LEFT )
                                                             .type() );
            }

            List<PoolSupplier<L, R, B>> returnMe = new ArrayList<>();

            // Create the retrievers for each pool
            for ( PoolRequest nextPool : this.getPoolRequests() )
            {
                TimeWindowOuter nextWindow = nextPool.getMetadata()
                                                     .getTimeWindow();

                Set<Feature> rightFeatures = this.getFeatures( nextPool.getMetadata(),
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
                    if ( this.getProject()
                             .hasGeneratedBaseline() )
                    {
                        builder.setBaselineGenerator( this.getBaselineGenerator() );
                    }
                    // Data-source baseline
                    else
                    {
                        Set<Feature> baselineFeatures = this.getFeatures( nextPool.getMetadata(),
                                                                          FeatureTuple::getBaseline );

                        Supplier<Stream<TimeSeries<B>>> baselineSupplier = this.getRetrieverFactory()
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
        catch ( DataAccessException | DeclarationException e )
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
     * windows. De-duplication only happens for datasets that are {@link DataType#OBSERVATIONS} or
     * {@link DataType#SIMULATIONS}.
     *
     * @param poolRequests the pool requests
     * @param type the type of data
     * @return a left-ish retriever for each time-window
     */

    private Map<TimeWindowOuter, Supplier<Stream<TimeSeries<L>>>> getLeftRetrievers( List<PoolRequest> poolRequests,
                                                                                     DataType type )
    {
        RetrieverFactory<L, R, B> factory = this.getRetrieverFactory();

        Set<TimeWindowOuter> timeWindows = poolRequests.stream()
                                                       .map( next -> next.getMetadata().getTimeWindow() )
                                                       .collect( Collectors.toSet() );

        Set<Feature> features = poolRequests.stream()
                                            .flatMap( next -> next.getMetadata().getFeatureTuples().stream() )
                                            .map( FeatureTuple::getLeft )
                                            .collect( Collectors.toSet() );

        // Observations or simulations? Then de-duplicate if possible.
        if ( type == DataType.OBSERVATIONS || type == DataType.SIMULATIONS )
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
                LOGGER.debug(
                        "While creating pools for {} pools, de-duplicated the retrievers of {} data from {} to {} "
                        + "using the union of time windows across all pools, which is {}.",
                        this.getPoolRequests().size(),
                        DatasetOrientation.LEFT,
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
     * Returns the upscaler for left-ish data.
     *
     * @return the upscaler
     */

    private TimeSeriesUpscaler<L> getLeftUpscaler()
    {
        return this.leftUpscaler;
    }

    /**
     * Returns the upscaler for right-ish data.
     *
     * @return the upscaler
     */

    private TimeSeriesUpscaler<R> getRightUpscaler()
    {
        return this.rightUpscaler;
    }

    /**
     * Returns the upscaler for baseline-ish data.
     *
     * @return the upscaler
     */

    private TimeSeriesUpscaler<R> getBaselineUpscaler()
    {
        return this.baselineUpscaler;
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
     * @return the left time shift
     */

    private Duration getLeftTimeShift()
    {
        return this.leftTimeShift;
    }

    /**
     * @return the right time shift
     */

    private Duration getRightTimeShift()
    {
        return this.rightTimeShift;
    }

    /**
     * @return the baseline time shift
     */

    private Duration getBaselineTimeShift()
    {
        return this.baselineTimeShift;
    }

    /**
     * @return the pair frequency
     */

    private Duration getPairFrequency()
    {
        return this.pairFrequency;
    }

    /**
     * Returns the retriever factory.
     *
     * @return the retriever factory
     */

    private RetrieverFactory<L, R, B> getRetrieverFactory()
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

    private Function<Set<Feature>, BaselineGenerator<R>> getBaselineGenerator()
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
     * @param metadata the metadata
     * @param featureGetter the feature-getter
     * @return the features from the metadata using the prescribed feature-getter
     * @throws NullPointerException if the metadata is null
     */

    private Set<Feature> getFeatures( PoolMetadata metadata,
                                      Function<FeatureTuple, Feature> featureGetter )
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

    private Set<Feature> getFeatures( Function<FeatureTuple, Feature> featureGetter )
    {
        Objects.requireNonNull( featureGetter );
        return this.getPoolRequests()
                   .stream()
                   .flatMap( next -> next.getMetadata().getFeatureTuples().stream() )
                   .map( featureGetter )
                   .collect( Collectors.toSet() );

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
                                                              .toList();

            TimeSeriesMetadata existingMetadata;

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
                        throw new IllegalArgumentException( "A climatological time-series required upscaling from "
                                                            + nextScale
                                                            + " to "
                                                            + desiredTimeScale
                                                            + ", but no upscaler was provided. The time-series "
                                                            + "metadata was: "
                                                            + nextSeries.getMetadata() );
                    }

                    String desiredUnit = this.getProject()
                                             .getMeasurementUnit();

                    RescaledTimeSeriesPlusValidation<L> rescaled = upscaler.upscale( nextSeries,
                                                                                     desiredTimeScale,
                                                                                     desiredUnit );

                    nextSeries = rescaled.getTimeSeries();

                    RescaledTimeSeriesPlusValidation.logScaleValidationWarnings( nextSeries,
                                                                                 rescaled.getValidationEvents() );

                    LOGGER.debug( "Upscaled a climatological time-series from {} to {}. The time-series metadata was: "
                                  + "{}.",
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

            this.validateUpscaledClimatology( climData, returnMe, desiredTimeScale );

            LOGGER.debug( "Created {} climatological time-series.", returnMe.size() );

            return returnMe.stream();
        };
    }

    /**
     * Verifies that one or more event values was produced when upscaling the climatology, given one or more event 
     * values prior to upscaling, and throws an exception if upscaling failed to produce any values.
     *
     * @param upscaled the upscaled time-series to check for events
     * @param unscaled the unscaled time-series to check for events
     * @param desiredTimeScale the desired time scale
     * @throws PoolException if no upscaled values were produced 
     */

    private void validateUpscaledClimatology( List<TimeSeries<L>> unscaled,
                                              List<TimeSeries<L>> upscaled,
                                              TimeScaleOuter desiredTimeScale )
    {
        int unscaledCount = unscaled.stream()
                                    .mapToInt( next -> next.getEvents().size() )
                                    .sum();

        int upscaledCount = upscaled.stream()
                                    .mapToInt( next -> next.getEvents().size() )
                                    .sum();

        if ( unscaledCount > 0 && upscaledCount == 0 )
        {
            throw new PoolException( "Failed to produce any upscaled event values for the climatological time-series, "
                                     + "despite encountering "
                                     + unscaledCount
                                     + " event values to upscale. Please check that the climatological time-series can "
                                     + "be upscaled to the desired time scale of "
                                     + desiredTimeScale
                                     + ". In general, upscaling can only be performed if there are sufficient event "
                                     + "values within each upscaled period (two or more) and they are equally spaced." );
        }

        LOGGER.debug( "Encountered {} events in the climatological time-series prior to upscaling and {} afterwards.",
                      unscaledCount,
                      upscaledCount );
    }

    /**
     * @param existingTimeScale the existing time scale
     * @param desiredTimeScale the desired time scale
     * @return true if both scales are not null and the desired time scale has a larger period
     */

    private boolean shouldUpscaleClimatology( TimeScaleOuter existingTimeScale, TimeScaleOuter desiredTimeScale )
    {
        return Objects.nonNull( existingTimeScale ) && Objects.nonNull( desiredTimeScale )
               && !existingTimeScale.equalsOrInstantaneous( desiredTimeScale );
    }

    /**
     * Creates the climatological data as needed.
     *
     * @param climatology the time-series supplier for the climatological data
     * @param climatologyMapper the mapper to transform the climatology from left-ish data to double values
     * @return the climatological data or null if no climatology is defined
     * @throws NullPointerException if the climatology is not null and the climateMapper is null
     */

    private Supplier<Climatology> createClimatology( Supplier<Stream<TimeSeries<L>>> climatology,
                                                     ToDoubleFunction<L> climatologyMapper )
    {
        return () -> {
            // Null if no climatology
            if ( Objects.isNull( climatology ) )
            {
                return null;
            }

            Objects.requireNonNull( climatologyMapper );

            Function<L, Double> mapper = climatologyMapper::applyAsDouble;
            List<TimeSeries<Double>> climData = climatology.get()
                                                           .map( next -> TimeSeriesSlicer.transform( next,
                                                                                                     mapper,
                                                                                                     null ) )
                                                           .toList();

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Discovered {} climatological time-series when generating the pools.",
                              climData.size() );
            }

            return Climatology.of( climData );
        };
    }

    /**
     * A baseline shim to map from baseline-ish to right-ish data.
     * @return the baseline shim
     */

    private Function<TimeSeries<B>,TimeSeries<R>> getBaselineShim()
    {
        return this.baselineShim;
    }

    /**
     * Hidden constructor.
     *
     * @param builder a builder
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the declaration is inconsistent with the type of pool expected
     */

    private PoolsGenerator( Builder<L, R, B> builder )
    {
        // Set then validate
        this.project = builder.project;
        this.retrieverFactory = builder.retrieverFactory;
        this.poolRequests = List.copyOf( builder.poolRequests );
        this.baselineGenerator = builder.baselineGenerator;
        this.pairer = builder.pairer;
        this.leftUpscaler = builder.leftUpscaler;
        this.rightUpscaler = builder.rightUpscaler;
        this.baselineUpscaler = builder.baselineUpscaler;
        this.climateAdmissibleValue = builder.climateAdmissibleValue;
        this.leftTransformer = builder.leftTransformer;
        this.rightTransformer = builder.rightTransformer;
        this.baselineTransformer = builder.baselineTransformer;
        this.leftFilter = builder.leftFilter;
        this.rightFilter = builder.rightFilter;
        this.baselineFilter = builder.baselineFilter;
        this.climateMapper = builder.climateMapper;
        this.crossPairer = builder.crossPairer;
        this.leftTimeShift = builder.leftTimeShift;
        this.rightTimeShift = builder.rightTimeShift;
        this.baselineTimeShift = builder.baselineTimeShift;
        this.pairFrequency = builder.pairFrequency;
        this.baselineShim = builder.baselineShim;

        String messageStart = "Cannot build the pool generator: ";

        Objects.requireNonNull( this.project, messageStart + "the project is missing." );
        Objects.requireNonNull( this.retrieverFactory, messageStart + "the retriever factory is missing." );

        Objects.requireNonNull( this.pairer, messageStart + "the pairer is missing." );
        Objects.requireNonNull( this.leftUpscaler, messageStart + "the upscaler for left values is missing" );
        Objects.requireNonNull( this.rightUpscaler, messageStart + "the upscaler for right values is missing." );
        Objects.requireNonNull( this.baselineUpscaler, messageStart + "the upscaler for baseline values is missing." );
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
    }
}
