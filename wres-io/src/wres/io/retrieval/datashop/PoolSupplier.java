package wres.io.retrieval.datashop;

import java.util.Objects;
import java.util.function.Supplier;

import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PairingException;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.scale.RescalingException;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesUpscaler;

/**
 * <p>Supplies a {@link PoolOfPairs}, which is used to compute one or more verification statistics. The overall 
 * responsibility of the {@link PoolSupplier} is to supply a {@link PoolOfPairs} on request. This is fulfilled by
 * completing several smaller activities in sequence, namely:</p> 
 * 
 * <ol>
 * <li>Consuming the (possibly pool-shaped) left/right/baseline data for pairing, which is supplied by retrievers;</li>
 * <li>Rescaling the data, where needed, so that pairs can be formed at the desired time scale;</li>
 * <li>Creating pairs;</li>
 * <li>Trimming pairs to the pool boundaries; and</li>
 * <li>Supplying the pool-shaped pairs.</li>
 * </ol>
 * 
 * @author james.brown@hydrosolved.com
 * @param <L> the type of left value in each pair
 * @param <R> the type of right value in each pair and, where applicable, the type of baseline value
 */

public class PoolSupplier<L, R> implements Supplier<PoolOfPairs<L, R>>
{

    /**
     * Climatological data source.
     */

    private final SupplyOrRetrieve<TimeSeries<L>> climatology;

    /**
     * Left data source. Optional, unless the climatological source is undefined.
     */

    private final SupplyOrRetrieve<TimeSeries<L>> left;

    /**
     * Right data source.
     */

    private final SupplyOrRetrieve<TimeSeries<R>> right;

    /**
     * Baseline data source. Optional.
     */

    private final SupplyOrRetrieve<TimeSeries<R>> baseline;

    /**
     * Pairer.
     */

    private final TimeSeriesPairer<L, R> pairer;

    /**
     * Upscaler for left-type data. Optional on construction, but may be exceptional if later required.
     */

    private final TimeSeriesUpscaler<L> leftUpscaler;

    /**
     * Upscaler for right-type data. Optional on construction, but may be exceptional if later required.
     */

    private final TimeSeriesUpscaler<R> rightUpscaler;

    /**
     * The desired time scale.
     */

    private final TimeScale desiredTimeScale;

    /**
     * Metadata for the mains pairs.
     */

    private final SampleMetadata metadata;

    /**
     * Metadata for the baseline pairs.
     */

    private final SampleMetadata baselineMetadata;

    /**
     * Returns a {@link PoolOfPairs} for metric calculation. Callers should cache the output for re-use. Each call of
     * this method initiates a separate retrieval.
     * 
     * @return a pool of pairs
     * @throws DataAccessException if the pool data could not be retrieved
     * @throws RescalingException if the pool data could not be rescaled
     * @throws PairingException if the pool data could not be paired
     * @throws NoSuchUnitConversionException if the data units could not be converted
     */

    @Override
    public PoolOfPairs<L, R> get()
    {
        // Create the main pairs

        // Create the baseline pairs

        // Create the climatology

        // Filter the pairs against the pool boundaries in the metadata
        
        // Create the pool

        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Builder for a {@link PoolSupplier}.
     * 
     * @author james.brown@hydrosolved.com
     * @param <L> the left type of paired value
     * @param <R> the right type of paired value and, where required, the baseline type
     */

    public static class PoolSupplierBuilder<L, R>
    {

        /**
         * Climatological data source. Optional.
         */

        private SupplyOrRetrieve<TimeSeries<L>> climatology;

        /**
         * Left data source. Optional, unless the climatological source is undefined.
         */

        private SupplyOrRetrieve<TimeSeries<L>> left;

        /**
         * Right data source.
         */

        private SupplyOrRetrieve<TimeSeries<R>> right;

        /**
         * Baseline data source. Optional.
         */

        private SupplyOrRetrieve<TimeSeries<R>> baseline;

        /**
         * Pairer.
         */

        private TimeSeriesPairer<L, R> pairer;

        /**
         * Upscaler for left-type data. Optional on construction, but may be exceptional if later required.
         */

        private TimeSeriesUpscaler<L> leftUpscaler;

        /**
         * Upscaler for right-type data. Optional on construction, but may be exceptional if later required.
         */

        private TimeSeriesUpscaler<R> rightUpscaler;

        /**
         * The desired time scale.
         */

        private TimeScale desiredTimeScale;

        /**
         * Metadata for the mains pairs.
         */

        private SampleMetadata metadata;

        /**
         * Metadata for the baseline pairs.
         */

        private SampleMetadata baselineMetadata;

        /**
         * @param climatology the climatology to set
         * @return the builder
         */
        public PoolSupplierBuilder<L, R> setClimatology( SupplyOrRetrieve<TimeSeries<L>> climatology )
        {
            this.climatology = climatology;

            return this;
        }

        /**
         * @param left the left to set
         * @return the builder
         */
        public PoolSupplierBuilder<L, R> setLeft( SupplyOrRetrieve<TimeSeries<L>> left )
        {
            this.left = left;

            return this;
        }

        /**
         * @param right the right to set
         * @return the builder
         */
        public PoolSupplierBuilder<L, R> setRight( SupplyOrRetrieve<TimeSeries<R>> right )
        {
            this.right = right;

            return this;
        }

        /**
         * @param baseline the baseline to set
         * @return the builder
         */
        public PoolSupplierBuilder<L, R> setBaseline( SupplyOrRetrieve<TimeSeries<R>> baseline )
        {
            this.baseline = baseline;

            return this;
        }

        /**
         * @param pairer the pairer to set
         * @return the builder
         */
        public PoolSupplierBuilder<L, R> setPairer( TimeSeriesPairer<L, R> pairer )
        {
            this.pairer = pairer;

            return this;
        }

        /**
         * @param leftUpscaler the leftUpscaler to set
         * @return the builder
         */
        public PoolSupplierBuilder<L, R> setLeftUpscaler( TimeSeriesUpscaler<L> leftUpscaler )
        {
            this.leftUpscaler = leftUpscaler;

            return this;
        }

        /**
         * @param rightUpscaler the rightUpscaler to set
         * @return the builder
         */
        public PoolSupplierBuilder<L, R> setRightUpscaler( TimeSeriesUpscaler<R> rightUpscaler )
        {
            this.rightUpscaler = rightUpscaler;

            return this;
        }

        /**
         * @param desiredTimeScale the desiredTimeScale to set
         * @return the builder
         */
        public PoolSupplierBuilder<L, R> setDesiredTimeScale( TimeScale desiredTimeScale )
        {
            this.desiredTimeScale = desiredTimeScale;

            return this;
        }

        /**
         * @param metadata the metadata to set
         * @return the builder
         */
        public PoolSupplierBuilder<L, R> setMetadata( SampleMetadata metadata )
        {
            this.metadata = metadata;

            return this;
        }

        /**
         * @param baselineMetadata the baselineMetadata to set
         * @return the builder
         */
        public PoolSupplierBuilder<L, R> setBaselineMetadata( SampleMetadata baselineMetadata )
        {
            this.baselineMetadata = baselineMetadata;

            return this;
        }

        /**
         * Builds a {@link PoolSupplier}.
         * 
         * @return a pool supplier
         */

        public PoolSupplier<L, R> build()
        {
            return new PoolSupplier<>( this );
        }
    }


    /**
     * Hidden constructor.  
     * 
     * @param builder the builder
     * @throws NullPointerException if a required input is null
     */

    private PoolSupplier( PoolSupplierBuilder<L, R> builder )
    {
        // Set
        this.climatology = builder.climatology;
        this.left = builder.left;
        this.right = builder.right;
        this.baseline = builder.baseline;
        this.pairer = builder.pairer;
        this.leftUpscaler = builder.leftUpscaler;
        this.rightUpscaler = builder.rightUpscaler;
        this.desiredTimeScale = builder.desiredTimeScale;
        this.metadata = builder.metadata;
        this.baselineMetadata = builder.baselineMetadata;

        // Validate
        String messageStart = "Cannot build the pool retriever: ";

        Objects.requireNonNull( this.right, messageStart + "add a right data source." );

        Objects.requireNonNull( this.pairer, messageStart + "add a pairer, in order to pair the data." );

        Objects.requireNonNull( this.desiredTimeScale, messageStart + "add the desired time scale." );

        Objects.requireNonNull( this.metadata, messageStart + "add the metadata for the main pairs." );

        // Needs a climatology source OR a left source
        if ( Objects.isNull( this.climatology ) && Objects.isNull( this.left ) )
        {
            throw new NullPointerException( messageStart + "add either a left or a climatological data source." );
        }

        // If adding a baseline, baseline metadata is needed. If not, it should not be supplied
        if ( Objects.isNull( this.baseline ) != Objects.isNull( this.baselineMetadata ) )
        {
            throw new NullPointerException( messageStart + "cannot add a baseline retriever without baseline "
                                            + "metadata and vice versa." );
        }

    }

}
