package wres.datamodel.pools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.Immutable;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;

/**
 * <p>An atomic collection of samples from which a statistic is computed using a metric. The samples may comprise paired 
 * or unpaired values. Optionally, it may contain a baseline dataset to be used in the same context (e.g. for skill 
 * scores) and a climatological dataset, which is used to derive quantiles from climatological probabilities.
 * 
 * <p>A dataset may contain values that correspond to a missing value identifier.
 * 
 * @param <T> the type of pooled data
 * @author James Brown
 */

@Immutable
public class Pool<T> implements Supplier<List<T>>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( Pool.class );

    /**
     * The verification pairs in an immutable list.
     */

    private final List<T> sampleData;

    /**
     * Metadata associated with the verification pairs.
     */

    private final PoolMetadata mainMeta;

    /**
     * The verification pairs for a baseline in an immutable list (may be null).
     */

    private final List<T> baselineSampleData;

    /**
     * Metadata associated with the baseline verification pairs (may be null).
     */

    private final PoolMetadata baselineMeta;

    /**
     * Climatological dataset. May be null.
     */

    private final VectorOfDoubles climatology;

    /**
     * Provides a view of the mini-pools from which this pool was constructed, otherwise, a view of the overall pool, 
     * i.e., the current pool, wrapped in a singleton list.
     */

    private final List<Pool<T>> miniPools;

    /**
     * Returns the pooled data.
     * 
     * @return the pooled data
     */
    public List<T> get()
    {
        return this.sampleData; // Immutable on construction
    }

    /**
     * Returns <code>true</code> if the sample has a baseline for skill calculations, <code>false</code> otherwise.
     * 
     * @return true if a baseline is defined, false otherwise
     */

    public boolean hasBaseline()
    {
        return Objects.nonNull( this.baselineSampleData );
    }

    /**
     * Returns <code>true</code> if the sample has a climatological dataset associated with it, <code>false</code> 
     * otherwise.
     * 
     * @return true if a climatological dataset is defined, false otherwise
     */

    public boolean hasClimatology()
    {
        return Objects.nonNull( this.climatology );
    }

    /**
     * Returns the metadata associated with the sample.
     * 
     * @return the metadata associated with the sample
     */

    public PoolMetadata getMetadata()
    {
        return this.mainMeta;
    }

    /**
     * Returns a climatological dataset if {@link #hasClimatology()} returns true, otherwise null.
     * 
     * @return a climatological dataset or null
     */

    public VectorOfDoubles getClimatology()
    {
        return this.climatology;
    }

    /**
     * Returns a view of the miniature pools from which this pool was constructed, else this pool wrapped in a singleton
     * list if this pool was built from raw data.
     * 
     * @return a view of the smaller pools from which this pool was built, else this pool, wrapped
     */

    public List<Pool<T>> getMiniPools()
    {
        if ( this.miniPools.isEmpty() )
        {
            return Collections.singletonList( this );
        }

        return this.miniPools; // Immutable on construction
    }

    /**
     * Returns the baseline data as a {@link Pool} or null if no baseline is defined.
     * 
     * @return the baseline
     */

    public Pool<T> getBaselineData()
    {
        if ( !this.hasBaseline() )
        {
            return null;
        }

        Builder<T> builder = new Builder<T>().setMetadata( this.baselineMeta );

        // Preserve the mini-pool view of the data
        List<Pool<T>> miniPools = this.getMiniPools();
        
        for( Pool<T> next : miniPools )
        {
            Pool<T> nextBaseline = new Builder<T>().setMetadata( next.baselineMeta )
                                                   .addData( next.baselineSampleData )
                                                   .setClimatology( next.climatology )
                                                   .build();
                        
            builder.addPool( nextBaseline, false );
        }
        
        return builder.build();
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "mainMetadata",
                                                                                     this.getMetadata() )
                                                                            .append( "mainData", this.get() )
                                                                            .append( "baselineMetadata",
                                                                                     this.baselineMeta )
                                                                            .append( "baselineData",
                                                                                     this.baselineSampleData )
                                                                            .append( "climatology",
                                                                                     this.getClimatology() )
                                                                            .toString();
    }

    /**
     * Construct an instance.
     * 
     * @param <T> the type of data
     * @param sampleData the data
     * @param meta the metadata
     * @return the sample data
     * @throws PoolException if the inputs are invalid
     */

    public static <T> Pool<T> of( List<T> sampleData, PoolMetadata meta )
    {
        Builder<T> builder = new Builder<>();

        return builder.addData( sampleData )
                      .setMetadata( meta )
                      .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof Pool ) )
        {
            return false;
        }

        if ( o == this )
        {
            return true;
        }

        // The mini-pools represent a view of the underlying data and are not part of a test for equality.
        Pool<?> input = (Pool<?>) o;

        boolean returnMe = input.hasClimatology() == this.hasClimatology()
                           && input.hasBaseline() == this.hasBaseline()
                           && input.getMetadata().equals( this.getMetadata() );

        if ( !returnMe )
        {
            return false;
        }

        // Start checking the actual data
        returnMe = returnMe && input.get().equals( this.get() );

        if ( !returnMe )
        {
            return false;
        }

        // Check the actual data        
        if ( this.hasClimatology() )
        {
            returnMe = returnMe && input.getClimatology().equals( this.getClimatology() );
        }

        if ( this.hasBaseline() )
        {
            // No need to check the composed type of baseline
            returnMe = returnMe && input.baselineSampleData.equals( this.baselineSampleData )
                       && input.baselineMeta.equals( this.baselineMeta );
        }
        
        return returnMe;
    }

    @Override
    public int hashCode()
    {
        // The mini-pools represent a view of the underlying data and are not part of the hash of the object.
        return Objects.hash( this.get(),
                             this.getMetadata(),
                             this.baselineSampleData,
                             this.baselineMeta,
                             this.getClimatology() );
    }

    /**
     * Construct an instance.
     * 
     * @param <T> the type of data
     * @param sampleData the data
     * @param sampleMeta the sample metadata
     * @param baselineData the baseline data
     * @param baselineMeta the baseline metadata
     * @param climatology the climatological data
     * @return the sample data
     * @throws PoolException if the inputs are invalid
     */

    public static <T> Pool<T> of( List<T> sampleData,
                                  PoolMetadata sampleMeta,
                                  List<T> baselineData,
                                  PoolMetadata baselineMeta,
                                  VectorOfDoubles climatology )
    {
        Builder<T> builder = new Builder<>();

        return builder.addData( sampleData )
                      .setMetadata( sampleMeta )
                      .addDataForBaseline( baselineData )
                      .setMetadataForBaseline( baselineMeta )
                      .setClimatology( climatology )
                      .build();
    }

    /**
     * A builder to build the metric input.
     */

    public static class Builder<T>
    {

        /**
         * Pairs.
         */
        private final List<T> sampleData = new ArrayList<>();

        /**
         * Pairs for baseline.
         */
        private final List<T> baselineSampleData = new ArrayList<>();

        /**
         * Mini-pools if this pool was built from smaller pools.
         */

        private final List<Pool<T>> miniPools = new ArrayList<>();

        /**
         * Climatology.
         */

        private VectorOfDoubles climatology;

        /**
         * Metadata for input.
         */

        private PoolMetadata mainMeta;

        /**
         * Metadata for baseline.
         */

        private PoolMetadata baselineMeta;

        /**
         * Sets the metadata associated with the input.
         * 
         * @param mainMeta the metadata
         * @return the builder
         */

        public Builder<T> setMetadata( PoolMetadata mainMeta )
        {
            this.mainMeta = mainMeta;
            return this;
        }

        /**
         * Sets the metadata associated with the baseline input.
         * 
         * @param baselineMeta the metadata for the baseline
         * @return the builder
         */

        public Builder<T> setMetadataForBaseline( PoolMetadata baselineMeta )
        {
            this.baselineMeta = baselineMeta;

            return this;
        }

        /**
         * Sets a climatological dataset for the input.
         * 
         * @param climatology the climatology
         * @return the builder
         */

        public Builder<T> setClimatology( VectorOfDoubles climatology )
        {
            this.climatology = climatology;

            return this;
        }

        /**
         * Adds sample data, appending to any existing sample data, as necessary.
         * 
         * @param sample the sample data
         * @return the builder
         * @throws NullPointerException if the input is null
         */

        public Builder<T> addData( T sample )
        {
            this.sampleData.add( sample );

            return this;
        }

        /**
         * Adds sample data for a baseline, which is used to calculate skill, appending to any existing baseline sample, as
         * necessary.
         * 
         * @param baselineSample the sample data for the baseline
         * @return the builder
         * @throws NullPointerException if the input is null
         */

        public Builder<T> addDataForBaseline( T baselineSample )
        {
            this.baselineSampleData.add( baselineSample );

            return this;
        }

        /**
         * Adds sample data, appending to any existing sample data, as necessary.
         * 
         * @param sampleData the sample data
         * @return the builder
         */

        public Builder<T> addData( List<T> sampleData )
        {
            if ( Objects.nonNull( sampleData ) )
            {
                this.sampleData.addAll( sampleData );
            }

            return this;
        }

        /** 
        * Adds sample data for a baseline, which is used to calculate skill, appending to any existing baseline sample, 
        * as necessary.
        * 
        * @param baselineSampleData the sample data for the baseline
        * @return the builder
        */

        public Builder<T> addDataForBaseline( List<T> baselineSampleData )
        {
            if ( Objects.nonNull( baselineSampleData ) )
            {
                this.baselineSampleData.addAll( baselineSampleData );
            }

            return this;
        }

        /**
         * Adds a pool of pairs to the builder.
         * 
         * @param pool the pool to add
         * @param mergeClimatology is true to merge the climatology of the input with any existing climatology
         * @return the builder
         * @throws NullPointerException if the input is null
         */

        public Builder<T> addPool( Pool<T> pool, boolean mergeClimatology )
        {
            Objects.requireNonNull( pool, "Cannot add a null pool to the builder." );

            LOGGER.debug( "Adding pool {} to the builder.", pool.getMetadata() );

            this.miniPools.add( pool );
            this.sampleData.addAll( pool.get() );

            // Merge metadata?
            if ( Objects.nonNull( this.mainMeta ) )
            {
                LOGGER.debug( "Merging metadata for pool {} into pool {}.", pool.getMetadata(), this.mainMeta );

                this.mainMeta = PoolSlicer.unionOf( List.of( this.mainMeta, pool.getMetadata() ) );
            }
            else
            {
                this.mainMeta = pool.getMetadata();
            }

            // Merge climatology?
            if ( mergeClimatology && Objects.nonNull( this.climatology ) && pool.hasClimatology() )
            {
                LOGGER.debug( "Merging climatology for pool {} into pool {}.", pool.getMetadata(), this.mainMeta );

                this.climatology = Slicer.concatenate( this.climatology, pool.getClimatology() );
            }
            else
            {
                this.climatology = pool.getClimatology();
            }

            if ( pool.hasBaseline() )
            {
                Pool<T> base = pool.getBaselineData();
                this.baselineSampleData.addAll( base.get() );

                // Merge metadata?
                if ( Objects.nonNull( this.baselineMeta ) )
                {
                    LOGGER.debug( "Merging metadata for baseline pool {} into baseline pool {}.",
                                  base.getMetadata(),
                                  this.baselineMeta );

                    this.baselineMeta = PoolSlicer.unionOf( List.of( this.baselineMeta,
                                                                     base.getMetadata() ) );
                }
                else
                {
                    this.baselineMeta = base.getMetadata();
                }
            }

            return this;
        }

        /**
         * Builds the metric input.
         * 
         * @return the metric input
         */

        public Pool<T> build()
        {
            return new Pool<>( this );
        }

    }

    /**
     * Construct with a builder.
     * 
     * @param b the builder
     * @throws PoolException if the pairs are invalid
     */

    private Pool( Builder<T> b )
    {
        //Ensure safe types
        this.sampleData = Collections.unmodifiableList( new ArrayList<>( b.sampleData ) );
        
        // If there is only one mini-pool, elide. The mini-pools view adds a reference for each data item and the 
        // default view is a single pool, so there is no need for the duplicate references.
        List<Pool<T>> miniPoolsInner = Collections.unmodifiableList( new ArrayList<>( b.miniPools ) );
        if( miniPoolsInner.size() == 1 )
        {
            this.miniPools = Collections.emptyList();   
        }
        else
        {
            this.miniPools = miniPoolsInner;
        }
        
        this.mainMeta = b.mainMeta;
        this.climatology = b.climatology;

        // Always set baseline metadata because null-status is validated
        this.baselineMeta = b.baselineMeta;

        // Baseline data? If metadata supplied or some data supplied, yes.
        if ( Objects.nonNull( this.baselineMeta ) || !b.baselineSampleData.isEmpty() )
        {
            this.baselineSampleData = Collections.unmodifiableList( new ArrayList<>( b.baselineSampleData ) );
        }
        else
        {
            this.baselineSampleData = null;
        }

        //Validate
        this.validateClimatologicalInput();
        this.validateMainInput();
        this.validateBaselineInput();
    }

    /**
     * Validates the main pairs and associated metadata after the constructor has copied it.
     * 
     * @throws PoolException if the input is invalid
     */

    private void validateMainInput()
    {
        if ( Objects.isNull( this.mainMeta ) )
        {
            throw new PoolException( "Specify non-null metadata for the metric input." );
        }

        if ( this.sampleData.contains( (T) null ) )
        {
            throw new PoolException( "One or more of the pairs is null." );
        }
    }

    /**
     * Validates the baseline pairs and associated metadata after the constructor has copied it.
     * 
     * @throws PoolException if the baseline input is invalid
     */

    private void validateBaselineInput()
    {
        if ( Objects.isNull( this.baselineSampleData ) != Objects.isNull( this.baselineMeta ) )
        {
            throw new PoolException( "Specify a non-null baseline input and associated metadata or leave both "
                                     + "null. The null status of the data and metadata, respectively, is: ["
                                     + Objects.isNull( this.baselineSampleData )
                                     + ","
                                     + Objects.isNull( this.baselineMeta )
                                     + "]" );
        }

        if ( Objects.nonNull( this.baselineSampleData ) && this.baselineSampleData.contains( (T) null ) )
        {
            throw new PoolException( "One or more of the baseline pairs is null." );
        }
    }

    /**
     * Validates the climatological input after the constructor has copied it.
     * 
     * @throws PoolException if the climatological input is invalid
     */

    private void validateClimatologicalInput()
    {
        // #65881: if a climatology is provided, it cannot be empty when some pairs exist
        if ( Objects.nonNull( this.getClimatology() ) && !this.get().isEmpty() )
        {
            if ( this.getClimatology().size() == 0 )
            {
                throw new PoolException( "Cannot build the paired data with an empty climatology: add one or "
                                         + "more values." );
            }

            if ( !Arrays.stream( this.getClimatology().getDoubles() ).anyMatch( Double::isFinite ) )
            {
                throw new PoolException( "Must have at least one non-missing value in the climatological "
                                         + "input" );
            }
        }
    }
}
