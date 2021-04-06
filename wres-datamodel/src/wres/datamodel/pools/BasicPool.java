package wres.datamodel.pools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.time.TimeSeries;

/**
 * A minimal implementation of a {@link Pool} that does not provide a time-series view of the values, i.e. the 
 * {@link Pool#get()} throws an {@link UnsupportedOperationException}.
 * 
 * @param <T> the data type
 * @author james.brown@hydrosolved.com
 */
public class BasicPool<T> implements Pool<T>
{

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

    private VectorOfDoubles climatology;

    @Override
    public boolean hasBaseline()
    {
        return Objects.nonNull( this.baselineSampleData );
    }

    @Override
    public boolean hasClimatology()
    {
        return Objects.nonNull( this.climatology );
    }

    @Override
    public List<T> getRawData()
    {
        return sampleData;
    }

    @Override
    public PoolMetadata getMetadata()
    {
        return mainMeta;
    }

    @Override
    public VectorOfDoubles getClimatology()
    {
        return climatology;
    }

    @Override
    public Pool<T> getBaselineData()
    {
        // TODO: return an empty baseline in all cases.
        if ( !this.hasBaseline() )
        {
            return null;
        }

        Builder<T> builder = new Builder<>();

        return builder.addData( this.baselineSampleData )
                      .setClimatology( this.climatology )
                      .setMetadata( this.baselineMeta )
                      .build();
    }

    @Override
    public String toString()
    {
        StringJoiner join = new StringJoiner( System.lineSeparator() );
        join.add( "Main data:" );
        sampleData.forEach( a -> join.add( a.toString() ) );
        if ( this.hasBaseline() )
        {
            join.add( "" ).add( "Baseline data:" );
            baselineSampleData.forEach( a -> join.add( a.toString() ) );
        }
        if ( this.hasClimatology() )
        {
            join.add( "" ).add( "Climatology:" );
            for ( Double next : climatology.getDoubles() )
            {
                join.add( next.toString() );
            }
        }
        return join.toString();
    }

    @Override
    public List<TimeSeries<T>> get()
    {
        throw new UnsupportedOperationException( "A BasicPool does not provide a time-series view of the data." );
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

    public static <T> BasicPool<T> of( List<T> sampleData, PoolMetadata meta )
    {
        Builder<T> builder = new Builder<>();

        return builder.addData( sampleData )
                      .setMetadata( meta )
                      .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof BasicPool ) )
        {
            return false;
        }

        BasicPool<?> input = (BasicPool<?>) o;

        boolean returnMe =
                input.getRawData().equals( this.getRawData() ) && input.getMetadata().equals( this.getMetadata() );

        returnMe = returnMe && input.hasClimatology() == this.hasClimatology()
                   && input.hasBaseline() == this.hasBaseline();

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
        return Objects.hash( this.getRawData(),
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

    public static <T> BasicPool<T> of( List<T> sampleData,
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
        List<T> sampleData = new ArrayList<>();

        /**
         * Pairs for baseline.
         */
        List<T> baselineSampleData = new ArrayList<>();

        /**
         * Climatology.
         */

        VectorOfDoubles climatology;

        /**
         * Metadata for input.
         */

        PoolMetadata mainMeta;

        /**
         * Metadata for baseline.
         */

        PoolMetadata baselineMeta;

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
            Objects.requireNonNull( sampleData );

            this.sampleData.addAll( sampleData );

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
            Objects.requireNonNull( baselineSampleData );

            this.baselineSampleData.addAll( baselineSampleData );

            return this;
        }

        /**
         * Adds a dataset to the builder.
         * 
         * @param data the data to add
         * @return the builder
         * @throws NullPointerException if the input is null
         */

        public Builder<T> addData( Pool<T> data )
        {
            Objects.requireNonNull( data );

            this.sampleData.addAll( data.getRawData() );
            this.mainMeta = data.getMetadata();
            this.climatology = data.getClimatology();

            if ( data.hasBaseline() )
            {
                Pool<T> base = data.getBaselineData();
                this.baselineSampleData.addAll( base.getRawData() );
                this.baselineMeta = base.getMetadata();
            }

            return this;
        }

        /**
         * Builds the metric input.
         * 
         * @return the metric input
         */

        public BasicPool<T> build()
        {
            return new BasicPool<>( this );
        }

    }

    /**
     * Construct with a builder.
     * 
     * @param b the builder
     * @throws PoolException if the pairs are invalid
     */

    BasicPool( Builder<T> b )
    {
        //Ensure safe types
        this.sampleData = Collections.unmodifiableList( b.sampleData );
        this.mainMeta = b.mainMeta;
        this.climatology = b.climatology;

        // Always set baseline metadata because null-status is validated
        this.baselineMeta = b.baselineMeta;

        // Baseline data?
        if ( Objects.nonNull( b.baselineMeta ) )
        {
            this.baselineSampleData = Collections.unmodifiableList( b.baselineSampleData );
        }
        else
        {
            this.baselineSampleData = null;
        }

        //Validate
        this.validateMainInput();
        this.validateBaselineInput();
        this.validateClimatologicalInput();
    }

    /**
     * Validates the main pairs and associated metadata after the constructor has copied it.
     * 
     * @throws PoolException if the input is invalid
     */

    private void validateMainInput()
    {

        if ( Objects.isNull( mainMeta ) )
        {
            throw new PoolException( "Specify non-null metadata for the metric input." );
        }

        if ( Objects.isNull( sampleData ) )
        {
            throw new PoolException( "Specify a non-null dataset for the metric input." );
        }

        if ( sampleData.contains( (T) null ) )
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
        if ( Objects.isNull( baselineSampleData ) != Objects.isNull( baselineMeta ) )
        {
            throw new PoolException( "Specify a non-null baseline input and associated metadata or leave both "
                                     + "null. The null status of the data and metadata, respectively, is: ["
                                     + Objects.isNull( baselineSampleData )
                                     + ","
                                     + Objects.isNull( baselineMeta )
                                     + "]" );
        }

        if ( Objects.nonNull( baselineSampleData ) && baselineSampleData.contains( (T) null ) )
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
        if ( Objects.nonNull( this.getClimatology() ) && !this.getRawData().isEmpty() )
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
