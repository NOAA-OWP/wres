package wres.datamodel.sampledata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.VectorOfDoubles;

/**
 * A minimal implementation of a {@link SampleData}.
 * 
 * @param <T> the data type
 * @author james.brown@hydrosolved.com
 */
public class SampleDataBasic<T> implements SampleData<T>
{

    /**
     * The verification pairs in an immutable list.
     */

    private final List<T> sampleData;

    /**
     * Metadata associated with the verification pairs.
     */

    private final SampleMetadata mainMeta;

    /**
     * The verification pairs for a baseline in an immutable list (may be null).
     */

    private final List<T> baselineSampleData;

    /**
     * Metadata associated with the baseline verification pairs (may be null).
     */

    private final SampleMetadata baselineMeta;

    /**
     * Climatological dataset. May be null.
     */

    private VectorOfDoubles climatology;

    @Override
    public List<T> getRawData()
    {
        return sampleData;
    }

    @Override
    public SampleMetadata getMetadata()
    {
        return mainMeta;
    }

    @Override
    public VectorOfDoubles getClimatology()
    {
        return climatology;
    }

    @Override
    public SampleData<T> getBaselineData()
    {
        // TODO: override hasBaseline to check the actual data
        // and then return an empty baseline in all cases. The check
        // should be the formal check. Returning a null from an API method
        // is coding graffiti

        if ( Objects.isNull( this.baselineSampleData ) )
        {
            return null;
        }

        SampleDataBuilder<T> builder = new SampleDataBasicBuilder<>();

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

    /**
     * Construct an instance.
     * 
     * @param <T> the type of data
     * @param sampleData the data
     * @param meta the metadata
     * @return the sample data
     * @throws SampleDataException if the inputs are invalid
     */

    public static <T> SampleDataBasic<T> of( List<T> sampleData, SampleMetadata meta )
    {
        SampleDataBasicBuilder<T> builder = new SampleDataBasicBuilder<>();

        return builder.addData( sampleData )
                      .setMetadata( meta )
                      .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof SampleDataBasic ) )
        {
            return false;
        }

        SampleDataBasic<?> input = (SampleDataBasic<?>) o;

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
     * @throws SampleDataException if the inputs are invalid
     */

    public static <T> SampleDataBasic<T> of( List<T> sampleData,
                                             SampleMetadata sampleMeta,
                                             List<T> baselineData,
                                             SampleMetadata baselineMeta,
                                             VectorOfDoubles climatology )
    {
        SampleDataBasicBuilder<T> builder = new SampleDataBasicBuilder<>();

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

    public static class SampleDataBasicBuilder<T> implements SampleDataBuilder<T>
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

        SampleMetadata mainMeta;

        /**
         * Metadata for baseline.
         */

        SampleMetadata baselineMeta;

        /**
         * Sets the metadata associated with the input.
         * 
         * @param mainMeta the metadata
         * @return the builder
         */

        public SampleDataBasicBuilder<T> setMetadata( SampleMetadata mainMeta )
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

        public SampleDataBasicBuilder<T> setMetadataForBaseline( SampleMetadata baselineMeta )
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

        public SampleDataBasicBuilder<T> setClimatology( VectorOfDoubles climatology )
        {
            this.climatology = climatology;

            return this;
        }

        @Override
        public SampleDataBasicBuilder<T> addData( T mainInput )
        {
            this.sampleData.add( mainInput );

            return this;
        }

        @Override
        public SampleDataBasicBuilder<T> addDataForBaseline( T baselineInput )
        {
            this.baselineSampleData.add( baselineInput );

            return this;
        }

        @Override
        public SampleDataBasicBuilder<T> addData( List<T> sampleData )
        {
            Objects.requireNonNull( sampleData );

            this.sampleData.addAll( sampleData );

            return this;
        }

        @Override
        public SampleDataBasicBuilder<T> addDataForBaseline( List<T> baselineSampleData )
        {
            Objects.requireNonNull( baselineSampleData );

            this.baselineSampleData.addAll( baselineSampleData );

            return this;
        }

        @Override
        public SampleDataBasic<T> build()
        {
            return new SampleDataBasic<>( this );
        }

    }

    /**
     * Construct with a builder.
     * 
     * @param b the builder
     * @throws SampleDataException if the pairs are invalid
     */

    SampleDataBasic( SampleDataBasicBuilder<T> b )
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
     * @throws SampleDataException if the input is invalid
     */

    private void validateMainInput()
    {

        if ( Objects.isNull( mainMeta ) )
        {
            throw new SampleDataException( "Specify non-null metadata for the metric input." );
        }

        if ( Objects.isNull( sampleData ) )
        {
            throw new SampleDataException( "Specify a non-null dataset for the metric input." );
        }

        if ( sampleData.contains( (T) null ) )
        {
            throw new SampleDataException( "One or more of the pairs is null." );
        }

    }

    /**
     * Validates the baseline pairs and associated metadata after the constructor has copied it.
     * 
     * @throws SampleDataException if the baseline input is invalid
     */

    private void validateBaselineInput()
    {
        if ( Objects.isNull( baselineSampleData ) != Objects.isNull( baselineMeta ) )
        {
            throw new SampleDataException( "Specify a non-null baseline input and associated metadata or leave both "
                                           + "null. The null status of the data and metadata, respectively, is: ["
                                           + Objects.isNull( baselineSampleData )
                                           + ","
                                           + Objects.isNull( baselineMeta )
                                           + "]" );
        }

        if ( Objects.nonNull( baselineSampleData ) && baselineSampleData.contains( (T) null ) )
        {
            throw new SampleDataException( "One or more of the baseline pairs is null." );
        }
    }

    /**
     * Validates the climatological input after the constructor has copied it.
     * 
     * @throws SampleDataException if the climatological input is invalid
     */

    private void validateClimatologicalInput()
    {
        // #65881: if a climatology is provided, it cannot be empty when some pairs exist
        if ( Objects.nonNull( this.getClimatology() ) && !this.getRawData().isEmpty() )
        {
            if ( this.getClimatology().size() == 0 )
            {
                throw new SampleDataException( "Cannot build the paired data with an empty climatology: add one or "
                                               + "more values." );
            }

            if ( !Arrays.stream( this.getClimatology().getDoubles() ).anyMatch( Double::isFinite ) )
            {
                throw new SampleDataException( "Must have at least one non-missing value in the climatological "
                                               + "input" );
            }
        }
    }

}
