package wres.datamodel.sampledata.pairs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBuilder;
import wres.datamodel.sampledata.SampleDataException;

/**
 * An abstract base class for storing zero or more paired values. 
 * 
 * Currently, there is no "Pair" type within WRES. Introducing a "Pair" type would require that primitive pairs were 
 * autoboxed/unboxed and that array types were replaced with wrapped types, both of which would 
 * introduce some overhead. However, it would also allow for better conditioning. For example, this class could then be 
 * constrained to store <code>T extends Pair</code>, rather than simply <code>T</code>. TODO: evaluate whether a 
 * generic "Pair" interface is warranted. The Apache Pair is a concrete type, not an interface, so wouldn't work, 
 * except as a composition to aid a specific implementation. 
 * 
 * @param <T> the paired type
 * @author james.brown@hydrosolved.com
 */
public abstract class Pairs<T> implements SampleData<T>
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
    public Iterator<T> iterator()
    {
        return sampleData.iterator();
    }

    @Override
    public String toString()
    {
        StringJoiner join = new StringJoiner( System.lineSeparator() );
        join.add( "Main pairs:" );
        sampleData.forEach( a -> join.add( a.toString() ) );
        if ( this.hasBaseline() )
        {
            join.add( "" ).add( "Baseline pairs:" );
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
     * A builder to build the metric input.
     */

    public abstract static class PairsBuilder<T> implements SampleDataBuilder<T>
    {

        /**
         * Pairs.
         */
        List<T> sampleData = new ArrayList<>();

        /**
         * Pairs for baseline.
         */
        List<T> baselineSampleData = null;

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

        public PairsBuilder<T> setMetadata( SampleMetadata mainMeta )
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

        public PairsBuilder<T> setMetadataForBaseline( SampleMetadata baselineMeta )
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

        public PairsBuilder<T> setClimatology( VectorOfDoubles climatology )
        {
            this.climatology = climatology;
            return this;
        }

        @Override
        public PairsBuilder<T> addData( T mainInput )
        {
           this.sampleData.add( mainInput );
            
            return this;
        }

        @Override
        public PairsBuilder<T> addDataForBaseline( T baselineInput )
        {
            if ( Objects.isNull( this.baselineSampleData ) )
            {
                this.baselineSampleData = new ArrayList<>();
            }

            this.baselineSampleData.add( baselineInput );

            return this;
        }

        @Override
        public PairsBuilder<T> addData( List<T> sampleData )
        {
            Objects.requireNonNull( sampleData, "Specify non-null sample data." );
            
            this.sampleData.addAll( sampleData );
            
            return this;
        }
        
        @Override
        public PairsBuilder<T> addDataForBaseline( List<T> baselineSampleData )
        {
            Objects.requireNonNull( baselineSampleData, "Specify non-null sample data for the baseline." );
            
            if ( Objects.isNull( this.baselineSampleData ) )
            {
                this.baselineSampleData = new ArrayList<>();
            }
            
            this.baselineSampleData.addAll( baselineSampleData );
            
            return this;        
        }
        
    }

    /**
     * Convenience method that returns the raw data associated with the baseline or null.
     * 
     * @return the raw pairs
     */

    List<T> getRawDataForBaseline()
    {
        if( Objects.isNull( baselineSampleData ) )
        {
            return null;
        }
        
        return Collections.unmodifiableList( baselineSampleData );
    }

    /**
     * Convenience method that returns the metadata associated with the baseline or null.
     * 
     * @return baseline metadata
     */

    SampleMetadata getMetadataForBaseline()
    {
        if( Objects.isNull( baselineMeta ) )
        {
            return null;
        }
        
        return baselineMeta;
    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws SampleDataException if the pairs are invalid
     */

    Pairs( final PairsBuilder<T> b )
    {
        //Ensure safe types
        this.sampleData = Collections.unmodifiableList( b.sampleData );
        this.mainMeta = b.mainMeta;
        this.climatology = b.climatology;

        // Baseline data?
        if ( Objects.nonNull( b.baselineSampleData ) )
        {
            this.baselineSampleData = Collections.unmodifiableList( b.baselineSampleData );
        }
        else
        {
            this.baselineSampleData = null;
        }

        // Always set baseline metadata because null-status is validated
        this.baselineMeta = b.baselineMeta;

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

        if ( sampleData.contains( null ) )
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
                                           + "null." );
        }

        if ( Objects.nonNull( baselineSampleData ) && baselineSampleData.contains( null ) )
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
        if ( Objects.nonNull( climatology ) )
        {
            if ( climatology.size() == 0 )
            {
                throw new SampleDataException( "Cannot build the paired data with an empty climatology: add one or "
                                               + "more values." );
            }

            if ( !Arrays.stream( climatology.getDoubles() ).anyMatch( Double::isFinite ) )
            {
                throw new SampleDataException( "Must have at least one non-missing value in the climatological "
                                               + "input" );
            }
        }
    }

}
