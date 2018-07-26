package wres.datamodel.inputs.pairs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputBuilder;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.metadata.Metadata;

/**
 * Abstract base class for storing pairs. 
 * 
 * Currently, there is no conditional "Pair" type within WRES. Introducing a generic "Pair" type would require that 
 * primitive pairs were autoboxed/unboxed and that array types were replaced with wrapped types, both of which would 
 * introduce some overhead. However, it would also allow for better conditioning. For example, this class would be 
 * constrained to store <code>T extends Pair</code>, rather than simply <code>T</code>.
 * 
 * @param <T> the paired type
 * @author james.brown@hydrosolved.com
 */
public abstract class BasicPairs<T> implements MetricInput<T>
{

    /**
     * The verification pairs in an immutable list.
     */

    private final List<T> mainInput;

    /**
     * Metadata associated with the verification pairs.
     */

    private final Metadata mainMeta;

    /**
     * The verification pairs for a baseline in an immutable list (may be null).
     */

    private final List<T> baselineInput;

    /**
     * Metadata associated with the baseline verification pairs (may be null).
     */

    private final Metadata baselineMeta;

    /**
     * Climatological dataset. May be null.
     */

    private VectorOfDoubles climatology;

    @Override
    public List<T> getRawData()
    {
        return mainInput;
    }

    @Override
    public Metadata getMetadata()
    {
        return mainMeta;
    }

    @Override
    public List<T> getRawDataForBaseline()
    {
        return baselineInput;
    }

    @Override
    public Metadata getMetadataForBaseline()
    {
        return baselineMeta;
    }

    @Override
    public VectorOfDoubles getClimatology()
    {
        return climatology;
    }

    @Override
    public Iterator<T> iterator()
    {
        return mainInput.iterator();
    }

    @Override
    public String toString()
    {
        StringJoiner join = new StringJoiner( System.lineSeparator() );
        join.add( "Main pairs:" );
        mainInput.forEach( a -> join.add( a.toString() ) );
        if ( hasBaseline() )
        {
            join.add( "" ).add( "Baseline pairs:" );
            baselineInput.forEach( a -> join.add( a.toString() ) );
        }
        if ( hasClimatology() )
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

    public abstract static class BasicPairsBuilder<T> implements MetricInputBuilder<T>
    {

        /**
         * Pairs.
         */
        List<T> mainInput = new ArrayList<>();

        /**
         * Pairs for baseline.
         */
        List<T> baselineInput = null;

        /**
         * Climatology.
         */

        VectorOfDoubles climatology;

        /**
         * Metadata for input.
         */

        Metadata mainMeta;

        /**
         * Metadata for baseline.
         */

        Metadata baselineMeta;

        /**
         * Sets the metadata associated with the input.
         * 
         * @param mainMeta the metadata
         * @return the builder
         */

        public BasicPairsBuilder<T> setMetadata( Metadata mainMeta )
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

        public BasicPairsBuilder<T> setMetadataForBaseline( Metadata baselineMeta )
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

        public BasicPairsBuilder<T> setClimatology( VectorOfDoubles climatology )
        {
            this.climatology = climatology;
            return this;
        }

        @Override
        public BasicPairsBuilder<T> addData( final List<T> mainInput )
        {
            if ( Objects.nonNull( mainInput ) )
            {
                this.mainInput.addAll( mainInput );
            }
            return this;
        }

        @Override
        public BasicPairsBuilder<T> addDataForBaseline( final List<T> baselineInput )
        {
            if ( Objects.nonNull( baselineInput ) )
            {
                if ( Objects.isNull( this.baselineInput ) )
                {
                    this.baselineInput = new ArrayList<>();
                }
                this.baselineInput.addAll( baselineInput );
            }
            return this;
        }

    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    BasicPairs( final BasicPairsBuilder<T> b )
    {
        //Ensure safe types
        this.mainInput = Collections.unmodifiableList( b.mainInput );
        this.mainMeta = b.mainMeta;
        this.climatology = b.climatology;

        // Baseline data?
        if ( Objects.nonNull( b.baselineInput ) )
        {
            this.baselineInput = Collections.unmodifiableList( b.baselineInput );
        }
        else
        {
            this.baselineInput = null;
        }

        // Always set baseline metadata because null-status is validated
        this.baselineMeta = b.baselineMeta;

        //Validate
        validateMainInput();
        validateBaselineInput();
        validateClimatologicalInput();
    }

    /**
     * Validates the main pairs and associated metadata after the constructor has copied it.
     * 
     * @throws MetricInputException if the input is invalid
     */

    private void validateMainInput()
    {

        if ( Objects.isNull( mainMeta ) )
        {
            throw new MetricInputException( "Specify non-null metadata for the metric input." );
        }

        if ( Objects.isNull( mainInput ) )
        {
            throw new MetricInputException( "Specify a non-null dataset for the metric input." );
        }

        if ( mainInput.contains( null ) )
        {
            throw new MetricInputException( "One or more of the pairs is null." );
        }

    }

    /**
     * Validates the baseline pairs and associated metadata after the constructor has copied it.
     * 
     * @throws MetricInputException if the baseline input is invalid
     */

    private void validateBaselineInput()
    {
        if ( Objects.isNull( baselineInput ) != Objects.isNull( baselineMeta ) )
        {
            throw new MetricInputException( "Specify a non-null baseline input and associated metadata or leave both "
                                            + "null." );
        }

        if ( Objects.nonNull( baselineInput ) && baselineInput.contains( null ) )
        {
            throw new MetricInputException( "One or more of the baseline pairs is null." );
        }

    }

    /**
     * Validates the climatological input after the constructor has copied it.
     * 
     * @throws MetricInputException if the climatological input is invalid
     */

    private void validateClimatologicalInput()
    {
        if ( Objects.nonNull( climatology ) )
        {
            if ( climatology.size() == 0 )
            {
                throw new MetricInputException( "Cannot build the paired data with an empty climatology: add one or "
                                                + "more values." );
            }

            if ( !Arrays.stream( climatology.getDoubles() ).anyMatch( Double::isFinite ) )
            {
                throw new MetricInputException( "Must have at least one non-missing value in the climatological "
                                                + "input" );
            }
        }
    }

}
