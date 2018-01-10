package wres.datamodel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import wres.datamodel.inputs.InsufficientDataException;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.Metadata;

/**
 * Immutable implementation of a store of verification pairs that comprise two single-valued, continuous numerical,
 * variables.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeSingleValuedPairs implements SingleValuedPairs
{

    /**
     * The verification pairs in an immutable list.
     */

    private final List<PairOfDoubles> mainInput;

    /**
     * Metadata associated with the verification pairs.
     */

    private final Metadata mainMeta;

    /**
     * The verification pairs for a baseline in an immutable list (may be null).
     */

    private final List<PairOfDoubles> baselineInput;

    /**
     * Metadata associated with the baseline verification pairs (may be null).
     */

    private final Metadata baselineMeta;

    /**
     * Climatological dataset. May be null.
     */

    private VectorOfDoubles climatology;

    @Override
    public List<PairOfDoubles> getData()
    {
        return mainInput;
    }

    @Override
    public Metadata getMetadata()
    {
        return mainMeta;
    }

    @Override
    public SingleValuedPairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        return DefaultDataFactory.getInstance().ofSingleValuedPairs( baselineInput, baselineMeta );
    }

    @Override
    public List<PairOfDoubles> getDataForBaseline()
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
    public Iterator<PairOfDoubles> iterator()
    {
        return mainInput.iterator();
    }

    /**
     * A {@link DefaultPairedInputBuilder} to build the metric input.
     */

    static class SingleValuedPairsBuilder extends DefaultPairedInputBuilder<PairOfDoubles>
    {

        /**
         * Pairs.
         */
        List<PairOfDoubles> mainInput;

        /**
         * Pairs for baseline.
         */
        List<PairOfDoubles> baselineInput;

        @Override
        public SingleValuedPairsBuilder addData( final List<PairOfDoubles> mainInput )
        {
            if ( Objects.nonNull( this.mainInput ) && Objects.nonNull( mainInput ) )
            {
                this.mainInput.addAll( mainInput );
            }
            else if ( Objects.nonNull( mainInput ) )
            {
                this.mainInput = new ArrayList<>( mainInput );
            }
            return this;
        }

        @Override
        public SingleValuedPairsBuilder addDataForBaseline( final List<PairOfDoubles> baselineInput )
        {
            if ( Objects.nonNull( this.baselineInput ) && Objects.nonNull( baselineInput ) )
            {
                this.baselineInput.addAll( baselineInput );
            }
            else if ( Objects.nonNull( baselineInput ) )
            {
                this.baselineInput = new ArrayList<>( baselineInput );
            }
            return this;
        }

        @Override
        public SafeSingleValuedPairs build()
        {
            return new SafeSingleValuedPairs( this );
        }
    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     * @throws InsufficientDataException if all inputs are non-finite
     */

    SafeSingleValuedPairs( final SingleValuedPairsBuilder b )
    {
        //Ensure safe types
        DefaultDataFactory factory = (DefaultDataFactory) DefaultDataFactory.getInstance();
        mainInput = factory.safePairOfDoublesList( b.mainInput );
        baselineInput = Objects.nonNull( b.baselineInput ) ? factory.safePairOfDoublesList( b.baselineInput ) : null;
        mainMeta = b.mainMeta;
        baselineMeta = b.baselineMeta;
        climatology = b.climatology;

        //Validate
        validateMainInput();
        validateBaselineInput();
        validateClimatologicalInput();
    }

    /**
     * Validates the main pairs and associated metadata after the constructor has copied it.
     * 
     * @throws MetricInputException if the input is invalid
     * @throws InsufficientDataException if all inputs are non-finite
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

        if ( mainInput.isEmpty() )
        {
            throw new MetricInputException( "Cannot build the paired data with an empty input: add one or more pairs." );
        }

        if ( !Slicer.hasOneOrMoreOf( mainInput, Double::isFinite ) )
        {
            throw new InsufficientDataException( "Must have at least one non-missing pair in main input." );
        }
    }

    /**
     * Validates the baseline pairs and associated metadata after the constructor has copied it.
     * 
     * @throws MetricInputException if the baseline input is invalid
     * @throws InsufficientDataException if all baseline inputs are non-finite
     */

    private void validateBaselineInput()
    {
        if ( Objects.isNull( baselineInput ) != Objects.isNull( baselineMeta ) )
        {
            throw new MetricInputException( "Specify a non-null baseline input and associated metadata or leave both "
                                            + "null." );
        }

        if ( Objects.nonNull( baselineInput ) )
        {
            if ( baselineInput.contains( null ) )
            {
                throw new MetricInputException( "One or more of the baseline pairs is null." );
            }

            if ( baselineInput.isEmpty() )
            {
                throw new MetricInputException( "Cannot build the paired data with an empty baseline: add one or more "
                                                + "pairs." );
            }

            if ( !Slicer.hasOneOrMoreOf( baselineInput, Double::isFinite ) )
            {
                throw new InsufficientDataException( "Must have at least one non-missing pair in baseline input." );
            }
        }
    }

    /**
     * Validates the climatological input after the constructor has copied it.
     * 
     * @throws MetricInputException if the climatological input is invalid
     * @throws InsufficientDataException if all climatological inputs are non-finite
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
                throw new InsufficientDataException( "Must have at least one non-missing value in the climatological "
                                                     + "input" );
            }
        }
    }

}
