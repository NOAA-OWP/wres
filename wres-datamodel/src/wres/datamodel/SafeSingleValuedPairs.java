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
     * A {@link DefaultMetricInputBuilder} to build the metric input.
     */

    static class SingleValuedPairsBuilder extends DefaultMetricInputBuilder<PairOfDoubles>
    {

        /**
         * Pairs.
         */
        List<PairOfDoubles> mainInput = new ArrayList<>();

        /**
         * Pairs for baseline.
         */
        List<PairOfDoubles> baselineInput = null;

        @Override
        public SingleValuedPairsBuilder addData( final List<PairOfDoubles> mainInput )
        {
            if ( Objects.nonNull( mainInput ) )
            {
                this.mainInput.addAll( mainInput );
            }
            return this;
        }

        @Override
        public SingleValuedPairsBuilder addDataForBaseline( final List<PairOfDoubles> baselineInput )
        {
            if ( Objects.nonNull( baselineInput ) )
            {
                if( Objects.isNull( this.baselineInput ) )
                {
                    this.baselineInput = new ArrayList<>();
                }
                this.baselineInput.addAll( baselineInput );
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
     * @throws InsufficientDataException if the climatological data is both non-null and without finite values
     */

    SafeSingleValuedPairs( final SingleValuedPairsBuilder b )
    {
        //Ensure safe types
        DefaultDataFactory factory = (DefaultDataFactory) DefaultDataFactory.getInstance();
        this.mainInput = factory.safePairOfDoublesList( b.mainInput );
        this.mainMeta = b.mainMeta;
        this.climatology = b.climatology;
        
        // Baseline data?
        if( Objects.nonNull( b.baselineInput ) )
        {
            this.baselineInput = factory.safePairOfDoublesList( b.baselineInput );
        }
        else 
        {
            this.baselineInput = null;
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
                                            + "unspecified." );
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
