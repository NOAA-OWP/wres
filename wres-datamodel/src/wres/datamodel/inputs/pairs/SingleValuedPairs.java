package wres.datamodel.inputs.pairs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.metadata.Metadata;

/**
 * Immutable store of verification pairs that comprise two single-valued, continuous numerical, variables. The 
 * single-valued variables are not necessarily deterministic (i.e. they may be probabilistic), but they do comprise 
 * single values, rather than multiple values.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SingleValuedPairs implements MetricInput<PairOfDoubles>
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

    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined. 
     * 
     * @return the baseline
     */
    
    public SingleValuedPairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        return DataFactory.ofSingleValuedPairs( baselineInput, baselineMeta );
    }

    @Override
    public List<PairOfDoubles> getRawData()
    {
        return mainInput;
    }

    @Override
    public Metadata getMetadata()
    {
        return mainMeta;
    }
    
    @Override
    public List<PairOfDoubles> getRawDataForBaseline()
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
     * A {@link PairedInputBuilder} to build the metric input.
     */

    public static class SingleValuedPairsBuilder extends PairedInputBuilder<PairOfDoubles>
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
        public SingleValuedPairs build()
        {
            return new SingleValuedPairs( this );
        }
    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    SingleValuedPairs( final SingleValuedPairsBuilder b )
    {
        //Ensure safe types
        this.mainInput = DataFactory.safePairOfDoublesList( b.mainInput );
        this.mainMeta = b.mainMeta;
        this.climatology = b.climatology;
        
        // Baseline data?
        if( Objects.nonNull( b.baselineInput ) )
        {
            this.baselineInput = DataFactory.safePairOfDoublesList( b.baselineInput );
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
