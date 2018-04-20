package wres.datamodel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.inputs.InsufficientDataException;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.Metadata;

/**
 * Immutable implementation of a store of verification pairs that comprise a single value and an ensemble of values.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeEnsemblePairs implements EnsemblePairs
{

    /**
     * The verification pairs in an immutable list.
     */

    private final List<PairOfDoubleAndVectorOfDoubles> mainInput;

    /**
     * Metadata associated with the verification pairs.
     */

    private final Metadata mainMeta;

    /**
     * The verification pairs for a baseline in an immutable list (may be null).
     */

    private final List<PairOfDoubleAndVectorOfDoubles> baselineInput;

    /**
     * Metadata associated with the baseline verification pairs (may be null).
     */

    private final Metadata baselineMeta;

    /**
     * Climatological dataset. May be null.
     */

    private VectorOfDoubles climatology;

    @Override
    public List<PairOfDoubleAndVectorOfDoubles> getRawData()
    {
        return mainInput;
    }

    @Override
    public Metadata getMetadata()
    {
        return mainMeta;
    }

    @Override
    public EnsemblePairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        return DefaultDataFactory.getInstance().ofEnsemblePairs( baselineInput, baselineMeta );
    }

    @Override
    public List<PairOfDoubleAndVectorOfDoubles> getDataForBaseline()
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
    public Iterator<PairOfDoubleAndVectorOfDoubles> iterator()
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
     * A {@link DefaultMetricInputBuilder} to build the metric input.
     */

    static class EnsemblePairsBuilder extends DefaultMetricInputBuilder<PairOfDoubleAndVectorOfDoubles>
    {

        /**
         * Pairs.
         */
        List<PairOfDoubleAndVectorOfDoubles> mainInput = new ArrayList<>();

        /**
         * Pairs for baseline.
         */
        List<PairOfDoubleAndVectorOfDoubles> baselineInput = null;

        @Override
        public EnsemblePairsBuilder addData( final List<PairOfDoubleAndVectorOfDoubles> mainInput )
        {
            if ( Objects.nonNull( mainInput ) )
            {
                this.mainInput.addAll( mainInput );
            }
            return this;
        }

        @Override
        public EnsemblePairsBuilder addDataForBaseline( final List<PairOfDoubleAndVectorOfDoubles> baselineInput )
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
        public SafeEnsemblePairs build()
        {
            return new SafeEnsemblePairs( this );
        }

    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     * @throws InsufficientDataException if the climatological data is both non-null and without finite values
     */

    SafeEnsemblePairs( final EnsemblePairsBuilder b )
    {
        //Ensure safe types
        DefaultDataFactory factory = (DefaultDataFactory) DefaultDataFactory.getInstance();
        this.mainInput = factory.safePairOfDoubleAndVectorOfDoublesList( b.mainInput );
        this.mainMeta = b.mainMeta;
        this.climatology = b.climatology;
        
        // Baseline data?
        if( Objects.nonNull( b.baselineInput ) )
        {
            this.baselineInput = factory.safePairOfDoubleAndVectorOfDoublesList( b.baselineInput );
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
