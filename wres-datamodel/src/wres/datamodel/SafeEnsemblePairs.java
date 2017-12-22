package wres.datamodel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

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
    public List<PairOfDoubleAndVectorOfDoubles> getData()
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
     * A {@link DefaultPairedInputBuilder} to build the metric input.
     */

    static class EnsemblePairsBuilder extends DefaultPairedInputBuilder<PairOfDoubleAndVectorOfDoubles>
    {

        /**
         * Pairs.
         */
        private List<PairOfDoubleAndVectorOfDoubles> mainInput;

        /**
         * Pairs for baseline.
         */
        private List<PairOfDoubleAndVectorOfDoubles> baselineInput;

        @Override
        public EnsemblePairsBuilder addData( final List<PairOfDoubleAndVectorOfDoubles> mainInput )
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
        public EnsemblePairsBuilder addDataForBaseline( final List<PairOfDoubleAndVectorOfDoubles> baselineInput )
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
     */

    SafeEnsemblePairs( final EnsemblePairsBuilder b )
    {
        //Ensure safe types
        DefaultDataFactory factory = (DefaultDataFactory) DefaultDataFactory.getInstance();
        mainInput = factory.safePairOfDoubleAndVectorOfDoublesList( b.mainInput );
        baselineInput =
                Objects.nonNull( b.baselineInput ) ? factory.safePairOfDoubleAndVectorOfDoublesList( b.baselineInput )
                                                   : null;
        mainMeta = b.mainMeta;
        baselineMeta = b.baselineMeta;
        climatology = b.climatology;

        //Validate
        if ( Objects.isNull( mainMeta ) )
        {
            throw new MetricInputException( "Specify non-null metadata for the metric input." );
        }
        if ( Objects.isNull( mainInput ) )
        {
            throw new MetricInputException( "Specify a non-null dataset for the metric input." );
        }
        if ( Objects.isNull( baselineInput ) != Objects.isNull( baselineMeta ) )
        {
            throw new MetricInputException( "Specify a non-null baseline input and associated metadata or leave both "
                                            + "null." );
        }
        if ( mainInput.contains( null ) )
        {
            throw new MetricInputException( "One or more of the pairs is null." );
        }
        if ( mainInput.isEmpty() )
        {
            throw new MetricInputException( "Cannot build the paired data with an empty input: add one or more pairs." );
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
        }
        if ( Objects.nonNull( climatology ) && climatology.size() == 0 )
        {
            throw new MetricInputException( "Cannot build the paired data with an empty baseline: add one or more "
                                            + "pairs." );
        }
        //Check contents
        checkEachPair( mainInput, baselineInput );
    }

    /**
     * Validates each pair in each input.
     * 
     * @param mainInput the main input
     * @param baselineInput the baseline input
     */

    private void checkEachPair( List<PairOfDoubleAndVectorOfDoubles> mainInput,
                                List<PairOfDoubleAndVectorOfDoubles> baselineInput )
    {
        //Main pairs
        for ( PairOfDoubleAndVectorOfDoubles next : mainInput )
        {
            if ( next.getItemTwo().length == 0 )
            {
                throw new MetricInputException( "One or more pairs has no ensemble members." );
            }
        }
        //Baseline
        if ( Objects.nonNull( baselineInput ) )
        {
            for ( PairOfDoubleAndVectorOfDoubles next : mainInput )
            {
                if ( next.getItemTwo().length == 0 )
                {
                    throw new MetricInputException( "One or more pairs in the baseline has no ensemble members." );
                }
            }
        }
    }

}
