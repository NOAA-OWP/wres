package wres.datamodel;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MetricOutputException;

/**
 * Immutable implementation of a store for box plot outputs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeBoxPlotOutput implements BoxPlotOutput
{

    /**
     * The boxes in an immutable list.
     */

    private final List<PairOfDoubleAndVectorOfDoubles> output;

    /**
     * The metadata associated with the output.
     */

    private final MetricOutputMetadata meta;

    /**
     * The dimension associated with the domain axis.
     */

    private final MetricDimension domainAxisDimension;

    /**
     * The dimension associated with the range axis (boxes).
     */

    private final MetricDimension rangeAxisDimension;

    /**
     * Probabilities associated with the whiskers for each box.
     */

    private VectorOfDoubles probabilities;

    @Override
    public MetricOutputMetadata getMetadata()
    {
        return meta;
    }

    @Override
    public List<PairOfDoubleAndVectorOfDoubles> getData()
    {
        return output;
    }

    @Override
    public Iterator<PairOfDoubleAndVectorOfDoubles> iterator()
    {
        return output.iterator();
    }

    @Override
    public VectorOfDoubles getProbabilities()
    {
        return probabilities;
    }

    @Override
    public MetricDimension getDomainAxisDimension()
    {
        return domainAxisDimension;
    }

    @Override
    public MetricDimension getRangeAxisDimension()
    {
        return rangeAxisDimension;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof SafeBoxPlotOutput ) )
        {
            return false;
        }
        final SafeBoxPlotOutput v = (SafeBoxPlotOutput) o;
        //Check probabilities
        if ( !getProbabilities().equals( v.getProbabilities() ) )
        {
            return false;
        }
        //Check dimensions
        if ( !getDomainAxisDimension().equals( v.getDomainAxisDimension() ) )
        {
            return false;
        }
        if ( !getRangeAxisDimension().equals( v.getRangeAxisDimension() ) )
        {
            return false;
        }
        //Check pairs
        if ( v.output.size() != output.size() )
        {
            return false;
        }
        Iterator<PairOfDoubleAndVectorOfDoubles> it = iterator();
        for ( PairOfDoubleAndVectorOfDoubles next : v )
        {
            if ( !next.equals( it.next() ) )
            {
                return false;
            }
        }
        //Check metadata
        return meta.equals( v.getMetadata() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( meta, output, probabilities, domainAxisDimension, rangeAxisDimension );
    }

    /**
     * Construct the box plot output.
     * 
     * @param output the box plot data
     * @param probabilities the probabilities
     * @param meta the box plot metadata
     * @param domainAxisDimension the domain axis dimension
     * @param rangeAxisDimension the range axis dimension
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeBoxPlotOutput( List<PairOfDoubleAndVectorOfDoubles> output,
                       VectorOfDoubles probabilities,
                       MetricOutputMetadata meta,
                       MetricDimension domainAxisDimension,
                       MetricDimension rangeAxisDimension )
    {
        //Validate
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( "Specify non-null metadata." );
        }
        if ( Objects.isNull( domainAxisDimension ) )
        {
            throw new MetricOutputException( "Specify a non-null domain axis dimension." );
        }
        if ( Objects.isNull( rangeAxisDimension ) )
        {
            throw new MetricOutputException( "Specify a non-null range axis dimension." );
        }
        if ( Objects.isNull( probabilities ) )
        {
            throw new MetricOutputException( "Specify non-null probabilities." );
        }
        if ( output.isEmpty() )
        {
            throw new MetricOutputException( "Specify one or more boxes to store." );
        }
        if ( probabilities.size() < 2 )
        {
            throw new MetricOutputException( "Specify two or more probabilities for the whiskers." );
        }
        if ( probabilities.size() != output.get( 0 ).getItemTwo().length )
        {
            throw new MetricOutputException( "The number of probabilities does not match the number of whiskers "
                                             + "associated with each box." );
        }
        //Check contents
        checkEachProbability( probabilities );
        checkEachBox( output );

        //Ensure safe types
        DefaultDataFactory factory = (DefaultDataFactory) DefaultDataFactory.getInstance();
        this.output = factory.safePairOfDoubleAndVectorOfDoublesList( output );
        this.probabilities = factory.safeVectorOf( probabilities );
        this.domainAxisDimension = domainAxisDimension;
        this.rangeAxisDimension = rangeAxisDimension;
        this.meta = meta;
    }

    /**
     * Validates each box in each input.
     * 
     * @param boxes the boxes
     */

    private void checkEachBox( List<PairOfDoubleAndVectorOfDoubles> boxes )
    {
        int check = boxes.get( 0 ).getItemTwo().length;
        for ( PairOfDoubleAndVectorOfDoubles next : boxes )
        {
            if ( next.getItemTwo().length == 0 )
            {
                throw new MetricOutputException( "One or more boxes are missing whiskers." );
            }
            if ( next.getItemTwo().length != check )
            {
                throw new MetricOutputException( "One or more boxes has a different number of whiskers than "
                                                 + "input probabilities." );
            }
        }
    }

    /**
     * Validates each probability.
     * 
     * @param probabilities the probabilities
     */

    private void checkEachProbability( VectorOfDoubles probabilities )
    {
        for ( double next : probabilities.getDoubles() )
        {
            if ( next < 0.0 || next > 1.0 )
            {
                throw new MetricOutputException( "One or more of the probabilities is invalid." );
            }
        }
    }


}
