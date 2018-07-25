package wres.datamodel.outputs;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.DataFactory;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.MetricOutputMetadata;

/**
 * Immutable store of outputs associated with a box plot. Contains a {@link PairOfDoubleAndVectorOfDoubles} where the 
 * left side is a single value and the right side comprises the "whiskers" (quantiles) associated with a single box.
 * 
 * @author james.brown@hydrosolved.com
 */
public class BoxPlotOutput
        implements MetricOutput<List<PairOfDoubleAndVectorOfDoubles>>, Iterable<PairOfDoubleAndVectorOfDoubles>
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

    /**
     * Returns an instance from the inputs.
     * 
     * @param output the box plot data
     * @param probabilities the probabilities
     * @param meta the box plot metadata
     * @param domainAxisDimension the domain axis dimension
     * @param rangeAxisDimension the range axis dimension
     * @throws MetricOutputException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static BoxPlotOutput of( List<PairOfDoubleAndVectorOfDoubles> output,
                                    VectorOfDoubles probabilities,
                                    MetricOutputMetadata meta,
                                    MetricDimension domainAxisDimension,
                                    MetricDimension rangeAxisDimension )
    {
        return new BoxPlotOutput( output, probabilities, meta, domainAxisDimension, rangeAxisDimension );
    }

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

    /**
     * Returns the probabilities associated with the whiskers (quantiles) in each box. The probabilities are stored
     * in the same order as the quantiles.
     * 
     * @return the probabilities associated with the whiskers of each box
     */

    public VectorOfDoubles getProbabilities()
    {
        return this.probabilities;
    }

    /**
     * Returns the dimension associated with the left side of the pairing, i.e. the value against which each box is
     * plotted on the domain axis. 
     * 
     * @return the domain axis dimension
     */

    public MetricDimension getDomainAxisDimension()
    {
        return this.domainAxisDimension;
    }

    /**
     * Returns the dimension associated with the right side of the pairing, i.e. the values associated with the 
     * whiskers of each box. 
     * 
     * @return the range axis dimension
     */

    public MetricDimension getRangeAxisDimension()
    {
        return this.rangeAxisDimension;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof BoxPlotOutput ) )
        {
            return false;
        }
        final BoxPlotOutput v = (BoxPlotOutput) o;
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

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );

        joiner.add( "PROBABILITIES: " + probabilities );

        joiner.add( "BOXES:" );
        output.forEach( nextBox -> joiner.add( nextBox.toString() ) );

        return joiner.toString();
    }

    /**
     * Hidden constructor.
     * 
     * @param output the box plot data
     * @param probabilities the probabilities
     * @param meta the box plot metadata
     * @param domainAxisDimension the domain axis dimension
     * @param rangeAxisDimension the range axis dimension
     * @throws MetricOutputException if any of the inputs are invalid
     */

    private BoxPlotOutput( List<PairOfDoubleAndVectorOfDoubles> output,
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
        if ( probabilities.size() < 2 )
        {
            throw new MetricOutputException( "Specify two or more probabilities for the whiskers." );
        }
        if ( !output.isEmpty() && probabilities.size() != output.get( 0 ).getItemTwo().length )
        {
            throw new MetricOutputException( "The number of probabilities does not match the number of whiskers "
                                             + "associated with each box." );
        }

        //Check contents
        checkEachProbability( probabilities );
        checkEachBox( output );

        //Ensure safe types
        this.output = DataFactory.safePairOfDoubleAndVectorOfDoublesList( output );
        this.probabilities = DataFactory.safeVectorOf( probabilities );
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
        if ( !boxes.isEmpty() )
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
