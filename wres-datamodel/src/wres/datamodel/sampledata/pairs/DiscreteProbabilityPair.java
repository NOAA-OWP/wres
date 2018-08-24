package wres.datamodel.sampledata.pairs;

import wres.datamodel.sampledata.SampleDataException;

/**
 * <p>Immutable pair of primitive double values that represents a probability.
 *
 * @author james.brown@hydrosolved.com
 */
public class DiscreteProbabilityPair extends SingleValuedPair
{

    /**
     * Returns an instance from the input.
     * 
     * @param left the left side of the pair
     * @param right the right side of the pair
     * @return a pair of booleans
     */

    public static DiscreteProbabilityPair of( double left, double right )
    {
        return new DiscreteProbabilityPair( left, right );
    }

    /**
     * Hidden constructor.
     * 
     * @param left the first item
     * @param right the second item
     * @throws SampleDataException if the left and right values are not within [0,1]
     */

    private DiscreteProbabilityPair( double left, double right )
    {
        super( left, right );

        if ( left < 0.0 || left > 1.0 )
        {
            throw new SampleDataException( "The left side of the discrete probability pair must be greater than or "
                                            + "equal to zero and less than or equal to one." );
        }
        if ( right < 0.0 || right > 1.0 )
        {
            throw new SampleDataException( "The right side of the discrete probability pair must be greater than or "
                    + "equal to zero and less than or equal to one." );
        }
    }

}
