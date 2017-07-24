package wres.datamodel.metric;

import java.util.List;
import java.util.function.Function;

import wres.datamodel.PairOfBooleans;
import wres.datamodel.PairOfDoubles;

/**
 * A utility class for slicing/dicing and transforming datasets associated with verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface Slicer
{

    /**
     * Returns the left side of a list of {@link PairOfDoubles} as a primitive array of doubles.
     * 
     * @param input the input pairs
     * @return the left side
     */
    
    double[] getLeftSide(List<PairOfDoubles> input);
    
    /**
     * Returns the right side of a list of {@link PairOfDoubles} as a primitive array of doubles.
     * 
     * @param input the input pairs
     * @return the right side
     */
    
    double[] getRightSide(List<PairOfDoubles> input);    
        
    /**
     * Produces {@link DichotomousPairs} from a {@link SingleValuedPairs} by applying a mapper function to the input. 
     * 
     * @param input the single-valued pairs
     * @param mapper the function that maps single-valued pairs to dichotomous pairs
     * @return the dichotomous pairs
     */
    
    DichotomousPairs transformPairs(SingleValuedPairs input, Function<PairOfDoubles,PairOfBooleans> mapper);

}
