package wres.datamodel;

import java.util.List;

import wres.datamodel.MetricConstants.MetricDimension;

/**
 * Store the output associated with a box plot in a {@link PairOfDoubleAndVectorOfDoubles} where the left side is a 
 * single value and the right side comprises the "whiskers" (quantiles) associated with a single box.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface BoxPlotOutput
        extends MetricOutput<List<PairOfDoubleAndVectorOfDoubles>>, Iterable<PairOfDoubleAndVectorOfDoubles>
{

    /**
     * Returns the probabilities associated with the whiskers (quantiles) in each box. The probabilities are stored
     * in the same order as the quantiles.
     * 
     * @return the probabilities associated with the whiskers of each box
     */
    
    VectorOfDoubles getProbabilities();
    
    /**
     * Returns the dimension associated with the left side of the pairing, i.e. the value against which each box is
     * plotted on the domain axis. 
     * 
     * @return the domain axis dimension
     */
    
    MetricDimension getDomainAxisDimension();
    
    /**
     * Returns the dimension associated with the right side of the pairing, i.e. the values associated with the 
     * whiskers of each box. 
     * 
     * @return the range axis dimension
     */
    
    MetricDimension getRangeAxisDimension();    
    
}
