package wres.datamodel.metric;

import java.util.List;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.Pair;
import wres.datamodel.PairOfBooleans;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.VectorOfDoubles;

/**
 * A factory class for producing metric inputs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricInputFactory extends MetricDataFactory
{

    /**
     * Construct the dichotomous input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    DichotomousPairs ofDichotomousPairs(final List<VectorOfBooleans> pairs, final Metadata meta);

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    MulticategoryPairs ofMulticategoryPairs(final List<VectorOfBooleans> pairs, final Metadata meta);

    /**
     * Construct the discrete probability input without any pairs for a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param meta the metadata
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs,
                                                                      final Metadata meta);
    /**
     * Construct the discrete probability input with a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param basePairs the baseline pairs
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs,
                                                                      final List<PairOfDoubles> basePairs,
                                                                      final Metadata mainMeta,
                                                                      final Metadata baselineMeta);

    /**
     * Construct the single-valued input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs, final Metadata meta);

    /**
     * Construct the single-valued input with a baseline.
     * 
     * @param pairs the single-valued pairs
     * @param basePairs the baseline pairs
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs,
                                                        final List<PairOfDoubles> basePairs,
                                                        final Metadata mainMeta,
                                                        final Metadata baselineMeta);
    
    /**
     * Return a {@link PairOfDoubles} from two double values.
     *  
     * @param left the left value
     * @param right the right value
     * @return the pair
     */
    
    public PairOfDoubles pairOf(final double left, final double right);
    
    /**
     * Return a {@link PairOfBooleans} from two boolean values.
     *  
     * @param left the first value
     * @param right the second value
     * @return the pair
     */    
    
    public PairOfBooleans pairOf(final boolean left, final boolean right);
    
    /**
     * Return a {@link PairOfDoubleAndVectorOfDoubles} from a double value and a double vector of values.
     *  
     * @param left the first value
     * @param right the second value
     * @return the pair
     */        
    
    public PairOfDoubleAndVectorOfDoubles pairOf(final double left, final double[] right);
    
    /**
     * Return a {@link PairOfDoubleAndVectorOfDoubles} from a double value and a double vector of values.
     *  
     * @param left the first value
     * @param right the second value
     * @return the pair
     */        
    
    public PairOfDoubleAndVectorOfDoubles pairOf(final Double left, final Double[] right);

    /**
     * Return a {@link Pair} from two double vectors.
     *  
     * @param left the first value
     * @param right the second value
     * @return the pair
     */     
    
    public Pair<VectorOfDoubles, VectorOfDoubles> pairOf(final double[] left, final double[] right);

    /**
     * Return a {@link VectorOfDoubles} from a vector of doubles
     *  
     * @param vec the vector of doubles
     * @return the vector
     */     
    
    public VectorOfDoubles vectorOf(final double[] vec);
    
    /**
     * Return a {@link VectorOfDoubles} from a vector of doubles
     *  
     * @param vec the vector of doubles
     * @return the vector
     */         

    public VectorOfDoubles vectorOf(final Double[] vec);
    
    /**
     * Return a {@link VectorOfBooleans} from a vector of booleans
     *  
     * @param vec the vector of booleans
     * @return the vector
     */         

    public VectorOfBooleans vectorOf(final boolean[] vec);
    
    /**
     * Return a {@link VectorOfBooleans} from a vector of booleans
     *  
     * @param vec the vector of booleans
     * @return the vector
     */         

    public MatrixOfDoubles matrixOf(final double[][] vec);
    
    
}
