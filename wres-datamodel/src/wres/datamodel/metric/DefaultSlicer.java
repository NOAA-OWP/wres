package wres.datamodel.metric;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import wres.datamodel.PairOfBooleans;
import wres.datamodel.PairOfDoubles;

/**
 * Default implementation of a utility class for slicing/dicing and transforming datasets associated with verification
 * metrics. TODO: reconcile this class with the Slicer in wres.datamodel.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class DefaultSlicer implements Slicer
{

    /**
     * Data factory for transformations.
     */

    private final DataFactory dataFac;

    /**
     * Instance of the slicer.
     */

    private static DefaultSlicer instance = null;

    /**
     * Returns an instance of a {@link Slicer}.
     * 
     * @return a {@link DataFactory}
     */

    public static Slicer getInstance()
    {
        if(Objects.isNull(instance))
        {
            instance = new DefaultSlicer();
        }
        return instance;
    }

    /**
     * Null error message.
     */
    private static final String NULL_ERROR = "Specify a non-null input to slice.";

    @Override
    public double[] getLeftSide(List<PairOfDoubles> input)
    {
        Objects.requireNonNull(input, NULL_ERROR);
        return input.stream().mapToDouble(PairOfDoubles::getItemOne).toArray();
    }

    @Override
    public double[] getRightSide(List<PairOfDoubles> input)
    {
        Objects.requireNonNull(input, NULL_ERROR);
        return input.stream().mapToDouble(PairOfDoubles::getItemTwo).toArray();
    }

    @Override
    public DichotomousPairs transformPairs(SingleValuedPairs input, Function<PairOfDoubles, PairOfBooleans> mapper)
    {
        Objects.requireNonNull(input, NULL_ERROR);
        Objects.requireNonNull(mapper, "Specify a non-null mapper function.");
        List<PairOfDoubles> mainPairs = input.getData();
        List<PairOfBooleans> mainPairsTransformed = new ArrayList<PairOfBooleans>();
        mainPairs.stream().map(mapper).forEach(mainPairsTransformed::add);
        Metadata metaTransformed =
                                 dataFac.getMetadataFactory().getMetadata(input.getMetadata(),
                                                                          dataFac.getMetadataFactory().getDimension());
        if(input.hasBaseline())
        {
            List<PairOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfBooleans> basePairsTransformed = new ArrayList<PairOfBooleans>();
            basePairs.stream().map(mapper).forEach(basePairsTransformed::add);
            Metadata metaBaseTransformed = dataFac.getMetadataFactory()
                                                  .getMetadata(input.getMetadataForBaseline(),
                                                               dataFac.getMetadataFactory().getDimension());
            return dataFac.ofDichotomousPairsFromAtomic(mainPairsTransformed,
                                                        basePairsTransformed,
                                                        metaTransformed,
                                                        metaBaseTransformed);
        }
        return dataFac.ofDichotomousPairsFromAtomic(mainPairsTransformed, metaTransformed);
    }

    /**
     * Hidden constructor.
     */

    private DefaultSlicer()
    {
        dataFac = DefaultDataFactory.getInstance();
    }

}
