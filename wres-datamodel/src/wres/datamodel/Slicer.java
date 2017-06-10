package wres.datamodel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Functions for slicing and dicing data types from the data model.
 */
public class Slicer
{
    private Slicer()
    {
        // prevent direct construction, this is a utility class.
    }

    /**
     * Get repeated left doubles based on double count in right side
     *
     * No guarantees on order.
     *
     * @param pairs the list of pairs to use
     * @return a double[] of the values of the left side repeated for count in right side
     */
    public static double[] getItemsOneForEachItemTwo(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        List<Double> result = new ArrayList<>();

        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            for (int i = 0; i < pair.getItemTwo().length; i++)
            {
                result.add(pair.getItemOne());
            }
        }
        return result.stream()
                     .mapToDouble(Double::doubleValue)
                     .toArray();
    }

    /**
     * Get a List of simple pairs from a List of PairOfDoubleAndVectorOfDoubles
     */
    public static List<PairOfDoubles> getFlatDoublePairs(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        List<PairOfDoubles> result = new ArrayList<>();
        DataFactory df = DataFactory.instance();
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            double[] itemTwo = pair.getItemTwo();
            for (int i = 0; i < itemTwo.length; i++)
            {
                PairOfDoubles p = df.pairOf(pair.getItemOne(), itemTwo[i]);
                result.add(p);
            }
        }
        return Collections.unmodifiableList(result);
    }
}
