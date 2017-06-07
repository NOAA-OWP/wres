package wres.datamodel;

import java.util.Arrays;
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
     * Get a flattened array of values from a list of pairs.
     * No guarantees on order.
     * @param pairs the list of pairs
     * @return a double[] of all the values in the list of pairs.
     */
    public static double[] flatArray(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        double[] first = pairs.stream()
                              .mapToDouble(PairOfDoubleAndVectorOfDoubles::getItemOne)
                              .toArray();
        double[] second = pairs.stream()
                               .flatMapToDouble(p ->
                                                Arrays.stream(p.getItemTwo()))
                               .toArray();

        double[] result = new double[first.length+second.length];

        int i = 0;
        for (int j = 0; j < first.length; i++, j++)
        {
            result[i] = first[j];
        }

        for (int k = 0; k < second.length; i++, k++)
        {
            result[i] = second[k];
        }

        return result;
    }
}
