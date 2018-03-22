package wres.datamodel;

import java.util.Set;

import wres.datamodel.ThresholdConstants.ThresholdType;

/**
 * A container of {@link Threshold} by {@link ThresholdType}.
 * 
 * TODO: eliminate this container when possible, in favor of {@link ThresholdsByMetric}.
 * 
 * @author james.brown@hydrosolved.com
 */

public interface ThresholdsByType
{

    /**
     * Returns the thresholds by {@link ThresholdType}.
     * 
     * @param type the type of threshold
     * @return the thresholds for a specified type
     * @throws NullPointerException if the input is null
     */

    Set<Threshold> getThresholdsByType( ThresholdType type );

    /**
     * Returns <code>true</code> if the store contains thresholds for the specified type, otherwise <code>false</code>.
     * 
     * @param type the type of threshold 
     * @return true if the store contains the type, otherwise false 
     * @throws NullPointerException if the input is null
     */

    boolean contains( ThresholdType type );

    /**
     * Returns the set of {@link ThresholdType} in the store.
     * 
     * @return the threshold types stored
     */

    Set<ThresholdType> getAllThresholdTypes();

    /**
     * Combines the input with the contents of the current container, return a new container that reflects the union
     * of the two.
     * 
     * @param thresholds the thresholds
     * @return the union of the input and the current thresholds
     * @throws NullPointerException if the input is null
     */

    ThresholdsByType union( ThresholdsByType thresholds );

    /**
     * Builder.
     */

    interface ThresholdsByTypeBuilder
    {

        /**
         * Adds a map of thresholds.
         * 
         * @param thresholds the thresholds
         * @param thresholdType the threshold type
         * @return the builder
         * @throws NullPointerException if any input is null
         */

        ThresholdsByTypeBuilder addThresholds( Set<Threshold> thresholds,
                                               ThresholdType thresholdType );

        /**
         * Builds a {@link ThresholdsByType}.
         * 
         * @return the container
         */

        ThresholdsByType build();
    }


}
