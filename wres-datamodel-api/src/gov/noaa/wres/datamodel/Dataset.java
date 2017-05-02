package gov.noaa.wres.datamodel;

/**
 * Abstract base class for an immutable dataset that does not contain any spatial or temporal information. Extend this
 * class to develop datasets for particular measurement scales and datasets that are referenced in space or time (i.e.
 * for metrics that explicitly use spatial or temporal information).
 *
 * @author james.brown@hydrosolved.com
 */

public interface Dataset<T>
{
    /**
     * Return the values in the dataset.
     * 
     * @return the values
     */

    T getValues();

    /**
     * Returns the number of elements in the dataset.
     * 
     * @return the number of elements
     */

    int size();
}
