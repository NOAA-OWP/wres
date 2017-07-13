package wres.datamodel.metric;

import java.util.Objects;

/**
 * A class that uniquely identifies a {@link MetricInput} or a {@link MetricOutput} to a user.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface DatasetIdentifier
{

    /**
     * Optional geospatial identifier (e.g. location identifier) for the metric data.
     * 
     * @return the geospatial identifier associated with the metric data or null
     */

    String getGeospatialID();

    /**
     * Optional variable identifier for the metric data.
     * 
     * @return the variable identifier associated with the metric data or null
     */

    String getVariableID();

    /**
     * Optional scenario identifier for the metric data, such as the modeling scenario for which evaluation is being
     * conducted.
     * 
     * @return the scenario identifier associated with the metric data or null
     */

    String getScenarioID();

    /**
     * Optional scenario identifier for the baseline metric data, such as the modeling scenario against which the metric
     * output is benchmarked.
     * 
     * @return the identifier associated with the baseline metric data or null
     */

    String getScenarioIDForBaseline();

    /**
     * Returns true if a {@link #getGeospatialID()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getGeospatialID()} returns non-null, false otherwise.
     */

    default boolean hasGeospatialID() {
        return Objects.nonNull(getGeospatialID());
    }

    /**
     * Returns true if a {@link #getVariableID()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getVariableID()} returns non-null, false otherwise.
     */

    default boolean hasVariableID() {
        return Objects.nonNull(getVariableID());
    }

    /**
     * Returns true if a {@link #getScenarioID()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getScenarioID()} returns non-null, false otherwise.
     */

    default boolean hasScenarioID() {
        return Objects.nonNull(getScenarioID());
    }

    /**
     * Returns true if a {@link #getScenarioIDForBaseline()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getScenarioIDForBaseline()} returns non-null, false otherwise.
     */

    default boolean hasScenarioIDForBaseline() {
        return Objects.nonNull(getScenarioIDForBaseline());
    }

}
