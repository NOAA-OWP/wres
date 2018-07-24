package wres.datamodel.metadata;

import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.inputs.MetricInput;
import wres.datamodel.outputs.MetricOutput;

/**
 * A class that uniquely identifies a {@link MetricInput} or a {@link MetricOutput} to a user.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DatasetIdentifier
{
    /**
     * Geospatial identifier.
     */
    final Location geospatialID;

    /**
     * variable identifier.
     */
    final String variableID;

    /**
     * Scenario identifier.
     */
    final String scenarioID;

    /**
     * Scenario identifier for a baseline.
     */

    final String baselineScenarioID;

    /**
     * Returns an instance from the inputs.
     * 
     * @param geospatialID the optional geospatial identifier
     * @param variableID the optional variable identifier
     * @param scenarioID the optional scenario identifier
     * @param baselineScenarioID the optional baseline scenario identifier
     * @return a dataset identifier
     * @throws NullPointerException if the geospatialID, variableID and scenarioID are all null
     */

    public static DatasetIdentifier of( Location geospatialID,
                                        String variableID,
                                        String scenarioID,
                                        String baselineScenarioID )
    {
        return new DatasetIdentifier( geospatialID, variableID, scenarioID, baselineScenarioID );
    }


    /**
     * Optional geospatial identifier for the metric data.
     * 
     * @return the geospatial identifier associated with the metric data or null
     */

    public Location getGeospatialID()
    {
        return geospatialID;
    }

    /**
     * Optional variable identifier for the metric data.
     * 
     * @return the variable identifier associated with the metric data or null
     */

    public String getVariableID()
    {
        return variableID;
    }

    /**
     * Optional scenario identifier for the metric data, such as the modeling scenario for which evaluation is being
     * conducted.
     * 
     * @return the scenario identifier associated with the metric data or null
     */

    public String getScenarioID()
    {
        return scenarioID;
    }

    /**
     * Optional scenario identifier for the baseline metric data, such as the modeling scenario against which the metric
     * output is benchmarked.
     * 
     * @return the identifier associated with the baseline metric data or null
     */

    public String getScenarioIDForBaseline()
    {
        return baselineScenarioID;
    }

    /**
     * Returns true if a {@link #getGeospatialID()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getGeospatialID()} returns non-null, false otherwise.
     */

    public boolean hasGeospatialID()
    {
        return Objects.nonNull( getGeospatialID() );
    }

    /**
     * Returns true if a {@link #getVariableID()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getVariableID()} returns non-null, false otherwise.
     */

    public boolean hasVariableID()
    {
        return Objects.nonNull( getVariableID() );
    }

    /**
     * Returns true if a {@link #getScenarioID()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getScenarioID()} returns non-null, false otherwise.
     */

    public boolean hasScenarioID()
    {
        return Objects.nonNull( getScenarioID() );
    }

    /**
     * Returns true if a {@link #getScenarioIDForBaseline()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getScenarioIDForBaseline()} returns non-null, false otherwise.
     */

    public boolean hasScenarioIDForBaseline()
    {
        return Objects.nonNull( getScenarioIDForBaseline() );
    }

    @Override
    public String toString()
    {
        final StringJoiner b = new StringJoiner( ",", "[", "]" );
        if ( hasGeospatialID() )
        {
            b.add( getGeospatialID().toString() );
        }
        if ( hasVariableID() )
        {
            b.add( getVariableID() );
        }
        if ( hasScenarioID() )
        {
            b.add( getScenarioID() );
        }
        if ( hasScenarioIDForBaseline() )
        {
            b.add( getScenarioIDForBaseline() );
        }
        return b.toString();
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof DatasetIdentifier ) )
        {
            return false;
        }
        final DatasetIdentifier check = (DatasetIdentifier) o;
        boolean returnMe = hasGeospatialID() == check.hasGeospatialID()
                           && hasVariableID() == check.hasVariableID()
                           && hasScenarioID() == check.hasScenarioID()
                           && hasScenarioIDForBaseline() == check.hasScenarioIDForBaseline();
        if ( hasGeospatialID() )
        {
            returnMe = returnMe && getGeospatialID().equals( check.getGeospatialID() );
        }
        if ( hasVariableID() )
        {
            returnMe = returnMe && getVariableID().equals( check.getVariableID() );
        }
        if ( hasScenarioID() )
        {
            returnMe = returnMe && getScenarioID().equals( check.getScenarioID() );
        }
        if ( hasScenarioIDForBaseline() )
        {
            returnMe = returnMe && getScenarioIDForBaseline().equals( check.getScenarioIDForBaseline() );
        }
        return returnMe;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( getGeospatialID(), getVariableID(), getScenarioID(), getScenarioIDForBaseline() );
    }

    /**
     * Hidden constructor.
     * 
     * @param geospatialID the geospatial identifier
     * @param variableID the variable identifier
     * @param scenarioID the scenario identifier
     * @param baselineScenarioID the baseline scenario identifier
     * @throws NullPointerException if the geospatialID, variableID and scenarioID are all null
     */

    private DatasetIdentifier( Location geospatialID,
                               String variableID,
                               String scenarioID,
                               String baselineScenarioID )
    {
        if ( Objects.isNull( geospatialID ) && Objects.isNull( variableID ) && Objects.isNull( scenarioID ) )
        {
            throw new NullPointerException( "One of the location, variable and scenario identifiers must be non-null." );
        }

        this.geospatialID = geospatialID;
        this.variableID = variableID;
        this.scenarioID = scenarioID;
        this.baselineScenarioID = baselineScenarioID;
    }


}
