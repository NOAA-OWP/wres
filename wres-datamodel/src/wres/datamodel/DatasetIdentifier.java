package wres.datamodel;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.Statistic;

/**
 * A class that uniquely identifies a {@link SampleData} or a {@link Statistic} to a user.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DatasetIdentifier
{
    /**
     * Geospatial identifier.
     */
    final Location geospatialId;

    /**
     * variable identifier.
     */
    final String variableId;

    /**
     * Scenario identifier.
     */
    final String scenarioId;

    /**
     * Scenario identifier for a baseline (e.g., when this dataset represents a statistic for a skill score).
     */
    final String baselineScenarioId;

    /**
     * Context for the dataset within the project declaration. For example, {@link LeftOrRightOrBaseline#RIGHT} would
     * represent a <code>right</right> dataset or pairs or statistics derived with a <code>right</code> dataset.
     */
    final LeftOrRightOrBaseline leftOrRightOrBaseline;

    /**
     * Returns an instance from the inputs.
     * 
     * @param geospatialId the optional geospatial identifier
     * @param variableId the optional variable identifier
     * @param scenarioId the optional scenario identifier
     * @param baselineScenarioId the optional baseline scenario identifier
     * @param leftOrRightOrBaseline the context for the dataset as it relates to the declaration
     * @return a dataset identifier
     * @throws NullPointerException if the geospatialID, variableID and scenarioID are all null
     */

    public static DatasetIdentifier of( Location geospatialId,
                                        String variableId,
                                        String scenarioId,
                                        String baselineScenarioId,
                                        LeftOrRightOrBaseline leftOrRightOrBaseline )
    {
        return new DatasetIdentifier( geospatialId, variableId, scenarioId, baselineScenarioId, leftOrRightOrBaseline );
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param geospatialId the optional geospatial identifier
     * @param variableId the optional variable identifier
     * @param scenarioId the optional scenario identifier
     * @param baselineScenarioId the optional baseline scenario identifier
     * @return a dataset identifier
     * @throws NullPointerException if the geospatialID, variableID and scenarioID are all null
     */

    public static DatasetIdentifier of( Location geospatialId,
                                        String variableId,
                                        String scenarioId,
                                        String baselineScenarioId )
    {
        return new DatasetIdentifier( geospatialId, variableId, scenarioId, baselineScenarioId, null );
    }

    /**
     * Returns a new dataset identifier with an override for the {@link DatasetIdentifier#getScenarioIDForBaseline()}.
     * 
     * @param identifier the dataset identifier
     * @param baselineScenarioId a scenario identifier for a baseline dataset
     * @return a dataset identifier
     */

    public static DatasetIdentifier of( DatasetIdentifier identifier, String baselineScenarioId )
    {
        return DatasetIdentifier.of( identifier.getGeospatialID(),
                                     identifier.getVariableID(),
                                     identifier.getScenarioID(),
                                     baselineScenarioId,
                                     identifier.getLeftOrRightOrBaseline() );
    }

    /**
     * Returns a dataset identifier.
     * 
     * @param geospatialID an optional geospatial identifier (may be null)
     * @param variableID an optional variable identifier (may be null)
     * @param scenarioID an optional scenario identifier (may be null)
     * @return a dataset identifier
     */

    public static DatasetIdentifier of( final Location geospatialID,
                                        final String variableID,
                                        final String scenarioID )
    {
        return DatasetIdentifier.of( geospatialID, variableID, scenarioID, null );
    }

    /**
     * Returns a dataset identifier.
     * 
     * @param geospatialID an optional geospatial identifier (may be null)
     * @param variableID an optional variable identifier (may be null)
     * @return a dataset identifier
     */

    public static DatasetIdentifier of( final Location geospatialID, final String variableID )
    {
        return DatasetIdentifier.of( geospatialID, variableID, null, null );
    }

    /**
     * Optional geospatial identifier for the metric data.
     * 
     * @return the geospatial identifier associated with the metric data or null
     */

    public Location getGeospatialID()
    {
        return this.geospatialId;
    }

    /**
     * Optional variable identifier for the metric data.
     * 
     * @return the variable identifier associated with the metric data or null
     */

    public String getVariableID()
    {
        return this.variableId;
    }

    /**
     * Optional scenario identifier for the metric data, such as the modeling scenario for which evaluation is being
     * conducted.
     * 
     * @return the scenario identifier associated with the metric data or null
     */

    public String getScenarioID()
    {
        return this.scenarioId;
    }

    /**
     * Optional scenario identifier for the baseline metric data, such as the modeling scenario against which the metric
     * output is benchmarked.
     * 
     * @return the identifier associated with the baseline metric data or null
     */

    public String getScenarioIDForBaseline()
    {
        return this.baselineScenarioId;
    }

    /**
     * Optional {@link LeftOrRightOrBaseline} identifier for the dataset, whether for input data, pairs or statistics.
     * 
     * @return the context for the dataset within the declaration
     */

    public LeftOrRightOrBaseline getLeftOrRightOrBaseline()
    {
        return this.leftOrRightOrBaseline;
    }

    /**
     * Returns true if a {@link #getGeospatialID()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getGeospatialID()} returns non-null, false otherwise.
     */

    public boolean hasGeospatialID()
    {
        return Objects.nonNull( this.getGeospatialID() );
    }

    /**
     * Returns true if a {@link #getVariableID()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getVariableID()} returns non-null, false otherwise.
     */

    public boolean hasVariableID()
    {
        return Objects.nonNull( this.getVariableID() );
    }

    /**
     * Returns true if a {@link #getScenarioID()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getScenarioID()} returns non-null, false otherwise.
     */

    public boolean hasScenarioID()
    {
        return Objects.nonNull( this.getScenarioID() );
    }

    /**
     * Returns true if a {@link #getScenarioIDForBaseline()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getScenarioIDForBaseline()} returns non-null, false otherwise.
     */

    public boolean hasScenarioIDForBaseline()
    {
        return Objects.nonNull( this.getScenarioIDForBaseline() );
    }

    /**
     * Returns true if a {@link #getLeftOrRightOrBaseline()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getLeftOrRightOrBaseline()} returns non-null, false otherwise.
     */

    public boolean hasLeftOrRightOrBaseline()
    {
        return Objects.nonNull( this.getLeftOrRightOrBaseline() );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "geospatialId",
                                                                                     this.getGeospatialID() )
                                                                            .append( "variableId",
                                                                                     this.getVariableID() )
                                                                            .append( "scenarioId",
                                                                                     this.getScenarioID() )
                                                                            .append( "baselineScenarioId",
                                                                                     this.getScenarioIDForBaseline() )
                                                                            .append( "pairContext",
                                                                                     this.getLeftOrRightOrBaseline() )
                                                                            .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof DatasetIdentifier ) )
        {
            return false;
        }

        if ( o == this )
        {
            return true;
        }

        DatasetIdentifier check = (DatasetIdentifier) o;

        return Objects.equals( this.getGeospatialID(), check.getGeospatialID() )
               && Objects.equals( this.getLeftOrRightOrBaseline(), check.getLeftOrRightOrBaseline() )
               && Objects.equals( this.getVariableID(), check.getVariableID() )
               && Objects.equals( this.getScenarioID(), check.getScenarioID() )
               && Objects.equals( this.getScenarioIDForBaseline(), check.getScenarioIDForBaseline() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getGeospatialID(),
                             this.getVariableID(),
                             this.getScenarioID(),
                             this.getScenarioIDForBaseline(),
                             this.getLeftOrRightOrBaseline() );
    }

    /**
     * Hidden constructor.
     * 
     * @param geospatialID the geospatial identifier
     * @param variableID the variable identifier
     * @param scenarioID the scenario identifier
     * @param baselineScenarioID the baseline scenario identifier
     * @param leftOrRightOrBaseline the context for the dataset as it relates to the declaration
     * @throws NullPointerException if the geospatialID, variableID and scenarioID are all null
     */

    private DatasetIdentifier( Location geospatialID,
                               String variableID,
                               String scenarioID,
                               String baselineScenarioID,
                               LeftOrRightOrBaseline leftOrRightOrBaseline )
    {
        if ( Objects.isNull( geospatialID ) && Objects.isNull( variableID ) && Objects.isNull( scenarioID ) )
        {
            throw new NullPointerException( "One of the location, variable and scenario identifiers must be non-null." );
        }

        this.geospatialId = geospatialID;
        this.variableId = variableID;
        this.scenarioId = scenarioID;
        this.baselineScenarioId = baselineScenarioID;
        this.leftOrRightOrBaseline = leftOrRightOrBaseline;
    }


}
