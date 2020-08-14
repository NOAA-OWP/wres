package wres.datamodel;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.Statistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;

/**
 * A class that uniquely identifies a {@link SampleData} or a {@link Statistic} to a user.
 * 
 * @author james.brown@hydrosolved.com
 * @deprecated The {@link Evaluation} and {@link Pool} replace this representation.
 */
@Deprecated( since = "5.0", forRemoval = true )
public class DatasetIdentifier
{
    /**
     * Geospatial identifier.
     */
    final FeatureTuple featureTuple;

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
     * @param featureTuple the optional location
     * @param variableId the optional variable identifier
     * @param scenarioId the optional scenario identifier
     * @param baselineScenarioId the optional baseline scenario identifier
     * @param leftOrRightOrBaseline the context for the dataset as it relates to the declaration
     * @return a dataset identifier
     * @throws NullPointerException if the geospatialId, variableId and scenarioId are all null
     */

    public static DatasetIdentifier of( FeatureTuple featureTuple,
                                        String variableId,
                                        String scenarioId,
                                        String baselineScenarioId,
                                        LeftOrRightOrBaseline leftOrRightOrBaseline )
    {
        return new DatasetIdentifier( featureTuple, variableId, scenarioId, baselineScenarioId, leftOrRightOrBaseline );
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param featureTuple the optional location
     * @param variableId the optional variable identifier
     * @param scenarioId the optional scenario identifier
     * @param baselineScenarioId the optional baseline scenario identifier
     * @return a dataset identifier
     * @throws NullPointerException if the geospatialId, variableId and scenarioId are all null
     */

    public static DatasetIdentifier of( FeatureTuple featureTuple,
                                        String variableId,
                                        String scenarioId,
                                        String baselineScenarioId )
    {
        return new DatasetIdentifier( featureTuple, variableId, scenarioId, baselineScenarioId, null );
    }

    /**
     * Returns a new dataset identifier with an override for the {@link DatasetIdentifier#getScenarioNameForBaseline()}.
     * 
     * @param identifier the dataset identifier
     * @param baselineScenarioId a scenario identifier for a baseline dataset
     * @return a dataset identifier
     */

    public static DatasetIdentifier of( DatasetIdentifier identifier, String baselineScenarioId )
    {
        return DatasetIdentifier.of( identifier.getFeatureTuple(),
                                     identifier.getVariableName(),
                                     identifier.getScenarioName(),
                                     baselineScenarioId,
                                     identifier.getLeftOrRightOrBaseline() );
    }

    /**
     * Returns a dataset identifier.
     * 
     * @param featureTuple an optional location (may be null)
     * @param variableId an optional variable identifier (may be null)
     * @param scenarioId an optional scenario identifier (may be null)
     * @return a dataset identifier
     */

    public static DatasetIdentifier of( final FeatureTuple featureTuple,
                                        final String variableId,
                                        final String scenarioId )
    {
        return DatasetIdentifier.of( featureTuple, variableId, scenarioId, null );
    }

    /**
     * Returns a dataset identifier.
     * 
     * @param featureTuple an optional location (may be null)
     * @param variableId an optional variable identifier (may be null)
     * @return a dataset identifier
     */

    public static DatasetIdentifier of( final FeatureTuple featureTuple, final String variableId )
    {
        return DatasetIdentifier.of( featureTuple, variableId, null, null );
    }

    /**
     * Optional location for the dataset.
     * 
     * @return the location or null
     */

    public FeatureTuple getFeatureTuple()
    {
        return this.featureTuple;
    }

    /**
     * Optional variable identifier for the metric data.
     * 
     * @return the variable identifier associated with the metric data or null
     */

    public String getVariableName()
    {
        return this.variableId;
    }

    /**
     * Optional scenario identifier for the metric data, such as the modeling scenario for which evaluation is being
     * conducted.
     * 
     * @return the scenario identifier associated with the metric data or null
     */

    public String getScenarioName()
    {
        return this.scenarioId;
    }

    /**
     * Optional scenario identifier for the baseline metric data, such as the modeling scenario against which the metric
     * output is benchmarked.
     * 
     * @return the identifier associated with the baseline metric data or null
     */

    public String getScenarioNameForBaseline()
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
     * Returns true if a {@link #getFeatureTuple()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getFeatureTuple()} returns non-null, false otherwise.
     */

    public boolean hasFeatureTuple()
    {
        return Objects.nonNull( this.getFeatureTuple() );
    }

    /**
     * Returns true if a {@link #getVariableName()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getVariableName()} returns non-null, false otherwise.
     */

    public boolean hasVariableName()
    {
        return Objects.nonNull( this.getVariableName() );
    }

    /**
     * Returns true if a {@link #getScenarioName()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getScenarioName()} returns non-null, false otherwise.
     */

    public boolean hasScenarioName()
    {
        return Objects.nonNull( this.getScenarioName() );
    }

    /**
     * Returns true if a {@link #getScenarioNameForBaseline()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getScenarioNameForBaseline()} returns non-null, false otherwise.
     */

    public boolean hasScenarioNameForBaseline()
    {
        return Objects.nonNull( this.getScenarioNameForBaseline() );
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
                                                                            .append( "featureTuple",
                                                                                     this.getFeatureTuple() )
                                                                            .append( "variableId",
                                                                                     this.getVariableName() )
                                                                            .append( "scenarioId",
                                                                                     this.getScenarioName() )
                                                                            .append( "baselineScenarioId",
                                                                                     this.getScenarioNameForBaseline() )
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

        return Objects.equals( this.getFeatureTuple(), check.getFeatureTuple() )
               && Objects.equals( this.getLeftOrRightOrBaseline(), check.getLeftOrRightOrBaseline() )
               && Objects.equals( this.getVariableName(), check.getVariableName() )
               && Objects.equals( this.getScenarioName(), check.getScenarioName() )
               && Objects.equals( this.getScenarioNameForBaseline(), check.getScenarioNameForBaseline() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getFeatureTuple(),
                             this.getVariableName(),
                             this.getScenarioName(),
                             this.getScenarioNameForBaseline(),
                             this.getLeftOrRightOrBaseline() );
    }

    /**
     * Hidden constructor.
     * 
     * @param featureTuple the geospatial identifier
     * @param variableName the variable identifier
     * @param scenarioName the scenario identifier
     * @param baselineScenarioName the baseline scenario identifier
     * @param leftOrRightOrBaseline the context for the dataset as it relates to the declaration
     * @throws NullPointerException if the geospatialID, variableID and scenarioID are all null
     */

    private DatasetIdentifier( FeatureTuple featureTuple,
                               String variableName,
                               String scenarioName,
                               String baselineScenarioName,
                               LeftOrRightOrBaseline leftOrRightOrBaseline )
    {
        if ( Objects.isNull( featureTuple ) && Objects.isNull( variableName ) && Objects.isNull( scenarioName ) )
        {
            throw new NullPointerException( "One of the location, variable and scenario identifiers must be non-null." );
        }

        this.featureTuple = featureTuple;
        this.variableId = variableName;
        this.scenarioId = scenarioName;
        this.baselineScenarioId = baselineScenarioName;
        this.leftOrRightOrBaseline = leftOrRightOrBaseline;
    }


}
