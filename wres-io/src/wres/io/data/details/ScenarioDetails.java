package wres.io.data.details;

import java.util.Objects;

import wres.io.grouping.DualString;
import wres.util.Internal;

/**
 * Represents details about a type of forecast (such as short range, long range, analysis and assimilation, etc)
 */
@Internal(exclusivePackage = "wres.io")
public class ScenarioDetails extends CachedDetail<ScenarioDetails, DualString> {
    private String scenarioName = null;
    private Integer scenarioID = null;
    private String scenarioType = null;

    private DualString key;

    public void setScenarioType(String scenarioType)
    {
        this.scenarioType = scenarioType;
    }

    public String getScenarioType()
    {
        return this.scenarioType;
    }

    @Override
    public DualString getKey() {
        if (this.key == null)
        {
            this.key = new DualString( this.scenarioName, this.scenarioType );
        }
        return this.key;
    }

    @Override
    public Integer getId() {
        return this.scenarioID;
    }

    @Override
    protected String getIDName() {
        return "scenario_id";
    }

    @Override
    public void setID(Integer id) {
        this.scenarioID = id;
    }

    public void setScenarioName( String scenarioName )
    {
        if ( this.scenarioName == null || !this.scenarioName.equalsIgnoreCase(
                scenarioName ))
        {
            this.scenarioName = scenarioName;
            this.scenarioID = null;
        }
    }

    @Override
    protected String getInsertSelectStatement() {
        StringBuilder script = new StringBuilder();

        script.append("WITH new_scenario AS").append(NEWLINE);
        script.append("(").append(NEWLINE);
        script.append("     INSERT INTO wres.Scenario (scenario_name, scenario_type)").append(NEWLINE);
        script.append("     SELECT '").append(this.scenarioName ).append( "', '").append( this.scenarioType).append( "'").append( NEWLINE);
        script.append("     WHERE NOT EXISTS (").append(NEWLINE);
        script.append("         SELECT 1").append(NEWLINE);
        script.append("         FROM wres.Scenario S").append(NEWLINE);
        script.append("         WHERE S.scenario_name = '").append(this.scenarioName ).append( "'").append( NEWLINE);
        script.append("             AND S.scenario_type = '").append(this.scenarioType ).append("'").append(NEWLINE);
        script.append("     )").append(NEWLINE);
        script.append("     RETURNING scenario_id").append(NEWLINE);
        script.append(")").append(NEWLINE);
        script.append("SELECT scenario_id").append(NEWLINE);
        script.append("FROM new_scenario").append(NEWLINE);
        script.append(NEWLINE);
        script.append("UNION").append(NEWLINE);
        script.append(NEWLINE);
        script.append("SELECT scenario_id").append(NEWLINE);
        script.append("FROM wres.Scenario S").append(NEWLINE);
        script.append("WHERE S.scenario_name = '").append(this.scenarioName ).append( "'").append(NEWLINE);
        script.append("     AND S.scenario_type = '").append(this.scenarioType).append("'").append(NEWLINE);

        return script.toString();
    }

    @Override
    public int compareTo(ScenarioDetails other) {
        int equality;

        if (other == null)
        {
            equality = 1;
        }
        else {
            if (this.scenarioName == null && other.scenarioName == null)
            {
                equality = 0;
            }
            else if (this.scenarioName == null && other.scenarioName != null)
            {
                equality = -1;
            }
            else if (this.scenarioName != null && other.scenarioName == null)
            {
                equality = 1;
            }
            else
            {
                equality = this.scenarioName.trim().compareTo( other.scenarioName.trim() );
            }

            if (equality == 0) {
                if (this.scenarioType == null && other.scenarioType == null)
                {
                    equality = 0;
                }
                else if (this.scenarioType == null && other.scenarioType != null)
                {
                    equality = -1;
                }
                else if (this.scenarioType != null && other.scenarioType == null)
                {
                    equality = 1;
                }
                else
                {
                    equality = this.scenarioType.trim().compareTo( other.scenarioType.trim() );
                }
            }
        }
        return equality;
    }

    @Override
    public boolean equals( Object obj )
    {
        if (obj == null)
        {
            return false;
        }

        return this.hashCode() == obj.hashCode();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.scenarioID, this.scenarioName, this.scenarioType);
    }
}
