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
        String script = "WITH new_scenario AS" + NEWLINE +
                        "(" + NEWLINE +
                        "     INSERT INTO wres.Scenario (scenario_name, scenario_type)"
                        + NEWLINE +
                        "     SELECT '" + this.scenarioName + "', '"
                        + this.scenarioType + "'" + NEWLINE +
                        "     WHERE NOT EXISTS (" + NEWLINE +
                        "         SELECT 1" + NEWLINE +
                        "         FROM wres.Scenario S" + NEWLINE +
                        "         WHERE S.scenario_name = '" + this.scenarioName
                        + "'" + NEWLINE +
                        "             AND S.scenario_type = '"
                        + this.scenarioType + "'" + NEWLINE +
                        "     )" + NEWLINE +
                        "     RETURNING scenario_id" + NEWLINE +
                        ")" + NEWLINE +
                        "SELECT scenario_id" + NEWLINE +
                        "FROM new_scenario" + NEWLINE +
                        NEWLINE +
                        "UNION" + NEWLINE +
                        NEWLINE +
                        "SELECT scenario_id" + NEWLINE +
                        "FROM wres.Scenario S" + NEWLINE +
                        "WHERE S.scenario_name = '" + this.scenarioName + "'"
                        + NEWLINE +
                        "     AND S.scenario_type = '" + this.scenarioType + "'"
                        + NEWLINE;

        return script;
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
        return obj != null &&
               obj instanceof ScenarioDetails &&
               this.hashCode() == obj.hashCode();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.scenarioID, this.scenarioName, this.scenarioType);
    }
}
