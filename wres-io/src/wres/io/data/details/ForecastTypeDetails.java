package wres.io.data.details;

import wres.util.Internal;

/**
 * Represents details about a type of forecast (such as short range, long range, analysis and assimilation, etc)
 */
@Internal(exclusivePackage = "wres.io")
public class ForecastTypeDetails extends CachedDetail<ForecastTypeDetails, String> {
    private String description = null;
    private Integer forecastRangeID = null;
    private int timestep;

    public int getTimestep()
    {
        return this.timestep;
    }

    public void setTimestep(int timestep)
    {
        this.timestep = timestep;
    }

    @Override
    public String getKey() {
        return this.description;
    }

    @Override
    public Integer getId() {
        return this.forecastRangeID;
    }

    @Override
    protected String getIDName() {
        return "forecastrange_id";
    }

    @Override
    public void setID(Integer id) {
        this.forecastRangeID = id;
    }

    public void setDescription(String description)
    {
        if (this.description == null || !this.description.equalsIgnoreCase(description))
        {
            this.description = description;
            this.forecastRangeID = null;
        }
    }

    @Override
    protected String getInsertSelectStatement() {
        StringBuilder script = new StringBuilder();

        script.append("WITH new_type AS").append(NEWLINE);
        script.append("(").append(NEWLINE);
        script.append("     INSERT INTO wres.ForecastType (type_name, timestep, step_count)").append(NEWLINE);
        script.append("     SELECT '").append(this.description).append("', ").append(this.timestep).append(", 0").append(NEWLINE);
        script.append("     WHERE NOT EXISTS (").append(NEWLINE);
        script.append("         SELECT 1").append(NEWLINE);
        script.append("         FROM wres.ForecastType FT").append(NEWLINE);
        script.append("         WHERE FT.type_name = '").append(this.description).append("'").append(NEWLINE);
        script.append("     )").append(NEWLINE);
        script.append("     RETURNING forecasttype_id").append(NEWLINE);
        script.append(")").append(NEWLINE);
        script.append("SELECT forecasttype_id").append(NEWLINE);
        script.append("FROM new_type").append(NEWLINE);
        script.append(NEWLINE);
        script.append("UNION").append(NEWLINE);
        script.append(NEWLINE);
        script.append("SELECT forecasttype_id").append(NEWLINE);
        script.append("FROM wres.ForecastType FT").append(NEWLINE);
        script.append("WHERE FT.type_name = '").append(this.description).append("';");

        return script.toString();
    }

    @Override
    public int compareTo(ForecastTypeDetails rangeDetails) {
        int equality;

        if (rangeDetails == null)
        {
            equality = 1;
        }
        else {
            equality = ((Integer) this.timestep).compareTo(rangeDetails.timestep);

            if (equality == 0) {
                if (this.description == null && rangeDetails.description == null) {
                    equality = 0;
                } else if (this.description != null && rangeDetails.description == null) {
                    equality = 1;
                } else if (this.description == null && rangeDetails.description != null) {
                    equality = -1;
                } else {
                    equality = this.description.compareTo(rangeDetails.description);
                }
            }
        }
        return equality;
    }
}
