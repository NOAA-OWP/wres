package wres.io.data.details;

/**
 * Represents details about a type of forecast (such as short range, long range, analysis and assimilation, etc)
 */
public class ForecastTypeDetails extends CachedDetail<ForecastTypeDetails, String> {
    private final String description = null;
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

    @Override
    protected String getInsertSelectStatement() {
        return null;
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
