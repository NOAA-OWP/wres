package wres.io.retrieval.scripting;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.data.details.ProjectDetails;

class PersistenceForecastScripter extends Scripter
{
    private static final String NEWLINE = System.lineSeparator();

    // To call super, we need an integer
    // I doubt that "progress" or "sequenceStep" have any meaning for the
    // persistence forecast (other than following contract of Scripter), so
    // use a placeholder for those values.
    private static final int DUMMY = -9;

    private final Instant zeroDate;
    private final String variablePositionClause;
    private List<Instant> instantsToGetValuesFor;

    PersistenceForecastScripter( ProjectDetails projectDetails,
                                 DataSourceConfig dataSourceConfig,
                                 Feature feature )
            throws SQLException
    {
        super( projectDetails, dataSourceConfig, feature, DUMMY, DUMMY );

        String zeroDate = this.getProjectDetails().getInitialObservationDate(
                this.getDataSourceConfig(),
                this.getFeature() );
        String isoZeroDate = zeroDate.replace(" ", "T" )
                                     .replace( "'", "" )
                             + "Z";
        this.zeroDate = Instant.parse( isoZeroDate );
        this.variablePositionClause = super.getVariablePositionClause();
    }

    public void setInstantsToGetValueFor( List<Instant> forecastBasisTimes )
    {
        this.instantsToGetValuesFor = forecastBasisTimes;
    }

    @Override
    String formScript()
    {
        Objects.requireNonNull( this.instantsToGetValuesFor,
                                "Persistence Forecast depends on a basis time." );

        StringJoiner outerResult =
                new StringJoiner( NEWLINE + "UNION" + NEWLINE,
                                  "SELECT basis_time, persistence_time, observed_value FROM (" + NEWLINE,
                                  NEWLINE + ") AS times");

        for ( Instant basisTime : this.instantsToGetValuesFor )
        {
            String result =
                    "(" + NEWLINE
                    + "    SELECT "
                    + basisTime.getEpochSecond() + " AS basis_time," + NEWLINE
                    + "        EXTRACT( epoch from o.observation_time ) AS persistence_time,"
                    + NEWLINE
                    + "        o.observed_value AS observed_value" +NEWLINE
                    + "    FROM wres.observation AS o" + NEWLINE
                    + "    INNER JOIN wres.projectsource AS ps" + NEWLINE
                    + "        ON ps.source_id = o.source_id" + NEWLINE
                    + "    WHERE o.observed_value IS NOT NULL" + NEWLINE
                    + "        AND ps.project_id = "
                    + getProjectDetails().getId() + NEWLINE
                    + "        AND ps.member = 'baseline'" + NEWLINE
                    + "        AND o." + this.variablePositionClause + NEWLINE
                    + "        AND o.observation_time >= '"
                    + this.getZeroDate() + "'" + NEWLINE

                    // The next line is intentionally exclusive to avoid picking
                    // t0's value.
                    + "        AND o.observation_time < '"
                    + basisTime.toString() + "'" + NEWLINE
                    + "    ORDER BY o.observation_time DESC" + NEWLINE
                    + "    LIMIT 1" + NEWLINE
                    + ")";

            outerResult.add( result );
        }

        return outerResult.toString();
    }

    @Override
    String getBaseDateName()
    {
        return "persistence_time";
    }

    @Override
    String getValueDate()
    {
        return this.getBaseDateName();
    }

    private Instant getZeroDate()
    {
        return this.zeroDate;
    }

    @Override
    public String toString()
    {
        StringJoiner result = new StringJoiner( ",", "PersistenceForecastScripter: ", "" );
        for ( Instant instant : this.instantsToGetValuesFor )
        {
            result.add( instant.toString() );
        }
        return result.toString();
    }
}
