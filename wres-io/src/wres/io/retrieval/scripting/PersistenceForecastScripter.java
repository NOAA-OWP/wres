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
    private final String variableFeatureClause;
    // The Integer is the count of values expected for a basis time
    private List<Instant> instantsToGetValuesFor;

    PersistenceForecastScripter( ProjectDetails projectDetails,
                                 DataSourceConfig dataSourceConfig,
                                 Feature feature,
                                 List<Instant> instantsToGetValuesFor )
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
        this.variableFeatureClause = super.getVariableFeatureClause();
        this.instantsToGetValuesFor = instantsToGetValuesFor;
    }

    @Override
    String formScript()
    {
        Objects.requireNonNull( this.instantsToGetValuesFor,
                                "Persistence Forecast depends on a basis time." );

        // Find the latest basis time for this chunk aka MetricInput:
        Instant latestBasisTime = this.getZeroDate();

        for ( Instant basisTime : this.getInstantsToGetValuesFor() )
        {
            if ( basisTime.isAfter( latestBasisTime ) )
            {
                latestBasisTime = basisTime;
            }
        }

        // Get ALL the values from zerodate to latest basis time for this chunk.
        // The caller then will have to do some work on the results to find the
        // correct data for each basis time. The reason for this is rescaling.
        return "SELECT ( EXTRACT( epoch from o.observation_time ) * 1000 )::bigint AS valid_time,"
               + NEWLINE
               + "    o.observed_value AS observed_value," + NEWLINE
               + "    o.measurementunit_id" + NEWLINE
               + "FROM wres.observation AS o" + NEWLINE
               + "INNER JOIN wres.projectsource AS ps" + NEWLINE
               + "    ON ps.source_id = o.source_id" + NEWLINE
               + "WHERE o.observed_value IS NOT NULL" + NEWLINE
               + "    AND ps.project_id = "
               + getProjectDetails().getId() + NEWLINE
               + "    AND ps.member = 'baseline'" + NEWLINE
               + "    AND o." + this.variableFeatureClause + NEWLINE
               + "    AND o.observation_time >= '"
               + this.getZeroDate() + "'" + NEWLINE

               // The next line is intentionally inclusive to pick t0's value.
               // InputRetriever counts on this.
               + "    AND o.observation_time <= '"
               + latestBasisTime.toString() + "'" + NEWLINE
               + "ORDER BY o.observation_time DESC";
                // Removing limit because we *can't* limit it due to scaling
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
        result.add( this.instantsToGetValuesFor.toString() );
        return result.toString();
    }

    private List<Instant> getInstantsToGetValuesFor()
    {
        return this.instantsToGetValuesFor;
    }
}
