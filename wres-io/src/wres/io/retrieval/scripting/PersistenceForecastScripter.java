package wres.io.retrieval.scripting;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;

class PersistenceForecastScripter extends Scripter
{
    private static final String NEWLINE = System.lineSeparator();

    private final Instant zeroDate;
    private final Duration timeScaleWidth;
    private final String variablePositionClause;
    private List<Instant> instantsToGetValuesFor;

    PersistenceForecastScripter( ProjectDetails projectDetails,
                                 DataSourceConfig dataSourceConfig,
                                 Feature feature,
                                 int progress,
                                 int sequenceStep )
            throws SQLException, NoDataException
    {
        super( projectDetails, dataSourceConfig, feature, progress, sequenceStep );

        String zeroDate = this.getProjectDetails().getZeroDate(
                this.getDataSourceConfig(),
                this.getFeature() );
        String isoZeroDate = zeroDate.replace(" ", "T" )
                                     .replace( "'", "" )
                             + "Z";
        Instant usualZeroDate = Instant.parse( isoZeroDate );
        this.timeScaleWidth = ConfigHelper.getDurationFromTimeScale( projectDetails.getScale() );
        this.zeroDate = usualZeroDate.minus( this.timeScaleWidth );
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
            StringBuilder result = new StringBuilder();

            result.append( "(" ).append( NEWLINE );
            result.append( "    SELECT " );
            result.append( basisTime.getEpochSecond() );
            result.append( " AS basis_time,").append( NEWLINE );
            result.append("        EXTRACT( epoch from o.observation_time ) AS persistence_time," )
                  .append( NEWLINE );
            result.append( "        o.observed_value AS observed_value" )
                  .append( NEWLINE );
            result.append( "    FROM wres.observation AS o" ).append( NEWLINE );
            result.append( "    INNER JOIN wres.projectsource AS ps" )
                  .append( NEWLINE );
            result.append( "        ON ps.source_id = o.source_id" )
                  .append( NEWLINE );
            result.append( "    WHERE o.observed_value IS NOT NULL" )
                  .append( NEWLINE );
            result.append( "        AND ps.project_id = " );
            result.append( getProjectDetails().getId() ).append( NEWLINE );
            result.append( "        AND ps.member = 'baseline'" ).append( NEWLINE );

            result.append( "        AND o.");
            result.append( this.variablePositionClause ).append( NEWLINE );
            result.append( "        AND o.observation_time >= '" );

            result.append( this.getZeroDate() );
            result.append( "'" ).append( NEWLINE );

            // The next line is intentionally exclusive to avoid picking t0's value.
            result.append( "        AND o.observation_time < '" );

            result.append( basisTime.toString() );
            result.append( "'" ).append( NEWLINE );
            result.append( "    ORDER BY o.observation_time DESC" )
                  .append( NEWLINE );
            result.append( "    LIMIT 1" ).append( NEWLINE );
            result.append( ")" );

            outerResult.add( result.toString() );
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
