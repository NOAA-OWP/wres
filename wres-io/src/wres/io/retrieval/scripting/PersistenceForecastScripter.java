package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.io.config.OrderedSampleMetadata;
import wres.util.CalculationException;

class PersistenceForecastScripter extends Scripter
{
    private final Instant zeroDate;
    private final String variableFeatureClause;
    // The Integer is the count of values expected for a basis time
    private List<Instant> instantsToGetValuesFor;

    PersistenceForecastScripter( OrderedSampleMetadata sampleMetadata,
                                 DataSourceConfig dataSourceConfig,
                                 List<Instant> instantsToGetValuesFor )
            throws SQLException
    {
        super( sampleMetadata, dataSourceConfig);

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
    String formScript() throws IOException
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

        this.addLine("-- ", this.getSampleMetadata());
        this.addLine("-- ", this.getDescription());
        this.addLine("SELECT O.observation_time AS valid_time,");

        try
        {
            this.addTab().addLine(
                    "O.observation_time - INTERVAL '",
                    this.getProjectDetails().getScale().getPeriod(),
                    " ",
                    this.getProjectDetails().getScale().getUnit(),
                    "' AS earliest_time,");
        }
        catch ( CalculationException e )
        {
            throw new IOException( "The scale of the persistence data could not be calculated." );
        }

        this.addTab().addLine("O.observed_value AS observed_value,");
        this.addTab().addLine("O.measurementunit_id");
        this.addLine("FROM wres.Observation O");
        this.addLine("INNER JOIN (");
        this.addTab().addLine("SELECT PS.source_id");
        this.addTab().addLine("FROM wres.ProjectSource PS");
        this.addTab().addLine("WHERE PS.project_id = ", this.getProjectDetails().getId());
        this.addTab(  2  ).addLine("AND PS.member = 'baseline'");
        this.addTab().addLine(") AS PS");
        this.addTab(  2  ).addLine("ON PS.source_id = O.source_id");
        this.addLine("WHERE O.", this.variableFeatureClause);
        this.addTab().addLine("AND O.observation_time >= '", this.getZeroDate(), "'");
        this.addTab().addLine("AND O.observation_time <= '", latestBasisTime, "'");
        this.addTab().addLine("AND O.observed_value IS NOT NULL");
        this.addLine("GROUP BY O.observation_time, O.observed_value, O.measurementunit_id");
        this.addLine("ORDER BY O.observation_time DESC;");

        return this.toString();
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

    private String getDescription()
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
