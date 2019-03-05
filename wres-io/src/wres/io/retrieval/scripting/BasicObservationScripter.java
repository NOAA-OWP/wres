package wres.io.retrieval.scripting;

import java.sql.SQLException;

import wres.config.generated.DataSourceConfig;
import wres.io.config.OrderedSampleMetadata;

class BasicObservationScripter extends Scripter
{
    BasicObservationScripter( OrderedSampleMetadata sampleMetadata, DataSourceConfig dataSourceConfig)
    {
        super( sampleMetadata, dataSourceConfig);
    }

    @Override
    String formScript() throws SQLException
    {
        this.addLine("-- ", this.getSampleMetadata());
        this.addLine("SELECT ARRAY[O.observed_value] AS measurements,");
        this.addTab().add( "'{1}'::int[] AS members,")
            .addTab().addLine("-- Assign each observation to the 'default' member");
        this.applyValueDate();
        this.applyBasisTime();
        this.addLine("    0 AS lead,");
        this.addLine("    O.measurementunit_id");
        this.addLine("FROM wres.Observation O");

        this.applyVariableFeatureClause();

        this.applyEarliestDateConstraint();
        this.applyLatestDateConstraint();

        this.applySeasonConstraint();
        this.applyProjectConstraint();
        return this.getScript();
    }

    private void applyBasisTime()
    {
        this.addTab().add("(EXTRACT(epoch FROM O.observation_time)");

        if (this.getTimeShift() != null)
        {
            this.add(" + ", this.getTimeShift().getSeconds());
        }

        this.addLine( ")::bigint AS basis_epoch_time," );
    }

    private void applyProjectConstraint()
    {
        this.addLine( "    AND EXISTS (" );
        this.addLine( "        SELECT 1" );
        this.addLine( "        FROM wres.ProjectSource PS");
        this.addLine( "        WHERE PS.project_id = ", this.getProjectDetails().getId());
        this.addLine( "            AND PS.member = ", this.getMember());
        this.addLine( "            AND PS.source_id = O.source_id");
        this.addLine( "    )");
    }

    @Override
    protected void applyEarliestDateConstraint() throws SQLException
    {
        this.add( "    AND ", this.getZeroDate(), " <= ", this.getBaseDateName() );
        this.applyTimeShift();
        this.addLine();
    }

    private String getZeroDate() throws SQLException
    {
        if (this.zeroDate == null)
        {
            this.zeroDate = this.getProjectDetails().getInitialObservationDate(
                    this.getDataSourceConfig(),
                    this.getFeature()
            );
        }
        return this.zeroDate;
    }

    @Override
    String getBaseDateName()
    {
        return "O.observation_time";
    }

    @Override
    String getValueDate()
    {
        return this.getBaseDateName();
    }

    private String zeroDate;
}
