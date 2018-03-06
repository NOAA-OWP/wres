package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;
import wres.util.TimeHelper;

class PoolingForecastScripter extends Scripter
{
    protected PoolingForecastScripter( ProjectDetails projectDetails,
                                       DataSourceConfig dataSourceConfig,
                                       Feature feature,
                                       int progress,
                                       int sequenceStep)
    {
        super( projectDetails, dataSourceConfig, feature, progress, sequenceStep );
    }

    @Override
    String formScript() throws SQLException, IOException
    {
        this.applyCommonTableExpression();
        this.addLine("SELECT ");
        this.applyScaleMember();
        this.applyValueDate();
        this.addLine("    F.lead,");
        this.applyMeasurementArray();
        this.addLine("    F.measurementunit_id");
        this.addLine( "FROM forecasts F" );
        this.applyGroupAndOrderBy();

        return this.getScript();
    }

    private void applyCommonTableExpression()
            throws SQLException, IOException
    {
        // TODO: Break out into separate functions
        this.addLine("WITH forecasts AS");
        this.addLine("(");
        this.addLine("    SELECT F.basis_time,");
        this.addLine( "       F.valid_time,");
        this.addLine("        F.lead,");
        this.addLine("        F.forecasted_value,");
        this.addLine("        F.ensemble_id,");
        this.addLine("        F.measurementunit_id");
        this.addLine("    FROM wres.Forecasts F");
        this.addLine("    WHERE F.variable_id = ", this.getVariableID());

        long frequency = TimeHelper.unitsToLeadUnits( this.getProjectDetails().getIssuePoolingWindowUnit(), this.getProjectDetails().getIssuePoolingWindowFrequency() );
        long span = TimeHelper.unitsToLeadUnits( this.getProjectDetails().getIssuePoolingWindowUnit(), this.getProjectDetails().getIssuePoolingWindowPeriod());

        this.addLine( "        AND F.feature_id = ", Features.getFeatureID(this.getFeature()));
        this.addTab(2).addLine("AND ", this.getProjectDetails().getLeadQualifier( this.getFeature(), this.getProgress(), "F" ));

        this.add( "        AND F.basis_time >= ('", this.getProjectDetails().getEarliestIssueDate(), "'::timestamp without time zone + (INTERVAL '1 HOUR' * ");
        this.add( frequency );
        this.addLine(" ) * ", this.getSequenceStep(), ")");
        this.add( "        AND F.basis_time <= ('", this.getProjectDetails().getEarliestIssueDate() );
        this.add("'::timestamp without time zone + (INTERVAL '1 HOUR' * ", frequency, ") * ", this.getSequenceStep(), ")");

        if (span > 0)
        {
            this.addLine( " + INTERVAL '", span, " HOUR'" );
        }
        else
        {
            this.addLine();
        }

        this.applyEnsembleConstraint();

        this.addLine( "        AND EXISTS (" );
        this.addLine( "            SELECT 1");
        this.addLine( "            FROM wres.ForecastSource FS");
        this.addLine( "            INNER JOIN wres.ProjectSource PS" );
        this.addLine( "                ON PS.source_id = FS.source_id" );
        this.addLine( "            WHERE PS.project_id = ", this.getProjectDetails().getId() );
        this.addLine( "                AND PS.member = ", this.getMember() );
        this.addLine( "                AND FS.forecast_id = F.timeseries_id" );
        this.addLine( "        )" );
        this.addLine(")");
    }

    private void applyMeasurementArray()
    {
        this.addLine("    ARRAY_AGG(F.forecasted_value ORDER BY F.ensemble_id) AS measurements,");
    }

    private void applyGroupAndOrderBy()
    {
        this.addLine( "GROUP BY F.basis_time, F.valid_time, F.lead, F.measurementunit_id" );
        this.addLine( "ORDER BY F.basis_time, F.valid_time, F.lead;" );
    }

    private void applyScaleMember() throws IOException
    {
        if ( !this.getProjectDetails().shouldScale(this.getDataSourceConfig()) )
        {
            this.add("F.lead");
        }
        else
        {
            int leadModifier = this.getProgress();
            leadModifier *= this.getProjectDetails().getScale().getFrequency();
            leadModifier += this.getLeadOffset();
            leadModifier += 1;

            this.add( "(F.lead - ", leadModifier, ") % " );
            this.add( TimeHelper.unitsToLeadUnits( this.getProjectDetails()
                                                       .getScale().getUnit().value(),
                                                   this.getProjectDetails()
                                                       .getScale().getPeriod() ) );
        }
        this.addLine( " AS scale_member," );
    }

    private void applyEnsembleConstraint() throws SQLException
    {
        if (this.getDataSourceConfig().getEnsemble() != null &&
            ! this.getDataSourceConfig().getEnsemble().isEmpty())
        {
            int includeCount = 0;
            int excludeCount = 0;
            StringJoiner
                    include = new StringJoiner( ",", "ANY('{", "}'::integer[])");
            StringJoiner exclude = new StringJoiner(",", "ANY('{", "}'::integer[])");

            for ( EnsembleCondition condition : this.getDataSourceConfig().getEnsemble())
            {
                if ( condition.isExclude() )
                {
                    excludeCount++;
                    exclude.add(String.valueOf( Ensembles.getEnsembleID( condition)));
                }
                else
                {
                    includeCount++;
                    include.add( String.valueOf( Ensembles.getEnsembleID( condition ) ) );
                }
            }

            if (includeCount > 0)
            {
                this.addLine( "    AND F.ensemble_id = ", include.toString() );
            }

            if (excludeCount > 0)
            {
                this.addLine( "    AND NOT F.ensemble_id = ", exclude.toString() );
            }
        }
    }

    @Override
    String getBaseDateName()
    {
        return "F.basis_time";
    }

    @Override
    String getValueDate()
    {
        return "F.valid_time";
    }

    private int getLeadOffset() throws IOException
    {
        Integer offset;
        try
        {
            offset =
                    this.getProjectDetails().getLeadOffset( this.getFeature() );
        }
        catch ( SQLException e )
        {
            throw new IOException("The offset for '" +
                                  ConfigHelper.getFeatureDescription( this.getFeature() ) +
                                  "' could not be evaluated.");
        }

        if (offset == null)
        {
            throw new NoDataException( "There is not a valid offset for '" + ConfigHelper
                    .getFeatureDescription(this.getFeature()) + "'." );
        }

        return offset;
    }
}
