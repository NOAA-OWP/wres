/**
 * 
 */
package wres.io.utilities;

import java.sql.SQLException;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Variables;
import wres.io.data.details.ProjectDetails;
import wres.util.Strings;
/**
 * @author Christopher Tubbs
 *
 */
public final class ScriptGenerator
{
    private ScriptGenerator (){}
    
    private static final String NEWLINE = System.lineSeparator();

    public static String formVariablePositionLoadScript( final ProjectDetails projectDetails, final boolean selectObservedPositions)
            throws NoDataException, SQLException
    {

        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine( "WITH forecast_positions AS" );
        script.addLine( "(");
        script.addTab().addLine("SELECT TS.variableposition_id, feature_id");
        script.addTab().addLine("FROM wres.TimeSeries TS");
        script.addTab().addLine("INNER JOIN wres.VariableByFeature VBF");
        script.addTab( 2 ).addLine( "ON VBF.variableposition_id = TS.variableposition_id");
        script.addTab().addLine("WHERE VBF.variable_id = ", projectDetails.getRightVariableID());

        boolean addedFeature = false;
        for (Feature feature : projectDetails.getProjectConfig().getPair().getFeature())
        {
            if (!addedFeature)
            {
                addedFeature = true;
                script.addTab( 2 ).addLine("AND (");
                script.addTab( 3 );
            }
            else
            {
                script.addTab( 3 ).add("OR");
            }

            script.add(" (");

            boolean addedParameter = false;
            if (feature.getComid() != null)
            {
                script.add(" comid = ", feature.getComid());
                addedParameter = true;
            }

            if ( Strings.hasValue( feature.getLocationId()))
            {
                if (addedParameter)
                {
                    script.add(" AND");
                }
                else
                {
                    addedParameter = true;
                }

                script.add(" lid = '", feature.getLocationId().toUpperCase(), "'");
            }

            if (Strings.hasValue( feature.getHuc() ))
            {
                if (addedParameter)
                {
                    script.add(" AND");
                }
                else
                {
                    addedParameter = true;
                }

                script.add(" huc LIKE '", feature.getHuc(), "%'");
            }

            if (Strings.hasValue( feature.getRfc() ))
            {
                if (addedParameter)
                {
                    script.add(" AND");
                }
                else
                {
                    addedParameter = true;
                }

                script.add(" rfc = '", feature.getRfc().toUpperCase(), "'");
            }

            if (Strings.hasValue( feature.getGageId() ))
            {
                if (addedParameter)
                {
                    script.add(" AND");
                }

                script.add(" gage_id = '", feature.getGageId());
            }

            script.addLine(" )");
        }

        if (addedFeature)
        {
            script.addTab( 2 ).addLine( ")" );
        }

        script.addTab(2).addLine("AND EXISTS (");
        script.addTab( 3 ).addLine( "SELECT 1");
        script.addTab( 3 ).addLine( "FROM wres.ProjectSource PS");
        script.addTab( 3 ).addLine( "INNER JOIN wres.ForecastSource FS");
        script.addTab(  4  ).addLine(  "ON FS.source_id = PS.source_id");
        script.addTab( 3 ).addLine( "WHERE PS.project_id = ", projectDetails.getId());
        script.addTab(  4  ).addLine(  "AND PS.member = 'right'");
        script.addTab(  4  ).addLine(  "AND FS.forecast_id = TS.timeseries_id");
        script.addTab(2).addLine(")");
        script.addTab().addLine("GROUP BY TS.variableposition_id, VBF.feature_id");
        script.add(")");

        if (selectObservedPositions)
        {
            script.addLine( "," );
            script.addLine( "observation_positions AS " );
            script.addLine( "(" );
            script.addTab()
                  .addLine( "SELECT VBF.variableposition_id, feature_id" );
            script.addTab().addLine( "FROM wres.observation O" );
            script.addTab().addLine( "INNER JOIN wres.VariableByFeature VBF" );
            script.addTab( 2 )
                  .addLine( "ON VBF.variableposition_id = O.variableposition_id" );
            script.addTab()
                  .addLine( "WHERE VBF.variable_id = ",
                            projectDetails.getLeftVariableID() );

            addedFeature = false;
            for ( Feature feature : projectDetails.getProjectConfig()
                                                  .getPair()
                                                  .getFeature() )
            {
                if ( !addedFeature )
                {
                    addedFeature = true;
                    script.addTab( 2 ).addLine( "AND (" );
                    script.addTab( 3 );
                }
                else
                {
                    script.addTab( 3 ).add( "OR" );
                }

                script.add( " (" );

                boolean addedParameter = false;
                if ( feature.getComid() != null )
                {
                    script.add( " comid = ", feature.getComid() );
                    addedParameter = true;
                }

                if ( Strings.hasValue( feature.getLocationId() ) )
                {
                    if ( addedParameter )
                    {
                        script.add( " AND" );
                    }
                    else
                    {
                        addedParameter = true;
                    }

                    script.add( " lid = '",
                                feature.getLocationId().toUpperCase(),
                                "'" );
                }

                if ( Strings.hasValue( feature.getHuc() ) )
                {
                    if ( addedParameter )
                    {
                        script.add( " AND" );
                    }
                    else
                    {
                        addedParameter = true;
                    }

                    script.add( " huc LIKE '", feature.getHuc(), "%'" );
                }

                if ( Strings.hasValue( feature.getRfc() ) )
                {
                    if ( addedParameter )
                    {
                        script.add( " AND" );
                    }
                    else
                    {
                        addedParameter = true;
                    }

                    script.add( " rfc = '",
                                feature.getRfc().toUpperCase(),
                                "'" );
                }

                if ( Strings.hasValue( feature.getGageId() ) )
                {
                    if ( addedParameter )
                    {
                        script.add( " AND" );
                    }

                    script.add( " gage_id = '", feature.getGageId() );
                }

                script.addLine( " )" );
            }

            if (addedFeature)
            {
                script.addTab( 2 ).addLine( ")" );
            }

            script.addTab(2).addLine("AND EXISTS (");
            script.addTab( 3 ).addLine( "SELECT 1" );
            script.addTab( 3 ).addLine( "FROM wres.ProjectSource PS" );
            script.addTab( 3 ).addLine( "WHERE PS.project_id = ", projectDetails.getId() );
            script.addTab(  4  ).addLine(  "AND PS.member = 'left'");
            script.addTab(  4  ).addLine(  "AND PS.source_id = O.source_id");
            script.addTab(2).addLine(")");
            script.addTab().addLine("GROUP BY VBF.variableposition_id, feature_id");
            script.addLine(")");
        }
        else
        {
            script.addLine();
        }

        script.addLine("SELECT FP.variableposition_id AS forecast_position,");

        if (selectObservedPositions)
        {
            script.addTab().addLine( "O.variableposition_id AS observation_position," );
        }

        script.addTab().addLine("F.comid,");
        script.addTab().addLine("F.gage_id,");
        script.addTab().addLine("F.huc,");
        script.addTab().addLine("F.lid");
        script.addLine("FROM forecast_positions FP");

        if (selectObservedPositions)
        {
            script.addLine("INNER JOIN observation_positions O");
            script.addTab().addLine("ON O.feature_id = FP.feature_id");
        }

        script.addLine("INNER JOIN wres.Feature F");
        script.addTab().addLine("ON FP.feature_id = F.feature_id;");

        return script.toString();
    }

    public static String formIssuePoolCountScript(
            ProjectDetails projectDetails,
            Feature feature)
            throws SQLException
    {
        ScriptBuilder script = new ScriptBuilder();

        String timeSeriesVariablePosition =
                ConfigHelper.getVariablePositionClause(
                        feature,
                        Variables.getVariableID( projectDetails.getRight() ),
                        "TS"
                );

        script.addLine("SELECT FLOOR(((EXTRACT( epoch FROM AGE(LEAST(MAX(initialization_date), '",
                       projectDetails.getLatestIssueDate(), "'), '",
                       projectDetails.getEarliestIssueDate(),
                       "')) / 3600) / (",
                       projectDetails.getIssuePoolingWindowPeriod() - projectDetails.getIssuePoolingWindowFrequency(),
                       ")) - 1)::int AS window_count");
        script.addLine("FROM wres.TimeSeries TS");
        script.addLine("WHERE ", timeSeriesVariablePosition);
        script.addLine("    AND EXISTS (");
        script.addLine("        SELECT 1");
        script.addLine("        FROM wres.ForecastSource FS");
        script.addLine("        INNER JOIN wres.ProjectSource PS");
        script.addLine("            ON PS.source_id = FS.source_id");
        script.addLine("        WHERE PS.project_id = ", projectDetails.getId());
        script.addLine("            AND PS.member = 'right'");
        script.addLine("            AND FS.forecast_id = TS.timeseries_id");
        script.addLine("    );");


        return script.toString();
    }

    public static String generateInitialObservationDateScript( ProjectDetails projectDetails,
                                                               DataSourceConfig simulation,
                                                               Feature feature)
            throws SQLException
    {

        if (simulation == null)
        {
            return null;
        }

        StringBuilder script = new StringBuilder(  );

        script.append( "SELECT MIN(O.observation_time)::text AS zero_date" ).append(NEWLINE);
        script.append("FROM wres.Observation O").append(NEWLINE);
        script.append("WHERE ")
              .append(ConfigHelper.getVariablePositionClause( feature,
                                                              Variables.getVariableID( simulation ),
                                                              "O"))
              .append(NEWLINE);
        script.append("    AND EXISTS (").append(NEWLINE);
        script.append("        SELECT 1").append(NEWLINE);
        script.append("        FROM wres.ProjectSource PS").append(NEWLINE);
        script.append("        WHERE PS.project_id = ").append(projectDetails.getId()).append(NEWLINE);
        script.append("            AND PS.member = ");

        if (projectDetails.getRight().equals( simulation ))
        {
            script.append(ProjectDetails.RIGHT_MEMBER);
        }
        else if (projectDetails.getLeft().equals( simulation ))
        {
            script.append(ProjectDetails.LEFT_MEMBER);
        }
        else
        {
            script.append(ProjectDetails.BASELINE_MEMBER);
        }

        script.append(NEWLINE);

        script.append("            AND PS.source_id = O.source_id").append(NEWLINE);
        script.append("        )").append(NEWLINE);

        if (projectDetails.getEarliestDate() != null)
        {
            script.append("     AND O.observation_time >= '")
                  .append(projectDetails.getEarliestDate())
                  .append( "'" )
                  .append(NEWLINE);
        }

        if (projectDetails.getLatestDate() != null)
        {
            script.append("     AND O.observation_time <= '")
                  .append(projectDetails.getLatestDate())
                  .append( "'" )
                  .append(NEWLINE);
        }

        script.append( ";" );

        return script.toString();
    }
}
