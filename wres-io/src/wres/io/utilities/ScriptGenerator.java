/**
 * 
 */
package wres.io.utilities;

import java.sql.SQLException;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.Polygon;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Variables;
import wres.io.data.details.ProjectDetails;
import wres.util.Strings;
import wres.util.TimeHelper;

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
        script.addTab().addLine("SELECT VP.variableposition_id, feature_id, comid, gage_id, lid, huc, latitude, longitude");
        script.addTab().addLine("FROM wres.VariablePosition VP");
        script.addTab().addLine("INNER JOIN wres.Feature F");
        script.addTab(  2  ).addLine("ON F.feature_id = VP.x_position");
        script.addTab().addLine("WHERE VP.variable_id = ", projectDetails.getRightVariableID());

        boolean addedFeature = false;
        for (Feature feature : projectDetails.getProjectConfig().getPair().getFeature())
        {
            if (!Strings.hasValue( feature.getRfc() ) &&
                !Strings.hasValue( feature.getGageId() ) &&
                !Strings.hasValue( feature.getHuc() ) &&
                feature.getComid() == null &&
                !Strings.hasValue( feature.getLocationId() ) )
            {
                continue;
            }

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
                else
                {
                    addedParameter = true;
                }

                script.add(" gage_id = '", feature.getGageId());
            }

            if (feature.getPolygon() != null)
            {
                if (addedParameter)
                {
                    script.add(" ADD");
                }
                else
                {
                    addedParameter = true;
                }

                StringJoiner pointJoiner = new StringJoiner(
                        "), (",
                        " POINT(F.longitude, F.latitude) <@ POLYGON '((",
                        ")) '"
                );

                for ( Polygon.Point point : feature.getPolygon().getPoint())
                {
                    pointJoiner.add( point.getLongitude() + ", " + point.getLatitude() );
                }

                script.add(pointJoiner.toString());
            }

            if (feature.getCoordinate() != null)
            {
                Double radianLatitude = Math.toRadians(feature.getCoordinate().getLatitude());

                // This is the approximate distance between longitudinal degrees at
                // the equator
                final Double distanceAtEquator = 111321.0;

                // This is an approximation
                Double distanceOfOneDegree = Math.cos(radianLatitude) * distanceAtEquator;

                // We take the max between the approximate distance and 0.00005 because
                // 0.00005 serves as a decent epsilon for database distance comparison.
                // If the distance is much smaller than that, float point error
                // can exclude the requested location, even if the coordinates were
                // spot on.
                Double rangeInDegrees = Math.max( feature.getCoordinate().getRange() / distanceOfOneDegree, 0.00005);

                if (addedParameter)
                {
                    script.add(" AND");
                }
                else
                {
                    addedParameter = true;
                }

                // The feature is within the number of degrees from given point
                script.add(" longitude IS NOT NULL AND ",
                           "latitude IS NOT NULL AND ",
                           "SQRT(",
                               "(",
                                   feature.getCoordinate().getLongitude(),
                                   " - F.longitude",
                               ")^2 + ",
                               "(",
                                   feature.getCoordinate().getLatitude(),
                                   " - F.latitude",
                               ")^2",
                           ") <= ", rangeInDegrees);
            }

            if (feature.getCircle() != null)
            {
                if (addedParameter)
                {
                    script.add(" AND");
                }

                // The feature is within the diameter of a circle centered at
                // the given point
                script.add(" F.longitude IS NOT NULL AND F.latitude IS NOT NULL AND ",
                           "POINT(F.longitude, F.latitude) <@ CIRCLE '((",
                           feature.getCircle().getLongitude(), ", ",
                           feature.getCircle().getLatitude(), "), ",
                           feature.getCircle().getDiameter(), ")'");
            }

            script.addLine(" )");
        }

        if (addedFeature)
        {
            script.addTab( 2 ).addLine( ")" );
        }

        script.addTab(  2  ).addLine("AND EXISTS (");
        script.addTab(   3   ).addLine("SELECT 1");
        script.addTab(   3   ).addLine("FROM wres.TimeSeries TS");
        script.addTab(   3   ).addLine("INNER JOIN wres.ForecastSource FS");
        script.addTab(    4    ).addLine("ON TS.timeseries_id = FS.forecast_id");
        script.addTab(   3   ).addLine("INNER JOIN wres.ProjectSource PS");
        script.addTab(    4    ).addLine("ON PS.source_id = FS.source_id");
        script.addTab(   3   ).addLine("WHERE PS.project_id = ", projectDetails.getId());
        script.addTab(    4    ).addLine("AND PS.member = 'right'");
        script.addTab(    4    ).addLine("AND TS.variableposition_id = VP.variableposition_id");
        script.addTab(    4    ).addLine("AND EXISTS (");
        script.addTab(     5     ).addLine("SELECT 1");
        script.addTab(     5     ).addLine("FROM wres.ForecastValue FV");
        script.addTab(     5     ).addLine("WHERE FV.timeseries_id = TS.timeseries_id");
        script.addTab(    4    ).addLine(")");
        script.addTab(   3   ).addLine(")");
        script.addTab().addLine("GROUP BY VP.variableposition_id, F.feature_id");
        script.addLine(")");

        if (selectObservedPositions)
        {
            script.addLine( "," );
            script.addLine( "observation_positions AS " );
            script.addLine( "(" );
            script.addTab().addLine("SELECT VP.variableposition_id, feature_id");
            script.addTab().addLine("FROM wres.VariablePosition VP");
            script.addTab().addLine("INNER JOIN wres.Feature F");
            script.addTab(  2  ).addLine("ON F.feature_id = VP.x_position");
            script.addTab().addLine("WHERE VP.variable_id = ", projectDetails.getLeftVariableID());

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

                if (feature.getPolygon() != null)
                {
                    if (addedParameter)
                    {
                        script.add(" ADD");
                    }
                    else
                    {
                        addedParameter = true;
                    }

                    StringJoiner pointJoiner = new StringJoiner(
                            "), (",
                            " POINT(longitude, latitude) <@ POLYGON '((",
                            ")) '"
                    );

                    for ( Polygon.Point point : feature.getPolygon().getPoint())
                    {
                        pointJoiner.add( point.getLongitude() + ", " + point.getLatitude() );
                    }

                    script.add(pointJoiner.toString());
                }

                if (feature.getCoordinate() != null)
                {
                    Double radianLatitude = Math.toRadians(feature.getCoordinate().getLatitude());

                    // This is the approximate distance between longitudinal degrees at
                    // the equator
                    final Double distanceAtEquator = 111321.0;

                    // This is an approximation
                    Double distanceOfOneDegree = Math.cos(radianLatitude) * distanceAtEquator;

                    // We take the max between the approximate distance and 0.00005 because
                    // 0.00005 serves as a decent epsilon for database distance comparison.
                    // If the distance is much smaller than that, float point error
                    // can exclude the requested location, even if the coordinates were
                    // spot on.
                    Double rangeInDegrees = Math.max( feature.getCoordinate().getRange() / distanceOfOneDegree, 0.00005);

                    if (addedParameter)
                    {
                        script.add(" AND");
                    }
                    else
                    {
                        addedParameter = true;
                    }

                    // The feature is within the number of degrees from given point
                    script.add(" longitude IS NOT NULL AND ",
                               "latitude IS NOT NULL AND ",
                               "SQRT(",
                               "(",
                               feature.getCoordinate().getLongitude(),
                               " - longitude",
                               ")^2 + ",
                               "(",
                               feature.getCoordinate().getLatitude(),
                               " - latitude",
                               ")^2",
                               ") <= ", rangeInDegrees);
                }

                if (feature.getCircle() != null)
                {
                    if (addedParameter)
                    {
                        script.add(" AND");
                    }

                    // The feature is within the diameter of a circle centered at
                    // the given point
                    script.add(" longitude IS NOT NULL AND latitude IS NOT NULL AND ",
                               "POINT(longitude, latitude) <@ CIRCLE '((",
                               feature.getCircle().getLongitude(), ", ",
                               feature.getCircle().getLatitude(), "), ",
                               feature.getCircle().getDiameter(), ")'");
                }

                script.addLine( " )" );
            }

            if (addedFeature)
            {
                script.addTab( 2 ).addLine( ")" );
            }

            script.addTab(  2  ).addLine("AND EXISTS (");
            script.addTab(   3   ).addLine("SELECT 1");
            script.addTab(   3   ).addLine("FROM wres.Observation O");
            script.addTab(   3   ).addLine("INNER JOIN wres.ProjectSource PS");
            script.addTab(    4    ).addLine("ON PS.source_id = O.source_id");
            script.addTab(   3   ).addLine("WHERE PS.project_id = ", projectDetails.getId());
            script.addTab(    4    ).addLine("AND PS.member = 'left'");
            script.addTab(    4    ).addLine("AND O.variableposition_id = VP.variableposition_id");
            script.addTab(  2  ).addLine(")");
            script.addTab(  2  ).addLine("GROUP BY VP.variableposition_id, feature_id");
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

        script.addTab().addLine("FP.comid,");
        script.addTab().addLine("FP.gage_id,");
        script.addTab().addLine("FP.huc,");
        script.addTab().addLine("FP.lid,");
        script.addTab().addLine("FP.latitude,");
        script.addTab().addLine("FP.longitude");
        script.addLine("FROM forecast_positions FP");

        if (selectObservedPositions)
        {
            script.addLine("INNER JOIN observation_positions O");
            script.addTab().addLine("ON O.feature_id = FP.feature_id");
        }

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

        long period = TimeHelper.unitsToLeadUnits(projectDetails.getIssuePoolingWindowUnit(), projectDetails.getIssuePoolingWindowPeriod());
        long frequency = TimeHelper.unitsToLeadUnits( projectDetails.getIssuePoolingWindowUnit(), projectDetails.getIssuePoolingWindowFrequency() );

        long distanceBetween;

        if (period == frequency)
        {
            distanceBetween = period;
        }
        else if (period == 0)
        {
            distanceBetween = frequency;
        }
        else
        {
            distanceBetween = period - frequency;
        }

        script.addLine("SELECT FLOOR(((EXTRACT( epoch FROM AGE(LEAST(MAX(initialization_date), '",
                       projectDetails.getLatestIssueDate(), "') - INTERVAL '",
                       period - frequency,
                       " HOURS', '",
                       projectDetails.getEarliestIssueDate(),
                       "')) / 3600) / (",
                       distanceBetween,
                       ")))::int AS window_count");
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

        script.append( "SELECT '''' || MIN(O.observation_time)::text || '''' AS zero_date" ).append(NEWLINE);
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

    public static String generateInitialForecastDateScript( ProjectDetails projectDetails,
                                                            DataSourceConfig forecastConfig,
                                                            Feature feature)
            throws SQLException
    {
        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("SELECT '''' || MIN(TS.initialization_date)::text || '''' AS zero_date" );
        script.addLine("FROM wres.TimeSeries TS");
        script.addLine("WHERE ", ConfigHelper.getVariablePositionClause( feature, Variables.getVariableID( forecastConfig ) , "TS"));
        script.addTab().addLine("AND EXISTS (");
        script.addTab(  2  ).addLine("SELECT 1");
        script.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        script.addTab(  2  ).addLine("INNER JOIN wres.ForecastSource FS");
        script.addTab(   3   ).addLine( "ON FS.source_id = PS.source_id");
        script.addTab(  2  ).addLine("WHERE PS.project_id = ", projectDetails.getId());
        script.addTab(   3   ).addLine( "AND PS.member = ", projectDetails.getInputName( forecastConfig ));
        script.addTab(   3   ).addLine( "AND FS.forecast_id = TS.timeseries_id");
        script.addTab(  2  ).add(")");

        if (projectDetails.getEarliestIssueDate() != null)
        {
            script.addLine();
            script.addTab(  2  );
            script.add("AND TS.initialization_date >= '", projectDetails.getEarliestIssueDate(), "'");
        }

        if (projectDetails.getLatestIssueDate() != null)
        {
            script.addLine();
            script.addTab(  2  );
            script.add("AND TS.initialization_date <= '", projectDetails.getLatestIssueDate(), "'");
        }

        script.add( ";" );

        return script.toString();
    }
}
