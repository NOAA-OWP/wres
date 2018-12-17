/**
 * 
 */
package wres.io.project;

import java.sql.SQLException;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.Polygon;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Variables;
import wres.io.utilities.DataScripter;
import wres.io.utilities.NoDataException;
import wres.util.CalculationException;
import wres.util.Strings;
import wres.util.TimeHelper;

/**
 * @author Christopher Tubbs
 *
 */
final class ProjectScriptGenerator
{
    private final Project project;

    ProjectScriptGenerator(final Project project)
    {
        this.project = project;
    }


    /**
     * @throws NoDataException when ?
     */

    DataScripter formVariableFeatureLoadScript() throws SQLException
    {

        DataScripter script = new DataScripter(  );

        script.addLine( "WITH forecast_features AS" );
        script.addLine( "(");
        script.addTab().addLine("SELECT VF.variablefeature_id, F.feature_id, comid, gage_id, lid, huc, latitude, longitude");
        script.addTab().addLine("FROM wres.VariableFeature VF");
        script.addTab().addLine("INNER JOIN wres.Feature F");
        script.addTab(  2  ).addLine("ON F.feature_id = VF.feature_id");
        script.addTab().addLine( "WHERE VF.variable_id = ", project.getRightVariableID());

        boolean addedFeature = false;
        for (Feature feature : project.getProjectConfig().getPair().getFeature())
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

                script.add(" region = '", feature.getRfc().toUpperCase(), "'");
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

            if (Strings.hasValue(feature.getState()))
            {
                if( addedParameter )
                {
                    script.add(" AND");
                }
                else
                {
                    addedParameter = true;
                }

                script.add(" state = '", feature.getState().toUpperCase(), "'");
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
        script.addTab(   3   ).addLine("INNER JOIN wres.TimeSeriesSource TSS");
        script.addTab(    4    ).addLine("ON TS.timeseries_id = TSS.timeseries_id");
        script.addTab(   3   ).addLine("INNER JOIN wres.ProjectSource PS");
        script.addTab(    4    ).addLine("ON PS.source_id = TSS.source_id");
        script.addTab(   3   ).addLine( "WHERE PS.project_id = ", project.getId());
        script.addTab(    4    ).addLine("AND PS.member = 'right'");
        script.addTab(    4    ).addLine("AND TS.variablefeature_id = VF.variablefeature_id");
        script.addTab(    4    ).addLine("AND EXISTS (");
        script.addTab(     5     ).addLine("SELECT 1");
        script.addTab(     5     ).addLine("FROM wres.TimeSeriesValue TSV");
        script.addTab(     5     ).addLine("WHERE TSV.timeseries_id = TS.timeseries_id");
        script.addTab(    4    ).addLine(")");
        script.addTab(   3   ).addLine(")");
        script.addTab().addLine("GROUP BY VF.variablefeature_id, F.feature_id");
        script.add(")");

        script.addLine( "," );
        script.addLine( "observation_features AS " );
        script.addLine( "(" );
        script.addTab().addLine("SELECT VF.variablefeature_id, VF.feature_id");
        script.addTab().addLine("FROM wres.VariableFeature VF");
        script.addTab().addLine("INNER JOIN wres.Feature F");
        script.addTab(  2  ).addLine("ON F.feature_id = VF.feature_id");
        script.addTab().addLine( "WHERE VF.variable_id = ", project.getLeftVariableID());

        addedFeature = false;
        for ( Feature feature : project.getProjectConfig()
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

            if (Strings.hasValue(feature.getState()))
            {
                if( addedParameter )
                {
                    script.add(" AND");
                }
                else
                {
                    addedParameter = true;
                }

                script.add(" state = '", feature.getState().toUpperCase(), "'");
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

                script.add( " region = '",
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
        script.addTab(   3   ).addLine( "WHERE PS.project_id = ", project.getId());
        script.addTab(    4    ).addLine("AND PS.member = 'left'");
        script.addTab(    4    ).addLine("AND O.variablefeature_id = VF.variablefeature_id");
        script.addTab(  2  ).addLine(")");
        script.addTab(  2  ).addLine("GROUP BY VF.variablefeature_id, VF.feature_id");
        script.addLine(")");

        script.addLine("SELECT FP.variablefeature_id AS forecast_feature,");
        script.addTab().addLine( "O.variablefeature_id AS observation_feature," );
        script.addTab().addLine("FP.comid,");
        script.addTab().addLine("FP.gage_id,");
        script.addTab().addLine("FP.huc,");
        script.addTab().addLine("FP.lid,");
        script.addTab().addLine("FP.latitude,");
        script.addTab().addLine("FP.longitude");
        script.addLine("FROM forecast_features FP");
        script.addLine("INNER JOIN observation_features O");
        script.addTab().addLine("ON O.feature_id = FP.feature_id");

        return script;
    }

    DataScripter formIssuePoolCounter(Feature feature) throws SQLException
    {
        long period = TimeHelper.unitsToLeadUnits( project.getIssuePoolingWindowUnit(), project.getIssuePoolingWindowPeriod());
        long frequency = TimeHelper.unitsToLeadUnits( project.getIssuePoolingWindowUnit(), project.getIssuePoolingWindowFrequency() );

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

        DataScripter script = new DataScripter(  );
        script.setHighPriority( true );

        if ( project.usesGriddedData( project.getRight() ))
        {
            // TODO: Experiment with removing the floor to determine if the issue dates extend beyond the upper limit correctly
            script.addLine("SELECT FLOOR (");
            script.addTab().addLine("(");
            script.addTab(  2  ).addLine("(");
            script.addTab(   3   ).addLine("EXTRACT (");
            script.addTab(    4    ).addLine("epoch FROM AGE (");
            script.addTab(     5     ).addLine("LEAST (");
            script.addTab(      6      ).addLine("MAX(S.output_time),");
            script.addTab(      6      ).addLine( "'", project.getLatestIssueDate(), "'");
            script.addTab(     5     ).addLine(") - INTERVAL '", period - frequency, " MINUTES',");
            script.addTab(     5     ).addLine( "'", project.getEarliestIssueDate(), "'");
            script.addTab(    4    ).addLine(")");
            script.addTab(   3   ).addLine(")");
            script.addTab(  2  ).addLine(") / 60");
            script.addTab().addLine(") / (", distanceBetween, ")");
            script.addLine(")::int AS window_count");
            script.addLine("FROM wres.Source S");
            script.addLine("WHERE EXISTS (");
            script.addTab().addLine("SELECT 1");
            script.addTab().addLine("FROM wres.ProjectSource PS");
            script.addTab().addLine( "WHERE PS.project_id = ", project.getId());
            script.addTab(  2  ).addLine( "AND PS.member = ", Project.RIGHT_MEMBER);
            script.addTab(  2  ).addLine("AND PS.source_id = S.source_id");
            script.addLine(");");
        }
        else
        {
            String timeSeriesVariableFeature =
                ConfigHelper.getVariableFeatureClause(
                        feature,
                        Variables.getVariableID( project.getRight() ),
                        "TS"
                );

            // TODO: Experiment with removing the floor to determine if the issue dates extend beyond the upper limit correctly
            script.addLine( "SELECT FLOOR(((EXTRACT( epoch FROM AGE(LEAST(MAX(initialization_date), '",
                            project.getLatestIssueDate(), "') - INTERVAL '",
                           period - frequency,
                            " MINUTES', '",
                            project.getEarliestIssueDate(),
                            "')) / 60) / (",
                            distanceBetween,
                            ")))::int AS window_count");
            script.addLine("FROM wres.TimeSeries TS");
            script.addLine("WHERE ", timeSeriesVariableFeature);
            script.addLine("    AND EXISTS (");
            script.addLine("        SELECT 1");
            script.addLine("        FROM wres.TimeSeriesSource TSS");
            script.addLine("        INNER JOIN wres.ProjectSource PS");
            script.addLine("            ON PS.source_id = TSS.source_id");
            script.addLine( "        WHERE PS.project_id = ", project.getId());
            script.addLine("            AND PS.member = 'right'");
            script.addLine("            AND TSS.timeseries_id = TS.timeseries_id");
            script.addLine("    );");
        }

        return script;
    }

    DataScripter generateInitialObservationDateScript( DataSourceConfig simulation, Feature feature) throws SQLException
    {
        if (simulation == null)
        {
            return null;
        }

        DataScripter script = new DataScripter(  );

        script.addLine( "SELECT '''' || MIN(O.observation_time)::text || '''' AS zero_date" );
        script.addLine("FROM wres.Observation O");
        script.addLine("WHERE ",
                       ConfigHelper.getVariableFeatureClause(
                               feature,
                               Variables.getVariableID( simulation ),
                               "O"
                       )
        );
        script.addTab().addLine("AND EXISTS (");
        script.addTab(  2  ).addLine("SELECT 1");
        script.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        script.addTab(  2  ).addLine("WHERE PS.project_id = ", project.getId());
        script.addTab(   3   ).add("AND PS.member = ");

        if ( project.getRight().equals( simulation ))
        {
            script.addLine( Project.RIGHT_MEMBER);
        }
        else if ( project.getLeft().equals( simulation ))
        {
            script.addLine( Project.LEFT_MEMBER);
        }
        else
        {
            script.addLine( Project.BASELINE_MEMBER);
        }

        script.addTab(   3   ).addLine("AND PS.source_id = O.source_id");
        script.addTab().addLine(")");

        if ( project.getEarliestDate() != null)
        {
            script.addTab().addLine("AND O.observation_time >= '", project.getEarliestDate(), "'" );
        }

        if ( project.getLatestDate() != null)
        {
            script.addTab().addLine("AND O.observation_time <= '", project.getLatestDate(), "'" );
        }

        script.add( ";" );

        return script;
    }

    DataScripter generateInitialForecastDateScript( DataSourceConfig forecastConfig, Feature feature) throws SQLException
    {
        DataScripter script = new DataScripter(  );

        script.addLine("SELECT '''' || MIN(TS.initialization_date)::text || '''' AS zero_date" );
        script.addLine("FROM wres.TimeSeries TS");
        script.addLine("WHERE ", ConfigHelper.getVariableFeatureClause( feature, Variables.getVariableID( forecastConfig ) , "TS"));
        script.addTab().addLine("AND EXISTS (");
        script.addTab(  2  ).addLine("SELECT 1");
        script.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        script.addTab(  2  ).addLine("INNER JOIN wres.TimeSeriesSource TSS");
        script.addTab(   3   ).addLine( "ON TSS.source_id = PS.source_id");
        script.addTab(  2  ).addLine( "WHERE PS.project_id = ", project.getId());
        script.addTab(   3   ).addLine( "AND PS.member = ", project.getInputName( forecastConfig ));
        script.addTab(   3   ).addLine( "AND TSS.timeseries_id = TS.timeseries_id");
        script.addTab(  2  ).add(")");

        if ( project.getEarliestIssueDate() != null)
        {
            script.addLine();
            script.addTab(  2  );
            script.add( "AND TS.initialization_date >= '", project.getEarliestIssueDate(), "'");
        }

        if ( project.getLatestIssueDate() != null)
        {
            script.addLine();
            script.addTab(  2  );
            script.add( "AND TS.initialization_date <= '", project.getLatestIssueDate(), "'");
        }

        script.add( ";" );

        return script;
    }

    DataScripter createForecastLagScript(final DataSourceConfig dataSourceConfig, final Feature feature)
            throws CalculationException
    {
        // This script will tell us the maximum distance between
        // sequential forecasts for a feature for this project.
        // We don't need the intended distance. If we go with the
        // intended distance (say 3 hours), but the user just doesn't
        // have or chooses not to use a forecast, resulting in a gap of
        // 6 hours, we'll encounter an error because we're aren't
        // accounting for that weird gap. By going with the maximum, we
        // ensure that we will always cover that gap.
        DataScripter script = new DataScripter();
        script.addLine("WITH initialization_lag AS");
        script.addLine("(");
        script.addTab().addLine("SELECT (");
        script.addTab(  2  ).addLine("EXTRACT (");
        script.addTab(   3   ).addLine( "epoch FROM AGE (");
        script.addTab(    4    ).addLine( "TS.initialization_date,");
        script.addTab(    4    ).addLine( "(");
        script.addTab(     5     ).addLine("LAG(TS.initialization_date) OVER (ORDER BY TS.initialization_date)");
        script.addTab(    4    ).addLine( ")");
        script.addTab(   3   ).addLine(")");
        script.addTab(  2  ).addLine(") / 60)::int AS lag -- Divide by 60 to convert seconds into minutes");
        script.addTab().addLine("FROM wres.TimeSeries TS");
        script.addTab().add("WHERE ");
        try
        {
            script.addLine(ConfigHelper.getVariableFeatureClause(
                    feature,
                    Variables.getVariableID( dataSourceConfig ),
                    "TS" )
            );
        }
        catch ( SQLException e )
        {
            throw new CalculationException( "The variable is needed to calculate the "
                                            + "maximum distance between forecasts for " +
                                            ConfigHelper.getFeatureDescription( feature ) +
                                            ", but it could not be loaded",
                                            e );
        }
        script.addTab(  2  ).addLine("AND EXISTS (");
        script.addTab(   3   ).addLine("SELECT 1");
        script.addTab(   3   ).addLine("FROM wres.TimeSeriesSource TSS");
        script.addTab(   3   ).addLine("INNER JOIN wres.ProjectSource PS");
        script.addTab(    4    ).addLine("ON PS.source_id = TSS.source_id");
        script.addTab(   3   ).addLine("WHERE PS.project_id = ", this.project.getId());
        script.addTab(    4    ).addLine("AND PS.member = ", this.project.getInputName( dataSourceConfig ));
        script.addTab(    4    ).addLine("AND TSS.timeseries_id = TS.timeseries_id");
        script.addTab(  2  ).addLine(")");
        script.addTab().addLine("GROUP BY TS.initialization_date");
        script.addTab().addLine("ORDER BY TS.initialization_date");
        script.addLine(")");
        script.addLine("SELECT max(IL.lag) AS typical_gap");
        script.addLine("-- We take the max to ensure that values are contained within;");
        script.addLine("--   If we try to take the mode, we risk being too granular");
        script.addLine("--   and trying to select values that aren't there.");
        script.addLine("FROM initialization_lag IL");
        script.addLine("WHERE IL.lag IS NOT NULL;");

        return script;
    }

    DataScripter createLastLeadScript(final Feature feature) throws CalculationException
    {
        boolean usesGriddedData;

        try
        {
            usesGriddedData = this.project.usesGriddedData( this.project.getRight() );
        }
        catch ( SQLException e )
        {
            throw new CalculationException( "To determine the last possible lead to "
                                            + "evaluate, the system needs to know whether "
                                            + "or not ingested data is gridded, but that "
                                            + "could not be loaded.",
                                            e );
        }

        if (usesGriddedData)
        {
            return this.createLastGriddedLeadScript();
        }

        return this.createLastVectorLeadScript(feature);
    }

    private DataScripter createLastGriddedLeadScript()
    {
        DataScripter script = new DataScripter(  );

        script.addLine("SELECT MAX(S.lead) AS last_lead");
        script.addLine("FROM (");
        script.addTab().addLine("SELECT PS.source_id");
        script.addTab().addLine("FROM wres.ProjectSource PS");
        script.addTab().addLine("WHERE PS.project_id = ", this.project.getId());
        script.addTab(  2  ).addLine( "AND PS.member = ", Project.RIGHT_MEMBER);
        script.addLine(") AS PS");
        script.addLine("INNER JOIN wres.Source S");
        script.addTab().addLine("ON S.source_id = PS.source_id");

        boolean whereAdded = false;

        if (this.project.getMinimumLead() != Integer.MAX_VALUE)
        {
            whereAdded = true;
            script.addLine("WHERE S.lead >= ", this.project.getMinimumLead());
        }

        if (this.project.getMaximumLead() != Integer.MIN_VALUE)
        {
            if (whereAdded)
            {
                script.addTab().add("AND ");
            }
            else
            {
                whereAdded = true;
                script.add("WHERE ");
            }

            script.addLine("S.lead <= ", this.project.getMaximumLead());
        }

        if (Strings.hasValue(this.project.getEarliestIssueDate()))
        {
            if (whereAdded)
            {
                script.addTab().add("AND ");
            }
            else
            {
                whereAdded = true;
                script.add("WHERE ");
            }

            script.addLine("S.output_time >= '", this.project.getEarliestIssueDate(), "'");
        }

        if (Strings.hasValue( this.project.getLatestIssueDate() ))
        {
            if (whereAdded)
            {
                script.addTab().add("AND ");
            }
            else
            {
                whereAdded = true;
                script.add("WHERE ");
            }

            script.addLine("S.output_time <= '", this.project.getLatestIssueDate(), "'");
        }

        if (Strings.hasValue( this.project.getEarliestDate() ))
        {
            if (whereAdded)
            {
                script.addTab().add("AND ");
            }
            else
            {
                whereAdded = true;
                script.add("WHERE ");
            }

            script.addLine("S.output_time + INTERVAL '1 MINUTE' * S.lead >= '", this.project.getEarliestDate(), "'");
        }

        if (Strings.hasValue( this.project.getLatestDate() ))
        {
            if (whereAdded)
            {
                script.addTab().add("AND ");
            }
            else
            {
                script.add("WHERE ");
            }

            script.addLine("S.output_time + INTERVAL '1 MINUTE' * S.lead <= '", this.project.getLatestDate(), "'");
        }

        return script;
    }

    private DataScripter createLastVectorLeadScript(final Feature feature) throws CalculationException
    {
        DataScripter script = new DataScripter();

        if ( ConfigHelper.isForecast( this.project.getRight() ) )
        {
            script.addLine("SELECT MAX(TSV.lead) AS last_lead");
            script.addLine("FROM wres.TimeSeries TS");
            script.addLine("INNER JOIN wres.TimeSeriesValue TSV");
            script.addTab().addLine("ON TS.timeseries_id = TSV.timeseries_id");
            try
            {
                script.addLine("WHERE " +
                               ConfigHelper.getVariableFeatureClause( feature,
                                                                      this.project.getRightVariableID(),
                                                                      "TS" ));
            }
            catch ( SQLException e )
            {
                throw new CalculationException( "The variable is needed to determine the "
                                                + "last lead for " +
                                                ConfigHelper.getFeatureDescription( feature ) +
                                                ", but it could not be loaded.",
                                                e);
            }

            if ( this.project.getMaximumLead() != Integer.MAX_VALUE )
            {
                script.addTab().addLine("AND TSV.lead <= " + this.project.getMaximumLead( ));
            }

            if ( this.project.getMinimumLead() != Integer.MIN_VALUE )
            {
                script.addTab().addLine("AND TSV.lead >= " + this.project.getMinimumLead( ));
            }

            if ( Strings.hasValue( this.project.getEarliestIssueDate()))
            {
                script.addTab().addLine("AND TS.initialization_date >= '" + this.project.getEarliestIssueDate() + "'");
            }

            if (Strings.hasValue( this.project.getLatestIssueDate()))
            {
                script.addTab().addLine("AND TS.initialization_date <= '" + this.project.getLatestIssueDate() + "'");
            }

            if ( Strings.hasValue( this.project.getEarliestDate() ))
            {
                script.addTab().addLine("AND TS.initialization_date + INTERVAL '1 MINUTE' * TSV.lead >= '" + this.project.getEarliestDate() + "'");
            }

            if (Strings.hasValue( this.project.getLatestDate() ))
            {
                script.addTab().addLine("AND TS.initialization_date + INTERVAL '1 MINUTE' * TSV.lead <= '" + this.project.getLatestDate() + "'");
            }

            script.addTab().addLine("AND EXISTS (");
            script.addTab(  2  ).addLine("SELECT 1");
            script.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
            script.addTab(  2  ).addLine("INNER JOIN wres.TimeSeriesSource TSS");
            script.addTab(   3   ).addLine("ON TSS.source_id = PS.source_id");
            script.addTab(  2  ).addLine("WHERE PS.project_id = " + this.project.getId());
            script.addTab(   3   ).addLine( "AND PS.member = " + Project.RIGHT_MEMBER);
            script.addTab(   3   ).addLine("AND TSS.timeseries_id = TS.timeseries_id");

            if (ConfigHelper.usesNetCDFData( this.project.getProjectConfig() ))
            {
                script.addTab(   3   ).addLine("AND TSS.lead = TSV.lead");
            }

            script.addTab().addLine(");");
        }
        else
        {
            script.addLine("SELECT COUNT(*)::int AS last_lead");
            script.addLine("FROM wres.Observation O");
            script.addLine("INNER JOIN wres.ProjectSource PS");
            script.addTab().addLine("ON PS.source_id = O.source_id");
            script.addLine("WHERE PS.project_id = " + this.project.getId());
            try
            {
                script.addTab().addLine("AND " +
                                        ConfigHelper.getVariableFeatureClause(
                                                feature,
                                                this.project.getRightVariableID(),
                                                "O;" ));
            }
            catch ( SQLException e )
            {
                throw new CalculationException( "The variable is needed to determine the "
                                                + "number of observations for " +
                                                ConfigHelper.getFeatureDescription( feature ) +
                                                ", but it could not be loaded.",
                                                e);
            }
        }

        return script;
    }
}
