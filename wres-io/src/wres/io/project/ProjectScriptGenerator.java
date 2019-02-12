/**
 * 
 */
package wres.io.project;

import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Objects;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.Polygon;
import wres.io.config.ConfigHelper;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.utilities.DataScripter;
import wres.util.CalculationException;
import wres.util.Strings;
import wres.util.TimeHelper;

/**
 * Houses the logic used to create SQL scripts based on a project
 * @author Christopher Tubbs
 */
final class ProjectScriptGenerator
{
    // Since this class is only used for helper functions, we don't want anything to instantiate it
    private ProjectScriptGenerator(){}

    /**
     * Creates a script that retrieves a mapping between forecasted and observed features
     * @param project The project whose variables and features to load
     * @throws SQLException Thrown when the ID for the left or right variables cannot be loaded
     */
    static DataScripter formVariableFeatureLoadScript(final Project project) throws SQLException
    {
        // First, select all forecasted variable feature IDs, feature IDs, and feature metadata
        //    Whose variable is used as right hand data and located within the confines of the project's
        //    feature specification that are used within this project
        // Next, select all observed variable feature IDs and feature IDs
        //    Whose variable is used as left hand data and located within the confines of the project's
        //    feature specification that are used within this project
        // Then join the two resulting data sets based on their shared feature ids and return
        //    - The observed location's corresponding variable feature ID
        //    - The forecasted location's corresponding variable feature ID
        //    - Metadata about the shared feature that may be used for identification
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

                // is the approximate distance between longitudinal degrees at
                // the equator
                final Double distanceAtEquator = 111321.0;

                // is an approximation
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

                // is the approximate distance between longitudinal degrees at
                // the equator
                final Double distanceAtEquator = 111321.0;

                // is an approximation
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

    /**
     * Creates a script that determines the number of pools across forecast issue dates that will need to be evaluated
     * @param project The project whose issue pools to find
     * @param feature The feature whose forecasted data will be evaluated
     * @return The script that may be used to retrieve the number of pools across forecast issue dates to evaluate
     * @throws SQLException Thrown if it could not be determined whether or not the evaluated data is gridded
     * @throws SQLException Thrown if the ID of the forecasted variable could not be loaded
     * @throws SQLException Thrown when a clause to limit selection based on variable and location could not be formed
     */
    static DataScripter formIssuePoolCounter(final Project project, final Feature feature) throws SQLException
    {
        long period = TimeHelper.unitsToLeadUnits( project.getIssuePoolingWindowUnit(),
                                                   project.getIssuePoolingWindowPeriod());
        long frequency = TimeHelper.unitsToLeadUnits( project.getIssuePoolingWindowUnit(),
                                                      project.getIssuePoolingWindowFrequency() );

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

        // Ensure that this logic isn't blocked by the logic that calls it
        script.setHighPriority( true );

        // Since gridded data and vector data are stored differently, data must be selected from different tables,
        // resulting in seperate scripts
        boolean usesGriddedData;
        
        try
        {
            usesGriddedData = project.usesGriddedData( project.getRight() );
        }
        catch ( SQLException exception )
        {
            throw new SQLException( "It could not be determined whether or not project utilizes gridded data.",
                                    exception );
        }

        if ( usesGriddedData )
        {
            // Where "e" is the earliest of either the last forecast for the data set or the last allowable forecast,
            // where "s" is the date of the earliest allowable forecast,
            // where "a" is the amount of time between "e" and "s",
            // where "p" is "a" converted from seconds to minutes,
            // where "o" is the number of minutes between issue pools,
            // and where "c" is the amount of times "o" may occur within "p",
            // find the minimum whole number for "c" across all sources attached as right hand data for the project

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
            int rightVariableID;
            
            try
            {
                rightVariableID = Variables.getVariableID( project.getRight() );
            }
            catch ( SQLException e )
            {
                throw new SQLException(
                        "Identification information for what forecasted variable to evaluate could not be loaded.",
                        e );
            }

            String timeSeriesVariableFeature;
            try
            {
                timeSeriesVariableFeature = ConfigHelper.getVariableFeatureClause(
                        feature,
                        rightVariableID,
                        "TS"
                );
            }
            catch ( SQLException e )
            {
                throw new SQLException(
                        "The clause used to limit what variable and feature to evaluate could not be formed",
                        e );
            }

            // Where "e" is the earliest of either the last forecast for the data set or the last allowable forecast,
            // where "s" is the date of the earliest allowable forecast,
            // where "a" is the amount of time between "e" and "s",
            // where "p" is "a" converted from seconds to minutes,
            // where "o" is the number of minutes between issue pools,
            // and where "c" is the amount of times "o" may occur within "p",
            // find the minimum whole number for "c" across all times series attached as right hand
            // data for the project at the specified location

            // TODO: Experiment with removing the floor to determine if the issue dates extend beyond the upper limit correctly
            script.addLine("SELECT FLOOR(");
            script.addTab().addLine("(");
            script.addTab(  2  ).addLine("(");
            script.addTab(   3   ).addLine("EXTRACT (");
            script.addTab(    4    ).addLine("epoch FROM AGE (");
            script.addTab(     5     ).addLine("LEAST (");
            script.addTab(      6      ).addLine("MAX(initialization_date),");
            script.addTab(      6      ).addLine("'", project.getLatestIssueDate(), "'");
            script.addTab(     5     ).addLine(") - INTERVAL '", period - frequency, " ", TimeHelper.LEAD_RESOLUTION, "',");
            script.addTab(     5     ).addLine("'", project.getEarliestIssueDate(), "'");
            script.addTab(    4    ).addLine(")");
            script.addTab(   3   ).addLine(")");
            script.addTab(  2  ).addLine(") / 60");
            script.addTab().addLine(") / (", distanceBetween, ")");
            script.addLine(")::int AS window_count");
            script.addLine("FROM wres.TimeSeries TS");
            script.addLine("WHERE ", timeSeriesVariableFeature);
            script.addTab().addLine("AND EXISTS (");
            script.addTab(  2  ).addLine("SELECT 1");
            script.addTab(  2  ).addLine("FROM wres.TimeSeriesSource TSS");
            script.addTab(  2  ).addLine("INNER JOIN wres.ProjectSource PS");
            script.addTab(   3   ).addLine("ON PS.source_id = TSS.source_id");
            script.addTab(  2  ).addLine("WHERE PS.project_id = ", project.getId());
            script.addTab(   3   ).addLine("AND PS.member = ", Project.RIGHT_MEMBER);
            script.addTab(   3   ).addLine("AND TSS.timeseries_id = TS.timeseries_id");
            script.addTab().add(");");
        }

        return script;
    }

    /**
     * Creates a script that determines the earliest observation for a location
     * @param project The projects whose first observation date to find
     * @param observationConfig An observation data source specification
     * @param feature The location to evaluate
     * @return The script that determines the earliest observation
     * @throws SQLException Thrown when the observation variable ID could not be loaded
     * @throws SQLException Thrown when a clause to limit selection based on variable and location could not be formed 
     */
    static DataScripter generateInitialObservationDateScript(
            final Project project,
            final DataSourceConfig observationConfig,
            final Feature feature)
            throws SQLException
    {
        if (observationConfig == null)
        {
            return null;
        }
        
        int observedVariableID;
        String observedVariableFeatureClause;
        
        try
        {
            observedVariableID = Variables.getVariableID( observationConfig );
        }
        catch(SQLException exception)
        {
            throw new SQLException(
                    "Identification information for what observed variable to evaluate could not be loaded.",
                    exception );
        }
        
        try
        {
            observedVariableFeatureClause = ConfigHelper.getVariableFeatureClause( feature, observedVariableID,  "O" );
        }
        catch ( SQLException e )
        {
            throw new SQLException(
                    "The clause used to limit what variable and feature to evaluate could not be formed",
                    e );
        }

        DataScripter script = new DataScripter(  );

        script.addLine( "SELECT '''' || MIN(O.observation_time)::text || '''' AS zero_date" );
        script.addLine("FROM wres.Observation O");
        script.addLine("WHERE ", observedVariableFeatureClause);
        script.addTab().addLine("AND EXISTS (");
        script.addTab(  2  ).addLine("SELECT 1");
        script.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        script.addTab(  2  ).addLine("WHERE PS.project_id = ", project.getId());
        script.addTab(   3   ).add("AND PS.member = ");

        if ( project.getRight().equals( observationConfig ))
        {
            script.addLine( Project.RIGHT_MEMBER);
        }
        else if ( project.getLeft().equals( observationConfig ))
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

    /**
     * Creates a script that determines the earliest forecast for a location
     * @param project The project whose earliest forecast will be found
     * @param forecastConfig A forecast data source specification
     * @param feature The location to evaluate
     * @return The script that determines the earliest forecast
     * @throws SQLException Thrown when the forecast variable ID could not be loaded
     * @throws SQLException Thrown when a clause to limit selection based on variable and location could not be formed 
     */
    static DataScripter generateInitialForecastDateScript(final Project project,
                                                          final DataSourceConfig forecastConfig,
                                                          final Feature feature)
            throws SQLException
    {
        int forecastVariableID;
        String forecastVariableFeatureClause;
        
        try
        {
            forecastVariableID = Variables.getVariableID( forecastConfig );
        }
        catch(SQLException exception)
        {
            throw new SQLException(
                    "Identification information for what forecasted variable to evaluate could not be loaded.",
                    exception );
        }
        
        try
        {
            forecastVariableFeatureClause = ConfigHelper.getVariableFeatureClause( feature, forecastVariableID, "TS");
        }
        catch(SQLException exception)
        {
            throw new SQLException(
                    "The clause used to limit what variable and feature to evaluate could not be formed",
                    exception );
        }
        
        DataScripter script = new DataScripter(  );

        script.addLine("SELECT '''' || MIN(TS.initialization_date)::text || '''' AS zero_date" );
        script.addLine("FROM wres.TimeSeries TS");
        script.addLine("WHERE ", forecastVariableFeatureClause);
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

    /**
     * Creates a script that determines the largest distance between sequential forecasts
     * @param project The project whose forecast lag will be determined
     * @param dataSourceConfig A forecast data source configuration
     * @param feature The location to evaluate
     * @return A script that determines the largest distance between sequential forecasts
     * @throws CalculationException Thrown when a clause to limit selection based on variable and location could not be formed
     */
    static DataScripter createForecastLagScript(final Project project,
                                                final DataSourceConfig dataSourceConfig,
                                                final Feature feature)
            throws CalculationException
    {
        // script will tell us the maximum distance between
        // sequential forecasts for a feature for project.
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
        script.addTab(   3   ).addLine("WHERE PS.project_id = ", project.getId());
        script.addTab(    4    ).addLine("AND PS.member = ", project.getInputName( dataSourceConfig ));
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

    /**
     * Creates a script that will determine the last lead time present in a forecast for a location
     * @param project The project whose last lead needs to be found
     * @param feature The location to evaluate
     * @return A script that will determine the last lead time present in a forecast
     * @throws CalculationException Thrown if it could not be determined if forecasted data was gridded or not
     * @throws CalculationException Thrown if a script to determine the last lead for vector data could not be formed
     */
    static DataScripter createLastLeadScript(final Project project, final Feature feature) throws CalculationException
    {
        // Since gridded metadata is not distributed throughout multiple tables,
        // we need a seperate script for gridded data
        boolean usesGriddedData;

        try
        {
            usesGriddedData = project.usesGriddedData( project.getRight() );
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
            return createLastGriddedLeadScript(project);
        }

        return createLastVectorLeadScript(project, feature);
    }

    /**
     * Creates a script that will gather all unique time scales and time steps for all observed values for the project
     * 
     * TODO: use {@link Duration} for the lead durations
     * 
     * @param projectId The identifier of the project whose forecast time scales and steps will be found
     * @param features The features to evaluate
     * @param minimumLead the earliest lead duration
     * @param maximumLead the latest lead duration
     * @return A script that will gather all unique time scales and time steps for all forecasted values
     * @throws SQLException Thrown if a list of ids for all features for a project could not be formed
     * @throws NullPointerException if the collection of features is null
     */
    static DataScripter createObservedTimeRetriever( final int projectId,
                                                     final Collection<FeatureDetails> features,
                                                     final int minimumLead,
                                                     final int maximumLead,
                                                     final LeftOrRightOrBaseline sourceType )
            throws SQLException
    {
        Objects.requireNonNull( features );
        
        Objects.requireNonNull( sourceType );
        
        DataScripter scripter = new DataScripter();

        scripter.addLine("WITH differences AS");
        scripter.addLine("(");
        scripter.addTab().addLine("SELECT observation_time - lag(observation_time) OVER (ORDER BY variablefeature_id, observation_time) AS difference,");
        scripter.addTab(  2  ).addLine("scale_period,");
        scripter.addTab(  2  ).addLine("scale_function,");
        scripter.addTab(  2  ).addLine("variablefeature_id,");
        scripter.addTab(  2  ).addLine("lag(variablefeature_id) OVER (ORDER BY variablefeature_id) AS previous_variable_feature");
        scripter.addTab().addLine("FROM wres.Observation O");
        scripter.addTab().addLine("WHERE EXISTS (");
        scripter.addTab(   3   ).addLine("SELECT 1");
        scripter.addTab(   3   ).addLine("FROM wres.ProjectSource PS");
        scripter.addTab(   3   ).addLine("WHERE PS.project_id = ", projectId );
        scripter.addTab(    4    ).addLine("AND O.source_id = PS.source_id");
        scripter.addTab(    4    ).addLine("AND PS.member = '", sourceType.value(), "'" );
        scripter.addTab(  2  ).addLine(")");
        scripter.addTab(  2  ).addLine("AND EXISTS (");
        scripter.addTab(   3   ).addLine("SELECT 1");
        scripter.addTab(   3   ).addLine("FROM wres.VariableByFeature VBF");
        scripter.addTab(   3   ).addLine("WHERE VBF.variablefeature_id = O.variablefeature_id");
        scripter.addTab(    4    ).addLine("AND VBF.feature_id = ", createAnyFeatureStatement( features ,6 ));
        scripter.addTab(  2  ).addLine(")");
        scripter.addLine(")");
        scripter.addLine("SELECT EXTRACT(EPOCH FROM difference) / 60 AS time_step,");
        scripter.addTab().addLine("scale_period,");
        scripter.addTab().addLine("scale_function");
        scripter.addLine("FROM differences D");
        scripter.addLine("WHERE D.difference IS NOT NULL");
        scripter.addTab().addLine("AND D.variablefeature_id = D.previous_variable_feature");
        scripter.add("GROUP BY difference, scale_period, scale_function;");

        return scripter;
    }

    /**
     * Creates a script that will gather all unique time scales and time steps for all forecasted values for the project
     * 
     * TODO: use {@link Duration} for the lead durations
     * 
     * @param projectId The identifier of the project whose forecast time scales and steps will be found
     * @param features The features to evaluate
     * @param minimumLead the earliest lead duration
     * @param maximumLead the latest lead duration
     * @return A script that will gather all unique time scales and time steps for all forecasted values
     * @throws SQLException Thrown if a list of ids for all features for a project could not be formed
     * @throws NullPointerException if the collection of features is null or the sourceType is null
     */
    static DataScripter createForecastTimeRetriever( final int projectId,
                                                     final Collection<FeatureDetails> features,
                                                     final int minimumLead,
                                                     final int maximumLead,
                                                     final LeftOrRightOrBaseline sourceType )
            throws SQLException
    {
        Objects.requireNonNull( features );
        
        Objects.requireNonNull( sourceType );
        
        DataScripter scripter = new DataScripter();

        scripter.addLine("WITH differences AS");
        scripter.addLine("(");
        scripter.addTab().addLine("SELECT lead - lag(lead) OVER (ORDER BY TSV.timeseries_id, lead) AS difference,");
        scripter.addTab(  2  ).addLine("TS.timeseries_id,");
        scripter.addTab(  2  ).addLine("TS.scale_function,");
        scripter.addTab(  2  ).addLine("TS.scale_period,");
        scripter.addTab(  2  ).addLine("lag(TSV.timeseries_id) OVER (ORDER BY TSV.timeseries_id, lead) AS previous_timeseries_id");
        scripter.addTab().addLine("FROM wres.TimeSeriesValue TSV");
        scripter.addTab().addLine("INNER JOIN (");
        scripter.addTab(  2  ).addLine("SELECT TS.timeseries_id, TS.scale_function, TS.scale_period");
        scripter.addTab(  2  ).addLine("FROM wres.TimeSeries TS");
        scripter.addTab(  2  ).addLine("WHERE EXISTS (");
        scripter.addTab(    4    ).addLine("SELECT 1");
        scripter.addTab(    4    ).addLine("FROM wres.TimeSeriesSource TSS");
        scripter.addTab(    4    ).addLine("INNER JOIN wres.ProjectSource PS");
        scripter.addTab(     5     ).addLine("ON PS.source_id = TSS.source_id");
        scripter.addTab(    4    ).addLine("WHERE PS.project_id = ", projectId );
        scripter.addTab(     5     ).addLine("AND TSS.timeseries_id = TS.timeseries_id");
        scripter.addTab(     5     ).addLine("AND PS.member = '", sourceType.value(), "'" );
        scripter.addTab(   3   ).addLine(")");
        scripter.addTab(   3   ).addLine("AND EXISTS (");
        scripter.addTab(    4    ).addLine("SELECT 1");
        scripter.addTab(    4    ).addLine("FROM wres.VariableFeature VBF");
        scripter.addTab(    4    ).addLine("WHERE VBF.variablefeature_id = TS.variablefeature_id");
        scripter.addTab(     5     ).addLine("AND VBF.feature_id = ", createAnyFeatureStatement( features, 6 ));
        scripter.addTab(   3   ).addLine(")");
        scripter.addTab().addLine(") AS TS");
        scripter.addTab(  2  ).addLine("ON TS.timeseries_id = TSV.timeseries_id");

        if (minimumLead != Integer.MIN_VALUE)
        {
            scripter.addTab().addLine("WHERE lead > ", minimumLead );
        }
        else
        {
            scripter.addTab().addLine("WHERE lead > 0");
        }

        if (maximumLead != Integer.MAX_VALUE)
        {
            scripter.addTab(  2  ).addLine("AND lead <= ", maximumLead );
        }
        else
        {
            // Set the ceiling to 100 hours. If the maximum lead is 72 hours, then this
            // should not behave much differently than having no clause at all.
            // If real maximum was 2880 hours, 100 will provide a large enough sample
            // size and produce the correct values in a slightly faster fashion.
            // In one data set, leaving out causes to take 11.5s .
            // That was even with a subset of the real data (1 month vs 30 years).
            // If we cut it to 100 hours, it now takes 1.6s. Still not great, but
            // much faster
            int ceiling = TimeHelper.durationToLead( Duration.of( 100, ChronoUnit.HOURS) );
            scripter.addTab(  2  ).addLine( "AND lead <= ", ceiling );
        }

        scripter.addLine(")");
        scripter.addLine("SELECT D.difference AS time_step, D.scale_period, D.scale_function");
        scripter.addLine("FROM differences D");
        scripter.addLine("WHERE D.difference IS NOT NULL");
        scripter.addTab().addLine("AND D.timeseries_id = D.previous_timeseries_id");
        scripter.add("GROUP BY D.difference, D.scale_period, D.scale_function;");
        
        return scripter;
    }
    
    /**
     * Returns the time scale and time step information for a gridded source of observations
     * 
     * @param projectId the project identifier
     * @param sourceType the source type
     * @return the script to retrieve the time step and time scale information
     */

    static DataScripter createGriddedObservedTimeRetriever( final int projectId,
                                                            final LeftOrRightOrBaseline sourceType )
    {
        Objects.requireNonNull( sourceType );

        DataScripter scripter = new DataScripter();

        scripter.addLine( "SELECT EXTRACT(epoch FROM AGE(D.valid_time, D.previous_valid_time)) / 60 AS time_step," );
        scripter.addTab().addLine( "1 AS scale_period, -- We don't currently store gridded scaling information" );
        scripter.addTab().addLine( "'UNKNOWN' AS scale_function" );
        scripter.addLine( "FROM (" );
        scripter.addTab().addLine( "SELECT S.output_time + INTERVAL '1 MINUTE' * S.lead AS valid_time," );
        scripter.addTab( 2 ).addLine( "lag(output_time) OVER (ORDER BY output_time, lead) + " );
        scripter.addTab( 3 ).addLine( "INTERVAL '1 MINUTE' * lag(lead) OVER (ORDER BY OUTPUT_TIME, lead)" );
        scripter.addTab( 4 ).addLine( "AS previous_valid_time" );
        scripter.addTab().addLine( "FROM wres.Source S" );
        scripter.addTab().addLine( "WHERE EXISTS (" );
        scripter.addTab( 2 ).addLine( "SELECT 1" );
        scripter.addTab( 2 ).addLine( "FROM wres.ProjectSource PS" );
        scripter.addTab( 2 ).addLine( "WHERE PS.project_id = ", projectId );
        scripter.addTab( 3 ).addLine( "AND PS.member = '", sourceType.value(), "'" );
        scripter.addTab( 3 ).addLine( "AND PS.source_id = S.source_id" );
        scripter.addTab().addLine( ")" );
        scripter.addTab( 2 ).addLine( "AND NOT is_point_data" );
        scripter.addLine( ") AS D" );
        scripter.addLine( "WHERE D.previous_valid_time IS NOT NULL" );
        scripter.add( "GROUP BY AGE(D.valid_time, D.previous_valid_time);" );

        return scripter;
    }
    
    /**
     * Returns the time scale and time step information for a gridded source of forecasts
     * 
     * @param projectId the project identifier
     * @param sourceType the source type
     * @return the script to retrieve the time step and time scale information
     */

    static DataScripter createGriddedForecastTimeRetriever( final int projectId,
                                                            final LeftOrRightOrBaseline sourceType )
    {
        Objects.requireNonNull( sourceType );

        DataScripter scripter = new DataScripter();

        scripter.addLine( "SELECT D.difference AS time_step," );
        scripter.addTab().addLine( "1 AS scale_period, -- We don't currently store gridded scaling information" );
        scripter.addTab().addLine( "'UNKNOWN' AS scale_function" );
        scripter.addLine( "FROM (" );
        scripter.addTab().addLine( "SELECT lead - lag(lead) OVER (ORDER BY output_time, lead) AS difference," );
        scripter.addTab( 2 ).addLine( "output_time," );
        scripter.addTab( 2 ).addLine( "lag(output_time) OVER (ORDER BY output_time, lead) AS previous_output_time" );
        scripter.addTab().addLine( "FROM wres.Source S" );
        scripter.addTab().addLine( "WHERE EXISTS (" );
        scripter.addTab( 2 ).addLine( "SELECT 1" );
        scripter.addTab( 2 ).addLine( "FROM wres.ProjectSource PS" );
        scripter.addTab( 2 ).addLine( "WHERE PS.project_id = ", projectId );
        scripter.addTab( 3 ).addLine( "AND PS.member = '", sourceType.value(), "'" );
        scripter.addTab( 3 ).addLine( "AND PS.source_id = S.source_id" );
        scripter.addTab().addLine( ")" );
        scripter.addTab( 2 ).addLine( "AND NOT is_point_data" );
        scripter.addLine( ") AS D" );
        scripter.addLine( "WHERE D.output_time = D.previous_output_time" );
        scripter.add( "GROUP BY D.difference;" );

        return scripter;
    }  

    /**
     * Creates a SQL 'any' statement containing a list of all ids for features evaluated in a project
     * @param features The features to consider
     * @param tabOver The number of tabs to use to indent the list items
     * @return A SQL 'any' statement containing a list of all feature ids used in a project
     * @throws SQLException Thrown if all feature ids for the project could not be loaded
     * @throws NullPointerException if the collection of features is null
     */
    private static String createAnyFeatureStatement( final Collection<FeatureDetails> features, final int tabOver )
            throws SQLException
    {
        StringBuilder seperator = new StringBuilder();
        seperator.append( "," );
        seperator.append( System.lineSeparator() );

        for ( int tabIndex = 0; tabIndex < tabOver; ++tabIndex )
        {
            seperator.append( "    " );
        }

        StringJoiner anyJoiner = new StringJoiner( seperator.toString(), "ANY('{", "}')" );

        for ( FeatureDetails featureDetails : features )
        {
            anyJoiner.add( String.valueOf( featureDetails.getId() ) );
        }

        return anyJoiner.toString();
    }

    /**
     * Creates a script that will determine that latest lead used for gridded data within a project
     * @param project The project whose gridded data will be evaluated
     * @return A script that will determine the latest lead used for gridded data
     */
    private static DataScripter createLastGriddedLeadScript(final Project project)
    {
        DataScripter script = new DataScripter(  );

        script.addLine("SELECT MAX(S.lead) AS last_lead");
        script.addLine("FROM (");
        script.addTab().addLine("SELECT PS.source_id");
        script.addTab().addLine("FROM wres.ProjectSource PS");
        script.addTab().addLine("WHERE PS.project_id = ", project.getId());
        script.addTab(  2  ).addLine( "AND PS.member = ", Project.RIGHT_MEMBER);
        script.addLine(") AS PS");
        script.addLine("INNER JOIN wres.Source S");
        script.addTab().addLine("ON S.source_id = PS.source_id");

        boolean whereAdded = false;

        if (project.getMinimumLead() != Integer.MAX_VALUE)
        {
            whereAdded = true;
            script.addLine("WHERE S.lead >= ", project.getMinimumLead());
        }

        if (project.getMaximumLead() != Integer.MIN_VALUE)
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

            script.addLine("S.lead <= ", project.getMaximumLead());
        }

        if (Strings.hasValue(project.getEarliestIssueDate()))
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

            script.addLine("S.output_time >= '", project.getEarliestIssueDate(), "'");
        }

        if (Strings.hasValue( project.getLatestIssueDate() ))
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

            script.addLine("S.output_time <= '", project.getLatestIssueDate(), "'");
        }

        if (Strings.hasValue( project.getEarliestDate() ))
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

            script.addLine("S.output_time + INTERVAL '1 MINUTE' * S.lead >= '", project.getEarliestDate(), "'");
        }

        if (Strings.hasValue( project.getLatestDate() ))
        {
            if (whereAdded)
            {
                script.addTab().add("AND ");
            }
            else
            {
                script.add("WHERE ");
            }

            script.addLine("S.output_time + INTERVAL '1 MINUTE' * S.lead <= '", project.getLatestDate(), "'");
        }

        return script;
    }

    /**
     * Creates a script that will determine the latest lead used for vector data within a project
     * @param project The project whose vector data will be evaluated
     * @param feature The feature that has forecasted values at different lead times
     * @return A script that will determine the latest lead used for vector data
     * @throws CalculationException Thrown when a clause to limit selection based on variable and location could not be formed
     */
    private static DataScripter createLastVectorLeadScript(final Project project, final Feature feature)
            throws CalculationException
    {
        String variableFeatureClause;
        
        try
        {
            variableFeatureClause = ConfigHelper.getVariableFeatureClause(
                    feature,
                    project.getRightVariableID(),
                    ""
            );
        }
        catch ( SQLException exception )
        {
            throw new CalculationException(
                    "The clause used to limit what variable and feature to evaluate could not be formed",
                    exception );
        }
        
        DataScripter script = new DataScripter();

        if ( ConfigHelper.isForecast( project.getRight() ) )
        {
            script.addLine("SELECT MAX(TSV.lead) AS last_lead");
            script.addLine("FROM wres.TimeSeries TS");
            script.addLine("INNER JOIN wres.TimeSeriesValue TSV");
            script.addTab().addLine("ON TS.timeseries_id = TSV.timeseries_id");
            script.addLine("WHERE ", variableFeatureClause);

            if ( project.getMaximumLead() != Integer.MAX_VALUE )
            {
                script.addTab().addLine("AND TSV.lead <= " + project.getMaximumLead( ));
            }

            if ( project.getMinimumLead() != Integer.MIN_VALUE )
            {
                script.addTab().addLine("AND TSV.lead >= " + project.getMinimumLead( ));
            }

            if ( Strings.hasValue( project.getEarliestIssueDate()))
            {
                script.addTab().addLine("AND TS.initialization_date >= '" + project.getEarliestIssueDate() + "'");
            }

            if (Strings.hasValue( project.getLatestIssueDate()))
            {
                script.addTab().addLine("AND TS.initialization_date <= '" + project.getLatestIssueDate() + "'");
            }

            if ( Strings.hasValue( project.getEarliestDate() ))
            {
                script.addTab().addLine("AND TS.initialization_date + INTERVAL '1 MINUTE' * TSV.lead >= '" + project.getEarliestDate() + "'");
            }

            if (Strings.hasValue( project.getLatestDate() ))
            {
                script.addTab().addLine("AND TS.initialization_date + INTERVAL '1 MINUTE' * TSV.lead <= '" + project.getLatestDate() + "'");
            }

            script.addTab().addLine("AND EXISTS (");
            script.addTab(  2  ).addLine("SELECT 1");
            script.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
            script.addTab(  2  ).addLine("INNER JOIN wres.TimeSeriesSource TSS");
            script.addTab(   3   ).addLine("ON TSS.source_id = PS.source_id");
            script.addTab(  2  ).addLine("WHERE PS.project_id = " + project.getId());
            script.addTab(   3   ).addLine( "AND PS.member = " + Project.RIGHT_MEMBER);
            script.addTab(   3   ).addLine("AND TSS.timeseries_id = TS.timeseries_id");

            if (ConfigHelper.usesNetCDFData( project.getProjectConfig() ))
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
            script.addLine("WHERE PS.project_id = " + project.getId());
            script.addTab().add("AND ", variableFeatureClause);
        }

        return script;
    }
}
