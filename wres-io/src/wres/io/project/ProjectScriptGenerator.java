package wres.io.project;

import java.sql.SQLException;
import java.util.StringJoiner;

import wres.config.generated.Feature;
import wres.config.generated.Polygon;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.util.Strings;

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

    static DataScripter createIntersectingFeaturesScript( Database database,
                                                          Project project )
            throws SQLException
    {
        // First, select all forecasted variable feature IDs, feature IDs, and feature metadata
        //    Whose variable is used as right hand data and located within the confines of the project's
        //    feature specification that are used within this project
        // Next, select all observed variable feature IDs and feature IDs
        //    Whose variable is used as left hand data and located within the confines of the project's
        //    feature specification that are used within this project
        // Then join the two resulting data sets based on their shared feature ids and return
        //    - Metadata about the shared feature that may be used for identification
        DataScripter script = new DataScripter( database );

        script.addLine( "WITH right_features AS" );
        script.addLine( "(");
        script.addTab().addLine( "SELECT VF.feature_id" );
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

                script.add(" gage_id = '", feature.getGageId(), "'" );
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

        script.addTab(  2  ).addLine("AND EXISTS ");
        script.addTab(  2  ).addLine( "(" );

        script.addTab(   3   ).addLine( "SELECT 1" );
        script.addTab(   3   ).addLine( "FROM wres.TimeSeries TS" );
        script.addTab(   3   ).addLine( "INNER JOIN wres.ProjectSource PS" );
        script.addTab(    4    ).addLine( "ON PS.source_id = TS.source_id" );
        script.addTab(   3   ).addLine( "WHERE PS.project_id = ",
                                          project.getId() );
        script.addTab(    4    ).addLine( "AND PS.member = 'right'" );
        script.addTab(    4    ).addLine( "AND TS.variablefeature_id = VF.variablefeature_id" );
        // Do NOT additionally inspect wres.TimeSeriesValue. See #70130.

        script.addTab(   3   ).addLine( "UNION ALL" );
        script.addTab(   3   ).addLine( "SELECT 1" );
        script.addTab(   3   ).addLine( "FROM wres.Observation O" );
        script.addTab(   3   ).addLine( "INNER JOIN wres.ProjectSource PS" );
        script.addTab(    4    ).addLine( "ON PS.source_id = O.source_id" );
        script.addTab(   3   ).addLine( "WHERE PS.project_id = ",
                                          project.getId() );
        script.addTab(    4    ).addLine( "AND PS.member = 'right'" );
        script.addTab(    4    ).addLine( "AND O.variablefeature_id = VF.variablefeature_id" );

        script.addTab(   3   ).addLine(")");
        script.addTab().addLine( "GROUP BY VF.feature_id" );
        script.add(")");

        script.addLine( "," );
        script.addLine( "left_features AS " );
        script.addLine( "(" );
        script.addTab().addLine( "SELECT VF.feature_id" );
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

                script.add( " gage_id = '", feature.getGageId(), "'" );
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
        script.addTab(   3   ).addLine( "FROM wres.TimeSeries TS" );
        script.addTab(   3   ).addLine( "INNER JOIN wres.ProjectSource PS" );
        script.addTab(    4    ).addLine( "ON PS.source_id = TS.source_id" );
        script.addTab(   3   ).addLine( "WHERE PS.project_id = ",
                                        project.getId() );
        script.addTab(    4    ).addLine( "AND PS.member = 'left'" );
        script.addTab(    4    ).addLine( "AND TS.variablefeature_id = VF.variablefeature_id" );
        // Do NOT additionally inspect wres.TimeSeriesValue. See #70130.

        script.addTab(   3   ).addLine( "UNION ALL" );
        script.addTab(   3   ).addLine("SELECT 1");
        script.addTab(   3   ).addLine("FROM wres.Observation O");
        script.addTab(   3   ).addLine("INNER JOIN wres.ProjectSource PS");
        script.addTab(    4    ).addLine("ON PS.source_id = O.source_id");
        script.addTab(   3   ).addLine( "WHERE PS.project_id = ", project.getId());
        script.addTab(    4    ).addLine("AND PS.member = 'left'");
        script.addTab(    4    ).addLine("AND O.variablefeature_id = VF.variablefeature_id");

        // #65881
        script.addTab(    4    ).addLine( "AND O.observed_value IS NOT NULL" );
        script.addTab(  2  ).addLine(")");
        script.addTab(  2  ).addLine( "GROUP BY VF.feature_id" );
        script.addLine(")");

        script.addLine( "SELECT RF.feature_id" );
        script.addLine("FROM right_features RF");
        script.addLine("INNER JOIN left_features LF");
        script.addTab().addLine("ON LF.feature_id = RF.feature_id");

        return script;
    }

}
