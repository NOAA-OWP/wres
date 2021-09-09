package wres.io.project;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.StringJoiner;

import wres.config.generated.Feature;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

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
     * @param database The database to use
     * @param projectId The wres.project row id to look for intersecting data.
     * @param featureDeclarations Original or generated feature declarations.
     * @param hasBaseline Whether the project has a baseline dataset.
     * @param isGrouped is true if the features are part of a group
     * @return the script
     */

    static DataScripter createIntersectingFeaturesScript( Database database,
                                                          long projectId,
                                                          List<Feature> featureDeclarations,
                                                          boolean hasBaseline,
                                                          boolean isGrouped )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( featureDeclarations );

        DataScripter script = new DataScripter( database );

        String tempTableName = ProjectScriptGenerator.generateTempTableName( projectId,
                                                                             featureDeclarations,
                                                                             isGrouped );
        boolean featuresDeclared = !featureDeclarations.isEmpty();

        if ( featuresDeclared )
        {
            if ( hasBaseline )
            {
                script.addLine( "CREATE TEMPORARY TABLE " + tempTableName
                                + " ( left_name VARCHAR, right_name VARCHAR, baseline_name VARCHAR ) ;" );
            }
            else
            {
                script.addLine( "CREATE TEMPORARY TABLE " + tempTableName
                                + " ( left_name VARCHAR, right_name VARCHAR ) ;" );
            }

            String insertStatement = ProjectScriptGenerator.generateInsertStatement( tempTableName,
                                                                                     featureDeclarations,
                                                                                     hasBaseline );
            script.addLine( insertStatement );
        }


        if ( hasBaseline )
        {
            script.addLine( "SELECT L.feature_id left_id, R.feature_id right_id, B.feature_id baseline_id" );
        }
        else
        {
            script.addLine( "SELECT L.feature_id left_id, R.feature_id right_id" );
        }

        if ( featuresDeclared )
        {
            script.addLine( "FROM " + tempTableName + " X" );
            script.addLine( "INNER JOIN wres.Feature L ON X.left_name = L.name" );
        }
        else
        {
            // When no features are specified, default to matching names in both
            // the left and right datasets. This assumes that a name is used
            // consistently within a dataset ingested for an evaluation.
            script.addLine( "FROM wres.Feature L");
            script.addLine( "INNER JOIN wres.Feature R ON" );
            script.addTab().addLine( "L.name = R.name" );
        }

        script.addLine( "AND EXISTS" );
        script.addLine( "(" );
        script.addTab().addLine( "SELECT 1" );
        script.addTab().addLine( "FROM wres.TimeSeries TS" );
        script.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
        script.addTab().addLine( "ON PS.source_id = TS.source_id" );
        script.addTab().addLine( "WHERE PS.project_id = ", projectId );
        script.addTab( 2 ).addLine( "AND PS.member = 'left'" );
        script.addTab( 2 ).addLine( "AND TS.feature_id = L.feature_id" );
        // Do NOT additionally inspect wres.TimeSeriesValue. See #70130.
        script.addLine( ")" );

        if ( featuresDeclared )
        {
            script.addLine( "INNER JOIN wres.Feature R ON X.right_name = R.name" );
        }

        script.addLine( "AND EXISTS" );
        script.addLine( "(" );
        script.addTab().addLine( "SELECT 1" );
        script.addTab().addLine( "FROM wres.TimeSeries TS" );
        script.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
        script.addTab().addLine( "ON PS.source_id = TS.source_id" );
        script.addTab().addLine( "WHERE PS.project_id = ", projectId );
        script.addTab( 2 ).addLine( "AND PS.member = 'right'" );
        script.addTab( 2 ).addLine( "AND TS.feature_id = R.feature_id" );

        script.addLine(")");

        if ( hasBaseline )
        {
            // Baseline is optional for a given declaration, but when there is
            // a baseline dataset, require there be data for the baseline for a
            // feature for any pairs to be generated for that feature.

            if ( featuresDeclared )
            {
                script.addLine( "INNER JOIN wres.Feature B ON X.baseline_name = B.name" );
            }
            else
            {
                // When no features are specified, default to matching names in
                // both the left and baseline datasets. This assumes that a name
                // is used consistently within each dataset ingested for an
                // evaluation.
                script.addLine( "INNER JOIN wres.Feature B ON " );
                script.addTab().addLine( "L.name = B.name" );
            }

            script.addLine( "AND EXISTS" );
            script.addLine( "(" );
            script.addTab().addLine( "SELECT 1" );
            script.addTab().addLine( "FROM wres.TimeSeries TS" );
            script.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
            script.addTab().addLine( "ON PS.source_id = TS.source_id" );
            script.addTab().addLine( "WHERE PS.project_id = ", projectId );
            script.addTab( 2 ).addLine( "AND PS.member = 'baseline'" );
            script.addTab( 2 ).addLine( "AND TS.feature_id = B.feature_id" );
            script.addLine( ")" );
        }

        // Cannot drop here because we need the script to return data. Let the
        // database system remove the temp table when it sees fit, e.g. at
        // end of session or restart of database server or whatever.
        // script.addLine( "DROP TABLE " + tempTableName + ";" );

        return script;
    }


    /**
     * Creates a script that retrieves the mostly commonly occurring measurement unit among all sources for each side of
     * data (left, right or baseline) and then selects the right or left or baseline among them as the representative
     * unit, in that order. 
     * 
     * @param database The database to use
     * @param projectId The wres.project row id to look for intersecting data.
     * @return the script
     */

    static DataScripter createUnitScript( Database database,
                                          long projectId )
    {
        DataScripter script = new DataScripter( database );

        /* For a given project, select the most common measurement unit (by source)" );
           for the right-ish sources. All time-series have units. */
        script.addLine( "SELECT MU.unit_name," );
        script.addTab().addLine( "T.measurementunit_id," );
        script.addTab().addLine( "PS.member" );
        script.addLine( "FROM wres.TimeSeries T" );
        script.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
        script.addTab( 2 ).addLine( "ON PS.source_id = T.source_id" );
        script.addTab().addLine( "INNER JOIN wres.MeasurementUnit MU" );
        script.addTab( 2 ).addLine( "ON MU.measurementunit_id = T.measurementunit_id" );
        script.addLine( "WHERE PS.project_id = ?" );
        script.addArgument( projectId );
        script.addTab().addLine( "AND PS.member='right'" );
        script.addLine( "GROUP BY T.measurementunit_id," );
        script.addTab().addLine( "PS.member," );
        script.addTab().addLine( "MU.unit_name" );
        script.addLine( "ORDER BY COUNT( T.measurementunit_id ) DESC" );
        script.setMaxRows( 1 );

        return script;
    }

    /**
     * Creates a script that retrieves the distinct varaible names for a given side of data.
     * 
     * @param database The database, not null
     * @param projectId The project identifier
     * @param lrb The side of data, not null
     * @return the script
     * @throws NullPointerException if any nullable input is null
     */
    
    static DataScripter createVariablesScript( Database database,
                                               long projectId,
                                               LeftOrRightOrBaseline lrb )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( lrb );
        
        DataScripter script = new DataScripter( database );
        
        script.addLine( "SELECT DISTINCT TS.variable_name" );
        script.addLine( "FROM wres.TimeSeries TS" );
        script.addLine( "INNER JOIN wres.ProjectSource PS ON" );
        script.addTab().addLine( "PS.source_id = TS.source_id" );
        script.addLine( "WHERE PS.project_id = ?" );
        script.addArgument( projectId );
        script.addTab().addLine( "AND PS.member = ?" );
        script.addArgument( lrb.toString().toLowerCase() );
        script.setMaxRows( 2 ); // Adds jdbc limit plus sql LIMIT if supported
        
        return script;
    }  

    /**
     * Returns a script that checks whether the condition on the ensemble name is valid.
     *  
     * @param database the database, not null
     * @param ensembleName the ensemble name
     * @param projectId the project id
     * @param exclude is true if the ensemble name should be excluded, false to include
     * @return a scripter that checks ensemble condition validity
     */

    static DataScripter getIsValidEnsembleCondition( Database database,
                                                     String ensembleName,
                                                     long projectId,
                                                     boolean exclude )
    {
        Objects.requireNonNull( database );

        DataScripter script = new DataScripter( database );

        String condition = "=";
        if ( exclude )
        {
            condition = "!=";
        }

        /* For a given project, determine whether an ensemble condition will select
           some time-series data. */
        script.addLine( "SELECT EXISTS (" );
        script.addTab().addLine( "SELECT 1" );
        script.addTab().addLine( "FROM wres.Ensemble E" );
        script.addTab().addLine( "INNER JOIN wres.TimeSeries TS" );
        script.addTab( 2 ).addLine( "ON TS.ensemble_id = E.ensemble_id" );
        script.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
        script.addTab( 2 ).addLine( "ON PS.source_id = TS.source_id" );
        script.addTab().addLine( "WHERE PS.project_id = ?" );
        script.addArgument( projectId );
        script.addTab( 2 ).addLine( "AND E.ensemble_name ", condition, " ?" );
        script.addArgument( ensembleName );
        script.addLine( ");" );

        return script;
    }


    /**
     * Returns a temporary table name. Can't use the current time in millis as
     * sole seed because the collision to avoid would be when this method runs
     * at the same millisecond-on-the-clock. Try to avoid collision by using
     * nanos and some attribute of the project datasets and features given.
     * Add additional qualification for a feature group because the features 
     * may be common across singletons and groups and groups may appear more 
     * than once with a different name, i.e., the features list is not 
     * sufficient qualification.
     * @param projectId the project identifier
     * @param features the features
     * @param isGrouped is true if the features are grouped
     * @return A temporary table name.
     */

    private static String generateTempTableName( long projectId,
                                                 List<Feature> features,
                                                 boolean isGrouped )
    {
        int qualifier = 0;
        if( isGrouped )
        {
            qualifier = new Random().nextInt();
        }
        
        Random random = new Random( Instant.now()
                                           .getNano()
                                    + features.hashCode()
                                    + qualifier );
        long someNumber = random.nextLong();

        while ( someNumber < 0 )
        {
            someNumber = random.nextLong();
        }

        return "wres_project_" + projectId + "_feature_correlation_"
               + someNumber;
    }

    /**
     * Create an insert statement for feature correlations. May be two or three
     * columns, depending on no-baseline vs has-baseline respectively.
     * @param tempTableName The name of the temporary table to use.
     * @param features The list of features declared (or generated).
     * @param hasBaseline True if baseline is used, false otherwise.
     * @return The SQL INSERT statement or a blank string if no features added.
     */

    private static String generateInsertStatement( String tempTableName,
                                                   List<Feature> features,
                                                   boolean hasBaseline )
    {
        String start = "INSERT INTO "
                       + tempTableName;

        if ( hasBaseline )
        {
            start += " ( left_name, right_name, baseline_name ) ";
        }
        else
        {
            start += " ( left_name, right_name ) ";
        }

        start += "VALUES" + System.lineSeparator() + "( ";

        StringJoiner joiner = new StringJoiner( " ),"
                                                + System.lineSeparator()
                                                + "( ",
                                                start,
                                                " );" );
        boolean anyFeaturesWereAdded = false;
        for ( Feature feature : features )
        {
            String toAdd;
            String leftName = feature.getLeft();
            String rightName = feature.getRight();
            String baselineName = feature.getBaseline();

            if ( Objects.nonNull( leftName )
                 && Objects.nonNull( rightName ) )
            {
                toAdd = "'" + validateStringForSql( leftName ) + "', "
                        + "'" + validateStringForSql( rightName ) + "'";

                if ( hasBaseline )
                {
                    if ( Objects.nonNull( baselineName ) )
                    {
                        toAdd += ", '" + validateStringForSql( baselineName )
                                 + "'";
                        joiner.add( toAdd );
                        anyFeaturesWereAdded = true;
                    }
                }
                else
                {
                    joiner.add( toAdd );
                    anyFeaturesWereAdded = true;
                }
            }
        }

        if ( anyFeaturesWereAdded )
        {
            return joiner.toString();
        }
        else
        {
            // In the case where no features were added, return empty string.
            // No features will be inserted (if features were declared).
            return "";
        }
    }

    /**
     * Use an allowed-character list to prevent control chars and/or ; and '
     *
     * Only here because the commons-lang method is deprecated, commons-text
     * does not provide a method to escape sql, and esapi only has something
     * rudimentary as well (and has a particular DBMS name in it).
     *
     * TODO: replace with a more robust 3rd party implementation that accounts
     * for various attacks, encodings, etc.
     *
     * @param possiblyDangerousString Potential sql injection attack String.
     * @return the same original String if it passes validation.
     * @throws IllegalArgumentException If dangerous char found.
     */
    static String validateStringForSql( String possiblyDangerousString )
    {
        possiblyDangerousString.chars()
                               .forEach( c -> {
                                   if ( !Character.isAlphabetic( c )
                                        && !Character.isDigit( c )
                                        && !Character.isSpaceChar( c )
                                        && !Character.isIdeographic( c )
                                        && Character.getType( c ) != Character.DASH_PUNCTUATION
                                        && Character.getType( c ) != Character.CONNECTOR_PUNCTUATION )
                                       throw new IllegalArgumentException( "Unsupported char '" + c + " found" );
                               } );

        return possiblyDangerousString;
    }
}
