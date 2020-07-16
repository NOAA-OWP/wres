package wres.io.project;

import java.util.List;
import java.util.Objects;

import wres.config.generated.Feature;
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
     */

    static DataScripter createIntersectingFeaturesScript( Database database,
                                                          int projectId,
                                                          List<Feature> featureDeclarations )
    {
        DataScripter script = new DataScripter( database );

        script.addLine( "SELECT L.feature_id left_id, R.feature_id right_id, B.feature_id baseline_id" );
        script.addLine( "FROM wres.Feature L");
        script.addLine( "INNER JOIN wres.Feature R ON" );
        script.addLine( "(" );

        boolean addedFeature = false;

        for ( Feature featureDeclaration : featureDeclarations )
        {
            if ( Objects.nonNull( featureDeclaration.getLeft() )
                 && Objects.nonNull( featureDeclaration.getRight() ) )
            {
                if ( addedFeature )
                {
                    script.addTab().addLine( "OR" );
                }

                script.addTab().addLine( "(" );
                script.addTab( 2 ).add( "L.name = '" );
                script.add( validateStringForSql( featureDeclaration.getLeft() ) );
                script.add( "' AND R.name = '" );
                script.add( validateStringForSql( featureDeclaration.getRight() ) );
                script.addLine( "'" );
                script.addTab().addLine( ")" );
                addedFeature = true;
            }
        }

        if ( !addedFeature )
        {
            // When no features are specified, default to matching names in both
            // the left and right datasets. This assumes that a name is used
            // consistently within a dataset ingested for an evaluation.
            script.addTab().addLine( "L.name = R.name" );
        }

        script.addLine( ")" );
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

        // Baseline is optional for each feature pair. In other words it can be
        // null and shouldn't filter out pairs of left/right where they exist.
        // TODO: review if this is true (that non-existence of baseline data
        // should not cause filtering out of features).
        script.addLine( "LEFT JOIN wres.Feature B ON " );
        script.addLine( "(" );

        boolean addedBaselineFeature = false;

        for ( Feature featureDeclaration : featureDeclarations )
        {
            if ( Objects.nonNull( featureDeclaration.getLeft() )
                 && Objects.nonNull( featureDeclaration.getBaseline() ) )
            {
                if ( addedBaselineFeature )
                {
                    script.addTab().addLine( "OR" );
                }

                script.addTab().addLine( "(" );
                script.addTab( 2 ).add( "L.name = '" );
                script.add( validateStringForSql( featureDeclaration.getLeft() ) );
                script.add( "' AND B.name = '" );
                script.add( validateStringForSql( featureDeclaration.getBaseline() ) );
                script.addLine( "'" );
                script.addTab().addLine( ")" );
                addedBaselineFeature = true;
            }
        }

        if ( !addedBaselineFeature )
        {
            // When no features are specified, default to matching names in both
            // the left and baseline datasets. This assumes that a name is used
            // consistently within a dataset ingested for an evaluation.
            script.addTab().addLine( "L.name = B.name" );
        }

        script.addLine( ")" );
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

        return script;
    }

    /**
     * Replace ' with ` and replace ; with ,
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
    private static String validateStringForSql( String possiblyDangerousString )
    {
        possiblyDangerousString.chars()
                               .forEach( c -> {
                                   if ( !Character.isAlphabetic( c )
                                        && !Character.isDigit( c )
                                        && !Character.isSpaceChar( c )
                                        && !Character.isIdeographic( c ) )
                                       throw new IllegalArgumentException( "Invalid char found" );
                               } );

        return possiblyDangerousString;
    }
}
