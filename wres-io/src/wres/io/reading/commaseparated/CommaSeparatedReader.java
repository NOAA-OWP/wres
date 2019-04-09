package wres.io.reading.commaseparated;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;

import wres.config.FeaturePlus;
import wres.config.generated.Feature;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Helps read files of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 */

public class CommaSeparatedReader
{

    /**
     * Reads a CSV source that contains one or more thresholds for each of several features. Places the results into 
     * a {@link Map} whose keys are {@link FeaturePlus} and whose values comprise a {@link Set} of {@link Threshold}. 
     * 
     * @param commaSeparated the source of comma separated values
     * @param isProbability is true to read probability thresholds
     * @param condition the threshold condition
     * @param dataType the threshold data type
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param units the optional units associated with the threshold values
     * @return a map of thresholds by feature
     * @throws IOException if the source cannot be read or contains unexpected input
     * @throws NullPointerException if the source is null or the condition is null
     * @throws IllegalArgumentException if one or more features failed with expected problems, such as 
     *            all thresholds missing, thresholds that contain non-numeric input and thresholds that 
     *            are invalid (e.g. probability thresholds that are out-of-bounds). 
     */

    public static Map<FeaturePlus, Set<Threshold>> readThresholds( Path commaSeparated,
                                                                   boolean isProbability,
                                                                   Operator condition,
                                                                   ThresholdDataType dataType,
                                                                   Double missingValue,
                                                                   MeasurementUnit units )
            throws IOException
    {
        Objects.requireNonNull( commaSeparated, "Specify a non-null source of comma separated thresholds to read." );

        Objects.requireNonNull( condition, "Specify a non-null condition in order to read the thresholds." );

        Objects.requireNonNull( dataType, "Specify a non-null data type in order to read the thresholds." );

        Map<FeaturePlus, Set<Threshold>> returnMe = new TreeMap<>();

        // Rather than drip-feeding failures, collect all expected failure types, which
        // are IllegalArgumentException and NumberFormatException and propagate at the end.
        // Propagate all unexpected failure types immediately

        // Set of identifiers for features that failed with an inconsistency between labels
        Set<String> featuresThatFailedWithLabelInconsistency = new TreeSet<>();

        // Set of identifiers for features that failed with all missing values
        Set<String> featuresThatFailedWithAllThresholdsMissing = new TreeSet<>();

        // Set of identifiers for features that failed with non-numeric input
        Set<String> featuresThatFailedWithNonNumericInput = new TreeSet<>();

        // Set of identifiers for features that failed with other wrong input
        // identified by IllegalArgumentException
        Set<String> featuresThatFailedWithOtherWrongInput = new TreeSet<>();

        // Feature count
        int totalFeatures = 0;

        // Read the input
        try ( BufferedReader input = Files.newBufferedReader( commaSeparated, StandardCharsets.UTF_8 ) )
        {
            String nextLine = input.readLine();
            String[] header = nextLine.split( "\\s*(,)\\s*" );
            String[] labels = null;

            // Header?
            // Yes: process it
            if ( header.length > 0 && "locationId".equalsIgnoreCase( header[0] ) )
            {
                // locationId allowed without labels
                if ( header.length > 1 )
                {
                    labels = Arrays.copyOfRange( header, 1, header.length );
                }

                // Move to next line, which is not a header
                nextLine = input.readLine();
            }

            // Process each feature for which information is present
            while ( Objects.nonNull( nextLine ) && !nextLine.isBlank() )
            {

                String[] featureThresholds = nextLine.split( "\\s*(,)\\s*" );

                String locationId = featureThresholds[0];

                // Feature maps on locationId only              
                Feature feature = new Feature( null,
                                               null,
                                               null,
                                               null,
                                               null,
                                               locationId,
                                               null,
                                               null,
                                               null,
                                               null,
                                               null,
                                               null,
                                               null );
                FeaturePlus nextFeature = FeaturePlus.of( feature );


                try
                {
                    returnMe.put( nextFeature,
                                  CommaSeparatedReader.getAllThresholdsForOneFeature( isProbability,
                                                                                      condition,
                                                                                      dataType,
                                                                                      labels,
                                                                                      featureThresholds,
                                                                                      missingValue,
                                                                                      units ) );
                }
                // Catch expected exceptions and propagate finally to avoid drip-feeding
                catch ( LabelInconsistencyException e )
                {
                    featuresThatFailedWithLabelInconsistency.add( locationId );
                }
                catch ( AllThresholdsMissingException e )
                {
                    featuresThatFailedWithAllThresholdsMissing.add( locationId );
                }
                catch ( NumberFormatException e )
                {
                    featuresThatFailedWithNonNumericInput.add( locationId );
                }
                catch ( IllegalArgumentException e )
                {
                    featuresThatFailedWithOtherWrongInput.add( locationId );
                }

                // Move to next line
                nextLine = input.readLine();

                totalFeatures++;
            }

        }

        // Propagate any exceptions that were caught to avoid drip-feeding
        CommaSeparatedReader.throwExceptionIfOneOrMoreFailed( totalFeatures,
                                                              commaSeparated,
                                                              featuresThatFailedWithLabelInconsistency,
                                                              featuresThatFailedWithAllThresholdsMissing,
                                                              featuresThatFailedWithNonNumericInput,
                                                              featuresThatFailedWithOtherWrongInput );

        return returnMe;
    }

    /**
     * Mutates the input map, reading all thresholds for one feature.
     * 
     * @param isProbability is true to read probability thresholds, false for value thresholds
     * @param condition the threshold condition
     * @param dataType the threshold data type
     * @param labels the optional labels (may be null)
     * @param featureThresholds the next set of thresholds to process for a given feature, including the feature label
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param units the optional units associated with the threshold values
     * @throws NullPointerException if the featureThresholds is null
     * @throws LabelInconsistencyException if the number of labels is inconsistent with the number of thresholds
     * @throws NumberFormatException if one of the thresholds was not a number
     * @throws AllMissingThresholdsException if all thresholds matched the missing value
     * @return the thresholds for one feature
     */

    private static Set<Threshold> getAllThresholdsForOneFeature( boolean isProbability,
                                                                 Operator condition,
                                                                 ThresholdDataType dataType,
                                                                 String[] labels,
                                                                 String[] featureThresholds,
                                                                 Double missingValue,
                                                                 MeasurementUnit units )
    {

        Objects.requireNonNull( featureThresholds );


        if ( Objects.nonNull( labels ) && featureThresholds.length - 1 != labels.length )
        {
            throw new LabelInconsistencyException( "One or more lines contained a different number "
                                                   + "of thresholds than labels." );
        }

        return CommaSeparatedReader.getThresholds( Arrays.copyOfRange( featureThresholds,
                                                                       1,
                                                                       featureThresholds.length ),
                                                   labels,
                                                   isProbability,
                                                   condition,
                                                   dataType,
                                                   missingValue,
                                                   units );
    }

    /**
     * Generates a {@link Set} of {@link Threshold} from the input string. 
     * 
     * @param input the comma separated input string
     * @param labels a set of labels (as many as thresholds) or null
     * @param isProbability is true to build probability thresholds, false for value thresholds
     * @param condition the threshold condition
     * @param dataType the threshold data type
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param units the optional units associated with the threshold values
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the threshold content is inconsistent with the type of threshold
     *            or all threshold values match the missingValue 
     * @throws NumberFormatException if the strings cannot be parsed to numbers                 
     */

    private static Set<Threshold> getThresholds( String[] input,
                                                 String[] labels,
                                                 boolean isProbability,
                                                 Operator condition,
                                                 ThresholdDataType dataType,
                                                 Double missingValue,
                                                 MeasurementUnit units )
    {
        Objects.requireNonNull( input, "Specify a non-null input in order to read the thresholds." );

        Set<Threshold> returnMe = new TreeSet<>();

        // Define possibly null labels for iteration 
        String[] iterateLabels = labels;
        if ( Objects.isNull( labels ) )
        {
            iterateLabels = new String[input.length];
        }

        // Iterate through the thresholds

        // Count the number of missing thresholds
        int missingCount = 0;

        for ( int i = 0; i < input.length; i++ )
        {
            double threshold = Double.parseDouble( input[i] );

            // Non-missing value?
            if ( Objects.isNull( missingValue ) || Math.abs( missingValue - threshold ) > .00000001 )
            {

                // Probability thresholds
                if ( isProbability )
                {
                    returnMe.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( threshold ),
                                                                    condition,
                                                                    dataType,
                                                                    iterateLabels[i],
                                                                    units ) );
                }
                // Ordinary thresholds
                else
                {
                    returnMe.add( Threshold.of( OneOrTwoDoubles.of( threshold ),
                                                condition,
                                                dataType,
                                                iterateLabels[i],
                                                units ) );
                }
            }
            else
            {
                missingCount++;
            }
        }

        // Cannot have all missing thresholds
        if ( missingCount == input.length && Objects.nonNull( missingValue ) )
        {
            throw new AllThresholdsMissingException( "All thresholds matched the missing value of '"
                                                     + missingValue
                                                     + "'" );
        }

        return returnMe;
    }

    /**
     * Used to identify an exception that originates from an inconsistency between the number
     * of labels and the number of thresholds.
     */

    private static class LabelInconsistencyException extends IllegalArgumentException
    {
        private static final long serialVersionUID = 4507239538788881616L;

        private LabelInconsistencyException( String message )
        {
            super( message );
        }
    }

    /**
     * Used to identify an exception that originates from all thresholds matching the missing
     * value.
     */

    private static class AllThresholdsMissingException extends IllegalArgumentException
    {

        private static final long serialVersionUID = -7265565803593007243L;

        private AllThresholdsMissingException( String message )
        {
            super( message );
        }
    }

    /**
     * Throws an exception if one or more of the inputs contains elements, and decorates the
     * exception with the type of failure, based on the input.
     * 
     * @param totalFeatures the total number of features processed
     * @param pathToThresholds the path to the CSV thresholds-by-feature
     * @param featuresThatFailedWithLabelInconsistency features that failed with an inconsistency between labels and thresholds
     * @param featuresThatFailedWithAllThresholdsMissing features that failed with all thresholds missing
     * @param featuresThatFailedWithNonNumericInput features that failed with non-numeric input
     * @param featuresThatFailedWithOtherWrongInput features that failed with other wrong input
     * @throws IllegalArgumentException if one or more features failed
     */

    private static void throwExceptionIfOneOrMoreFailed( int totalFeatures,
                                                         Path pathToThresholds,
                                                         Set<String> featuresThatFailedWithLabelInconsistency,
                                                         Set<String> featuresThatFailedWithAllThresholdsMissing,
                                                         Set<String> featuresThatFailedWithNonNumericInput,
                                                         Set<String> featuresThatFailedWithOtherWrongInput )
    {
        StringJoiner exceptionMessage = new StringJoiner( " " );

        int failCount =
                featuresThatFailedWithLabelInconsistency.size() + featuresThatFailedWithAllThresholdsMissing.size()
                        + featuresThatFailedWithNonNumericInput.size()
                        + featuresThatFailedWithOtherWrongInput.size();

        if ( failCount > 0 )
        {
            exceptionMessage.add( "When processing thresholds by feature, " + failCount
                                  + " of "
                                  + totalFeatures
                                  + " features contained in '"
                                  + pathToThresholds
                                  + "' failed with exceptions, as follows." );
        }

        if ( !featuresThatFailedWithLabelInconsistency.isEmpty() )
        {
            exceptionMessage.add( "These features failed with an inconsistency between "
                                  + "the number of labels and the number of thresholds: "
                                  + featuresThatFailedWithLabelInconsistency
                                  + "." );
        }

        if ( !featuresThatFailedWithAllThresholdsMissing.isEmpty() )
        {
            exceptionMessage.add( "These features failed because "
                                  + "all thresholds matched the missing value: "
                                  + featuresThatFailedWithAllThresholdsMissing
                                  + "." );
        }

        if ( !featuresThatFailedWithNonNumericInput.isEmpty() )
        {
            exceptionMessage.add( "These features failed with non-numeric input: "
                                  + featuresThatFailedWithNonNumericInput
                                  + "." );
        }

        if ( !featuresThatFailedWithOtherWrongInput.isEmpty() )
        {
            exceptionMessage.add( "These features failed with invalid input for the threshold type: "
                                  + featuresThatFailedWithOtherWrongInput
                                  + "." );
        }

        // Throw exception if required
        if ( exceptionMessage.length() > 0 )
        {
            throw new IllegalArgumentException( exceptionMessage.toString() );
        }
    }

    /**
     * Do not construct.
     */

    private CommaSeparatedReader()
    {
    }

}
