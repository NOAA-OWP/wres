package wres.io.reading.commaseparated;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.DoubleUnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConfigException;
import wres.config.generated.FeatureType;
import wres.config.generated.ThresholdFormat;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.FeatureKey;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.io.retrieval.UnitMapper;
import wres.system.SystemSettings;

/**
 * Helps read files of Comma Separated Values (CSV).
 * 
 * @author james.brown@hydrosolved.com
 */

public class ThresholdReader
{

    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdReader.class );

    /**
     * Reads a CSV source that contains one or more thresholds for each of several features. Places the results into 
     * a {@link Map} whose keys are {@link FeatureKey} and whose values comprise a {@link Set} of {@link ThresholdOuter}.
     * 
     * @param systemSettings the system settings used to help resolve a path to thresholds
     * @param threshold the threshold configuration
     * @param units the (optional) existing measurement units associated with the threshold values; if null, equal to 
     *            the evaluation units
     * @param unitMapper a measurement unit mapper
     * @return a map of thresholds by feature name
     * @throws IOException if the source cannot be read or contains unexpected input
     * @throws NullPointerException if the source is null or the condition is null
     * @throws IllegalArgumentException if one or more features failed with expected problems, such as 
     *            all thresholds missing, thresholds that contain non-numeric input and thresholds that 
     *            are invalid (e.g. probability thresholds that are out-of-bounds). 
     */

    public static Map<String, Set<ThresholdOuter>>
    readThresholds( SystemSettings systemSettings,
                    ThresholdsConfig threshold,
                    MeasurementUnit units,
                    UnitMapper unitMapper )
            throws IOException
    {
        Objects.requireNonNull( threshold, "Specify a non-null source of thresholds to read." );

        ThresholdsConfig.Source nextSource = (ThresholdsConfig.Source) threshold.getCommaSeparatedValuesOrSource();

        // Pre-validate path
        if ( Objects.isNull( nextSource.getValue() ) )
        {
            throw new MetricConfigException( threshold,
                                             "Specify a non-null path to read for the external "
                                                        + "source of thresholds." );
        }
        // Validate format
        if ( nextSource.getFormat() != ThresholdFormat.CSV )
        {
            throw new MetricConfigException( threshold,
                                             "Unsupported source format for thresholds '"
                                                        + nextSource.getFormat()
                                                        + "'" );
        }

        // Missing value?
        Double missingValue = null;

        if ( Objects.nonNull( nextSource.getMissingValue() ) )
        {
            missingValue = Double.parseDouble( nextSource.getMissingValue() );
        }

        //Path TODO: permit web thresholds. 
        // See #59422
        // Construct a path using the SystemSetting wres.dataDirectory when
        // the specified source is not absolute.
        URI uri = nextSource.getValue();
        Path commaSeparated;

        if ( !uri.isAbsolute() )
        {
            commaSeparated = systemSettings.getDataDirectory()
                                           .resolve( uri.getPath() );
            LOGGER.debug( "Transformed relative URI {} to Path {}.",
                          uri,
                          commaSeparated );
        }
        else
        {
            commaSeparated = Paths.get( uri );
        }

        // Condition: default to greater
        ThresholdConstants.Operator operator = ThresholdConstants.Operator.GREATER;
        if ( Objects.nonNull( threshold.getOperator() ) )
        {
            operator = DataFactory.getThresholdOperator( threshold );
        }

        // Data type: default to left
        ThresholdConstants.ThresholdDataType dataType = ThresholdConstants.ThresholdDataType.LEFT;
        if ( Objects.nonNull( threshold.getApplyTo() ) )
        {
            dataType = DataFactory.getThresholdDataType( threshold.getApplyTo() );
        }

        // The type of feature
        FeatureType featureType = nextSource.getFeatureType();

        ThresholdDataTypes dataTypes = new ThresholdDataTypes( dataType, featureType, threshold.getType(), operator );

        return ThresholdReader.readThresholds( commaSeparated,
                                               dataTypes,
                                               missingValue,
                                               units,
                                               unitMapper );
    }

    /**
     * Reads a CSV source that contains one or more thresholds for each of several features. Places the results into 
     * a {@link Map} whose keys are {@link String} and whose values comprise a {@link Set} of {@link ThresholdOuter}.
     * 
     * @param commaSeparated the path to the comma separated values
     * @param dataTypes the threshold data types
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param units the (optional) existing measurement units associated with the threshold values; if null, equal to 
     *            the evaluation units 
     * @param unitMapper a measurement unit mapper
     * @return a map of thresholds by feature name
     * @throws IOException if the source cannot be read or contains unexpected input
     * @throws NullPointerException if the source is null or the condition is null
     * @throws IllegalArgumentException if one or more features failed with expected problems, such as 
     *            all thresholds missing, thresholds that contain non-numeric input and thresholds that 
     *            are invalid (e.g. probability thresholds that are out-of-bounds). 
     */

    private static Map<String, Set<ThresholdOuter>> readThresholds( Path commaSeparated,
                                                                    ThresholdDataTypes dataTypes,
                                                                    Double missingValue,
                                                                    MeasurementUnit units,
                                                                    UnitMapper unitMapper )
            throws IOException
    {
        Map<String, Set<ThresholdOuter>> returnMe = new TreeMap<>();

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

        // Internal unit mapper
        InnerUnitMapper innerUnitMapper = InnerUnitMapper.of( units, unitMapper );

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

                String featureName = featureThresholds[0];

                try
                {
                    returnMe.put( featureName,
                                  ThresholdReader.getAllThresholdsForOneFeature( dataTypes,
                                                                                 labels,
                                                                                 featureThresholds,
                                                                                 missingValue,
                                                                                 innerUnitMapper ) );
                }
                // Catch expected exceptions and propagate finally to avoid drip-feeding
                catch ( LabelInconsistencyException e )
                {
                    featuresThatFailedWithLabelInconsistency.add( featureName );
                }
                catch ( AllThresholdsMissingException e )
                {
                    featuresThatFailedWithAllThresholdsMissing.add( featureName );
                }
                catch ( NumberFormatException e )
                {
                    featuresThatFailedWithNonNumericInput.add( featureName );
                }
                catch ( IllegalArgumentException e )
                {
                    featuresThatFailedWithOtherWrongInput.add( featureName );
                }

                // Move to next line
                nextLine = input.readLine();

                totalFeatures++;
            }

        }

        // Propagate any exceptions that were caught to avoid drip-feeding
        ThresholdReader.throwExceptionIfOneOrMoreFailed( totalFeatures,
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
     * @param dataType the threshold data types
     * @param labels the optional labels (may be null)
     * @param featureThresholds the next set of thresholds to process for a given feature, including the feature label
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param unitMapper a mapper for the measurement units
     * @throws NullPointerException if the featureThresholds is null
     * @throws LabelInconsistencyException if the number of labels is inconsistent with the number of thresholds
     * @throws NumberFormatException if one of the thresholds was not a number
     * @return the thresholds for one feature
     */

    private static Set<ThresholdOuter> getAllThresholdsForOneFeature( ThresholdDataTypes dataType,
                                                                 String[] labels,
                                                                 String[] featureThresholds,
                                                                 Double missingValue,
                                                                 InnerUnitMapper unitMapper )
    {

        Objects.requireNonNull( featureThresholds );

        if ( Objects.nonNull( labels ) && featureThresholds.length - 1 != labels.length )
        {
            throw new LabelInconsistencyException( "One or more lines contained a different number "
                                                   + "of thresholds than labels." );
        }

        // Threshold type: default to probability
        ThresholdConstants.ThresholdGroup thresholdType = ThresholdConstants.ThresholdGroup.PROBABILITY;
        if ( Objects.nonNull( dataType.getThresholdType() ) )
        {
            thresholdType = DataFactory.getThresholdGroup( dataType.getThresholdType() );
        }

        // Default to probability
        boolean isProbability = thresholdType == ThresholdConstants.ThresholdGroup.PROBABILITY;

        return ThresholdReader.getThresholds( Arrays.copyOfRange( featureThresholds,
                                                                  1,
                                                                  featureThresholds.length ),
                                              labels,
                                              isProbability,
                                              dataType.getOperator(),
                                              dataType.getThresholdDataType(),
                                              missingValue,
                                              unitMapper );
    }

    /**
     * Generates a {@link Set} of {@link ThresholdOuter} from the input string.
     * 
     * @param input the comma separated input string
     * @param labels a set of labels (as many as thresholds) or null
     * @param isProbability is true to build probability thresholds, false for value thresholds
     * @param condition the threshold condition
     * @param dataType the threshold data type
     * @param missingValue an optional missing value identifier to ignore (may be null)
     * @param unitMapper a mapper for the measurement units
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the threshold content is inconsistent with the type of threshold
     *            or all threshold values match the missingValue 
     * @throws NumberFormatException if the strings cannot be parsed to numbers                 
     */

    private static Set<ThresholdOuter> getThresholds( String[] input,
                                                      String[] labels,
                                                      boolean isProbability,
                                                      Operator condition,
                                                      ThresholdDataType dataType,
                                                      Double missingValue,
                                                      InnerUnitMapper unitMapper )
    {
        Objects.requireNonNull( input, "Specify a non-null input in order to read the thresholds." );

        Set<ThresholdOuter> returnMe = new TreeSet<>();

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
                    returnMe.add( ThresholdOuter.ofProbabilityThreshold( OneOrTwoDoubles.of( threshold ),
                                                                    condition,
                                                                    dataType,
                                                                    iterateLabels[i],
                                                                    unitMapper.getDesiredMeasurementUnit() ) );
                }
                // Ordinary thresholds
                else
                {
                    double thresholdInDesiredUnits = unitMapper.getValueInDesiredUnits( threshold );

                    returnMe.add( ThresholdOuter.of( OneOrTwoDoubles.of( thresholdInDesiredUnits ),
                                                condition,
                                                dataType,
                                                iterateLabels[i],
                                                unitMapper.getDesiredMeasurementUnit() ) );
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
            exceptionMessage.add( System.lineSeparator() );

        }

        if ( !featuresThatFailedWithLabelInconsistency.isEmpty() )
        {
            exceptionMessage.add( "    These features failed with an inconsistency between "
                                  + "the number of labels and the number of thresholds: "
                                  + featuresThatFailedWithLabelInconsistency
                                  + "." );
            exceptionMessage.add( System.lineSeparator() );

        }

        if ( !featuresThatFailedWithAllThresholdsMissing.isEmpty() )
        {
            exceptionMessage.add( "    These features failed because "
                                  + "all thresholds matched the missing value: "
                                  + featuresThatFailedWithAllThresholdsMissing
                                  + "." );
            exceptionMessage.add( System.lineSeparator() );
        }

        if ( !featuresThatFailedWithNonNumericInput.isEmpty() )
        {
            exceptionMessage.add( "    These features failed with non-numeric input: "
                                  + featuresThatFailedWithNonNumericInput
                                  + "." );
            exceptionMessage.add( System.lineSeparator() );

        }

        if ( !featuresThatFailedWithOtherWrongInput.isEmpty() )
        {
            exceptionMessage.add( "    These features failed with invalid input for the threshold type: "
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
     * Inner units mapper.
     */

    private static class InnerUnitMapper
    {
        /**
         * Existing measurement units.
         */
        private final MeasurementUnit existingUnit;

        /**
         * A general unit mapper.
         */

        private final UnitMapper generalUnitMapper;

        /**
         * A mapper to create desired measurement units.
         */
        private final DoubleUnaryOperator specificUnitMapper;

        /**
         * Create an instance.
         * @param existingUnits the existing measurement unit
         * @param desiredUnitMapper a mapper to create desired measurement units
         * @return an inner mapper
         */

        private static InnerUnitMapper of( MeasurementUnit existingUnits, UnitMapper desiredUnitMapper )
        {
            return new InnerUnitMapper( existingUnits, desiredUnitMapper );
        }

        /**
         * Returns the desired measurement unit.
         * @return the desired measurement unit
         */

        private MeasurementUnit getDesiredMeasurementUnit()
        {
            return MeasurementUnit.of( this.generalUnitMapper.getDesiredMeasurementUnitName() );
        }

        /**
         * Maps an input value in the existing measurement units to the desired units. If the existing units are
         * unknown, the input value is returned.
         * 
         * @param valueInExistingUnits a threshold value in existing units
         * @return the thresholds value in desired units
         */

        private double getValueInDesiredUnits( double valueInExistingUnits )
        {
            // No existing units, desired units assumed
            if ( Objects.isNull( this.existingUnit ) )
            {
                return valueInExistingUnits;
            }

            return this.specificUnitMapper.applyAsDouble( valueInExistingUnits );
        }

        /**
         * Create an instance.
         * @param existingUnit the existing measurement unit
         * @param generalUnitMapper a mapper to create desired measurement units
         */

        private InnerUnitMapper( MeasurementUnit existingUnit, UnitMapper generalUnitMapper )
        {
            this.existingUnit = existingUnit;
            this.generalUnitMapper = generalUnitMapper;

            if ( Objects.nonNull( this.existingUnit ) )
            {
                this.specificUnitMapper = generalUnitMapper.getUnitMapper( existingUnit.toString() );
            }
            else
            {
                this.specificUnitMapper = null;
            }
        }
    }

    /**
     * Package of threshold data types.
     */

    private static class ThresholdDataTypes
    {
        private final ThresholdDataType thresholdDataType;
        private final ThresholdType thresholdType;
        private final FeatureType featureType;
        private final Operator operator;

        /**
         * Construct.
         * 
         * @param thresholdDataType the threshold data type
         * @param featureType the feature type
         * @param thresholdType the threshold type
         * @param operator the threshold operator
         */
        private ThresholdDataTypes( ThresholdDataType thresholdDataType,
                                    FeatureType featureType,
                                    ThresholdType thresholdType,
                                    Operator operator )
        {
            this.thresholdDataType = thresholdDataType;
            this.featureType = featureType;
            this.thresholdType = thresholdType;
            this.operator = operator;
        }

        /**
         * @return the threshold data type.
         */

        private ThresholdDataType getThresholdDataType()
        {
            return this.thresholdDataType;
        }

        /**
         * @return the threshold type.
         */

        private ThresholdType getThresholdType()
        {
            return this.thresholdType;
        }

        /**
         * @return the feature type.
         */

        private FeatureType getFeatureType()
        {
            return this.featureType;
        }

        /**
         * @return the threshold operator.
         */

        private Operator getOperator()
        {
            return this.operator;
        }
    }

    /**
     * Do not construct.
     */

    private ThresholdReader()
    {
    }

}
