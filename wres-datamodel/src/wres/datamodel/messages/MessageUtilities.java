package wres.datamodel.messages;

import java.util.List;
import java.util.Objects;

import com.google.protobuf.ProtocolStringList;

import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pairs;
import wres.statistics.generated.Pool;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.Season;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.ValueFilter;
import wres.statistics.generated.Outputs.CsvFormat;
import wres.statistics.generated.Outputs.PngFormat;
import wres.statistics.generated.Pairs.Pair;
import wres.statistics.generated.Pairs.TimeSeriesOfPairs;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * A collection of utilities for working with messages.
 * 
 * @author James Brown
 */

public class MessageUtilities
{

    /**
     * Compares the first {@link Evaluation} against the second and returns zero, a positive or negative value as to 
     * whether the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( Evaluation first, Evaluation second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }

        int compare = Integer.compare( first.getMetricCount(), second.getMetricCount() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = first.getDefaultBaseline().compareTo( second.getDefaultBaseline() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = MessageUtilities.compareListOfString( first.getEnsembleMemberSubsetList(),
                                                        second.getEnsembleMemberSubsetList() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = first.getBaselineDataName().compareTo( second.getBaselineDataName() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = first.getBaselineVariableName().compareTo( second.getBaselineVariableName() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = first.getLeftDataName().compareTo( second.getLeftDataName() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = first.getLeftVariableName().compareTo( second.getLeftVariableName() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = first.getRightDataName().compareTo( second.getRightDataName() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = first.getRightVariableName().compareTo( second.getRightVariableName() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = first.getMeasurementUnit().compareTo( second.getMeasurementUnit() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = MessageUtilities.compare( first.getOutputs(), second.getOutputs() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = MessageUtilities.compare( first.getSeason(), second.getSeason() );

        if ( compare != 0 )
        {
            return compare;
        }

        return MessageUtilities.compare( first.getValueFilter(), second.getValueFilter() );
    }

    /**
     * Compares the first {@link Outputs} against the second and returns zero, a positive or negative value as to 
     * whether the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( Outputs first, Outputs second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }

        int compare = Boolean.compare( first.hasProtobuf(), second.hasProtobuf() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Boolean.compare( first.hasNetcdf(), second.hasNetcdf() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Boolean.compare( first.hasCsv(), second.hasCsv() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Boolean.compare( first.hasPng(), second.hasPng() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Boolean.compare( first.hasSvg(), second.hasSvg() );

        if ( compare != 0 )
        {
            return compare;
        }

        // Compare the format options for formats that have options
        CsvFormat csv = first.getCsv();
        CsvFormat csvOther = second.getCsv();

        compare = csv.getOptions()
                     .getDecimalFormat()
                     .compareTo( csvOther.getOptions()
                                         .getDecimalFormat() );

        if ( compare != 0 )
        {
            return compare;
        }

        PngFormat png = first.getPng();
        PngFormat pngOther = second.getPng();

        compare = Integer.compare( png.getOptions().getHeight(), pngOther.getOptions().getHeight() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Integer.compare( png.getOptions().getWidth(), pngOther.getOptions().getWidth() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = png.getOptions().getLeadUnit().compareTo( pngOther.getOptions().getLeadUnit() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = png.getOptions().getShape().compareTo( pngOther.getOptions().getShape() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Integer.compare( png.getOptions().getIgnoreCount(), pngOther.getOptions().getIgnoreCount() );

        if ( compare != 0 )
        {
            return compare;
        }

        List<MetricName> names = png.getOptions().getIgnoreList();
        List<MetricName> otherNames = pngOther.getOptions().getIgnoreList();

        return MessageUtilities.compareListOfMetricName( names, otherNames );
    }

    /**
     * Compares the first {@link Season} against the second and returns zero, a positive or negative value as to whether 
     * the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( Season first, Season second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }

        int compare = Integer.compare( first.getStartDay(), second.getStartDay() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Integer.compare( first.getStartMonth(), second.getStartMonth() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Integer.compare( first.getEndDay(), second.getEndDay() );

        if ( compare != 0 )
        {
            return compare;
        }

        return Integer.compare( first.getEndMonth(), second.getEndMonth() );
    }

    /**
     * Compares the first {@link ValueFilter} against the second and returns zero, a positive or negative value as to 
     * whether the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( ValueFilter first, ValueFilter second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }

        int compare = Double.compare( first.getMinimumInclusiveValue(), second.getMinimumInclusiveValue() );

        if ( compare != 0 )
        {
            return compare;
        }

        return Double.compare( first.getMaximumInclusiveValue(), second.getMaximumInclusiveValue() );
    }

    /**
     * Compares the first {@link Pool} against the second and returns zero, a positive or negative value as to whether 
     * the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( Pool first, Pool second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }
        
        int compare = Boolean.compare( first.getIsBaselinePool(), second.getIsBaselinePool() );

        if ( compare != 0 )
        {
            return compare;
        }
        
        compare = Boolean.compare( first.hasTimeWindow(), second.hasTimeWindow() );

        if ( compare != 0 )
        {
            return compare;
        }
        
        compare = Boolean.compare( first.hasEventThreshold(), second.hasEventThreshold() );

        if ( compare != 0 )
        {
            return compare;
        }
        
        compare = Boolean.compare( first.hasDecisionThreshold(), second.hasDecisionThreshold() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Boolean.compare( first.hasTimeScale(), second.hasTimeScale() );

        if ( compare != 0 )
        {
            return compare;
        }
        
        compare = Boolean.compare( first.hasPairs(), second.hasPairs() );

        if ( compare != 0 )
        {
            return compare;
        }
        
        compare = MessageUtilities.compare( first.getTimeWindow(), second.getTimeWindow() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = MessageUtilities.compare( first.getEventThreshold(), second.getEventThreshold() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = MessageUtilities.compare( first.getDecisionThreshold(), second.getDecisionThreshold() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = MessageUtilities.compare( first.getTimeScale(), second.getTimeScale() );

        if ( compare != 0 )
        {
            return compare;
        }
        
        compare = MessageUtilities.compareListOfGeometryTuples( first.getGeometryTuplesList(),
                                                                second.getGeometryTuplesList() );

        if ( compare != 0 )
        {
            return compare;
        }

        return MessageUtilities.compare( first.getPairs(), second.getPairs() );
    }

    /**
     * Compares the first {@link Threshold} against the second and returns zero, a positive or negative value as to 
     * whether the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( Threshold first, Threshold second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }

        int compare = first.getOperator().compareTo( second.getOperator() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Boolean.compare( first.hasLeftThresholdValue(), second.hasLeftThresholdValue() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Boolean.compare( first.hasRightThresholdValue(), second.hasRightThresholdValue() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Boolean.compare( first.hasLeftThresholdProbability(), second.hasLeftThresholdProbability() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Boolean.compare( first.hasRightThresholdProbability(), second.hasRightThresholdProbability() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Double.compare( first.getLeftThresholdValue().getValue(),
                                  second.getLeftThresholdValue().getValue() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Double.compare( first.getRightThresholdValue().getValue(),
                                  second.getRightThresholdValue().getValue() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Double.compare( first.getLeftThresholdProbability().getValue(),
                                  second.getLeftThresholdProbability().getValue() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Double.compare( first.getRightThresholdProbability().getValue(),
                                  second.getRightThresholdProbability().getValue() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = first.getDataType().compareTo( second.getDataType() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = first.getName().compareTo( second.getName() );

        if ( compare != 0 )
        {
            return compare;
        }

        return first.getThresholdValueUnits().compareTo( second.getThresholdValueUnits() );
    }

    /**
     * Compares the first {@link TimeScale} against the second and returns zero, a positive or negative value as to 
     * whether the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( TimeScale first, TimeScale second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }

        int compare = first.getFunction().compareTo( second.getFunction() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Long.compare( first.getPeriod().getSeconds(), second.getPeriod().getSeconds() );

        if ( compare != 0 )
        {
            return compare;
        }

        return Integer.compare( first.getPeriod().getNanos(), second.getPeriod().getNanos() );
    }

    /**
     * Compares the first {@link TimeWindow} against the second and returns zero, a positive or negative value as to 
     * whether the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( TimeWindow first, TimeWindow second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }

        int compare = Long.compare( first.getEarliestReferenceTime().getSeconds(),
                                    second.getEarliestReferenceTime().getSeconds() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Integer.compare( first.getEarliestReferenceTime().getNanos(),
                                   second.getEarliestReferenceTime().getNanos() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Long.compare( first.getLatestReferenceTime().getSeconds(),
                                second.getLatestReferenceTime().getSeconds() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Integer.compare( first.getLatestReferenceTime().getNanos(),
                                   second.getLatestReferenceTime().getNanos() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Long.compare( first.getEarliestValidTime().getSeconds(),
                                second.getEarliestValidTime().getSeconds() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Integer.compare( first.getEarliestValidTime().getNanos(),
                                   second.getEarliestValidTime().getNanos() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Long.compare( first.getLatestValidTime().getSeconds(),
                                second.getLatestValidTime().getSeconds() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Integer.compare( first.getLatestValidTime().getNanos(),
                                   second.getLatestValidTime().getNanos() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Long.compare( first.getEarliestLeadDuration().getSeconds(),
                                second.getEarliestLeadDuration().getSeconds() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Integer.compare( first.getEarliestLeadDuration().getNanos(),
                                   second.getEarliestLeadDuration().getNanos() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Long.compare( first.getLatestLeadDuration().getSeconds(),
                                second.getLatestLeadDuration().getSeconds() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Integer.compare( first.getLatestLeadDuration().getNanos(),
                                   second.getLatestLeadDuration().getNanos() );

        if ( compare != 0 )
        {
            return compare;
        }

        return MessageUtilities.compareListOfReferenceTimeType( first.getReferenceTimeTypeList(),
                                                                second.getReferenceTimeTypeList() );
    }

    /**
     * Compares the first {@link GeometryTuple} against the second and returns zero, a positive or negative value as to 
     * whether the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( GeometryTuple first, GeometryTuple second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }

        int compare = MessageUtilities.compare( first.getLeft(), second.getLeft() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = MessageUtilities.compare( first.getRight(), second.getRight() );

        if ( compare != 0 )
        {
            return compare;
        }

        return MessageUtilities.compare( first.getBaseline(), second.getBaseline() );
    }

    /**
     * Compares the first {@link Geometry} against the second and returns zero, a positive or negative value as to 
     * whether the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( Geometry first, Geometry second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }

        int compare = Integer.compare( first.getSrid(), second.getSrid() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = first.getName().compareTo( second.getName() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = first.getDescription().compareTo( second.getDescription() );

        if ( compare != 0 )
        {
            return compare;
        }

        return first.getWkt().compareTo( second.getWkt() );
    }

    /**
     * Compares the first {@link Pairs} against the second and returns zero, a positive or negative value as to whether 
     * the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( Pairs first, Pairs second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }

        return MessageUtilities.compareListOfTimeSeriesOfPairs( first.getTimeSeriesList(), second.getTimeSeriesList() );
    }

    /**
     * Compares the first {@link TimeSeriesOfPairs} against the second and returns zero, a positive or negative value as 
     * to whether the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( TimeSeriesOfPairs first, TimeSeriesOfPairs second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }

        int compare = MessageUtilities.compareListOfReferenceTimes( first.getReferenceTimesList(),
                                                                    second.getReferenceTimesList() );

        if ( compare != 0 )
        {
            return compare;
        }

        return MessageUtilities.compareListOfPairs( first.getPairsList(), second.getPairsList() );
    }

    /**
     * Compares the first {@link ReferenceTime} against the second and returns zero, a positive or negative value as to 
     * whether the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( ReferenceTime first, ReferenceTime second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }

        int compare = first.getReferenceTimeType().compareTo( second.getReferenceTimeType() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Long.compare( first.getReferenceTime().getSeconds(),
                                second.getReferenceTime().getSeconds() );

        if ( compare != 0 )
        {
            return compare;
        }

        return Integer.compare( first.getReferenceTime().getNanos(),
                                second.getReferenceTime().getNanos() );
    }

    /**
     * Compares the first {@link Pair} against the second and returns zero, a positive or negative value as to whether 
     * the first description is equal to, greater than or less than the second description.
     * 
     * @param first the first description
     * @param second the second description
     * @return a number that is zero, negative or positive when first description is the same as, less than or greater 
     *            than the second, respectively
     * @throws NullPointerException if either input is null
     */

    public static int compare( Pair first, Pair second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        // For efficiency, check that the objects are identity equals
        if ( first == second )
        {
            return 0;
        }

        int compare = Long.compare( first.getValidTime().getSeconds(),
                                    second.getValidTime().getSeconds() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Integer.compare( first.getValidTime().getNanos(),
                                   second.getValidTime().getNanos() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Integer.compare( first.getLeftCount(),
                                   second.getLeftCount() );

        if ( compare != 0 )
        {
            return compare;
        }

        compare = Integer.compare( first.getRightCount(),
                                   second.getRightCount() );

        if ( compare != 0 )
        {
            return compare;
        }

        // Compare the doubles
        compare = MessageUtilities.compareListOfDoubleTo( first.getLeftList(),
                                                          second.getLeftList() );

        if ( compare != 0 )
        {
            return compare;
        }

        return MessageUtilities.compareListOfDoubleTo( first.getRightList(), second.getRightList() );
    }

    /**
     * Compares two lists of strings and returns a zero, negative or positive value as to whether the first list is
     * equal to, less than or greater than the second list. 
     * 
     * @param first the first list
     * @param second the second list
     * @throws NullPointerException if either list is null
     */

    private static int compareListOfString( ProtocolStringList first, ProtocolStringList second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        int compare = Integer.compare( first.size(), second.size() );

        if ( compare != 0 )
        {
            return compare;
        }

        int count = first.size();

        for ( int i = 0; i < count; i++ )
        {
            String firstString = first.get( i );
            String secondString = second.get( i );
            int compareString = firstString.compareTo( secondString );

            if ( compareString != 0 )
            {
                return compareString;
            }
        }

        return 0;
    }

    /**
     * Compares two lists of {@link MetricName} and returns a zero, negative or positive value as to whether the first 
     * list is equal to, less than or greater than the second list. 
     * 
     * @param first the first list
     * @param second the second list
     * @throws NullPointerException if either list is null
     */

    private static int compareListOfMetricName( List<MetricName> first, List<MetricName> second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        int compare = Integer.compare( first.size(), second.size() );

        if ( compare != 0 )
        {
            return compare;
        }

        int count = first.size();

        for ( int i = 0; i < count; i++ )
        {
            MetricName firstName = first.get( i );
            MetricName secondName = second.get( i );
            int compareName = firstName.compareTo( secondName );

            if ( compareName != 0 )
            {
                return compareName;
            }
        }

        return 0;
    }

    /**
     * Compares two lists of {@link ReferenceTimeType} and returns a zero, negative or positive value as to whether the 
     * first list is equal to, less than or greater than the second list. 
     * 
     * @param first the first list
     * @param second the second list
     * @throws NullPointerException if either list is null
     */

    private static int compareListOfReferenceTimeType( List<ReferenceTimeType> first, List<ReferenceTimeType> second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        int compare = Integer.compare( first.size(), second.size() );

        if ( compare != 0 )
        {
            return compare;
        }

        int count = first.size();

        for ( int i = 0; i < count; i++ )
        {
            ReferenceTimeType firstType = first.get( i );
            ReferenceTimeType secondType = second.get( i );
            int compareType = firstType.compareTo( secondType );

            if ( compareType != 0 )
            {
                return compareType;
            }
        }

        return 0;
    }

    /**
     * Compares two lists of {@link GeometryTuple} and returns a zero, negative or positive value as to whether the 
     * first list is equal to, less than or greater than the second list. 
     * 
     * @param first the first list
     * @param second the second list
     * @throws NullPointerException if either list is null
     */

    private static int compareListOfGeometryTuples( List<GeometryTuple> first, List<GeometryTuple> second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        int compare = Integer.compare( first.size(), second.size() );

        if ( compare != 0 )
        {
            return compare;
        }

        int count = first.size();

        for ( int i = 0; i < count; i++ )
        {
            int compareTuple = MessageUtilities.compare( first.get( i ), second.get( i ) );

            if ( compareTuple != 0 )
            {
                return compareTuple;
            }
        }

        return 0;
    }

    /**
     * Compares two lists of {@link TimeSeriesOfPairs} and returns a zero, negative or positive value as to whether the 
     * first list is equal to, less than or greater than the second list. 
     * 
     * @param first the first list
     * @param second the second list
     * @throws NullPointerException if either list is null
     */

    private static int compareListOfTimeSeriesOfPairs( List<TimeSeriesOfPairs> first, List<TimeSeriesOfPairs> second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        int compare = Integer.compare( first.size(), second.size() );

        if ( compare != 0 )
        {
            return compare;
        }

        int count = first.size();

        for ( int i = 0; i < count; i++ )
        {
            int compareTuple = MessageUtilities.compare( first.get( i ), second.get( i ) );

            if ( compareTuple != 0 )
            {
                return compareTuple;
            }
        }

        return 0;
    }

    /**
     * Compares two lists of {@link ReferenceTime} and returns a zero, negative or positive value as to whether the 
     * first list is equal to, less than or greater than the second list. 
     * 
     * @param first the first list
     * @param second the second list
     * @throws NullPointerException if either list is null
     */

    private static int compareListOfReferenceTimes( List<ReferenceTime> first, List<ReferenceTime> second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        int compare = Integer.compare( first.size(), second.size() );

        if ( compare != 0 )
        {
            return compare;
        }

        int count = first.size();

        for ( int i = 0; i < count; i++ )
        {
            int compareTuple = MessageUtilities.compare( first.get( i ), second.get( i ) );

            if ( compareTuple != 0 )
            {
                return compareTuple;
            }
        }

        return 0;
    }

    /**
     * Compares two lists of {@link Pair} and returns a zero, negative or positive value as to whether the 
     * first list is equal to, less than or greater than the second list. 
     * 
     * @param first the first list
     * @param second the second list
     * @throws NullPointerException if either list is null
     */

    private static int compareListOfPairs( List<Pair> first, List<Pair> second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        int compare = Integer.compare( first.size(), second.size() );

        if ( compare != 0 )
        {
            return compare;
        }

        int count = first.size();

        for ( int i = 0; i < count; i++ )
        {
            int compareTuple = MessageUtilities.compare( first.get( i ), second.get( i ) );

            if ( compareTuple != 0 )
            {
                return compareTuple;
            }
        }

        return 0;
    }

    /**
     * Compares two lists of {@link Double} and returns a zero, negative or positive value as to whether the 
     * first list is equal to, less than or greater than the second list. 
     * 
     * @param first the first list
     * @param second the second list
     * @throws NullPointerException if either list is null
     */

    private static int compareListOfDoubleTo( List<Double> first, List<Double> second )
    {
        Objects.requireNonNull( first );
        Objects.requireNonNull( second );

        int compare = Integer.compare( first.size(), second.size() );

        if ( compare != 0 )
        {
            return compare;
        }

        int count = first.size();

        for ( int i = 0; i < count; i++ )
        {
            int compareTuple = Double.compare( first.get( i ), second.get( i ) );

            if ( compareTuple != 0 )
            {
                return compareTuple;
            }
        }

        return 0;
    }

    /**
     * Do not construct.
     */

    private MessageUtilities()
    {
    }
}
