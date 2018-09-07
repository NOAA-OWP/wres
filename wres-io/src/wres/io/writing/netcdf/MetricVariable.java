package wres.io.writing.netcdf;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;

class MetricVariable
{
    static Collection<MetricVariable> getAll( Iterable<DoubleScoreStatistic> metricResults)
    {
        List<MetricVariable> variables = new ArrayList<>();

        for (DoubleScoreStatistic output : metricResults)
        {
            variables.add( new MetricVariable( output ) );
        }

        return variables;
    }

    private MetricVariable (final DoubleScoreStatistic output)
    {
        StatisticMetadata metadata = output.getMetadata();
        String metric = metadata.getMetricID().toString();
        OneOrTwoThresholds thresholds = metadata.getSampleMetadata().getThresholds();
        TimeWindow timeWindow = metadata.getSampleMetadata().getTimeWindow();

        this.name = MetricVariable.getName( output );
        this.longName = metric + " " + thresholds;

        this.measurementUnit = metadata.getMeasurementUnit().getUnit();
        this.sampleSize = metadata.getSampleSize();

        this.firstCondition = thresholds.first().getCondition().name();
        this.firstBound = thresholds.first().getValues().first();
        this.firstDataType = thresholds.first().getDataType().name();

        // Two thresholds available
        if (thresholds.hasTwo())
        {
            this.secondCondition = thresholds.second().getCondition().name();
            this.secondBound = thresholds.second().getValues().first();
            this.secondDataType = thresholds.second().getDataType().name();
        }
        else
        {
            this.secondCondition = "None";
            this.secondBound = null;
            this.secondDataType = "None";
        }

        // We only add timing information until we get a lead variable in
        this.earliestLead = (int)timeWindow.getEarliestLeadTimeInHours();
        this.latestLead = (int)timeWindow.getLatestLeadTimeInHours();

        if (!timeWindow.getEarliestTime().equals( Instant.MIN ))
        {
            this.earliestTime = timeWindow.getEarliestTime().toString();
        }
        else
        {
            this.earliestTime = "ALL";
        }

        if (!timeWindow.getLatestTime().equals( Instant.MAX ))
        {
            this.latestTime = timeWindow.getLatestTime().toString();
        }
        else
        {
            this.latestTime = "ALL";
        }
        
        // Add the time scale information if available
        if( metadata.getSampleMetadata().hasTimeScale() )
        {
            this.timeScalePeriod = metadata.getSampleMetadata().getTimeScale().getPeriod().toString();
            // Use the enum name()
            this.timeScaleFunction = metadata.getSampleMetadata().getTimeScale().getFunction().name();            
        }
        else
        {
            this.timeScalePeriod = "UNKNOWN";
            this.timeScaleFunction = "UNKNOWN";
        }
        
    }

    static String getName(final DoubleScoreStatistic output)
    {
        StatisticMetadata metadata = output.getMetadata();
        // We start with the raw name of the metric
        String name = metadata.getMetricID().toString().replace(" ", "_");

        // If the calling metric is associated with a threshold, combine
        // threshold information with it
        if ( metadata.getSampleMetadata().getThresholds() != null &&
             metadata.getSampleMetadata().getThresholds().first() != null &&
             metadata.getSampleMetadata().getThresholds().first().hasProbabilities())
        {
            // We first get the probability in raw percentage, i.e., 0.95 becomes 95
            Double probability =
                    metadata.getSampleMetadata().getThresholds().first().getProbabilities().first() * 100.0;

            // We now want to indicate that it is a probability and attach
            // the integer representation. If the probability ended up
            // being 37.25, we want the name "_Pr_3725" not "_Pr_37.25"
            name += "_Pr_" + probability.toString().replaceAll( "\\.", "" );
        }

        return name;
    }

    public String getName()
    {
        return this.name;
    }

    Map<String, Object> getAttributes()
    {

        Map<String, Object> attributes = new TreeMap<>(  );

        attributes.put("latest_time", this.latestTime);
        attributes.put("earliest_time", this.earliestTime);
        attributes.put( "earliest_lead", this.earliestLead );
        attributes.put("latest_lead", this.latestLead);
        attributes.put("first_data_type", this.firstDataType);
        attributes.put("second_data_type", this.secondDataType);
        attributes.put("first_bound", this.firstBound);
        attributes.put("second_bound", this.secondBound);
        attributes.put("unit", this.measurementUnit);
        attributes.put("long_name", this.longName);
        attributes.put("first_condition", this.firstCondition);
        attributes.put("second_condition", this.secondCondition);
        attributes.put("sample_size", this.sampleSize);
        attributes.put("time_scale_period", this.timeScalePeriod);
        attributes.put("time_scale_function", this.timeScaleFunction);
        

        return attributes;
    }

    private final String name;
    private final String longName;

    private final String measurementUnit;

    private final String firstCondition;
    private final Double firstBound;
    private final String firstDataType;

    private final String secondCondition;
    private final Double secondBound;
    private final String secondDataType;

    private final String earliestTime;
    private final String latestTime;
    
    private final String timeScalePeriod;
    private final String timeScaleFunction;

    private final int earliestLead;
    private final int latestLead;
    private final int sampleSize;
}
