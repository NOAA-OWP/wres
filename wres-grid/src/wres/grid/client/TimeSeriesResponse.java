package wres.grid.client;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.TreeMap;

import wres.config.FeaturePlus;

public class TimeSeriesResponse implements Response
{
    @Override
    public String getVariableName()
    {
        return this.variableName;
    }

    @Override
    public Integer getValueCount()
    {
        return this.valueCount;
    }

    @Override
    public Duration getLastLead()
    {
        Duration lastLead = null;

        for (List<Series> seriesList : this)
        {
            for (Series series : seriesList)
            {
                if (lastLead == null || series.getLastLead().compareTo( lastLead ) > 0)
                {
                    lastLead = series.getLastLead();
                }
            }
        }

        return lastLead;
    }

    static class TimeSeries implements Response.Series
    {
        TimeSeries( final FeaturePlus feature, final Instant issuedDate)
        {
            Objects.requireNonNull( feature, "No feature was supplied for this new TimeSeries" );
            Objects.requireNonNull( issuedDate,
                                    "No issued date was supplied for this new TimeSeries" );
            this.issuedDate = issuedDate;
            this.feature = feature;
            this.entries = new TreeMap<>();
        }

        void add(final Instant validDate, final double value, final String measurementUnit)
        {
            Objects.requireNonNull( validDate, "A valid date must be supplied in order to add a value" );
            Duration leadDuration = Duration.ofSeconds( validDate.getEpochSecond() - this.issuedDate.getEpochSecond() );
            TimeSeriesEntry updatedEntry;

            if (this.entries.containsKey( leadDuration ))
            {
                updatedEntry = ((TimeSeriesEntry)this.entries.get(leadDuration)).add( value, measurementUnit );
            }
            else
            {
                updatedEntry = new TimeSeriesEntry( validDate, leadDuration, value, measurementUnit );
            }

            this.entries.put(leadDuration, updatedEntry);
            if (this.lastLead == null || leadDuration.compareTo( lastLead ) > 0)
            {
                this.lastLead = leadDuration;
            }
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof TimeSeries &&
                   ( ( TimeSeries ) obj ).issuedDate.equals( this.issuedDate );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(this.issuedDate);
        }

        public FeaturePlus getFeature()
        {
            return feature;
        }

        public Instant getIssuedDate()
        {
            return issuedDate;
        }

        @Override
        public Duration getLastLead()
        {
            return this.lastLead;
        }

        private final Map<Duration, Response.Entry> entries;
        private final Instant issuedDate;
        private final FeaturePlus feature;
        private Duration lastLead;

        @Override
        public int compareTo( Response.Series series )
        {
            int equality = this.feature.compareTo( series.getFeature() );

            if (equality == 0)
            {
                equality = this.issuedDate.compareTo( series.getIssuedDate() );
            }
            return equality;
        }

        @Override
        public Iterator<Response.Entry> iterator()
        {
            return this.entries.values().iterator();
        }

        @Override
        public String toString()
        {
            return String.format( "Feature: %s, Start: %s",
                                  this.feature.toString(),
                                  this.issuedDate.toString() );
        }
    }

    static class TimeSeriesEntry implements Response.Entry
    {
        TimeSeriesEntry( final Instant validDate, final Duration lead, final Double value, final String measurementUnit)
        {
            this.validDate = validDate;
            this.lead = lead;
            List<Double> newValues = new ArrayList<>();
            this.measurementUnit = measurementUnit;
            newValues.add( value );
            this.values = Collections.unmodifiableList( newValues );
        }

        TimeSeriesEntry(final Instant validDate, final Duration lead, final List<Double> values, final String measurementUnit)
        {
            this.validDate = validDate;
            this.lead = lead;
            this.measurementUnit = measurementUnit;
            this.values = Collections.unmodifiableList( values );
        }

        TimeSeriesEntry add(Double value, final String measurementUnit)
        {
            List<Double> newValues = new ArrayList<>(this.values);
            newValues.add(value);
            return new TimeSeriesEntry( this.validDate, this.lead, newValues, measurementUnit );
        }

        public Instant getValidDate()
        {
            return validDate;
        }

        public Duration getLead()
        {
            return lead;
        }

        public String getMeasurementUnit()
        {
            return this.measurementUnit;
        }

        public double[] getMeasurements()
        {
            return this.values.stream().mapToDouble( i -> i ).toArray();
        }

        private final Instant validDate;
        private final Duration lead;
        private final List<Double> values;
        private final String measurementUnit;

        @Override
        public Iterator<Double> iterator()
        {
            return this.values.iterator();
        }

        @Override
        public int compareTo( Response.Entry doubles )
        {
            return this.lead.compareTo( doubles.getLead() );
        }

        @Override
        public String toString()
        {
            StringJoiner valueJoiner = new StringJoiner( ", ", "[", "]" );
            values.forEach( value -> valueJoiner.add(String.valueOf(value)) );
            return String.format( "Valid: %s, Lead: %s, Values: %s",
                                  this.validDate.toString(),
                                  this.lead.toString(),
                                  valueJoiner.toString());
        }
    }

    public TimeSeriesResponse()
    {
        this.timeSeriesPerFeature = new TreeMap<>(  );
    }

    public void setVariableName(String variableName)
    {
        this.variableName = variableName;
    }

    public void add(
            final FeaturePlus feature,
            final Instant issuedDate,
            final Instant validDate,
            final double value,
            final String measurementUnit
    )
    {
        if (!this.timeSeriesPerFeature.containsKey( feature ))
        {
            this.timeSeriesPerFeature.put(feature, new ArrayList<>());
        }

        Series series = this.timeSeriesPerFeature.get(feature)
                                                     .stream()
                                                     .filter( timeSeries -> timeSeries.getIssuedDate().equals( issuedDate ) )
                                                     .findFirst()
                                                     .orElse( null );

        if (series == null)
        {
            series = new TimeSeries( feature, issuedDate );
            this.timeSeriesPerFeature.get(feature).add( series );
        }

        ((TimeSeries)series).add( validDate, value, measurementUnit );
        this.valueCount++;
    }

    private final Map<FeaturePlus, List<Series>> timeSeriesPerFeature;
    private String variableName;
    private Integer valueCount = 0;

    @Override
    public Iterator<List<Series>> iterator()
    {
        return this.timeSeriesPerFeature.values().iterator();
    }
}
