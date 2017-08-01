package wres.datamodel.metric;

import java.util.HashMap;
import java.util.Objects;

/**
 * A factory class for constructing {@link Metadata} and associated objects.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class DefaultMetadataFactory implements MetadataFactory
{

    /**
     * Instance of the factory.
     */

    private static MetadataFactory instance = null;

    /**
     * For safety, set a limit on the number of objects allowed in the cache.
     */

    private static final int outputMetaCacheLimit = 1000000;

    /**
     * Cache of instances of {@link MetricOutputMetadata} by hash code. Typically, there will be relatively few unique
     * instances of {@link MetricOutputMetadata} that are common to a large number of {@link MetricOutput}. Caching
     * these instances will provide a substantial memory saving for a small CPU cost in retrieving them.
     */

    private final HashMap<Integer, MetricOutputMetadata> outputMetaCache;

    /**
     * Returns an instance of a {@link DataFactory}.
     * 
     * @return a {@link DataFactory}
     */

    public static MetadataFactory getInstance()
    {
        if(Objects.isNull(instance))
        {
            instance = new DefaultMetadataFactory();
        }
        return instance;
    }

    @Override
    public Metadata getMetadata(final int sampleSize, final Dimension dim, final DatasetIdentifier identifier)
    {
        return new MetadataImpl(sampleSize, dim, identifier);
    }

    @Override
    public MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                                  final Dimension dim,
                                                  final Dimension inputDim,
                                                  final MetricConstants metricID)
    {
        return getOutputMetadata(sampleSize, dim, inputDim, metricID, MetricConstants.MAIN, null);
    }

    @Override
    public MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                                  final Dimension dim,
                                                  final Dimension inputDim,
                                                  final MetricConstants metricID,
                                                  final MetricConstants componentID)
    {
        return getOutputMetadata(sampleSize, dim, inputDim, metricID, componentID, null);
    }

    @Override
    public MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                                  final Dimension dim,
                                                  final Dimension inputDim,
                                                  final MetricConstants metricID,
                                                  final MetricConstants componentID,
                                                  final DatasetIdentifier identifier)
    {
        class MetricOutputMetadataImpl extends MetadataImpl implements MetricOutputMetadata
        {

            private MetricOutputMetadataImpl()
            {
                super(sampleSize, dim, identifier);
            }

            @Override
            public MetricConstants getMetricID()
            {
                return metricID;
            }

            @Override
            public MetricConstants getMetricComponentID()
            {
                return componentID;
            }

            @Override
            public Dimension getInputDimension()
            {
                return inputDim;
            }

            @Override
            public boolean minimumEquals(final MetricOutputMetadata o)
            {
                return o.getMetricID() == getMetricID() && o.getMetricComponentID() == getMetricComponentID()
                    && o.getDimension().equals(getDimension()) && o.getInputDimension().equals(getInputDimension());
            }

            @Override
            public boolean equals(final Object o)
            {
                if(!(o instanceof MetricOutputMetadata))
                {
                    return false;
                }
                final MetricOutputMetadata p = ((MetricOutputMetadata)o);
                return super.equals(o) && p.getMetricID() == getMetricID()
                    && p.getMetricComponentID() == getMetricComponentID()
                    && p.getInputDimension().equals(getInputDimension());
            }

            @Override
            public int hashCode()
            {
                return super.hashCode() + getMetricID().hashCode() + getMetricComponentID().hashCode()
                    + getInputDimension().hashCode();
            }

            @Override
            public String toString()
            {
                String start = super.toString();
                start = start.replaceAll("]", ",");
                final StringBuilder b = new StringBuilder(start);
                b.append(metricID).append(",").append(componentID).append("]");
                return b.toString();
            }

        }
        //Return from cache if available
        final MetricOutputMetadata testMe = new MetricOutputMetadataImpl();
        final MetricOutputMetadata returnMe = outputMetaCache.get(testMe.hashCode());
        if(!Objects.isNull(returnMe))
        {
            return returnMe;
        }
        //Clear the cache if required
        if(outputMetaCache.size() > outputMetaCacheLimit)
        {
            outputMetaCache.clear();
        }
        //Increment the cache and return
        outputMetaCache.put(testMe.hashCode(), testMe);
        return testMe;
    }

    @Override
    public DatasetIdentifier getDatasetIdentifier(final String geospatialID,
                                                  final String variableID,
                                                  final String scenarioID,
                                                  final String baselineScenarioID)
    {
        return new DatasetIdentifierImpl(geospatialID, variableID, scenarioID, baselineScenarioID);
    }

    @Override
    public Dimension getDimension()
    {
        return getDimension("DIMENSIONLESS");
    }

    @Override
    public Dimension getDimension(final String dimension)
    {
        class DimensionImpl implements Dimension
        {
            /**
             * The dimension.
             */
            private final String dimension;

            public DimensionImpl(final String dimension)
            {
                Objects.requireNonNull("Specify a non-null dimension string.", dimension);
                this.dimension = dimension;
            }

            @Override
            public boolean hasDimension()
            {
                return !"DIMENSIONLESS".equals(dimension);
            }

            @Override
            public String getDimension()
            {
                return dimension;
            }

            @Override
            public boolean equals(final Object o)
            {
                return o instanceof Dimension && ((Dimension)o).hasDimension() == hasDimension()
                    && ((Dimension)o).getDimension().equals(getDimension());
            }

            @Override
            public int hashCode()
            {
                return Boolean.hashCode(hasDimension());
            }

            @Override
            public String toString()
            {
                return getDimension();
            }
        }
        return new DimensionImpl(dimension);
    }

    @Override
    public String getMetricName(final MetricConstants identifier)
    {
        switch(identifier)
        {
            case BIAS_FRACTION:
                return "BIAS FRACTION";
            case BRIER_SCORE:
                return "BRIER SCORE";
            case BRIER_SKILL_SCORE:
                return "BRIER SKILL SCORE";
            case COEFFICIENT_OF_DETERMINATION:
                return "COEFFICIENT OF DETERMINATION";
            case CONTINGENCY_TABLE:
                return "CONTINGENCY TABLE";
            case CORRELATION_PEARSONS:
                return "CORRELATION PEARSONS";
            case CRITICAL_SUCCESS_INDEX:
                return "CRITICAL SUCCESS INDEX";
            case EQUITABLE_THREAT_SCORE:
                return "EQUITABLE THREAT SCORE";
            case MEAN_ABSOLUTE_ERROR:
                return "MEAN ABSOLUTE ERROR";
            case MEAN_CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE:
                return "MEAN CONTINUOUS RANKED PROBABILITY SKILL SCORE";
            case MEAN_ERROR:
                return "MEAN_ERROR";
            case MEAN_SQUARE_ERROR:
                return "MEAN SQUARE ERROR";
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return "MEAN SQUARE ERROR SKILL SCORE";
            case PEIRCE_SKILL_SCORE:
                return "PEIRCE SKILL SCORE";
            case PROBABILITY_OF_DETECTION:
                return "PROBABILITY OF DETECTION";
            case PROBABILITY_OF_FALSE_DETECTION:
                return "PROBABILITY OF FALSE DETECTION";
            case RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM:
                return "RELATIVE OPERATING CHARACTERISTIC";
            case RELATIVE_OPERATING_CHARACTERISTIC_SCORE:
                return "RELATIVE OPERATING CHARACTERISTIC SCORE";    
            case RELIABILITY_DIAGRAM:
                return "RELIABILITY_DIAGRAM";                   
            case ROOT_MEAN_SQUARE_ERROR:
                return "ROOT MEAN SQUARE ERROR";
            default:
                throw new IllegalArgumentException("Unable to determine the metric name from the prescribed "
                    + "identifier '" + identifier + "'.");
        }
    }

    @Override
    public String getMetricShortName(final MetricConstants identifier)
    {
        switch(identifier)
        {
            case BIAS_FRACTION:
                return "BIAS";
            case BRIER_SCORE:
                return "BS";
            case BRIER_SKILL_SCORE:
                return "BSS";
            case COEFFICIENT_OF_DETERMINATION:
                return "CoD";
            case CONTINGENCY_TABLE:
                return "CONTINGENCY TABLE";
            case CORRELATION_PEARSONS:
                return "CORRELATION PEARSONS";
            case CRITICAL_SUCCESS_INDEX:
                return "CSI";
            case EQUITABLE_THREAT_SCORE:
                return "ETS";
            case MEAN_ABSOLUTE_ERROR:
                return "MAE";
            case MEAN_CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE:
                return "MEAN CRPSS";
            case MEAN_ERROR:
                return "MEAN_ERROR";
            case MEAN_SQUARE_ERROR:
                return "MSE";
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return "MSE-SS";
            case PEIRCE_SKILL_SCORE:
                return "PSS";
            case PROBABILITY_OF_DETECTION:
                return "PoD";
            case PROBABILITY_OF_FALSE_DETECTION:
                return "PoFD";
            case RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM:
                return "ROC";
            case RELATIVE_OPERATING_CHARACTERISTIC_SCORE:
                return "ROC SCORE";   
            case RELIABILITY_DIAGRAM:
                return "RELIABILITY_DIAGRAM";    
            case ROOT_MEAN_SQUARE_ERROR:
                return "RMSE";
            default:
                throw new IllegalArgumentException("Unable to determine the short metric name from the prescribed "
                    + "identifier '" + identifier + "'.");
        }
    }

    @Override
    public String getMetricComponentName(final MetricConstants identifier)
    {
        switch(identifier)
        {
            case MAIN:
                return "MAIN OUTPUT";
            case RELIABILITY:
                return "RELIABILITY";
            case RESOLUTION:
                return "RESOLUTION";
            case UNCERTAINTY:
                return "UNCERTAINTY";
            case TYPE_II_BIAS:
                return "TYPE II CONDITIONAL BIAS";
            case DISCRIMINATION:
                return "DISCRIMINATION";
            case SHARPNESS:
                return "SHARPNESS";
            case SAMPLE_SIZE:
                return "SAMPLE_SIZE";
            default:
                throw new IllegalArgumentException("Unable to determine the metric component name from the "
                    + "prescribed identifier '" + identifier + "'.");
        }
    }

    /**
     * Default implementation of {@link Metadata}.
     */

    private class MetadataImpl implements Metadata
    {
        private final int sampleSize;
        private final Dimension dim;
        private final DatasetIdentifier identifier;

        private MetadataImpl(final int sampleSize, final Dimension dim, final DatasetIdentifier identifier)
        {
            this.sampleSize = sampleSize;
            this.dim = dim;
            this.identifier = identifier;
        }

        @Override
        public int getSampleSize()
        {
            return sampleSize;
        }

        @Override
        public Dimension getDimension()
        {
            return dim;
        }

        @Override
        public DatasetIdentifier getIdentifier()
        {
            return identifier;
        }

        @Override
        public boolean equals(final Object o)
        {
            if(!(o instanceof Metadata))
            {
                return false;
            }
            final Metadata p = (Metadata)o;
            boolean returnMe = p.getSampleSize() == getSampleSize() && p.getDimension().equals(getDimension())
                && hasIdentifier() == p.hasIdentifier();
            if(hasIdentifier())
            {
                returnMe = returnMe && identifier.equals(p.getIdentifier());
            }
            return returnMe;
        }

        @Override
        public int hashCode()
        {
            int returnMe = Integer.hashCode(sampleSize) + getDimension().hashCode() + Boolean.hashCode(hasIdentifier());
            if(hasIdentifier())
            {
                returnMe += identifier.hashCode();
            }
            return returnMe;
        }

        @Override
        public String toString()
        {
            final StringBuilder b = new StringBuilder();
            if(hasIdentifier())
            {
                String appendMe = identifier.toString();
                appendMe = appendMe.replaceAll("]", ",");
                b.append(appendMe);
            }
            else
            {
                b.append("[");
            }
            b.append(sampleSize).append(",").append(dim).append("]");
            return b.toString();
        }

    }

    /**
     * Default implementation of {@link DatasetIdentifier}.
     */

    private class DatasetIdentifierImpl implements DatasetIdentifier
    {

        final String geospatialID;
        final String variableID;
        final String scenarioID;
        final String baselineScenarioID;

        private DatasetIdentifierImpl(final String geospatialID,
                                      final String variableID,
                                      final String scenarioID,
                                      final String baselineScenarioID)
        {
            this.geospatialID = geospatialID;
            this.variableID = variableID;
            this.scenarioID = scenarioID;
            this.baselineScenarioID = baselineScenarioID;
        }

        @Override
        public String getGeospatialID()
        {
            return geospatialID;
        }

        @Override
        public String getVariableID()
        {
            return variableID;
        }

        @Override
        public String getScenarioID()
        {
            return scenarioID;
        }

        @Override
        public String getScenarioIDForBaseline()
        {
            return baselineScenarioID;
        }

        @Override
        public String toString()
        {
            final StringBuilder b = new StringBuilder();
            b.append("[");
            if(hasGeospatialID())
            {
                b.append(getGeospatialID()).append(",");
            }
            if(hasVariableID())
            {
                b.append(getVariableID()).append(",");
            }
            if(hasScenarioID())
            {
                b.append(getScenarioID()).append(",");
            }
            if(hasScenarioIDForBaseline())
            {
                b.append(getScenarioIDForBaseline()).append(",");
            }
            b.append("]");
            return b.toString();
        }

        @Override
        public boolean equals(final Object o)
        {
            if(!(o instanceof DatasetIdentifier))
            {
                return false;
            }
            final DatasetIdentifier check = (DatasetIdentifier)o;
            boolean returnMe = hasGeospatialID() == check.hasGeospatialID() && hasVariableID() == check.hasVariableID()
                && hasScenarioID() == check.hasScenarioID()
                && hasScenarioIDForBaseline() == check.hasScenarioIDForBaseline();
            if(hasGeospatialID())
            {
                returnMe = returnMe && getGeospatialID().equals(check.getGeospatialID());
            }
            if(hasVariableID())
            {
                returnMe = returnMe && getVariableID().equals(check.getVariableID());
            }
            if(hasScenarioID())
            {
                returnMe = returnMe && getScenarioID().equals(check.getScenarioID());
            }
            if(hasScenarioIDForBaseline())
            {
                returnMe = returnMe && getScenarioIDForBaseline().equals(check.getScenarioIDForBaseline());
            }
            return returnMe;
        }

        @Override
        public int hashCode()
        {
            final String uniqueID = getGeospatialID() + getVariableID() + getScenarioID() + getScenarioIDForBaseline();
            return uniqueID.hashCode();
        }
    }

    /**
     * No argument constructor.
     */

    private DefaultMetadataFactory()
    {
        outputMetaCache = new HashMap<>();
    }

}
