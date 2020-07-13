package wres.io.thresholds.wrds.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import wres.config.FeaturePlus;
import wres.config.generated.Feature;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.retrieval.UnitMapper;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;

/**
 * Represents the combined elements that defined an atomic set of thresholds
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ThresholdDefinition implements Serializable {
    /**
     * Metadata describing where the values are valid and who produced them
     */
    ThresholdMetadata metadata;

    /**
     * @return The metadata for the combined set of thresholds
     */
    public ThresholdMetadata getMetadata() {
        return metadata;
    }

    /**
     * Sets the metadata for the definition
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param metadata The metadata for the combined set of thresholds
     */
    public void setMetadata(ThresholdMetadata metadata) {
        this.metadata = metadata;
    }

    /**
     * Gets the original, unprocessed set of threshold values
     * <br>
     *     <b>NOTE:</b> Many thresholds are calculated rather than observed
     *
     * @return The set of measured thresholds
     */
    public OriginalThresholdValues getOriginal_values() {
        return original_values;
    }

    public void setOriginal_values(OriginalThresholdValues original_values) {
        this.original_values = original_values;
    }

    public CalculatedThresholdValues getCalculated_values() {
        return calculated_values;
    }

    public void setCalculated_values(CalculatedThresholdValues calculated_values) {
        this.calculated_values = calculated_values;
    }

    public String getThresholdProvider() {
        return this.getMetadata().getThreshold_source();
    }

    public String getRatingProvider() {
        return this.getMetadata().getRating_source();
    }

    public Double getLowFlow(boolean getCalculated, UnitMapper unitMapper) {
        Double flow;

        if (getCalculated && this.getCalculated_values().getLow_flow() != null) {
            flow = this.getCalculated_values().getLow_flow();
        }
        else {
            flow = this.getOriginal_values().getLow_flow();
        }

        if (flow == null) {
            return null;
        }

        return this.getFlowUnitConversion(unitMapper).applyAsDouble(flow);
    }

    public Double getLowStage(boolean getCalculated, UnitMapper unitMapper) {
        Double stage;

        if (getCalculated && this.getCalculated_values().getLow_stage() != null) {
            stage = this.getCalculated_values().getLow_stage();
        }
        else {
            stage = this.getOriginal_values().getLow_stage();
        }

        if (stage == null) {
            return null;
        }

        return this.getStageUnitConversion(unitMapper).applyAsDouble(stage);
    }

    public Double getActionFlow(boolean getCalculated, UnitMapper unitMapper) {
        Double flow;

        if (getCalculated && this.getCalculated_values().getAction_flow() != null) {
            flow = this.getCalculated_values().getAction_flow();
        }
        else {
            flow = this.getOriginal_values().getAction_flow();
        }

        if (flow == null) {
            return null;
        }

        return this.getFlowUnitConversion(unitMapper).applyAsDouble(flow);
    }

    public Double getActionStage(boolean getCalculated, UnitMapper unitMapper) {
        Double stage;

        if (getCalculated && this.getCalculated_values().getAction_stage() != null) {
            stage = this.getCalculated_values().getAction_stage();
        }
        else {
            stage = this.getOriginal_values().getAction_stage();
        }

        if (stage == null) {
            return null;
        }

        return this.getStageUnitConversion(unitMapper).applyAsDouble(stage);
    }

    public Double getMinorFlow(boolean getCalculated, UnitMapper unitMapper) {
        Double flow;

        if (getCalculated && this.getCalculated_values().getMinor_flow() != null) {
            flow = this.getCalculated_values().getMinor_flow();
        }
        else {
            flow = this.getOriginal_values().getMinor_flow();
        }

        if (flow == null) {
            return null;
        }

        return this.getFlowUnitConversion(unitMapper).applyAsDouble(flow);
    }

    public Double getMinorStage(boolean getCalculated, UnitMapper unitMapper) {
        Double stage;

        if (getCalculated && this.getCalculated_values().getMinor_stage() != null) {
            stage = this.getCalculated_values().getMinor_stage();
        }
        else {
            stage = this.getOriginal_values().getMinor_stage();
        }

        if (stage == null) {
            return null;
        }

        return this.getStageUnitConversion(unitMapper).applyAsDouble(stage);
    }

    public Double getModerateFlow(boolean getCalculated, UnitMapper unitMapper) {
        Double flow;

        if (getCalculated && this.getCalculated_values().getModerate_flow() != null)  {
            flow = this.getCalculated_values().getModerate_flow();
        }
        else {
            flow = this.getOriginal_values().getModerate_flow();
        }

        if (flow == null) {
            return null;
        }

        return this.getFlowUnitConversion(unitMapper).applyAsDouble(flow);
    }

    public Double getModerateStage(boolean getCalculated, UnitMapper unitMapper) {
        Double stage;

        if (getCalculated && this.getCalculated_values().getModerate_stage() != null) {
            stage = this.getCalculated_values().getModerate_stage();
        }
        else {
            stage = this.getOriginal_values().getModerate_stage();
        }

        if (stage == null) {
            return null;
        }

        return this.getStageUnitConversion(unitMapper).applyAsDouble(stage);
    }

    public Double getMajorFlow(boolean getCalculated, UnitMapper unitMapper) {
        Double flow;

        if (getCalculated && this.getCalculated_values().getMajor_flow() != null) {
            flow = this.getCalculated_values().getMajor_flow();
        }
        else {
            flow = this.getOriginal_values().getMajor_flow();
        }

        if (flow == null) {
            return null;
        }

        return this.getFlowUnitConversion(unitMapper).applyAsDouble(flow);
    }

    public Double getMajorStage(boolean getCalculated, UnitMapper unitMapper) {
        Double stage;

        if (getCalculated && this.getCalculated_values().getMajor_stage() != null) {
            stage = this.getCalculated_values().getMajor_stage();
        }
        else {
            stage = this.getOriginal_values().getMajor_stage();
        }

        if (stage == null) {
            return null;
        }

        return this.getStageUnitConversion(unitMapper).applyAsDouble(stage);
    }

    public Double getBankfulFlow(boolean getCalculated, UnitMapper unitMapper) {
        Double flow;

        if (getCalculated && this.getCalculated_values().getBankfull_flow() != null) {
            flow = this.getCalculated_values().getBankfull_flow();
        }
        else {
            flow = this.getOriginal_values().getBankfull_flow();
        }

        if (flow == null) {
            return null;
        }

        return this.getFlowUnitConversion(unitMapper).applyAsDouble(flow);
    }

    public Double getBankfulStage(boolean getCalculated, UnitMapper unitMapper) {
        Double stage;

        if (getCalculated && this.getCalculated_values().getBankfull_stage() != null) {
            stage = this.getCalculated_values().getBankfull_stage();
        }
        else {
            stage = this.getOriginal_values().getBankfull_stage();
        }

        if (stage == null) {
            return null;
        }

        return this.getStageUnitConversion(unitMapper).applyAsDouble(stage);
    }

    public Double getRecordFlow(boolean getCalculated, UnitMapper unitMapper) {
        Double flow;

        if (getCalculated && this.getCalculated_values().getRecord_flow() != null) {
            flow = this.getCalculated_values().getRecord_flow();
        }
        else {
            flow = this.getOriginal_values().getRecord_flow();
        }

        if (flow == null) {
            return null;
        }

        return this.getFlowUnitConversion(unitMapper).applyAsDouble(flow);
    }

    public Double getRecordStage(boolean getCalculated, UnitMapper unitMapper) {
        Double stage;

        if (getCalculated && this.getCalculated_values().getRecord_stage() != null) {
            stage = this.getCalculated_values().getRecord_stage();
        }
        else {
            stage = this.getOriginal_values().getRecord_stage();
        }

        if (stage == null) {
            return null;
        }

        return this.getStageUnitConversion(unitMapper).applyAsDouble(stage);
    }

    public MeasurementUnit getFlowMeasurementUnit() {
        return MeasurementUnit.of(this.getMetadata().getFlow_unit());
    }

    public MeasurementUnit getStageMeasurementUnit() {
        return MeasurementUnit.of(this.getMetadata().getStage_unit());
    }

    OriginalThresholdValues original_values;
    CalculatedThresholdValues calculated_values;

    public Map<FeaturePlus, Set<ThresholdOuter>> getThresholds(
            WRDSThresholdType thresholdType,
            ThresholdConstants.Operator thresholdOperator,
            ThresholdConstants.ThresholdDataType dataType,
            boolean getCalculated,
            UnitMapper desiredUnitMapper
    ) {
        FeaturePlus feature = FeaturePlus.of(new Feature(
                null,
                null,
                null,
                null,
                null,
                this.getMetadata().getLocation_id(),
                null,
                null,
                null,
                null,
                null,
                null,
                null
        ));

        Set<ThresholdOuter> thresholds = new HashSet<>();

        if (thresholdType.equals(WRDSThresholdType.FLOW)) {
            thresholds = this.getFlowThresholds(getCalculated, thresholdOperator, dataType, desiredUnitMapper);
        }
        else {
            thresholds = this.getStageThresholds(getCalculated, thresholdOperator, dataType, desiredUnitMapper);
        }
        return Map.of(feature, thresholds);
    }

    private Set<ThresholdOuter> getFlowThresholds(
            boolean getCalculated,
            ThresholdConstants.Operator thresholdOperator,
            ThresholdConstants.ThresholdDataType dataType,
            UnitMapper desiredUnitMapper
    ) {
        Set<ThresholdOuter> thresholds = new HashSet<>();

        Double low = this.getLowFlow(getCalculated, desiredUnitMapper);
        Double action = this.getActionFlow(getCalculated, desiredUnitMapper);
        Double minor = this.getMinorFlow(getCalculated, desiredUnitMapper);
        Double moderate = this.getModerateFlow(getCalculated, desiredUnitMapper);
        Double major = this.getMajorFlow(getCalculated, desiredUnitMapper);
        Double bankful = this.getBankfulFlow(getCalculated, desiredUnitMapper);
        Double record = this.getRecordFlow(getCalculated, desiredUnitMapper);

        if (low != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(low),
                            thresholdOperator,
                            dataType,
                            "low",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        if (action != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(action),
                            thresholdOperator,
                            dataType,
                            "action",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        if (minor != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(minor),
                            thresholdOperator,
                            dataType,
                            "minor",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        if (moderate != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(moderate),
                            thresholdOperator,
                            dataType,
                            "moderate",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        if (major != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(major),
                            thresholdOperator,
                            dataType,
                            "major",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        if (bankful != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(bankful),
                            thresholdOperator,
                            dataType,
                            "bankfull",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        if (record != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(record),
                            thresholdOperator,
                            dataType,
                            "record",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        return thresholds;
    }

    private Set<ThresholdOuter> getStageThresholds(
            boolean getCalculated,
            ThresholdConstants.Operator thresholdOperator,
            ThresholdConstants.ThresholdDataType dataType,
            UnitMapper desiredUnitMapper
    ) {
        Set<ThresholdOuter> thresholds = new HashSet<>();

        Double low = this.getLowStage(getCalculated, desiredUnitMapper);
        Double action = this.getActionStage(getCalculated, desiredUnitMapper);
        Double minor = this.getMinorStage(getCalculated, desiredUnitMapper);
        Double moderate = this.getModerateStage(getCalculated, desiredUnitMapper);
        Double major = this.getMajorStage(getCalculated, desiredUnitMapper);
        Double bankful = this.getBankfulStage(getCalculated, desiredUnitMapper);
        Double record = this.getRecordStage(getCalculated, desiredUnitMapper);

        if (low != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(low),
                            thresholdOperator,
                            dataType,
                            "low",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        if (action != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(action),
                            thresholdOperator,
                            dataType,
                            "action",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        if (minor != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(minor),
                            thresholdOperator,
                            dataType,
                            "minor",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        if (moderate != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(moderate),
                            thresholdOperator,
                            dataType,
                            "moderate",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        if (major != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(major),
                            thresholdOperator,
                            dataType,
                            "major",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        if (bankful != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(bankful),
                            thresholdOperator,
                            dataType,
                            "bankfull",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        if (record != null) {
            thresholds.add(
                    ThresholdOuter.of(
                            OneOrTwoDoubles.of(record),
                            thresholdOperator,
                            dataType,
                            "record",
                            MeasurementUnit.of(desiredUnitMapper.getDesiredMeasurementUnitName())
                    )
            );
        }

        return thresholds;
    }

    private DoubleUnaryOperator getStageUnitConversion(UnitMapper mapper) {
        return mapper.getUnitMapper(this.getStageMeasurementUnit().getUnit());
    }

    private DoubleUnaryOperator getFlowUnitConversion(UnitMapper mapper) {
        return mapper.getUnitMapper(this.getFlowMeasurementUnit().getUnit());
    }
}
