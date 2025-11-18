package wres.reading.nwis.iv.response.variable;

import java.io.Serial;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * A variable code.
 */
@Setter
@Getter
public class VariableCode implements Serializable
{
    @Serial
    private static final long serialVersionUID = 4782260215264929265L;

    /** Value. */
    private String value;

    /** Network. */
    private String network;

    /** Vocabulary. */
    private String vocabulary;

    /** Variable ID. */
    private Integer variableID;

    /** Default value. */
    @JsonProperty( "default" )
    private Boolean defaultValue;
}
