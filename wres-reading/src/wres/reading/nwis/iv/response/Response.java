package wres.reading.nwis.iv.response;

import java.io.Serial;
import java.io.Serializable;

import jakarta.xml.bind.annotation.XmlRootElement;

import lombok.Getter;
import lombok.Setter;

/**
 * A WaterML response.
 */
@Setter
@Getter
@XmlRootElement
public class Response implements Serializable
{
    @Serial
    private static final long serialVersionUID = 5433948668020324606L;
    /** Name. */
    private String name;

    /** Declared type. */
    private String declaredType;

    /** Scope. */
    private String scope;

    /** Value. */
    private ResponseValue value;

    /** The nil status. */
    private Boolean nil;

    /** Whether there is global scope. */
    private Boolean globalScope;

    /** Whether the type is substituted. */
    private Boolean typeSubstituted;
}
