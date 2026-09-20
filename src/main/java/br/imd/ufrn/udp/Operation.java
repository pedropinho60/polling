package br.imd.ufrn.udp;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "operation"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = CreateOperation.class, name = "create"),
        @JsonSubTypes.Type(value = VoteOperation.class, name = "vote"),
        @JsonSubTypes.Type(value = GetOperation.class, name = "get")
})
public sealed interface Operation permits CreateOperation, GetOperation, VoteOperation {
}
