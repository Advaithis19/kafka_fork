package org.apache.kafka.common.acl;

import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Objects;

public class TagBinding {
    private final ResourcePattern pattern;
    private final KafkaPrincipal owner;
    private final KafkaPrincipal entity;
    private final AclOperation operation;

    public TagBinding(ResourcePattern pattern, KafkaPrincipal owner, KafkaPrincipal entity, AclOperation operation) {
        this.pattern = Objects.requireNonNull(pattern, "pattern");
        this.owner = Objects.requireNonNull(entity, "owner");
        this.entity = Objects.requireNonNull(entity, "entry");
        this.operation = Objects.requireNonNull(operation, "operation");
    }

    public ResourcePattern pattern() {
        return pattern;
    }

    public final KafkaPrincipal owner() {
        return owner;
    }

    public final KafkaPrincipal entity() {
        return entity;
    }

    public final AclOperation operation() {
        return operation;
    }

    @Override
    public String toString() {
        return "(pattern=" + pattern + ", owner=" + owner + ", entity=" + entity + ", operation=" + operation + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern, owner, entity, operation);
    }
}
