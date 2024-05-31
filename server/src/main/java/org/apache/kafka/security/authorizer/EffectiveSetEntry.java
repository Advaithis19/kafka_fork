package org.apache.kafka.security.authorizer;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import java.util.*;

import static org.apache.kafka.common.acl.AclOperation.*;

public class EffectiveSetEntry {
    public final KafkaPrincipal entity;
    public final AclOperation operation;

    public EffectiveSetEntry(KafkaPrincipal entity, AclOperation operation) {
        this.entity = SecurityUtils.parseKafkaPrincipal(String.valueOf(entity));
        this.operation = operation;
    }

    public static Set<AclOperation> supportedOperations(ResourceType resourceType) {
        if(resourceType == ResourceType.TOPIC) {
            return new HashSet<>(Arrays.asList(READ, WRITE));
        } else {
            throw new IllegalArgumentException("Not a concrete resource type");
        }
    }

    public static Errors authorizationError(ResourceType resourceType) {
        if(resourceType == ResourceType.TOPIC) {
            return Errors.TOPIC_AUTHORIZATION_FAILED;
        } else {
            throw new IllegalArgumentException("Authorization error type not known");
        }
    }

    @Override
    public String toString() {
        return this.entity + " has " + operation + " access for the topic";
    }
}
