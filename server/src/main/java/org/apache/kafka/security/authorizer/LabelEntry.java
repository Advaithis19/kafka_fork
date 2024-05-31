package org.apache.kafka.security.authorizer;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.util.Json;
import org.apache.kafka.server.util.json.DecodeJson;
import org.apache.kafka.server.util.json.JsonObject;
import org.apache.kafka.server.util.json.JsonValue;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.common.acl.AclOperation.*;

public class LabelEntry {
    private static final DecodeJson.DecodeString STRING = new DecodeJson.DecodeString();

    public final KafkaPrincipal entity;
    public final KafkaPrincipal owner;
    public final AclOperation operation;

    public LabelEntry(KafkaPrincipal entity, KafkaPrincipal owner, AclOperation operation) {
        this.entity = SecurityUtils.parseKafkaPrincipal(String.valueOf(entity));
        this.owner = SecurityUtils.parseKafkaPrincipal(String.valueOf(owner));
        this.operation = operation;
    }

    public static Set<LabelEntry> fromBytes(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0)
          return Collections.emptySet();

        Optional<JsonValue> jsonValue = Json.parseBytes(bytes);
        if (!jsonValue.isPresent())
            return Collections.emptySet();

        JsonObject js = jsonValue.get().asJsonObject();

        Set<LabelEntry> res = new HashSet<>();

        Iterator<JsonValue> aclsIter = js.apply("entries").asJsonArray().iterator();
        while (aclsIter.hasNext()) {
            JsonObject itemJs = aclsIter.next().asJsonObject();
            KafkaPrincipal entity = SecurityUtils.parseKafkaPrincipal(itemJs.apply("entity").to(STRING));
            KafkaPrincipal owner = SecurityUtils.parseKafkaPrincipal(itemJs.apply("owner").to(STRING));
            AclOperation operation = SecurityUtils.operation(itemJs.apply("operation").to(STRING));

            res.add(new LabelEntry(entity, owner, operation));
        }

        return res;
    }

    public static Map<String, Object> toJsonCompatibleMap(Set<LabelEntry> labelEntries) {
        Map<String, Object> res = new HashMap<>();
        res.put("entries", labelEntries.stream().map(LabelEntry::toMap).collect(Collectors.toList()));
        return res;
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

    public Map<String, Object> toMap() {
        Map<String, Object> res = new HashMap<>();
        res.put("entity", this.entity);
        res.put("owner", this.owner);
        res.put("operation", this.operation);
        return res;
    }

    @Override
    public String toString() {
        String entityType = this.operation == AclOperation.READ ? "READER" : "WRITER";
        return this.owner + " has designated " + this.entity + " as a " + entityType + " for the topic";
    }
}
