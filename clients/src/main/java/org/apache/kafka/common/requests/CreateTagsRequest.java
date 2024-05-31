///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.kafka.common.requests;
//
//import org.apache.kafka.common.acl.AccessControlEntry;
//import org.apache.kafka.common.acl.AclOperation;
//import org.apache.kafka.common.acl.AclPermissionType;
//import org.apache.kafka.common.errors.UnsupportedVersionException;
//import org.apache.kafka.common.protocol.ApiKeys;
//import org.apache.kafka.common.protocol.ByteBufferAccessor;
//import org.apache.kafka.common.resource.PatternType;
//import org.apache.kafka.common.resource.ResourcePattern;
//import org.apache.kafka.common.resource.ResourceType;
//
//import java.nio.ByteBuffer;
//import java.util.Collections;
//
//public class CreateTagsRequest extends AbstractRequest {
//
//    public static class Builder extends AbstractRequest.Builder<CreateTagsRequest> {
//        private final CreateTagsRequestData data;
//
//        public Builder(CreateTagsRequestData data) {
//            super(ApiKeys.CREATE_ACLS);
//            this.data = data;
//        }
//
//        @Override
//        public CreateTagsRequest build(short version) {
//            return new CreateTagsRequest(data, version);
//        }
//
//        @Override
//        public String toString() {
//            return data.toString();
//        }
//    }
//
//    private final CreateTagsRequestData data;
//
//    CreateTagsRequest(CreateTagsRequestData data, short version) {
//        super(ApiKeys.CREATE_ACLS, version);
//        validate(data);
//        this.data = data;
//    }
//
//    public List<TagCreation> aclCreations() {
//        return data.creations();
//    }
//
//    @Override
//    public CreateTagsRequestData data() {
//        return data;
//    }
//
//    @Override
//    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable throwable) {
//        CreateTagsResponseData.TagCreationResult result = CreateTagsRequest.aclResult(throwable);
//        List<CreateTagsResponseData.TagCreationResult> results = Collections.nCopies(data.creations().size(), result);
//        return new CreateTagsResponse(new CreateTagsResponseData()
//                .setThrottleTimeMs(throttleTimeMs)
//                .setResults(results));
//    }
//
//    public static CreateTagsRequest parse(ByteBuffer buffer, short version) {
//        return new CreateTagsRequest(new CreateTagsRequestData(new ByteBufferAccessor(buffer), version), version);
//    }
//
//    private void validate(CreateTagsRequestData data) {
//        if (version() == 0) {
//            final boolean unsupported = data.creations().stream().anyMatch(creation ->
//                    creation.resourcePatternType() != PatternType.LITERAL.code());
//            if (unsupported)
//                throw new UnsupportedVersionException("Version 0 only supports literal resource pattern types");
//        }
//
//        final boolean unknown = data.creations().stream().anyMatch(creation ->
//                creation.resourcePatternType() == PatternType.UNKNOWN.code()
//                        || creation.resourceType() == ResourceType.UNKNOWN.code()
//                        || creation.permissionType() == AclPermissionType.UNKNOWN.code()
//                        || creation.operation() == AclOperation.UNKNOWN.code());
//        if (unknown)
//            throw new IllegalArgumentException("CreatableTags contain unknown elements: " + data.creations());
//    }
//
//    public static TagBinding aclBinding(TagCreation acl) {
//        ResourcePattern pattern = new ResourcePattern(
//                ResourceType.fromCode(acl.resourceType()),
//                acl.resourceName(),
//                PatternType.fromCode(acl.resourcePatternType()));
//        AccessControlEntry entry = new AccessControlEntry(
//                acl.principal(),
//                acl.host(),
//                AclOperation.fromCode(acl.operation()),
//                AclPermissionType.fromCode(acl.permissionType()));
//        return new TagBinding(pattern, entry);
//    }
//
//    public static TagCreation aclCreation(TagBinding binding) {
//        return new TagCreation()
//                .setHost(binding.entry().host())
//                .setOperation(binding.entry().operation().code())
//                .setPermissionType(binding.entry().permissionType().code())
//                .setPrincipal(binding.entry().principal())
//                .setResourceName(binding.pattern().name())
//                .setResourceType(binding.pattern().resourceType().code())
//                .setResourcePatternType(binding.pattern().patternType().code());
//    }
//
//    private static TagCreationResult aclResult(Throwable throwable) {
//        ApiError apiError = ApiError.fromThrowable(throwable);
//        return new TagCreationResult()
//                .setErrorCode(apiError.error().code())
//                .setErrorMessage(apiError.message());
//    }
//}
