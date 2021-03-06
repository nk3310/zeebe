/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.client.event;

/**
 * POJO representing an event of type {@link TopicEventType#WORKFLOW}.
 */
public interface WorkflowEvent extends Event
{
    /**
     * @return the BPMN process id of the workflow.
     */
    String getBpmnProcessId();

    /**
     * @return the version of the deployed workflow this instance belongs to.
     */
    int getVersion();

    /**
     * @return the XML representation of the workflow.
     */
    String getBpmnXml();

    /**
     * @return the key of the deployment this workflow belongs to.
     */
    long getDeploymentKey();
}