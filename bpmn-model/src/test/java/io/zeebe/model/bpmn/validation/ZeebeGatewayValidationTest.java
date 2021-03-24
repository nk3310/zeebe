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
package io.zeebe.model.bpmn.validation;

import static io.zeebe.model.bpmn.validation.ExpectedValidationResult.expect;
import static java.util.Collections.singletonList;

import io.zeebe.model.bpmn.Bpmn;
import org.junit.runners.Parameterized.Parameters;

public class ZeebeGatewayValidationTest extends AbstractZeebeValidationTest {

  @Parameters(name = "{index}: {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .exclusiveGateway("gateway")
            .sequenceFlowId("flow1")
            .endEvent()
            .moveToLastExclusiveGateway()
            .sequenceFlowId("flow2")
            .conditionExpression("condition")
            .endEvent()
            .done(),
        singletonList(expect("flow1", "Must have a condition or be default flow"))
      },
      {
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .exclusiveGateway("gateway")
            .sequenceFlowId("flow")
            .condition("name", "foo")
            .defaultFlow()
            .endEvent()
            .done(),
        singletonList(expect("gateway", "Default flow must not have a condition"))
      },
      {"default-flow.bpmn", singletonList(expect("gateway", "Default flow must start at gateway"))},
      {
        Bpmn.createExecutableProcess("process")
            .startEvent("start")
            .serviceTask("task", b -> b.zeebeJobType("type"))
            .conditionExpression("foo")
            .endEvent()
            .done(),
        singletonList(
            expect("task", "Conditional sequence flows are only supported at exclusive gateway"))
      },
    };
  }
}
