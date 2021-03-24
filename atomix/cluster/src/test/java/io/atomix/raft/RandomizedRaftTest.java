/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
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
package io.atomix.raft;

import io.atomix.cluster.MemberId;
import io.zeebe.util.FileUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.EdgeCasesMode;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.ShrinkingMode;
import net.jqwik.api.lifecycle.AfterTry;
import net.jqwik.api.lifecycle.BeforeProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomizedRaftTest {

  private static final int OPERATION_SIZE = 10000;
  private static final Logger LOG = LoggerFactory.getLogger(RandomizedRaftTest.class);
  private ControllableRaftContexts raftContexts;
  private List<RaftOperation> operations;
  private List<MemberId> raftMembers;
  private Path raftDataDirectory;

  @BeforeProperty
  public void initOperations() {
    // Need members ids to generate pair operations
    final var servers =
        IntStream.range(0, 3)
            .mapToObj(String::valueOf)
            .map(MemberId::from)
            .collect(Collectors.toList());
    operations = RaftOperation.getDefaultRaftOperations();
    raftMembers = servers;
  }

  @AfterTry
  public void shutDownRaftNodes() throws IOException {
    raftContexts.shudown();
    FileUtil.deleteFolder(raftDataDirectory);
    raftDataDirectory = null;
  }

  @Property(tries = 10, shrinking = ShrinkingMode.OFF, edgeCases = EdgeCasesMode.NONE)
  void raftProperty(
      @ForAll("raftOperations") final List<RaftOperation> raftOperations,
      @ForAll("raftMembers") final List<MemberId> raftMembers,
      @ForAll("seeds") final long seed)
      throws Exception {

    setUpRaftNodes(new Random(seed));

    int step = 0;
    final var memberIter = raftMembers.iterator();
    for (final RaftOperation operation : raftOperations) {
      step++;

      final MemberId member = memberIter.next();
      LOG.info("{} on {}", operation, member);
      operation.run(raftContexts, member);
      raftContexts.assertOnlyOneLeader();

      if (step % 100 == 0) { // reading logs after every operation can be too slow
        raftContexts.assertAllLogsEqual();
        step = 0;
      }
    }

    raftContexts.assertAllLogsEqual();
  }

  @Provide
  Arbitrary<List<RaftOperation>> raftOperations() {
    final var operation = Arbitraries.of(operations);
    return operation.list().ofSize(OPERATION_SIZE);
  }

  @Provide
  Arbitrary<List<MemberId>> raftMembers() {
    final var members = Arbitraries.of(raftMembers);
    return members.list().ofSize(OPERATION_SIZE);
  }

  @Provide
  Arbitrary<Long> seeds() {
    return Arbitraries.longs();
  }

  private void setUpRaftNodes(final Random random) throws Exception {
    // Couldnot make @TempDir annotation work
    raftDataDirectory = Files.createTempDirectory(null);
    raftContexts = new ControllableRaftContexts(3);
    raftContexts.setup(raftDataDirectory, random);
  }
}
