package io.zeebe.broker.clustering2.base.topology;

import static io.zeebe.broker.clustering2.base.gossip.GossipCustomEventEncoding.*;

import java.util.ArrayList;
import java.util.List;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering2.base.topology.Topology.MemberInfo;
import io.zeebe.gossip.*;
import io.zeebe.gossip.dissemination.GossipSyncRequest;
import io.zeebe.gossip.membership.Member;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.raft.Raft;
import io.zeebe.raft.RaftStateListener;
import io.zeebe.raft.state.RaftState;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.LogUtil;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import org.agrona.*;
import org.slf4j.Logger;

public class TopologyManager extends Actor
{
    private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

    public static final DirectBuffer CONTACT_POINTS_EVENT_TYPE = BufferUtil.wrapString("contact_points");
    public static final DirectBuffer PARTITIONS_EVENT_TYPE = BufferUtil.wrapString("partitions");

    private final MembershipListener membershipListner = new MembershipListener();
    private final ContactPointsChangeListener contactPointsChangeListener = new ContactPointsChangeListener();
    private final ParitionChangeListener paritionChangeListener = new ParitionChangeListener();
    private final KnownContactPointsSyncHandler localContactPointsSycHandler = new KnownContactPointsSyncHandler();
    private final KnownPartitionsSyncHandler knownPartitionsSyncHandler = new KnownPartitionsSyncHandler();
    private final LocalRaftStateChangeListener localRaftStateChangeListener = new LocalRaftStateChangeListener();

    private final Topology topology;
    private final Gossip gossip;

    private List<TopologyMemberListener> topologyMemberListers = new ArrayList<>();

    public TopologyManager(Gossip gossip, MemberInfo localBroker)
    {
        this.gossip = gossip;
        this.topology = new Topology(localBroker);
    }

    @Override
    protected void onActorStarting()
    {
        gossip.addMembershipListener(membershipListner);

        gossip.addCustomEventListener(CONTACT_POINTS_EVENT_TYPE, contactPointsChangeListener);
        gossip.addCustomEventListener(PARTITIONS_EVENT_TYPE, paritionChangeListener);

        gossip.registerSyncRequestHandler(CONTACT_POINTS_EVENT_TYPE, localContactPointsSycHandler);
        gossip.registerSyncRequestHandler(PARTITIONS_EVENT_TYPE, knownPartitionsSyncHandler);

        publishLocalContactPoints();
    }

    @Override
    protected void onActorClosing()
    {
        gossip.removeCustomEventListener(paritionChangeListener);
        gossip.removeCustomEventListener(contactPointsChangeListener);

        // remove gossip sync handlers?
    }

    public void onRaftStarted(Raft raft)
    {
        actor.run(() ->
        {
            final LogStream logStream = raft.getLogStream();
            topology.updatePartition(logStream.getPartitionId(), logStream.getTopicName(), topology.getLocal(), raft.getState());
            raft.registerRaftStateListener(localRaftStateChangeListener);
            publishLocalPartitions();
        });
    }

    public void onRaftStopped(Raft raft)
    {
        actor.run(() ->
        {
            final LogStream logStream = raft.getLogStream();
            topology.removePartitionForMember(logStream.getPartitionId(), logStream.getTopicName(), topology.getLocal());
            publishLocalPartitions();
        });
    }

    private class LocalRaftStateChangeListener implements RaftStateListener
    {
        @Override
        public void onStateChange(int partitionId, DirectBuffer topicName, SocketAddress socketAddress, RaftState raftState)
        {
            actor.run(() ->
            {
                final MemberInfo memberInfo = topology.getMemberByAddress(socketAddress);
                topology.updatePartition(partitionId, topicName, memberInfo, raftState);
                publishLocalPartitions();
            });
        }
    }

    private class ContactPointsChangeListener implements GossipCustomEventListener
    {
        @Override
        public void onEvent(SocketAddress sender, DirectBuffer payload)
        {
            final SocketAddress senderCopy = new SocketAddress(sender);
            final DirectBuffer payloadCopy = BufferUtil.cloneBuffer(payload);

            actor.run(() ->
            {
                LOG.trace("Received API event from member {}.", senderCopy);

                int offset = 0;

                final SocketAddress managementApi = new SocketAddress();
                offset = readSocketAddress(offset, payloadCopy, managementApi);

                final SocketAddress clientApi = new SocketAddress();
                offset = readSocketAddress(offset, payloadCopy, clientApi);

                final SocketAddress replicationApi = new SocketAddress();
                readSocketAddress(offset, payloadCopy, replicationApi);

                final MemberInfo newMember = new MemberInfo(clientApi, managementApi, replicationApi);
                topology.addMember(newMember);
                notifyMemberAdded(newMember);
            });
        }
    }

    private class MembershipListener implements GossipMembershipListener
    {
        @Override
        public void onAdd(Member member)
        {
            // noop; we listen on the availability of contact points, see ContactPointsChangeListener
        }

        @Override
        public void onRemove(Member member)
        {
            final MemberInfo topologyMember = topology.getMemberByAddress(member.getAddress());
            if (topologyMember != null)
            {
                topology.removeMember(topologyMember);
                notifyMemberRemoved(topologyMember);
            }
        }
    }

    private class ParitionChangeListener implements GossipCustomEventListener
    {
        @Override
        public void onEvent(SocketAddress sender, DirectBuffer payload)
        {
            final SocketAddress senderCopy = new SocketAddress(sender);
            final DirectBuffer payloadCopy = BufferUtil.cloneBuffer(payload);

            actor.run(() ->
            {
                LOG.trace("Received raft state change event for member {}", senderCopy);

                final MemberInfo member = topology.getMemberByAddress(senderCopy);

                if (member != null)
                {
                    readPartitions(payloadCopy, 0, member, topology);
                }
                else
                {
                    LOG.trace("Received raft state change event for unknown member {}", senderCopy);
                }
            });
        }
    }

    private class KnownContactPointsSyncHandler implements GossipSyncRequestHandler
    {
        private final ExpandableArrayBuffer writeBuffer = new ExpandableArrayBuffer();

        @Override
        public ActorFuture<Void> onSyncRequest(GossipSyncRequest request)
        {
            return actor.call(() ->
            {
                LOG.trace("Got API sync request");

                for (MemberInfo member : topology.getMembers())
                {
                    final int length = writeSockedAddresses(member, writeBuffer, 0);
                    request.addPayload(member.getApiPort(), writeBuffer, 0, length);
                }

                LOG.trace("Send API sync response.");
            });
        }
    }

    private class KnownPartitionsSyncHandler implements GossipSyncRequestHandler
    {
        private final ExpandableArrayBuffer writeBuffer = new ExpandableArrayBuffer();

        @Override
        public ActorFuture<Void> onSyncRequest(GossipSyncRequest request)
        {
            return actor.call(() ->
            {
                LOG.trace("Got RAFT state sync request.");

                for (MemberInfo member : topology.getMembers())
                {
                    final int length = writeTopology(topology, writeBuffer, 0);
                    request.addPayload(member.getApiPort(), writeBuffer, 0, length);
                }

                LOG.trace("Send RAFT state sync response.");
            });
        }
    }

    private void publishLocalContactPoints()
    {
        final MutableDirectBuffer eventBuffer = new ExpandableArrayBuffer();
        final int eventLength = writeSockedAddresses(topology.getLocal(), eventBuffer, 0);

        gossip.publishEvent(CONTACT_POINTS_EVENT_TYPE, eventBuffer, 0, eventLength);
    }

    private void publishLocalPartitions()
    {
        final MutableDirectBuffer eventBuffer = new ExpandableArrayBuffer();
        final int length = writePartitions(topology.getLocal(), eventBuffer, 0);

        gossip.publishEvent(PARTITIONS_EVENT_TYPE, eventBuffer, 0, length);
    }

    public ActorFuture<Void> close()
    {
        return actor.close();
    }

    public ActorFuture<TopologyDto> getTopologyDto()
    {
        return actor.call(() ->
        {
            return topology.asDto();
        });
    }

    public void addTopologyMemberListener(TopologyMemberListener listener)
    {
        actor.run(() ->
        {
            topologyMemberListers.add(listener);

            // notify initially
            topology.getMembers().forEach((m) ->
            {
                LogUtil.catchAndLog(LOG, () -> listener.onMemberAdded(m, topology));
            });
        });
    }

    public void removeTopologyMemberListener(TopologyMemberListener listener)
    {
        actor.run(() ->
        {
            topologyMemberListers.remove(listener);
        });
    }

    private void notifyMemberAdded(MemberInfo memberInfo)
    {
        for (TopologyMemberListener listener : topologyMemberListers)
        {
            LogUtil.catchAndLog(LOG, () -> listener.onMemberAdded(memberInfo, topology));
        }
    }

    private void notifyMemberRemoved(MemberInfo memberInfo)
    {
        for (TopologyMemberListener listener : topologyMemberListers)
        {
            LogUtil.catchAndLog(LOG, () -> listener.onMemberRemoved(memberInfo, topology));
        }
    }

    public interface TopologyMemberListener
    {
        void onMemberAdded(MemberInfo memberInfo, Topology topology);
        void onMemberRemoved(MemberInfo memberInfo, Topology topology);
    }
}
