package io.zeebe.broker.clustering2.base.topology;

import java.util.ArrayList;
import java.util.List;

import io.zeebe.broker.clustering2.base.topology.TopologyDto.BrokerDto;
import io.zeebe.raft.state.RaftState;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.buffer.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;

public class Topology
{
    private final MemberInfo local;

    private final Int2ObjectHashMap<PartitionInfo> partitions = new Int2ObjectHashMap<>();
    private final List<MemberInfo> members = new ArrayList<>();

    private final Int2ObjectHashMap<MemberInfo> partitionLeaders = new Int2ObjectHashMap<>();
    private final Int2ObjectHashMap<List<MemberInfo>> partitionFollowers = new Int2ObjectHashMap<>();

    public Topology(MemberInfo localBroker)
    {
        this.local = localBroker;
        this.addMember(localBroker);
    }

    public MemberInfo getLocal()
    {
        return local;
    }

    public MemberInfo getMemberByAddress(SocketAddress apiAddress)
    {
        MemberInfo member = null;

        for (int i = 0; i < members.size() && member == null; i++)
        {
            final MemberInfo current = members.get(i);

            if (current.getApiPort().equals(apiAddress))
            {
                member = current;
            }
        }

        return member;
    }

    public List<MemberInfo> getMembers()
    {
        return members;
    }

    public PartitionInfo getParition(int partitionId)
    {
        return partitions.get(partitionId);
    }

    public MemberInfo getLeader(int partitionId)
    {
        return partitionLeaders.get(partitionId);
    }

    public List<MemberInfo> getFollowers(int partitionId)
    {
        return partitionFollowers.get(partitionId);
    }

    public void addMember(MemberInfo member)
    {
        // replace member if present
        if (!members.contains(member))
        {
            members.add(member);
        }
    }

    public void removeMember(MemberInfo member)
    {
        for (PartitionInfo partition : member.follower)
        {
            final List<MemberInfo> followers = partitionFollowers.get(partition.getParitionId());

            if (followers != null)
            {
                followers.remove(member);
            }
        }

        for (PartitionInfo partition : member.leader)
        {
            partitionLeaders.remove(partition.getParitionId());
        }

        members.remove(member);
    }

    public void removePartitionForMember(int partitionId, DirectBuffer topicName, MemberInfo memberInfo)
    {
        final PartitionInfo partition = partitions.get(partitionId);
        if (partition == null)
        {
            return;
        }

        memberInfo.leader.remove(partition);
        memberInfo.follower.remove(partition);

        final List<MemberInfo> followers = partitionFollowers.get(partitionId);
        if (followers != null)
        {
            followers.remove(memberInfo);
        }

        final MemberInfo member = partitionLeaders.get(partitionId);
        if (member.equals(memberInfo))
        {
            partitionLeaders.remove(partitionId);
        }
    }

    @SuppressWarnings("incomplete-switch")
    public void updatePartition(int paritionId, DirectBuffer topicName, MemberInfo member, RaftState state)
    {
        List<MemberInfo> followers = partitionFollowers.get(paritionId);

        PartitionInfo partition = partitions.get(paritionId);
        if (partition == null)
        {
            partition = new PartitionInfo(topicName, paritionId);
            partitions.put(paritionId, partition);
        }

        switch (state)
        {
            case LEADER:
                if (followers != null)
                {
                    followers.remove(member);
                }
                partitionLeaders.put(paritionId, member);

                member.follower.remove(partition);

                if (!member.leader.contains(partition))
                {
                    member.leader.add(partition);
                }
                break;

            case FOLLOWER:
                if (member.equals(partitionLeaders.get(paritionId)))
                {
                    partitionLeaders.remove(paritionId);
                }
                if (followers == null)
                {
                    followers = new ArrayList<>();
                    partitionFollowers.put(paritionId, followers);
                }
                if (!followers.contains(member))
                {
                    followers.add(member);
                }

                member.leader.remove(partition);

                if (!member.follower.contains(partition))
                {
                    member.follower.add(partition);
                }
                break;
        }
    }

    public TopologyDto asDto()
    {
        final TopologyDto dto = new TopologyDto();

        for (MemberInfo member : members)
        {
            final BrokerDto broker = dto.brokers().add();
            final SocketAddress apiContactPoint = member.getApiPort();
            broker.setHost(apiContactPoint.getHostBuffer(), 0, apiContactPoint.getHostBuffer().capacity());
            broker.setPort(apiContactPoint.port());

            for (PartitionInfo partition : member.getLeader())
            {
                final DirectBuffer topicName = BufferUtil.cloneBuffer(partition.getTopicName());

                broker.partitionStates()
                    .add()
                    .setPartitionId(partition.getParitionId())
                    .setTopicName(topicName, 0, topicName.capacity())
                    .setState(RaftState.LEADER);
            }

            for (PartitionInfo partition : member.getFollower())
            {
                final DirectBuffer topicName = BufferUtil.cloneBuffer(partition.getTopicName());

                broker.partitionStates()
                    .add()
                    .setPartitionId(partition.getParitionId())
                    .setTopicName(topicName, 0, topicName.capacity())
                    .setState(RaftState.LEADER);
            }
        }

        return dto;
    }

    public static class MemberInfo
    {
        private final SocketAddress apiPort;
        private final SocketAddress managementPort;
        private final SocketAddress replicationPort;

        private final List<PartitionInfo> leader = new ArrayList<>();
        private final List<PartitionInfo> follower = new ArrayList<>();

        public MemberInfo(SocketAddress apiPort,
            SocketAddress managementPort,
            SocketAddress replicationPort)
        {
            this.apiPort = apiPort;
            this.managementPort = managementPort;
            this.replicationPort = replicationPort;
        }

        public SocketAddress getApiPort()
        {
            return apiPort;
        }

        public SocketAddress getManagementPort()
        {
            return managementPort;
        }

        public SocketAddress getReplicationPort()
        {
            return replicationPort;
        }

        public List<PartitionInfo> getLeader()
        {
            return leader;
        }

        public List<PartitionInfo> getFollower()
        {
            return follower;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((apiPort == null) ? 0 : apiPort.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (getClass() != obj.getClass())
            {
                return false;
            }
            final MemberInfo other = (MemberInfo) obj;
            if (apiPort == null)
            {
                if (other.apiPort != null)
                {
                    return false;
                }
            }
            else if (!apiPort.equals(other.apiPort))
            {
                return false;
            }
            return true;
        }
    }

    public static class PartitionInfo
    {
        private final DirectBuffer topicName;
        private final int paritionId;

        public PartitionInfo(DirectBuffer topicName, int paritionId)
        {
            this.topicName = topicName;
            this.paritionId = paritionId;
        }

        public DirectBuffer getTopicName()
        {
            return topicName;
        }

        public int getParitionId()
        {
            return paritionId;
        }
    }
}
