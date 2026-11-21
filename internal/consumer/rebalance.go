// Package: consumer
// File: rebalance.go
// Core Work: Consumer group partition assignment strategies, heartbeat liveness monitoring, and rebalancing
// Exported: AssignmentStrategy, StrategyRange, StrategyRoundRobin, MemberMetadata, PartitionTarget, GroupCoordinator, GroupCoordinatorConfig, NewGroupCoordinator, ErrConsumerNotFound, ErrGroupEmpty
package consumer

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	// ErrConsumerNotFound indicates a heartbeat or query for an unregistered member ID.
	ErrConsumerNotFound = errors.New("walite: consumer member not found in group")

	// ErrGroupEmpty is returned when a rebalance occurs with zero active members.
	ErrGroupEmpty = errors.New("walite: consumer group has no active members")
)

// AssignmentStrategy defines the partition balancing strategy among group consumers.
type AssignmentStrategy int

const (
	// StrategyRange divides topic partitions into contiguous numeric chunks per consumer.
	StrategyRange AssignmentStrategy = iota

	// StrategyRoundRobin distributes partitions across topics in alternating round-robin order.
	StrategyRoundRobin
)

// String returns human-readable identifier for assignment strategy.
func (s AssignmentStrategy) String() string {
	switch s {
	case StrategyRange:
		return "range"
	case StrategyRoundRobin:
		return "round-robin"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// PartitionTarget identifies a specific topic partition.
type PartitionTarget struct {
	Topic       string
	PartitionID uint32
}

// MemberMetadata contains consumer registration info and liveness heartbeat timestamp.
type MemberMetadata struct {
	MemberID string
	Topics   []string
	LastSeen time.Time
}

// GroupCoordinatorConfig specifies timing thresholds and partition assignment algorithms.
type GroupCoordinatorConfig struct {
	GroupID        string
	Strategy       AssignmentStrategy
	SessionTimeout time.Duration
}

// GroupCoordinator manages consumer memberships, heartbeat liveliness, and deterministic partition rebalancing.
type GroupCoordinator struct {
	mu          sync.RWMutex
	cfg         GroupCoordinatorConfig
	members     map[string]*MemberMetadata
	assignments map[string][]PartitionTarget
	topics      map[string][]uint32
	generation  uint64
	closed      bool
}

// NewGroupCoordinator initializes a consumer group coordinator.
func NewGroupCoordinator(cfg GroupCoordinatorConfig) *GroupCoordinator {
	if cfg.SessionTimeout <= 0 {
		cfg.SessionTimeout = 10 * time.Second
	}

	return &GroupCoordinator{
		cfg:         cfg,
		members:     make(map[string]*MemberMetadata),
		assignments: make(map[string][]PartitionTarget),
		topics:      make(map[string][]uint32),
	}
}

// RegisterPartitions registers the available partition IDs for a given topic.
func (gc *GroupCoordinator) RegisterPartitions(topic string, partitions []uint32) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	partsCopy := make([]uint32, len(partitions))
	copy(partsCopy, partitions)
	sort.Slice(partsCopy, func(i, j int) bool {
		return partsCopy[i] < partsCopy[j]
	})
	gc.topics[topic] = partsCopy
}

// JoinGroup registers or updates a consumer member's subscriptions and refreshes its heartbeat.
func (gc *GroupCoordinator) JoinGroup(memberID string, topics []string) (uint64, error) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	topicsCopy := make([]string, len(topics))
	copy(topicsCopy, topics)
	sort.Strings(topicsCopy)

	gc.members[memberID] = &MemberMetadata{
		MemberID: memberID,
		Topics:   topicsCopy,
		LastSeen: time.Now(),
	}

	return gc.generation, nil
}

// Heartbeat refreshes the last seen timestamp of a registered consumer.
func (gc *GroupCoordinator) Heartbeat(memberID string) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	member, exists := gc.members[memberID]
	if !exists {
		return ErrConsumerNotFound
	}

	member.LastSeen = time.Now()
	return nil
}

// LeaveGroup unregisters a consumer from the group.
func (gc *GroupCoordinator) LeaveGroup(memberID string) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	if _, exists := gc.members[memberID]; !exists {
		return ErrConsumerNotFound
	}

	delete(gc.members, memberID)
	delete(gc.assignments, memberID)
	return nil
}

// Rebalance prunes expired members and recalculates partition assignments according to the configured strategy.
func (gc *GroupCoordinator) Rebalance() (map[string][]PartitionTarget, uint64, error) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	// 1. Prune expired members
	now := time.Now()
	for id, member := range gc.members {
		if now.Sub(member.LastSeen) > gc.cfg.SessionTimeout {
			delete(gc.members, id)
			delete(gc.assignments, id)
		}
	}

	if len(gc.members) == 0 {
		gc.assignments = make(map[string][]PartitionTarget)
		gc.generation++
		return nil, gc.generation, ErrGroupEmpty
	}

	// 2. Collect and sort active member IDs deterministically
	memberIDs := make([]string, 0, len(gc.members))
	for id := range gc.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	newAssignments := make(map[string][]PartitionTarget)
	for _, id := range memberIDs {
		newAssignments[id] = make([]PartitionTarget, 0)
	}

	// 3. Execute assignment strategy
	switch gc.cfg.Strategy {
	case StrategyRoundRobin:
		gc.assignRoundRobin(memberIDs, newAssignments)
	default: // StrategyRange
		gc.assignRange(memberIDs, newAssignments)
	}

	gc.assignments = newAssignments
	gc.generation++

	// Return defensive copy of assignments
	res := make(map[string][]PartitionTarget, len(newAssignments))
	for k, v := range newAssignments {
		copied := make([]PartitionTarget, len(v))
		copy(copied, v)
		res[k] = copied
	}

	return res, gc.generation, nil
}

func (gc *GroupCoordinator) assignRange(memberIDs []string, assignments map[string][]PartitionTarget) {
	// Group assignment by topic
	for topic, partitions := range gc.topics {
		if len(partitions) == 0 {
			continue
		}

		// Find members subscribed to this topic
		var subscribedMembers []string
		for _, id := range memberIDs {
			member := gc.members[id]
			for _, t := range member.Topics {
				if t == topic {
					subscribedMembers = append(subscribedMembers, id)
					break
				}
			}
		}

		if len(subscribedMembers) == 0 {
			continue
		}

		numParts := len(partitions)
		numMembers := len(subscribedMembers)

		numPartsPerMember := numParts / numMembers
		remainder := numParts % numMembers

		partIdx := 0
		for i, id := range subscribedMembers {
			assignedCount := numPartsPerMember
			if i < remainder {
				assignedCount++
			}

			for c := 0; c < assignedCount && partIdx < numParts; c++ {
				assignments[id] = append(assignments[id], PartitionTarget{
					Topic:       topic,
					PartitionID: partitions[partIdx],
				})
				partIdx++
			}
		}
	}
}

func (gc *GroupCoordinator) assignRoundRobin(memberIDs []string, assignments map[string][]PartitionTarget) {
	// Collect all registered partitions across topics and sort them deterministically
	var allTargets []PartitionTarget
	var sortedTopics []string
	for topic := range gc.topics {
		sortedTopics = append(sortedTopics, topic)
	}
	sort.Strings(sortedTopics)

	for _, topic := range sortedTopics {
		partitions := gc.topics[topic]
		for _, partID := range partitions {
			allTargets = append(allTargets, PartitionTarget{
				Topic:       topic,
				PartitionID: partID,
			})
		}
	}

	if len(allTargets) == 0 || len(memberIDs) == 0 {
		return
	}

	// Round-robin distribution among members subscribed to each partition target
	memberCursor := 0
	for _, target := range allTargets {
		// Find next subscribed member
		for attempts := 0; attempts < len(memberIDs); attempts++ {
			candidateID := memberIDs[memberCursor%len(memberIDs)]
			memberCursor++

			member := gc.members[candidateID]
			subscribed := false
			for _, t := range member.Topics {
				if t == target.Topic {
					subscribed = true
					break
				}
			}

			if subscribed {
				assignments[candidateID] = append(assignments[candidateID], target)
				break
			}
		}
	}
}

// GetAssignments returns the assigned partitions and current generation for a member.
func (gc *GroupCoordinator) GetAssignments(memberID string) ([]PartitionTarget, uint64, error) {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	if _, exists := gc.members[memberID]; !exists {
		return nil, 0, ErrConsumerNotFound
	}

	assigned := gc.assignments[memberID]
	copied := make([]PartitionTarget, len(assigned))
	copy(copied, assigned)

	return copied, gc.generation, nil
}

// ActiveMembers returns a list of all currently active consumer member IDs.
func (gc *GroupCoordinator) ActiveMembers() []string {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	members := make([]string, 0, len(gc.members))
	for id := range gc.members {
		members = append(members, id)
	}
	sort.Strings(members)
	return members
}

// Generation returns the current group generation epoch.
func (gc *GroupCoordinator) Generation() uint64 {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return gc.generation
}
