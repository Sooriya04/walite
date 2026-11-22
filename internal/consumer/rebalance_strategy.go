// Package: consumer
// File: rebalance_strategy.go
// Core Work: Consumer partition assignment algorithms (Range and RoundRobin strategies)
// Exported: none (internal methods on GroupCoordinator: assignRange, assignRoundRobin)
package consumer

/*
Functions Manifest & Usage:
- (gc *GroupCoordinator) assignRange(memberIDs []string, assignments map[string][]PartitionTarget): Assigns contiguous topic partition ranges.
- (gc *GroupCoordinator) assignRoundRobin(memberIDs []string, assignments map[string][]PartitionTarget): Evenly interleaves topic partitions across members.
*/

import (
	"sort"
)

func (gc *GroupCoordinator) assignRange(memberIDs []string, assignments map[string][]PartitionTarget) {
	// Collect and sort topics deterministically to prevent randomized map iteration
	var sortedTopics []string
	for topic := range gc.topics {
		sortedTopics = append(sortedTopics, topic)
	}
	sort.Strings(sortedTopics)

	// Group assignment by topic
	for _, topic := range sortedTopics {
		partitions := gc.topics[topic]
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

