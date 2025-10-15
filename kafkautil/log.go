package kafkautil

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// See: kafka/generated_errors.go

const DefaultErrLogLevel = zerolog.ErrorLevel

var LogLevelByErrCode = map[kafka.ErrorCode]zerolog.Level{
	//kafka.ErrBadMsg:                             DefaultErrLogLevel,
	//kafka.ErrBadCompression:                     DefaultErrLogLevel,
	//kafka.ErrDestroy:                            DefaultErrLogLevel,
	//kafka.ErrFail:                               DefaultErrLogLevel,
	//kafka.ErrTransport:                          DefaultErrLogLevel,
	//kafka.ErrCritSysResource:                    DefaultErrLogLevel,
	//kafka.ErrResolve:                            DefaultErrLogLevel,
	//kafka.ErrMsgTimedOut:                        DefaultErrLogLevel,
	//kafka.ErrPartitionEOF:                       DefaultErrLogLevel,
	//kafka.ErrUnknownPartition:                   DefaultErrLogLevel,
	//kafka.ErrFs:                                 DefaultErrLogLevel,
	//kafka.ErrUnknownTopic:                       DefaultErrLogLevel,
	kafka.ErrAllBrokersDown: zerolog.InfoLevel,
	//kafka.ErrInvalidArg:                         DefaultErrLogLevel,
	//kafka.ErrTimedOut:                           DefaultErrLogLevel,
	//kafka.ErrQueueFull:                          DefaultErrLogLevel,
	//kafka.ErrIsrInsuff:                          DefaultErrLogLevel,
	//kafka.ErrNodeUpdate:                         DefaultErrLogLevel,
	//kafka.ErrSsl:                                DefaultErrLogLevel,
	//kafka.ErrWaitCoord:                          DefaultErrLogLevel,
	//kafka.ErrUnknownGroup:                       DefaultErrLogLevel,
	//kafka.ErrInProgress:                         DefaultErrLogLevel,
	//kafka.ErrPrevInProgress:                     DefaultErrLogLevel,
	//kafka.ErrExistingSubscription:               DefaultErrLogLevel,
	//kafka.ErrAssignPartitions:                   DefaultErrLogLevel,
	//kafka.ErrRevokePartitions:                   DefaultErrLogLevel,
	//kafka.ErrConflict:                           DefaultErrLogLevel,
	//kafka.ErrState:                              DefaultErrLogLevel,
	//kafka.ErrUnknownProtocol:                    DefaultErrLogLevel,
	//kafka.ErrNotImplemented:                     DefaultErrLogLevel,
	//kafka.ErrAuthentication:                     DefaultErrLogLevel,
	//kafka.ErrNoOffset:                           DefaultErrLogLevel,
	//kafka.ErrOutdated:                           DefaultErrLogLevel,
	//kafka.ErrTimedOutQueue:                      DefaultErrLogLevel,
	//kafka.ErrUnsupportedFeature:                 DefaultErrLogLevel,
	//kafka.ErrWaitCache:                          DefaultErrLogLevel,
	//kafka.ErrIntr:                               DefaultErrLogLevel,
	//kafka.ErrKeySerialization:                   DefaultErrLogLevel,
	//kafka.ErrValueSerialization:                 DefaultErrLogLevel,
	//kafka.ErrKeyDeserialization:                 DefaultErrLogLevel,
	//kafka.ErrValueDeserialization:               DefaultErrLogLevel,
	//kafka.ErrPartial:                            DefaultErrLogLevel,
	//kafka.ErrReadOnly:                           DefaultErrLogLevel,
	//kafka.ErrNoent:                              DefaultErrLogLevel,
	//kafka.ErrUnderflow:                          DefaultErrLogLevel,
	//kafka.ErrInvalidType:                        DefaultErrLogLevel,
	//kafka.ErrRetry:                              DefaultErrLogLevel,
	//kafka.ErrPurgeQueue:                         DefaultErrLogLevel,
	//kafka.ErrPurgeInflight:                      DefaultErrLogLevel,
	//kafka.ErrFatal:                              DefaultErrLogLevel,
	//kafka.ErrInconsistent:                       DefaultErrLogLevel,
	//kafka.ErrGaplessGuarantee:                   DefaultErrLogLevel,
	//kafka.ErrMaxPollExceeded:                    DefaultErrLogLevel,
	//kafka.ErrUnknownBroker:                      DefaultErrLogLevel,
	//kafka.ErrNotConfigured:                      DefaultErrLogLevel,
	//kafka.ErrFenced:                             DefaultErrLogLevel,
	//kafka.ErrApplication:                        DefaultErrLogLevel,
	//kafka.ErrAssignmentLost:                     DefaultErrLogLevel,
	//kafka.ErrNoop:                               DefaultErrLogLevel,
	//kafka.ErrAutoOffsetReset:                    DefaultErrLogLevel,
	//kafka.ErrLogTruncation:                      DefaultErrLogLevel,
	//kafka.ErrInvalidDifferentRecord:             DefaultErrLogLevel,
	//kafka.ErrUnknown:                            DefaultErrLogLevel,
	//kafka.ErrNoError:                            DefaultErrLogLevel,
	//kafka.ErrOffsetOutOfRange:                   DefaultErrLogLevel,
	//kafka.ErrInvalidMsg:                         DefaultErrLogLevel,
	//kafka.ErrUnknownTopicOrPart:                 DefaultErrLogLevel,
	//kafka.ErrInvalidMsgSize:                     DefaultErrLogLevel,
	//kafka.ErrLeaderNotAvailable:                 DefaultErrLogLevel,
	//kafka.ErrNotLeaderForPartition:              DefaultErrLogLevel,
	//kafka.ErrRequestTimedOut:                    DefaultErrLogLevel,
	//kafka.ErrBrokerNotAvailable:                 DefaultErrLogLevel,
	//kafka.ErrReplicaNotAvailable:                DefaultErrLogLevel,
	//kafka.ErrMsgSizeTooLarge:                    DefaultErrLogLevel,
	//kafka.ErrStaleCtrlEpoch:                     DefaultErrLogLevel,
	//kafka.ErrOffsetMetadataTooLarge:             DefaultErrLogLevel,
	//kafka.ErrNetworkException:                   DefaultErrLogLevel,
	//kafka.ErrCoordinatorLoadInProgress:          DefaultErrLogLevel,
	//kafka.ErrCoordinatorNotAvailable:            DefaultErrLogLevel,
	//kafka.ErrNotCoordinator:                     DefaultErrLogLevel,
	//kafka.ErrTopicException:                     DefaultErrLogLevel,
	//kafka.ErrRecordListTooLarge:                 DefaultErrLogLevel,
	//kafka.ErrNotEnoughReplicas:                  DefaultErrLogLevel,
	//kafka.ErrNotEnoughReplicasAfterAppend:       DefaultErrLogLevel,
	//kafka.ErrInvalidRequiredAcks:                DefaultErrLogLevel,
	//kafka.ErrIllegalGeneration:                  DefaultErrLogLevel,
	//kafka.ErrInconsistentGroupProtocol:          DefaultErrLogLevel,
	//kafka.ErrInvalidGroupID:                     DefaultErrLogLevel,
	//kafka.ErrUnknownMemberID:                    DefaultErrLogLevel,
	//kafka.ErrInvalidSessionTimeout:              DefaultErrLogLevel,
	//kafka.ErrRebalanceInProgress:                DefaultErrLogLevel,
	//kafka.ErrInvalidCommitOffsetSize:            DefaultErrLogLevel,
	//kafka.ErrTopicAuthorizationFailed:           DefaultErrLogLevel,
	//kafka.ErrGroupAuthorizationFailed:           DefaultErrLogLevel,
	//kafka.ErrClusterAuthorizationFailed:         DefaultErrLogLevel,
	//kafka.ErrInvalidTimestamp:                   DefaultErrLogLevel,
	//kafka.ErrUnsupportedSaslMechanism:           DefaultErrLogLevel,
	//kafka.ErrIllegalSaslState:                   DefaultErrLogLevel,
	//kafka.ErrUnsupportedVersion:                 DefaultErrLogLevel,
	//kafka.ErrTopicAlreadyExists:                 DefaultErrLogLevel,
	//kafka.ErrInvalidPartitions:                  DefaultErrLogLevel,
	//kafka.ErrInvalidReplicationFactor:           DefaultErrLogLevel,
	//kafka.ErrInvalidReplicaAssignment:           DefaultErrLogLevel,
	//kafka.ErrInvalidConfig:                      DefaultErrLogLevel,
	//kafka.ErrNotController:                      DefaultErrLogLevel,
	//kafka.ErrInvalidRequest:                     DefaultErrLogLevel,
	//kafka.ErrUnsupportedForMessageFormat:        DefaultErrLogLevel,
	//kafka.ErrPolicyViolation:                    DefaultErrLogLevel,
	//kafka.ErrOutOfOrderSequenceNumber:           DefaultErrLogLevel,
	//kafka.ErrDuplicateSequenceNumber:            DefaultErrLogLevel,
	//kafka.ErrInvalidProducerEpoch:               DefaultErrLogLevel,
	//kafka.ErrInvalidTxnState:                    DefaultErrLogLevel,
	//kafka.ErrInvalidProducerIDMapping:           DefaultErrLogLevel,
	//kafka.ErrInvalidTransactionTimeout:          DefaultErrLogLevel,
	//kafka.ErrConcurrentTransactions:             DefaultErrLogLevel,
	//kafka.ErrTransactionCoordinatorFenced:       DefaultErrLogLevel,
	//kafka.ErrTransactionalIDAuthorizationFailed: DefaultErrLogLevel,
	//kafka.ErrSecurityDisabled:                   DefaultErrLogLevel,
	//kafka.ErrOperationNotAttempted:              DefaultErrLogLevel,
	//kafka.ErrKafkaStorageError:                  DefaultErrLogLevel,
	//kafka.ErrLogDirNotFound:                     DefaultErrLogLevel,
	//kafka.ErrSaslAuthenticationFailed:           DefaultErrLogLevel,
	//kafka.ErrUnknownProducerID:                  DefaultErrLogLevel,
	//kafka.ErrReassignmentInProgress:             DefaultErrLogLevel,
	//kafka.ErrDelegationTokenAuthDisabled:        DefaultErrLogLevel,
	//kafka.ErrDelegationTokenNotFound:            DefaultErrLogLevel,
	//kafka.ErrDelegationTokenOwnerMismatch:       DefaultErrLogLevel,
	//kafka.ErrDelegationTokenRequestNotAllowed:   DefaultErrLogLevel,
	//kafka.ErrDelegationTokenAuthorizationFailed: DefaultErrLogLevel,
	//kafka.ErrDelegationTokenExpired:             DefaultErrLogLevel,
	//kafka.ErrInvalidPrincipalType:               DefaultErrLogLevel,
	//kafka.ErrNonEmptyGroup:                      DefaultErrLogLevel,
	//kafka.ErrGroupIDNotFound:                    DefaultErrLogLevel,
	//kafka.ErrFetchSessionIDNotFound:             DefaultErrLogLevel,
	//kafka.ErrInvalidFetchSessionEpoch:           DefaultErrLogLevel,
	//kafka.ErrListenerNotFound:                   DefaultErrLogLevel,
	//kafka.ErrTopicDeletionDisabled:              DefaultErrLogLevel,
	//kafka.ErrFencedLeaderEpoch:                  DefaultErrLogLevel,
	//kafka.ErrUnknownLeaderEpoch:                 DefaultErrLogLevel,
	//kafka.ErrUnsupportedCompressionType:         DefaultErrLogLevel,
	//kafka.ErrStaleBrokerEpoch:                   DefaultErrLogLevel,
	//kafka.ErrOffsetNotAvailable:                 DefaultErrLogLevel,
	//kafka.ErrMemberIDRequired:                   DefaultErrLogLevel,
	//kafka.ErrPreferredLeaderNotAvailable:        DefaultErrLogLevel,
	//kafka.ErrGroupMaxSizeReached:                DefaultErrLogLevel,
	//kafka.ErrFencedInstanceID:                   DefaultErrLogLevel,
	//kafka.ErrEligibleLeadersNotAvailable:        DefaultErrLogLevel,
	//kafka.ErrElectionNotNeeded:                  DefaultErrLogLevel,
	//kafka.ErrNoReassignmentInProgress:           DefaultErrLogLevel,
	//kafka.ErrGroupSubscribedToTopic:             DefaultErrLogLevel,
	//kafka.ErrInvalidRecord:                      DefaultErrLogLevel,
	//kafka.ErrUnstableOffsetCommit:               DefaultErrLogLevel,
	//kafka.ErrThrottlingQuotaExceeded:            DefaultErrLogLevel,
	//kafka.ErrProducerFenced:                     DefaultErrLogLevel,
	//kafka.ErrResourceNotFound:                   DefaultErrLogLevel,
	//kafka.ErrDuplicateResource:                  DefaultErrLogLevel,
	//kafka.ErrUnacceptableCredential:             DefaultErrLogLevel,
	//kafka.ErrInconsistentVoterSet:               DefaultErrLogLevel,
	//kafka.ErrInvalidUpdateVersion:               DefaultErrLogLevel,
	//kafka.ErrFeatureUpdateFailed:                DefaultErrLogLevel,
	//kafka.ErrPrincipalDeserializationFailure:    DefaultErrLogLevel,
	//kafka.ErrUnknownTopicID:                     DefaultErrLogLevel,
	//kafka.ErrFencedMemberEpoch:                  DefaultErrLogLevel,
	//kafka.ErrUnreleasedInstanceID:               DefaultErrLogLevel,
	//kafka.ErrUnsupportedAssignor:                DefaultErrLogLevel,
	//kafka.ErrStaleMemberEpoch:                   DefaultErrLogLevel,
	//kafka.ErrUnknownSubscriptionID:              DefaultErrLogLevel,
	//kafka.ErrTelemetryTooLarge:                  DefaultErrLogLevel,
}

func LogKafkaError(err error) *zerolog.Event {
	var kErr kafka.Error
	var evt *zerolog.Event

	if err == nil {
		evt = log.Trace()
		return evt
	}

	if !errors.As(err, &kErr) {
		return log.Error().Err(err).Str("err-type", fmt.Sprintf("%T", kErr))
	}

	level, ok := LogLevelByErrCode[kErr.Code()]
	if ok {
		evt = log.WithLevel(level)
	} else {
		switch {
		case kErr.IsFatal():
			// Errori fatali → livello Error
			evt = log.Error().Err(kErr)
		case kErr.IsRetriable():
			// Errori retriabili → livello Warn (non bloccano, ma vanno monitorati)
			evt = log.Warn().Err(kErr)
		default:
			evt = log.Info().Err(kErr)
		}
	}

	evt.
		Interface("error", fmt.Sprintf("%v", kErr)).
		Int("code", int(kErr.Code())).
		Str("text", kErr.Code().String())

	//Bool("tx-requires-abort", kErr.TxnRequiresAbort()).
	//Bool("timeout", kErr.IsTimeout()).
	//Bool("retriable", kErr.IsRetriable()).
	//Bool("fatal", kErr.IsFatal()).
	//Int("code", int(kErr.Code())).
	//Str("text", kErr.Code().String()).
	//Interface("error", kErr.Error())

	return evt
}

func IsKafkaError(err error) bool {
	var kErr kafka.Error
	if errors.As(err, &kErr) {
		return true
	}

	return false
}

func KafkaErrorRequiresAbort(err error, OnNonKafkaErrors bool) bool {
	if err == nil {
		return false
	}

	var kErr kafka.Error
	if errors.As(err, &kErr) {
		// return kErr.TxnRequiresAbort()
		return true
	} else {
		return OnNonKafkaErrors
	}

}

func IsKafkaErrorState(err error) bool {
	if err == nil {
		return false
	}

	var kErr kafka.Error
	if errors.As(err, &kErr) {
		if kErr.Code() == kafka.ErrState {
			return true
		}
	}

	return false
}

func IsKafkaErrorFatal(err error) bool {
	if err == nil {
		return false
	}

	var kErr kafka.Error
	if errors.As(err, &kErr) {
		return kErr.IsFatal()
	}

	return false
}
