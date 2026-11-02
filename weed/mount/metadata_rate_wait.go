package mount

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Mountable (writeitai/seaweedfs, branch mountable): mountable.io's gateway
// counts a filesystem's metadata mutations in one-minute windows and refuses
// one over the rate with ResourceExhausted "metadata rate limit exceeded",
// before forwarding it. That refusal is definite and changed nothing, so a
// FUSE operation sends the mutation again once the rate allows instead of
// failing: work at the rate is slower, never lost. Only an interrupt of
// the waiting request ends the wait, with EINTR. Other refusals, the
// spending caps among them, fail as before. See filesystem-limits §4 and
// macos-mount §7 in writeitai/fsit; the macOS NFS adapter answers the same
// refusal with JUKEBOX and leaves the waiting to the NFS client.

// metadataRateMessage is the gateway's message for the refusal.
const metadataRateMessage = "metadata rate limit exceeded"

// Backoff between sends while the rate refuses them: the window is a
// minute, so the waits grow to a few seconds and no further.
var (
	metadataRateFirstWait = time.Second
	metadataRateMaxWait   = 8 * time.Second
)

// errMetadataRateInterrupted ends a wait the request's interrupt cut short;
// it is EINTR to errors.As.
var errMetadataRateInterrupted = fmt.Errorf("interrupted while waiting at the metadata rate: %w", syscall.EINTR)

type metadataRateWaitKey struct{}

// waitingAtMetadataRate marks ctx: a mutation sent under it through
// atMetadataRate waits out the metadata rate, until interrupt closes (nil:
// until ctx ends).
func waitingAtMetadataRate(ctx context.Context, interrupt <-chan struct{}) context.Context {
	if interrupt == nil {
		interrupt = make(chan struct{})
	}
	return context.WithValue(ctx, metadataRateWaitKey{}, interrupt)
}

// metadataRateRefused reports whether err is the gateway's metadata-rate
// refusal, as a gRPC status or as a stream mutation's error response.
func metadataRateRefused(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := util.ServerStatus(err); ok {
		return st.Code() == codes.ResourceExhausted && strings.Contains(st.Message(), metadataRateMessage)
	}
	var streamErr *streamMutateError
	if errors.As(err, &streamErr) {
		return strings.Contains(streamErr.msg, metadataRateMessage)
	}
	return false
}

// diskFullMessage is in the gateway's refusal of a create or rename while
// the cell's disk is nearly full ("disk full: no writable volumes"), and
// diskFullAssignment is the chunk assignment's whole error for it, which
// ends the assignment's failure text.
const (
	diskFullMessage    = "no writable volumes"
	diskFullAssignment = "disk full: no writable volumes"
)

// diskFullRefused reports whether err is that refusal, as a gRPC status or
// as a stream mutation's error response. It is ENOSPC, at once: neither
// waited out nor retried (Mountable).
func diskFullRefused(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := util.ServerStatus(err); ok {
		return st.Code() == codes.ResourceExhausted && strings.Contains(st.Message(), diskFullMessage)
	}
	var streamErr *streamMutateError
	if errors.As(err, &streamErr) {
		return strings.Contains(streamErr.msg, diskFullMessage)
	}
	return false
}

// metadataRateWaits reports whether ctx was marked by waitingAtMetadataRate,
// and the interrupt that ends its waits.
func metadataRateWaits(ctx context.Context) (<-chan struct{}, bool) {
	interrupt, ok := ctx.Value(metadataRateWaitKey{}).(<-chan struct{})
	return interrupt, ok
}

// atMetadataRate sends a mutation: once under an unmarked ctx, and under a
// marked one again after a backoff for as long as the gateway refuses it
// for the metadata rate. A wait the interrupt or ctx ends returns
// errMetadataRateInterrupted.
func atMetadataRate(ctx context.Context, what string, send func() error) error {
	err := send()
	interrupt, waits := metadataRateWaits(ctx)
	if !waits || !metadataRateRefused(err) {
		return err
	}
	glog.V(0).Infof("%s: waiting at the filesystem's metadata rate", what)
	started := time.Now()
	wait := metadataRateFirstWait
	for metadataRateRefused(err) {
		if !sleepAtMetadataRate(ctx, interrupt, wait) {
			glog.V(0).Infof("%s: interrupted after %v at the metadata rate", what, time.Since(started).Round(time.Second))
			return errMetadataRateInterrupted
		}
		if wait *= 2; wait > metadataRateMaxWait {
			wait = metadataRateMaxWait
		}
		err = send()
	}
	glog.V(0).Infof("%s: sent after %v at the metadata rate", what, time.Since(started).Round(time.Second))
	return err
}

// sleepAtMetadataRate waits about d, a little jittered so mutations
// refused together do not come back together; false if interrupted.
func sleepAtMetadataRate(ctx context.Context, interrupt <-chan struct{}, d time.Duration) bool {
	d += time.Duration(rand.Int64N(int64(d)/5 + 1))
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-interrupt:
	case <-ctx.Done():
	}
	return false
}
