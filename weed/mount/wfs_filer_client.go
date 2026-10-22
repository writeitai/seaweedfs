package mount

import (
	"context"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var _ = filer_pb.FilerClient(&WFS{})

// retryable is util.Retry's rule for a filer call's error, except that a
// definite refusal by the gateway is final when RefusalsAreFinal is set
// (Mountable).
func (wfs *WFS) retryable(err error) bool {
	if !util.IsTransientError(err) {
		return false
	}
	if !wfs.option.RefusalsAreFinal {
		return true
	}
	st, ok := util.ServerStatus(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		return true
	}
	for _, refusal := range []string{"quota", "spending_cap", "file too large", "metadata rate"} {
		if strings.Contains(st.Message(), refusal) {
			return false
		}
	}
	return true
}

func (wfs *WFS) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) (err error) {

	return util.RetryOnError("filer grpc", wfs.retryable, func() error {

		i := atomic.LoadInt32(&wfs.option.filerIndex)
		n := len(wfs.option.FilerAddresses)
		for x := 0; x < n; x++ {

			filerGrpcAddress := wfs.option.FilerAddresses[i].ToGrpcAddress()
			err = pb.WithGrpcClient(context.Background(), streamingMode, wfs.signature, func(grpcConnection *grpc.ClientConn) error {
				client := filer_pb.NewSeaweedFilerClient(grpcConnection)
				return fn(client)
			}, filerGrpcAddress, false, wfs.option.GrpcDialOption)

			if err != nil {
				// glog.V(0).Infof("WithFilerClient %d %v: %v", x, filerGrpcAddress, err)
			} else {
				atomic.StoreInt32(&wfs.option.filerIndex, i)
				return nil
			}

			i++
			if i >= int32(n) {
				i = 0
			}

		}
		return err
	})

}

func (wfs *WFS) AdjustedUrl(location *filer_pb.Location) string {
	if wfs.option.VolumeServerAccess == "publicUrl" {
		return location.PublicUrl
	}
	return location.Url
}

func (wfs *WFS) GetDataCenter() string {
	return wfs.option.DataCenter
}
