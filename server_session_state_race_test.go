package gortsplib

import (
	"bufio"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/conn"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/headers"
)

// a session set up on a connection is played from another connection:
// the reader of the first connection must read the session state under the
// same mutex that protects its writes.
func TestServerSessionStatePlayFromAnotherConn(t *testing.T) {
	var stream *ServerStream

	s := &Server{
		Handler: &testServerHandler{
			onDescribe: func(_ *ServerHandlerOnDescribeCtx) (*base.Response, *ServerStream, error) {
				return &base.Response{StatusCode: base.StatusOK}, stream, nil
			},
			onSetup: func(_ *ServerHandlerOnSetupCtx) (*base.Response, *ServerStream, error) {
				return &base.Response{StatusCode: base.StatusOK}, stream, nil
			},
			onPlay: func(_ *ServerHandlerOnPlayCtx) (*base.Response, error) {
				return &base.Response{StatusCode: base.StatusOK}, nil
			},
		},
		RTSPAddress: "127.0.0.1:0",
	}

	err := s.Start()
	require.NoError(t, err)
	defer s.Close()

	stream = &ServerStream{
		Server: s,
		Desc: &description.Session{Medias: []*description.Media{{
			Type:    description.MediaTypeVideo,
			Formats: []format.Format{&format.H264{PayloadTyp: 96, PacketizationMode: 1}},
		}}},
	}
	err = stream.Initialize()
	require.NoError(t, err)
	defer stream.Close()

	addr := s.NetListener().Addr().String()
	u := mustParseURL("rtsp://" + addr + "/stream")

	// connection A: DESCRIBE and SETUP over TCP, then it stays open
	nconnA, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer nconnA.Close()
	connA := conn.NewConn(bufio.NewReader(nconnA), nconnA)

	res, err := writeReqReadRes(connA, base.Request{
		Method: base.Describe,
		URL:    u,
		Header: base.Header{"CSeq": base.HeaderValue{"1"}},
	})
	require.NoError(t, err)
	require.Equal(t, base.StatusOK, res.StatusCode)

	inTH := &headers.Transport{
		Protocol:       headers.TransportProtocolTCP,
		Delivery:       new(headers.TransportDeliveryUnicast),
		Mode:           new(headers.TransportModePlay),
		InterleavedIDs: &[2]int{0, 1},
	}

	res, err = writeReqReadRes(connA, base.Request{
		Method: base.Setup,
		URL:    mustParseURL("rtsp://" + addr + "/stream/trackID=0"),
		Header: base.Header{
			"CSeq":      base.HeaderValue{"2"},
			"Transport": inTH.Marshal(),
		},
	})
	require.NoError(t, err)
	require.Equal(t, base.StatusOK, res.StatusCode)

	var sx headers.Session
	err = sx.Unmarshal(res.Header["Session"])
	require.NoError(t, err)

	// connection B: PLAY of the session set up by A
	nconnB, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer nconnB.Close()
	connB := conn.NewConn(bufio.NewReader(nconnB), nconnB)

	res, err = writeReqReadRes(connB, base.Request{
		Method: base.Play,
		URL:    u,
		Header: base.Header{
			"CSeq":    base.HeaderValue{"1"},
			"Session": base.HeaderValue{sx.Session},
		},
	})
	require.NoError(t, err)
	require.Equal(t, base.StatusOK, res.StatusCode)
}
