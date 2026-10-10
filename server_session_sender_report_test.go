package gortsplib

import (
	"bufio"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/conn"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/headers"
)

type senderReportTestSession struct {
	s      *Server
	stream *ServerStream
	ss     *ServerSession
	conn   *conn.Conn
	u      *base.URL
	sx     string
	cseq   int
}

func (st *senderReportTestSession) request(t *testing.T, method base.Method) {
	st.cseq++
	res, err := writeReqReadRes(st.conn, base.Request{
		Method: method,
		URL:    st.u,
		Header: base.Header{
			"CSeq":    base.HeaderValue{strconv.Itoa(st.cseq)},
			"Session": base.HeaderValue{st.sx},
		},
	})
	require.NoError(t, err)
	require.Equal(t, base.StatusOK, res.StatusCode)
}

// holdWriter pushes into the session's write queue a function that blocks
// the writer until the returned function is called with the error
// the function must return. It returns once the writer runs the function.
func (st *senderReportTestSession) holdWriter(t *testing.T) func(error) {
	running := make(chan struct{})
	release := make(chan error, 1)
	st.push(t, func() error {
		close(running)
		return <-release
	})
	t.Cleanup(func() {
		select {
		case release <- nil:
		default:
		}
	})

	select {
	case <-running:
	case <-time.After(10 * time.Second):
		t.Fatal("writer did not start")
	}

	return func(err error) { release <- err }
}

func (st *senderReportTestSession) push(t *testing.T, cb func() error) {
	st.ss.writerMutex.RLock()
	defer st.ss.writerMutex.RUnlock()
	require.True(t, st.ss.writer.Push(cb))
}

// newSenderReportTestSession sets up and plays a TCP session with one media,
// whose sender reports fall due every period.
func newSenderReportTestSession(t *testing.T, period time.Duration) *senderReportTestSession {
	st := &senderReportTestSession{}
	sessions := make(chan *ServerSession, 1)

	st.s = &Server{
		Handler: &testServerHandler{
			onDescribe: func(_ *ServerHandlerOnDescribeCtx) (*base.Response, *ServerStream, error) {
				return &base.Response{StatusCode: base.StatusOK}, st.stream, nil
			},
			onSetup: func(_ *ServerHandlerOnSetupCtx) (*base.Response, *ServerStream, error) {
				return &base.Response{StatusCode: base.StatusOK}, st.stream, nil
			},
			onPlay: func(ctx *ServerHandlerOnPlayCtx) (*base.Response, error) {
				select {
				case sessions <- ctx.Session:
				default:
				}
				return &base.Response{StatusCode: base.StatusOK}, nil
			},
			onPause: func(_ *ServerHandlerOnPauseCtx) (*base.Response, error) {
				return &base.Response{StatusCode: base.StatusOK}, nil
			},
		},
		RTSPAddress:        "127.0.0.1:0",
		SenderReportPeriod: period,
	}
	err := st.s.Start()
	require.NoError(t, err)
	t.Cleanup(st.s.Close)

	st.stream = &ServerStream{
		Server: st.s,
		Desc: &description.Session{Medias: []*description.Media{{
			Type:    description.MediaTypeVideo,
			Formats: []format.Format{&format.H264{PayloadTyp: 96, PacketizationMode: 1}},
		}}},
	}
	err = st.stream.Initialize()
	require.NoError(t, err)
	t.Cleanup(st.stream.Close)

	addr := st.s.NetListener().Addr().String()
	st.u = mustParseURL("rtsp://" + addr + "/stream")

	nconn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { nconn.Close() })
	st.conn = conn.NewConn(bufio.NewReader(nconn), nconn)

	inTH := &headers.Transport{
		Protocol:       headers.TransportProtocolTCP,
		Delivery:       new(headers.TransportDeliveryUnicast),
		Mode:           new(headers.TransportModePlay),
		InterleavedIDs: &[2]int{0, 1},
	}

	res, err := writeReqReadRes(st.conn, base.Request{
		Method: base.Setup,
		URL:    mustParseURL("rtsp://" + addr + "/stream/trackID=0"),
		Header: base.Header{
			"CSeq":      base.HeaderValue{"1"},
			"Transport": inTH.Marshal(),
		},
	})
	require.NoError(t, err)
	require.Equal(t, base.StatusOK, res.StatusCode)
	st.cseq = 1

	var sx headers.Session
	err = sx.Unmarshal(res.Header["Session"])
	require.NoError(t, err)
	st.sx = sx.Session

	st.request(t, base.Play)
	st.ss = <-sessions

	return st
}

// instrument counts the sender reports that fall due and the RTCP packets
// the writer writes. It must be called before the first RTP packet,
// that starts the sender report routine.
func (st *senderReportTestSession) instrument() (chan struct{}, *atomic.Int64) {
	sm := st.ss.setuppedMedias[st.stream.Desc.Medias[0]]
	sf := sm.formats[96]

	due := make(chan struct{}, 64)
	origWrite := sf.rtpSender.WritePacketRTCP
	sf.rtpSender.WritePacketRTCP = func(pkt rtcp.Packet) {
		origWrite(pkt)
		select {
		case due <- struct{}{}:
		default:
		}
	}

	var written atomic.Int64
	origInQueue := sm.writePacketRTCPInQueue
	sm.writePacketRTCPInQueue = func(payload []byte) error {
		written.Add(1)
		return origInQueue(payload)
	}

	return due, &written
}

func waitDue(t *testing.T, due chan struct{}, n int) {
	for range n {
		select {
		case <-due:
		case <-time.After(10 * time.Second):
			t.Fatal("sender report did not fall due")
		}
	}
}

// waitNextDue discards the reports that fell due so far and waits
// for a report that falls due after the call. Reports fall due one at a time
// on the sender's goroutine, so at most one of them can have started before
// the channel was drained, and seen an earlier state of the session
// (a writer that a PAUSE removed, a report still pending): the second
// notification after the drain comes from a report that started after it.
func waitNextDue(t *testing.T, due chan struct{}) {
	for {
		select {
		case <-due:
			continue
		default:
		}
		break
	}
	waitDue(t, due, 2)
}

// writeUntilMarker queues a marker behind everything already in the write
// queue, releases the writer and returns the number of RTCP packets written
// when the writer reached the marker: those that were queued before it.
// The marker takes the count itself, since after it the writer can go on
// writing a report that was queued behind it.
func (st *senderReportTestSession) writeUntilMarker(
	t *testing.T, release func(error), written *atomic.Int64,
) int64 {
	marker := make(chan int64, 1)
	st.push(t, func() error {
		marker <- written.Load()
		return nil
	})
	release(nil)

	select {
	case n := <-marker:
		return n
	case <-time.After(10 * time.Second):
		t.Fatal("writer did not reach the marker")
	}
	return 0
}

// a playing session whose writer is held must keep at most one pending
// sender report per format, however many periods pass.
func TestServerSessionSenderReportPendingBound(t *testing.T) {
	st := newSenderReportTestSession(t, 20*time.Millisecond)
	medi := st.stream.Desc.Medias[0]

	release := st.holdWriter(t)
	due, written := st.instrument()

	err := st.ss.WritePacketRTP(medi, &rtp.Packet{
		Header: rtp.Header{
			Version:     2,
			PayloadType: 96,
			SSRC:        0x38F27A2F,
		},
		Payload: []byte{0x65, 1, 2, 3}, // IDR
	})
	require.NoError(t, err)

	// the first report and 4 periods, all with the writer held
	waitDue(t, due, 5)

	// every report that fell due is counted, the skipped ones too
	require.Eventually(t, func() bool {
		return st.ss.Stats().Medias[medi].Formats[medi.Formats[0]].OutboundRTCPSenderReportsGenerated >= 5
	}, 10*time.Second, time.Millisecond)

	n := st.writeUntilMarker(t, release, written)
	t.Logf("sender reports queued behind the held writer: %d", n)
	require.Equal(t, int64(1), n)

	// once written, the next report is queued again
	waitNextDue(t, due)
	require.GreaterOrEqual(t, st.writeUntilMarker(t, func(error) {}, written), int64(2))
}

// waitWriterClosing waits until a goroutine is parked inside the Close()
// of a write queue, waiting for its writer to exit: by then the queue
// is closed and its pending entries are discarded.
func waitWriterClosing(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	buf := make([]byte, 1<<20)

	for {
		n := runtime.Stack(buf, true)
		for g := range strings.SplitSeq(string(buf[:n]), "\n\n") {
			if strings.Contains(g, "[chan receive") &&
				strings.Contains(g, "asyncprocessor.(*Processor).Close(") {
				return
			}
		}

		if time.Now().After(deadline) {
			t.Fatal("write queue was not closed")
		}
		runtime.Gosched()
	}
}

// a pending sender report discarded with the write queue of a PAUSE
// must not prevent the reports after the next PLAY.
func TestServerSessionSenderReportPendingAfterPause(t *testing.T) {
	st := newSenderReportTestSession(t, 20*time.Millisecond)
	medi := st.stream.Desc.Medias[0]

	release := st.holdWriter(t)
	due, written := st.instrument()

	err := st.ss.WritePacketRTP(medi, &rtp.Packet{
		Header: rtp.Header{
			Version:     2,
			PayloadType: 96,
			SSRC:        0x38F27A2F,
		},
		Payload: []byte{0x65, 1, 2, 3}, // IDR
	})
	require.NoError(t, err)

	// a report is queued behind the held writer
	waitDue(t, due, 1)

	// PAUSE closes the write queue, discarding the queued report.
	// The held function then returns an error, that stops the writer
	// without pulling anything else and without closing the session,
	// since the queue is already closed.
	st.cseq++
	err = st.conn.WriteRequest(&base.Request{
		Method: base.Pause,
		URL:    st.u,
		Header: base.Header{
			"CSeq":    base.HeaderValue{strconv.Itoa(st.cseq)},
			"Session": base.HeaderValue{st.sx},
		},
	})
	require.NoError(t, err)
	waitWriterClosing(t)
	release(fmt.Errorf("writer closed while held"))

	res, err := st.conn.ReadResponse()
	require.NoError(t, err)
	require.Equal(t, base.StatusOK, res.StatusCode)
	require.Zero(t, written.Load())

	st.request(t, base.Play)

	// the reports of the new write queue are written
	waitNextDue(t, due)
	require.NotZero(t, st.writeUntilMarker(t, func(error) {}, written))
}

// the public sender report period is the one the senders get, zero keeps
// the default, and a period set on the private field (by a test) is kept.
func TestServerSenderReportPeriod(t *testing.T) {
	for _, ca := range []struct {
		name    string
		set     time.Duration
		private time.Duration
		want    time.Duration
	}{
		{"default", 0, 0, 10 * time.Second},
		{"set", 20 * time.Millisecond, 0, 20 * time.Millisecond},
		{"private", 0, 30 * time.Millisecond, 30 * time.Millisecond},
	} {
		t.Run(ca.name, func(t *testing.T) {
			s := &Server{
				RTSPAddress:        "127.0.0.1:0",
				SenderReportPeriod: ca.set,
				senderReportPeriod: ca.private,
			}
			err := s.Start()
			require.NoError(t, err)
			defer s.Close()

			require.Equal(t, ca.want, s.senderReportPeriod)
		})
	}
}

// a Start that failed, or a restart after Close, takes the public
// sender report period set after it.
func TestServerSenderReportPeriodRestart(t *testing.T) {
	s := &Server{}
	err := s.Start()
	require.EqualError(t, err, "RTSPAddress not provided")

	s.RTSPAddress = "127.0.0.1:0"
	s.SenderReportPeriod = 20 * time.Millisecond
	err = s.Start()
	require.NoError(t, err)
	require.Equal(t, 20*time.Millisecond, s.senderReportPeriod)
	s.Close()

	s.SenderReportPeriod = 0
	err = s.Start()
	require.NoError(t, err)
	defer s.Close()
	require.Equal(t, 10*time.Second, s.senderReportPeriod)
}

// a negative sender report period is refused by Start,
// since the ticker of the senders cannot take it.
func TestServerSenderReportPeriodNegative(t *testing.T) {
	s := &Server{
		RTSPAddress:        "127.0.0.1:0",
		SenderReportPeriod: -time.Second,
	}
	err := s.Start()
	if err == nil {
		s.Close()
	}
	require.EqualError(t, err, "SenderReportPeriod (-1s) must not be negative")
}
