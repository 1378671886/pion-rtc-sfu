package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Peer struct {
	ws   *websocket.Conn
	pc   *webrtc.PeerConnection
	room string
	user int
	mu   sync.Mutex
}

type Room struct {
	peers  map[int]*Peer
	tracks map[int]*webrtc.TrackLocalStaticRTP // userId -> relay track
	mu     sync.RWMutex
}

var (
	rooms   = make(map[string]*Room)
	roomsMu sync.Mutex
)

func getOrCreateRoom(id string) *Room {
	roomsMu.Lock()
	defer roomsMu.Unlock()
	if r, ok := rooms[id]; ok {
		return r
	}
	r := &Room{
		peers:  make(map[int]*Peer),
		tracks: make(map[int]*webrtc.TrackLocalStaticRTP),
	}
	rooms[id] = r
	return r
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	roomId := r.URL.Query().Get("room")
	userIdStr := r.URL.Query().Get("user")
	userId, _ := strconv.Atoi(userIdStr)
	if roomId == "" || userId == 0 {
		http.Error(w, "missing room/user", 400)
		return
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("ws upgrade:", err)
		return
	}
	defer ws.Close()

	media := webrtc.MediaEngine{}
	media.RegisterDefaultCodecs()

	s := webrtc.SettingEngine{}
	s.SetEphemeralUDPPortRange(50000, 50100)

	api := webrtc.NewAPI(webrtc.WithMediaEngine(&media), webrtc.WithSettingEngine(s))

	pc, err := api.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:liuzirui.top:3478"}},
		},
	})
	if err != nil {
		log.Println("pc create:", err)
		return
	}
	defer pc.Close()

	room := getOrCreateRoom(roomId)

	peer := &Peer{ws: ws, pc: pc, room: roomId, user: userId}
	room.mu.Lock()
	room.peers[userId] = peer
	room.mu.Unlock()

	log.Printf("[SFU] user %d joined room %s (%d peers)", userId, roomId, len(room.peers))

	defer func() {
		room.mu.Lock()
		delete(room.peers, userId)
		delete(room.tracks, userId)
		room.mu.Unlock()
		log.Printf("[SFU] user %d left room %s (%d peers)", userId, roomId, len(room.peers))
	}()

	// 收到远端音轨 → 转发给房间其他人
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("[SFU] track from %d: %s", userId, track.Codec().MimeType)

		// 创建本地 relay track，streamId 带上 userId 方便前端识别
		localTrack, err := webrtc.NewTrackLocalStaticRTP(
			track.Codec().RTPCodecCapability,
			"audio",
			strconv.Itoa(userId),
		)
		if err != nil {
			log.Println("create relay track:", err)
			return
		}

		room.mu.Lock()
		room.tracks[userId] = localTrack

		// 添加到所有其他 peer
		for uid, p := range room.peers {
			if uid == userId {
				continue
			}
			p.mu.Lock()
			if _, err := p.pc.AddTrack(localTrack); err != nil {
				log.Printf("add track to %d failed: %v", uid, err)
			} else {
				log.Printf("[SFU] add track %d -> peer %d", userId, uid)
			}
			p.mu.Unlock()
		}
		room.mu.Unlock()

		// RTP 中继循环
		for {
			rtp, _, err := track.ReadRTP()
			if err != nil {
				break
			}
			if err := localTrack.WriteRTP(rtp); err != nil {
				log.Println("write rtp:", err)
				break
			}
		}

		// track 结束，清理
		room.mu.Lock()
		delete(room.tracks, userId)
		room.mu.Unlock()
	})

	// 当 AddTrack 触发重新协商时
	pc.OnNegotiationNeeded(func() {
		log.Printf("[SDP] user %d renegotiation, creating offer", userId)
		offer, err := pc.CreateOffer(nil)
		if err != nil {
			return
		}
		if err := pc.SetLocalDescription(offer); err != nil {
			return
		}
		resp, _ := json.Marshal(map[string]interface{}{
			"type": "offer",
			"sdp":  pc.LocalDescription().SDP,
		})
		peer.mu.Lock()
		ws.WriteMessage(websocket.TextMessage, resp)
		peer.mu.Unlock()
	})

	// ICE candidate 转发给客户端
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		log.Printf("[ICE] user %d candidate: %s", userId, c.ToJSON().Candidate)
		data, _ := json.Marshal(map[string]interface{}{
			"type":      "ice",
			"candidate": c.ToJSON().Candidate,
			"sdpMid":    c.ToJSON().SDPMid,
			"sdpMLineIndex": c.ToJSON().SDPMLineIndex,
		})
		peer.mu.Lock()
		ws.WriteMessage(websocket.TextMessage, data)
		peer.mu.Unlock()
	})

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("[ICE] user %d state: %s", userId, state.String())
	})

	// 信令循环
	for {
		_, msg, err := ws.ReadMessage()
		if err != nil {
			break
		}

		var sig map[string]interface{}
		if err := json.Unmarshal(msg, &sig); err != nil {
			continue
		}

		switch sig["type"] {
		case "offer":
			log.Printf("[SDP] user %d sent offer", userId)
			sdp := sig["sdp"].(string)
			offer := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdp}
			if err := pc.SetRemoteDescription(offer); err != nil {
				log.Println("set remote offer:", err)
				continue
			}
			answer, err := pc.CreateAnswer(nil)
			if err != nil {
				log.Println("create answer:", err)
				continue
			}
			if err := pc.SetLocalDescription(answer); err != nil {
				log.Println("set local answer:", err)
				continue
			}
			resp, _ := json.Marshal(map[string]interface{}{
				"type": "answer",
				"sdp":  answer.SDP,
			})
			peer.mu.Lock()
			ws.WriteMessage(websocket.TextMessage, resp)
			peer.mu.Unlock()

			// 将房间已有音轨转发给新 peer
			room.mu.RLock()
			for uid, t := range room.tracks {
				if uid != userId {
					if _, err := pc.AddTrack(t); err != nil {
						log.Printf("relay track %d to %d failed: %v", uid, userId, err)
					} else {
						log.Printf("[SFU] relay existing track %d -> new peer %d", uid, userId)
					}
				}
			}
			room.mu.RUnlock()

		case "answer":
			log.Printf("[SDP] user %d sent answer", userId)
			sdp := sig["sdp"].(string)
			answer := webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sdp}
			if err := pc.SetRemoteDescription(answer); err != nil {
				log.Println("set remote answer:", err)
			}

		case "ice":
			candidate := webrtc.ICECandidateInit{
				Candidate: sig["candidate"].(string),
			}
			if sdpMid, ok := sig["sdpMid"].(string); ok {
				candidate.SDPMid = &sdpMid
			}
			if idx, ok := sig["sdpMLineIndex"].(float64); ok {
				i := uint16(idx)
				candidate.SDPMLineIndex = &i
			}
			if err := pc.AddICECandidate(candidate); err != nil {
				log.Println("add ice:", err)
			}
		}
	}
}

func main() {
	http.HandleFunc("/sfu-ws", handleWS)
	log.Println("[SFU] pion-sfu listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
