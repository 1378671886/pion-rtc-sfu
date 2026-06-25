package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Peer struct {
	ws    *websocket.Conn
	pc    *webrtc.PeerConnection
	room  string
	user  int
	mu    sync.Mutex
	ready bool // 初始握手完成后置 true，防止 OnTrack 往未就绪 peer 加 track
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
	// 不限制 UDP 端口范围，让 OS 分配，避免端口冲突导致 ICE consent 响应丢失
	s.SetICETimeouts(15*time.Second, 30*time.Second, 5*time.Second)

	api := webrtc.NewAPI(webrtc.WithMediaEngine(&media), webrtc.WithSettingEngine(s))

	pc, err := api.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs:       []string{"turn:liuzirui.top:3478?transport=udp"},
				Username:   "turnuser",
				Credential: "Vo!ceTURN_2024_liuzirui",
			},
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
		// 仅当该 userId 仍指向当前 peer 时才清理（防止误删重连后的新 peer）
		if room.peers[userId] != peer {
			room.mu.Unlock()
			return
		}
		// 从其他 peer 移除该用户的 relay track，防止离开重连后 transceiver 累积
		for uid, p := range room.peers {
			if uid == userId {
				continue
			}
			for _, sender := range p.pc.GetSenders() {
				if sender.Track() != nil && sender.Track().StreamID() == strconv.Itoa(userId) {
					if err := p.pc.RemoveTrack(sender); err != nil {
						log.Printf("[SFU] remove track %d from %d failed: %v", userId, uid, err)
					} else {
						log.Printf("[SFU] removed track %d from peer %d", userId, uid)
					}
				}
			}
		}
		delete(room.peers, userId)
		delete(room.tracks, userId)
		room.mu.Unlock()
		log.Printf("[SFU] user %d left room %s (%d peers)", userId, roomId, len(room.peers))
	}()

	// 收到远端音轨 → 转发给房间其他人
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("[SFU] track from %d: %s", userId, track.Codec().MimeType)

		// 创建本地 relay track，用 userId+SSRC 保证 track ID 唯一，防止 msid 重复
		localTrack, err := webrtc.NewTrackLocalStaticRTP(
			track.Codec().RTPCodecCapability,
			fmt.Sprintf("audio-%d-%d", userId, track.SSRC()),
			strconv.Itoa(userId),
		)
		if err != nil {
			log.Println("create relay track:", err)
			return
		}

		room.mu.Lock()
		oldTrack := room.tracks[userId]
		room.tracks[userId] = localTrack

		// 添加到所有其他 peer（跳过未完成初始握手的 peer，避免 SDP 撞车）
		for uid, p := range room.peers {
			if uid == userId || !p.ready {
				continue
			}
			if oldTrack != nil {
				// 已有旧 track：用 ReplaceTrack 原地替换，不触发 renegotiation，避免 msid 重复
				replaced := false
				for _, sender := range p.pc.GetSenders() {
					if sender.Track() == oldTrack {
						if err := sender.ReplaceTrack(localTrack); err != nil {
							log.Printf("replace track %d for peer %d failed: %v", userId, uid, err)
						} else {
							log.Printf("[SFU] replaced track %d -> peer %d (old=%s new=%s)", userId, uid, oldTrack.ID(), localTrack.ID())
						}
						replaced = true
						break
					}
				}
				if !replaced {
					log.Printf("[SFU] old track %d not found in peer %d senders, adding new", userId, uid)
					if _, err := p.pc.AddTrack(localTrack); err != nil {
						log.Printf("add track to %d failed: %v", uid, err)
					}
				}
			} else {
				// 首次：正常添加
				if _, err := p.pc.AddTrack(localTrack); err != nil {
					log.Printf("add track to %d failed: %v", uid, err)
				} else {
					log.Printf("[SFU] add track %d -> peer %d", userId, uid)
				}
			}
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

		// track 结束，仅当 map 仍指向本 track 才清理（防止旧 track 误删新 track）
		room.mu.Lock()
		if room.tracks[userId] == localTrack {
			delete(room.tracks, userId)
		}
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
			log.Printf("[SFU] ws read error user %d: %v", userId, err)
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

			// 标记初始握手完成，此后 OnTrack 可以安全往该 peer 加 track
			if !peer.ready {
				peer.ready = true
			}

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
