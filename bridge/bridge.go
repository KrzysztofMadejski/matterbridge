package bridge

import (
	"strings"
	"sync"
	"time"

	"github.com/42wim/matterbridge/bridge/config"
	"github.com/sirupsen/logrus"
)

type Bridger interface {
	Send(msg config.Message) (string, error)
	Connect() error
	JoinChannel(channel config.ChannelInfo) error
	Disconnect() error
}

type Bridge struct {
	Bridger
	*sync.RWMutex

	Name           string
	Account        string
	Protocol       string
	Channels       map[string]config.ChannelInfo
	Joined         map[string]bool
	ChannelMembers *config.ChannelMembers
	Log            *logrus.Entry
	Config         config.Config
	General        *config.Protocol

	reconnectMutex      sync.RWMutex
	lastReconnectAtNano int64
}

type Config struct {
	*Bridge

	Remote chan config.Message
}

// Factory is the factory function to create a bridge
type Factory func(*Config) Bridger

func New(bridge *config.Bridge) *Bridge {
	accInfo := strings.Split(bridge.Account, ".")
	protocol := accInfo[0]
	name := accInfo[1]

	return &Bridge{
		RWMutex:  new(sync.RWMutex),
		Channels: make(map[string]config.ChannelInfo),
		Name:     name,
		Protocol: protocol,
		Account:  bridge.Account,
		Joined:   make(map[string]bool),
	}
}

func (b *Bridge) JoinChannels() error {
	return b.joinChannels(b.Channels, b.Joined)
}

// SetChannelMembers sets the newMembers to the bridge ChannelMembers
func (b *Bridge) SetChannelMembers(newMembers *config.ChannelMembers) {
	b.Lock()
	b.ChannelMembers = newMembers
	b.Unlock()
}

func (b *Bridge) joinChannels(channels map[string]config.ChannelInfo, exists map[string]bool) error {
	for ID, channel := range channels {
		if !exists[ID] {
			b.Log.Infof("%s: joining %s (ID: %s)", b.Account, channel.Name, ID)
			err := b.JoinChannel(channel)
			if err != nil {
				return err
			}
			exists[ID] = true
		}
	}
	return nil
}

// ReconnectGuarded reconnect while making sure there is only one reconnection process
func (br *Bridge) Reconnect() {
	// remember when the ask to reconnect came
	reconnectThreadStart := time.Now().UnixNano()

	// lock reconnecting in case multiple/stacked errors ask for reconnection
	br.reconnectMutex.Lock()
	defer br.reconnectMutex.Unlock()

	if reconnectThreadStart < br.lastReconnectAtNano {
		// we already reconnected since the ask came, drop it
		br.Log.Debug("Dropping reconnection request. Already reconnected")
		return
	}

	br.lastReconnectAtNano = reconnectThreadStart

	// actual reconnect
	// TODO it would be cool if bridges could override it, they might have different timeouts, etc.
	if err := br.Disconnect(); err != nil {
		br.Log.Errorf("Disconnect() %s failed: %s", br.Account, err)
	}
	time.Sleep(time.Second * 5)
RECONNECT:
	br.Log.Infof("Reconnecting %s", br.Account)
	err := br.Connect()
	if err != nil {
		br.Log.Errorf("Reconnection failed: %s. Trying again in 60 seconds", err)
		time.Sleep(time.Second * 60)
		goto RECONNECT
	}
	br.Joined = make(map[string]bool)
	if err := br.JoinChannels(); err != nil {
		br.Log.Errorf("JoinChannels() %s failed: %s", br.Account, err)
	}

	// mark when we did last reconnect and allow processing of other reconnect messages
	time.Sleep(time.Second * 1)
	br.lastReconnectAtNano = time.Now().UnixNano()
}

func (b *Bridge) GetBool(key string) bool {
	val, ok := b.Config.GetBool(b.Account + "." + key)
	if !ok {
		val, _ = b.Config.GetBool("general." + key)
	}
	return val
}

func (b *Bridge) GetInt(key string) int {
	val, ok := b.Config.GetInt(b.Account + "." + key)
	if !ok {
		val, _ = b.Config.GetInt("general." + key)
	}
	return val
}

func (b *Bridge) GetString(key string) string {
	val, ok := b.Config.GetString(b.Account + "." + key)
	if !ok {
		val, _ = b.Config.GetString("general." + key)
	}
	return val
}

func (b *Bridge) GetStringSlice(key string) []string {
	val, ok := b.Config.GetStringSlice(b.Account + "." + key)
	if !ok {
		val, _ = b.Config.GetStringSlice("general." + key)
	}
	return val
}

func (b *Bridge) GetStringSlice2D(key string) [][]string {
	val, ok := b.Config.GetStringSlice2D(b.Account + "." + key)
	if !ok {
		val, _ = b.Config.GetStringSlice2D("general." + key)
	}
	return val
}
