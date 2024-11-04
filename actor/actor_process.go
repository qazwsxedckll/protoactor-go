package actor

import (
	"fmt"
	"sync/atomic"
)

type ActorProcess struct {
	mailbox Mailbox
	dead    int32
}

var _ Process = &ActorProcess{}

func NewActorProcess(mailbox Mailbox) *ActorProcess {
	return &ActorProcess{
		mailbox: mailbox,
	}
}

func (ref *ActorProcess) SendUserMessage(pid *PID, message interface{}) {
	if ref.mailbox.UserMessageCount() >= 100000 && ref.mailbox.UserMessageCount()%100000 == 0 {
		if pid != nil {
			fmt.Printf("pid: %v, user message count: %v\n", pid.String(), ref.mailbox.UserMessageCount())
		} else {
			fmt.Printf("pid: nil, user message count: %v\n", ref.mailbox.UserMessageCount())
		}
	}
	ref.mailbox.PostUserMessage(message)
}

func (ref *ActorProcess) SendSystemMessage(_ *PID, message interface{}) {
	ref.mailbox.PostSystemMessage(message)
}

func (ref *ActorProcess) Stop(pid *PID) {
	atomic.StoreInt32(&ref.dead, 1)
	ref.SendSystemMessage(pid, stopMessage)
}
