package task

import (
	"fmt"
	"strings"
)

// What compaction (design §5.3) sends and keeps. Both texts are constants: a
// compaction request is byte-stable, the same span always renders the same.
const (
	// CompactSystem is the system prompt of every compaction request. The
	// request offers no tools and carries one user message, the transcript
	// of the span being compacted (Transcript).
	CompactSystem = "You compact the context of an agent's long-running task so that it fits the model's context window. " +
		"The user message is a transcript of the oldest part of the task's conversation after its request; " +
		"it may begin with a summary of even earlier work. " +
		"Write a faithful summary that will replace the transcript for the agent. Keep every fact, number, name, " +
		"identifier, path and decision; the results of tool calls that later work depends on (quote short ones exactly); " +
		"what was tried and failed, and why; the user's later instructions; open questions; and the current plan and next steps. " +
		"Do not invent anything, do not call tools, do not address the user. Write only the summary."

	// SummaryHeader opens every summary message, so the model reading the
	// registers knows that what follows stands for earlier messages.
	SummaryHeader = "[Summary of earlier work: the conversation between the request above and the messages below " +
		"was compacted into this summary to fit the context window.]\n\n"
)

// SummaryMessage is the message a compaction puts in place of the span it
// swapped out: a user message, so the roles around it stay valid for every
// provider (after the task's request it joins that user turn; before an
// assistant turn it is an ordinary user turn), marked Summary.
func SummaryMessage(text string) Message {
	m := Text(RoleUser, SummaryHeader+text)
	m.Summary = true
	return m
}

// SummaryText is the summary a summary message carries, without its
// header; empty for any other message.
func SummaryText(m Message) string {
	if !m.Summary {
		return ""
	}
	return strings.TrimPrefix(m.TextContent(), SummaryHeader)
}

// Unit is a run of registers, msgs[Start:End], that compaction keeps or swaps
// out whole: an assistant turn together with the tool messages answering it
// (Round), or a single user or system message (a steer, or the Summary of an
// earlier compaction). Complete: every tool call of the turn has its result
// in the unit, and every result in it answers one of its calls.
type Unit struct {
	Start, End int
	Round      bool
	Summary    bool
	Complete   bool
}

// Units splits msgs into the head that compaction always keeps verbatim (up
// to and including the first user message: the task's request) and the units
// after it. A tool message joins the unit before it; one with nothing before
// it is a unit of its own, never complete.
func Units(msgs []Message) (head int, units []Unit) {
	for i, m := range msgs {
		if m.Role == RoleUser {
			head = i + 1
			break
		}
	}
	for i := head; i < len(msgs); i++ {
		m := msgs[i]
		if m.Role == RoleTool && len(units) > 0 && units[len(units)-1].End == i && units[len(units)-1].Round {
			units[len(units)-1].End = i + 1
			continue
		}
		units = append(units, Unit{Start: i, End: i + 1, Round: m.Role == RoleAssistant, Summary: m.Summary})
	}
	for i := range units {
		u := &units[i]
		u.Complete = (u.Round || msgs[u.Start].Role != RoleTool) && CheckPairing(msgs[u.Start:u.End]) == nil
	}
	return head, units
}

// CheckPairing checks the pairing invariant of a run of messages: every tool
// call has exactly one result after it, and every result answers a call made
// before it.
func CheckPairing(msgs []Message) error {
	calls := map[string]bool{} // call id -> answered
	var order []string
	for _, m := range msgs {
		for _, p := range m.Parts {
			switch p.Kind {
			case PartToolCall:
				if _, dup := calls[p.CallID]; dup {
					return fmt.Errorf("tool call %s appears twice", p.CallID)
				}
				calls[p.CallID] = false
				order = append(order, p.CallID)
			case PartToolResult:
				answered, ok := calls[p.CallID]
				switch {
				case !ok:
					return fmt.Errorf("tool result %s answers no call before it", p.CallID)
				case answered:
					return fmt.Errorf("tool call %s has two results", p.CallID)
				}
				calls[p.CallID] = true
			}
		}
	}
	for _, id := range order {
		if !calls[id] {
			return fmt.Errorf("tool call %s has no result", id)
		}
	}
	return nil
}

// Transcript renders msgs as the text a compaction request carries: each
// message under a "### <role>" line, tool calls and results marked with
// their ids, an earlier summary under "### summary of earlier work".
// Reasoning is left out: it is private to its provider and often signed.
// The same messages always give the same bytes.
func Transcript(msgs []Message) string {
	var b strings.Builder
	for i, m := range msgs {
		if i > 0 {
			b.WriteString("\n\n")
		}
		switch {
		case m.Summary:
			b.WriteString("### summary of earlier work\n")
			b.WriteString(SummaryText(m))
			continue
		case m.Role == RoleTool:
			b.WriteString("### tool results")
		default:
			b.WriteString("### ")
			b.WriteString(string(m.Role))
		}
		for _, p := range m.Parts {
			switch p.Kind {
			case PartText:
				b.WriteString("\n")
				b.WriteString(p.Text)
			case PartToolCall:
				fmt.Fprintf(&b, "\n[tool call %s: %s %s]", p.CallID, p.Name, p.Args)
			case PartToolResult:
				if p.IsError {
					fmt.Fprintf(&b, "\n[result of %s, an error]\n%s", p.CallID, p.Text)
				} else {
					fmt.Fprintf(&b, "\n[result of %s]\n%s", p.CallID, p.Text)
				}
			}
		}
	}
	return b.String()
}

// CompactMessages is the messages of a compaction request for msgs: one user
// message holding their Transcript. The span goes as text, not as the turns
// themselves: a request that carries tool calls must offer tools to some
// providers, and a compaction request offers none.
func CompactMessages(msgs []Message) []Message {
	return []Message{Text(RoleUser, Transcript(msgs))}
}
