// Package transform contains opt-in response transformations selected by route
// rules. It does not infer behavior from provider or logical model names.
package transform

import (
	"strings"

	"github.com/mytecor/llm-gateway/internal/model"
)

type Kind string

const SplitThinking Kind = "split_thinking"

const openThinking = "<think>"
const closeThinking = "</think>"

type thinkingState uint8

const (
	thinkingPrefix thinkingState = iota
	thinkingReasoning
	thinkingContent
)

// thinkingSplitter retains only possible delimiter prefixes. In particular its
// suffix is ASCII, so emitting Unicode reasoning never cuts a UTF-8 character.
// Whitespace preceding a recognized block remains harmless content whitespace.
type thinkingSplitter struct {
	state   thinkingState
	pending string
}

func (s *thinkingSplitter) feed(text string) (content, reasoning string) {
	if s.state == thinkingContent {
		return text, ""
	}
	text = s.pending + text
	s.pending = ""
	if s.state == thinkingPrefix {
		n := len(text) - len(strings.TrimLeft(text, " \t\r\n"))
		content = text[:n]
		text = text[n:]
		if strings.HasPrefix(openThinking, text) && len(text) < len(openThinking) {
			s.pending = text
			return content, ""
		}
		if !strings.HasPrefix(text, openThinking) {
			s.state = thinkingContent
			return content + text, ""
		}
		s.state = thinkingReasoning
		text = text[len(openThinking):]
	}
	if index := strings.Index(text, closeThinking); index >= 0 {
		s.state = thinkingContent
		return content + text[index+len(closeThinking):], text[:index]
	}
	keep := 0
	for n := min(len(text), len(closeThinking)-1); n > 0; n-- {
		if strings.HasSuffix(text, closeThinking[:n]) {
			keep = n
			break
		}
	}
	s.pending = strings.Clone(text[len(text)-keep:])
	return content, text[:len(text)-keep]
}

func (s *thinkingSplitter) finish(truncated bool) (content, reasoning string, err *model.CallError) {
	pending := s.pending
	s.pending = ""
	if s.state == thinkingReasoning {
		if !truncated {
			return "", "", incompleteThinking()
		}
		return "", pending, nil
	}
	return pending, "", nil
}

func incompleteThinking() *model.CallError {
	return &model.CallError{Class: model.ErrorInvalid, Status: 502, Code: "incomplete_thinking", Scope: model.FailureScopeProvider, Replay: model.ReplayCompleted}
}
