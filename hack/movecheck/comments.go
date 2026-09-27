package main

import (
	"go/token"
	"strings"
	"unicode"
)

// commentEdits returns the edits that apply the rename map to the comments
// in a range of the hasher's file.
//
// Parameters:
//   - start and end delimit the range [start, end).
//
// It returns one edit for each comment in the range whose text changes
// (see renameWords), and nil without a rename map. A comment never
// overlaps an identifier, so the edits never overlap the ones edits
// returns.
func (h hasher) commentEdits(start, end token.Pos) []edit {
	if len(h.renames) == 0 {
		return nil
	}
	var edits []edit
	for _, group := range h.file.ast.Comments {
		for _, c := range group.List {
			if c.Pos() < start || c.End() > end {
				continue
			}
			if text := h.renameWords(c.Text); text != c.Text {
				edits = append(edits, edit{start: h.offset(c.Pos()), end: h.offset(c.End()), text: text})
			}
		}
	}
	return edits
}

// renameWords applies the rename map to the words of a comment.
//
// Parameters:
//   - text is the text of one comment.
//
// It returns the text with each word that the rename map names respelled
// as the new code names it in a comment: New when the identifier moves
// into the new directory's package or stays in its own, and pkg.New when
// it moves to another package. A word is a run of letters, digits and
// underscores. A word after a dot is kept, because it names a field, a
// method or a declaration of another package.
//
// The comment text is not resolved. So a word that names a local or a
// parameter of the same name is respelled as well.
func (h hasher) renameWords(text string) string {
	var b strings.Builder
	runes := []rune(text)
	for i := 0; i < len(runes); {
		if !isWordRune(runes[i]) {
			b.WriteRune(runes[i])
			i++
			continue
		}
		j := i
		for j < len(runes) && isWordRune(runes[j]) {
			j++
		}
		word := string(runes[i:j])
		if r, ok := h.renames[word]; ok && (i == 0 || runes[i-1] != '.') {
			word = h.commentSpelling(r)
		}
		b.WriteString(word)
		i = j
	}
	return b.String()
}

// commentSpelling returns how a comment in the new code names a renamed
// identifier.
//
// Parameters:
//   - r is the rename map's entry for the identifier.
func (h hasher) commentSpelling(r rename) string {
	if r.pkg == "" || h.into == nil || r.pkg == h.into.name {
		return r.name
	}
	return r.pkg + "." + r.name
}

// isWordRune reports whether a rune can be part of a Go identifier.
//
// Parameters:
//   - r is the rune.
func isWordRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
