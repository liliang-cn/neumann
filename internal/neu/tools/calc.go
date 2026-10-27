package tools

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"unicode"
)

// Calc evaluates an arithmetic expression exactly (rational numbers, no
// floating point): + - * / ^ (integer exponents), parentheses, unary minus,
// and the functions sum, avg, min, max, abs and round(x, places). The
// result is an integer, or a decimal with up to 10 places.
func Calc(expr string) (string, error) {
	if len(expr) > 20000 {
		return "", errors.New("expression too long (limit 20000 characters)")
	}
	p := &calcParser{s: expr}
	p.next()
	v, err := p.expr()
	if err != nil {
		return "", err
	}
	if p.tok.kind != tEOF {
		return "", fmt.Errorf("unexpected %q at offset %d", p.tok.text, p.tok.pos)
	}
	return formatRat(v), nil
}

func formatRat(v *big.Rat) string {
	if v.IsInt() {
		return v.Num().String()
	}
	s := v.FloatString(10)
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}

type tokKind int

const (
	tEOF tokKind = iota
	tNum
	tIdent
	tOp
)

type token struct {
	kind tokKind
	text string
	pos  int
}

type calcParser struct {
	s   string
	i   int
	tok token
}

func (p *calcParser) next() {
	for p.i < len(p.s) && unicode.IsSpace(rune(p.s[p.i])) {
		p.i++
	}
	start := p.i
	if p.i >= len(p.s) {
		p.tok = token{tEOF, "", start}
		return
	}
	c := p.s[p.i]
	switch {
	case c >= '0' && c <= '9' || c == '.':
		for p.i < len(p.s) && (p.s[p.i] >= '0' && p.s[p.i] <= '9' || p.s[p.i] == '.' || p.s[p.i] == '_') {
			p.i++
		}
		p.tok = token{tNum, strings.ReplaceAll(p.s[start:p.i], "_", ""), start}
	case unicode.IsLetter(rune(c)):
		for p.i < len(p.s) && (unicode.IsLetter(rune(p.s[p.i])) || p.s[p.i] >= '0' && p.s[p.i] <= '9') {
			p.i++
		}
		p.tok = token{tIdent, strings.ToLower(p.s[start:p.i]), start}
	default:
		p.i++
		if c == '*' && p.i < len(p.s) && p.s[p.i] == '*' { // ** as ^
			p.i++
			p.tok = token{tOp, "^", start}
			return
		}
		p.tok = token{tOp, string(c), start}
	}
}

func (p *calcParser) expr() (*big.Rat, error) {
	v, err := p.term()
	if err != nil {
		return nil, err
	}
	for p.tok.kind == tOp && (p.tok.text == "+" || p.tok.text == "-") {
		op := p.tok.text
		p.next()
		r, err := p.term()
		if err != nil {
			return nil, err
		}
		if op == "+" {
			v = new(big.Rat).Add(v, r)
		} else {
			v = new(big.Rat).Sub(v, r)
		}
	}
	return v, nil
}

func (p *calcParser) term() (*big.Rat, error) {
	v, err := p.unary()
	if err != nil {
		return nil, err
	}
	for p.tok.kind == tOp && (p.tok.text == "*" || p.tok.text == "/") {
		op := p.tok.text
		p.next()
		r, err := p.unary()
		if err != nil {
			return nil, err
		}
		if op == "*" {
			v = new(big.Rat).Mul(v, r)
		} else {
			if r.Sign() == 0 {
				return nil, errors.New("division by zero")
			}
			v = new(big.Rat).Quo(v, r)
		}
	}
	return v, nil
}

func (p *calcParser) unary() (*big.Rat, error) {
	if p.tok.kind == tOp && (p.tok.text == "-" || p.tok.text == "+") {
		neg := p.tok.text == "-"
		p.next()
		v, err := p.unary()
		if err != nil {
			return nil, err
		}
		if neg {
			v = new(big.Rat).Neg(v)
		}
		return v, nil
	}
	return p.power()
}

func (p *calcParser) power() (*big.Rat, error) {
	base, err := p.primary()
	if err != nil {
		return nil, err
	}
	if p.tok.kind == tOp && p.tok.text == "^" {
		p.next()
		exp, err := p.unary()
		if err != nil {
			return nil, err
		}
		if !exp.IsInt() || exp.Num().BitLen() > 10 {
			return nil, errors.New("exponent must be an integer between -1023 and 1023")
		}
		n := exp.Num().Int64()
		r := big.NewRat(1, 1)
		b := base
		if n < 0 {
			if base.Sign() == 0 {
				return nil, errors.New("division by zero")
			}
			b = new(big.Rat).Inv(base)
			n = -n
		}
		for range n {
			r.Mul(r, b)
		}
		return r, nil
	}
	return base, nil
}

func (p *calcParser) primary() (*big.Rat, error) {
	switch p.tok.kind {
	case tNum:
		v, ok := new(big.Rat).SetString(p.tok.text)
		if !ok {
			return nil, fmt.Errorf("bad number %q", p.tok.text)
		}
		p.next()
		return v, nil
	case tIdent:
		name, pos := p.tok.text, p.tok.pos
		p.next()
		if p.tok.kind != tOp || p.tok.text != "(" {
			return nil, fmt.Errorf("unknown name %q at offset %d (functions: sum, avg, min, max, abs, round)", name, pos)
		}
		p.next()
		var args []*big.Rat
		if !(p.tok.kind == tOp && p.tok.text == ")") {
			for {
				a, err := p.expr()
				if err != nil {
					return nil, err
				}
				args = append(args, a)
				if p.tok.kind == tOp && p.tok.text == "," {
					p.next()
					continue
				}
				break
			}
		}
		if p.tok.kind != tOp || p.tok.text != ")" {
			return nil, fmt.Errorf("expected ) at offset %d", p.tok.pos)
		}
		p.next()
		return callFunc(name, args)
	case tOp:
		if p.tok.text == "(" {
			p.next()
			v, err := p.expr()
			if err != nil {
				return nil, err
			}
			if p.tok.kind != tOp || p.tok.text != ")" {
				return nil, fmt.Errorf("expected ) at offset %d", p.tok.pos)
			}
			p.next()
			return v, nil
		}
		if p.tok.text == "," {
			return nil, fmt.Errorf("unexpected , at offset %d (write numbers without thousands separators)", p.tok.pos)
		}
	case tEOF:
		return nil, errors.New("unexpected end of expression")
	}
	return nil, fmt.Errorf("unexpected %q at offset %d", p.tok.text, p.tok.pos)
}

func callFunc(name string, args []*big.Rat) (*big.Rat, error) {
	need := func(n int) error {
		if len(args) < n {
			return fmt.Errorf("%s needs at least %d argument(s)", name, n)
		}
		return nil
	}
	switch name {
	case "sum", "avg":
		if err := need(1); err != nil {
			return nil, err
		}
		s := new(big.Rat)
		for _, a := range args {
			s.Add(s, a)
		}
		if name == "avg" {
			s.Quo(s, new(big.Rat).SetInt64(int64(len(args))))
		}
		return s, nil
	case "min", "max":
		if err := need(1); err != nil {
			return nil, err
		}
		best := args[0]
		for _, a := range args[1:] {
			if c := a.Cmp(best); name == "min" && c < 0 || name == "max" && c > 0 {
				best = a
			}
		}
		return best, nil
	case "abs":
		if len(args) != 1 {
			return nil, errors.New("abs takes one argument")
		}
		return new(big.Rat).Abs(args[0]), nil
	case "round":
		if len(args) < 1 || len(args) > 2 {
			return nil, errors.New("round takes (x) or (x, places)")
		}
		places := int64(0)
		if len(args) == 2 {
			if !args[1].IsInt() || args[1].Num().Int64() < 0 || args[1].Num().Int64() > 10 {
				return nil, errors.New("round places must be an integer 0..10")
			}
			places = args[1].Num().Int64()
		}
		s := args[0].FloatString(int(places)) // rounds half away from zero
		v, _ := new(big.Rat).SetString(s)
		return v, nil
	}
	return nil, fmt.Errorf("unknown function %q (functions: sum, avg, min, max, abs, round)", name)
}
