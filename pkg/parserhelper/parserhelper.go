package parserhelper

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

type inspector func(parser.Node, []parser.Node) error

func (f inspector) Visit(node parser.Node, path []parser.Node) (parser.Visitor, error) {
	if err := f(node, path); err != nil {
		return nil, err
	}

	return f, nil
}

func ExtractSelectors(query string) [][]*labels.Matcher {
	expr, _ := parser.ParseExpr(query)


	var selectors [][]*labels.Matcher
	inspect(expr, func(node parser.Node, _ []parser.Node) error {
		vs, ok := node.(*parser.VectorSelector)
		if ok {
			selectors = append(selectors, vs.LabelMatchers)
		}
		return nil
	})
	return selectors
}

func inspect(node parser.Node, f inspector) {
	//nolint: errcheck
	walk(inspector(f), node, nil)
}

func walk(v parser.Visitor, node parser.Node, path []parser.Node) error {
	var err error
	if v, err = v.Visit(node, path); v == nil || err != nil {
		return err
	}
	path = append(path, node)

	for _, e := range parser.Children(node) {
		if err := walk(v, e, path); err != nil {
			return err
		}
	}

	_, err = v.Visit(nil, nil)
	return err
}